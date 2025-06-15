use anyhow::{Result, anyhow};
use broker::Broker;
use common::random_str;
use coo::coo::Coordinator;
use grpcx::{
    brokercoosvc::{self, broker_coo_service_client::BrokerCooServiceClient},
    commonsvc::TopicPartitionMeta,
    smart_client::{SmartClient, extract_leader_address},
    topic_meta::TopicPartitionDetail,
};
use log::{error, info, warn};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use storagev2::{StorageReader, StorageWriter};
use tokio::{
    select,
    sync::{Mutex, watch},
    task::JoinHandle,
    time::{self, interval},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Status};

pub struct Builder<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    id: u32,
    broker: Option<Broker<SW, SR>>,
    coo: Option<Coordinator>,
    salve: Option<Slave>,
}

impl<SW, SR> Default for Builder<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    fn default() -> Self {
        Self {
            id: Default::default(),
            broker: Default::default(),
            coo: Default::default(),
            salve: Default::default(),
        }
    }
}

impl<SW, SR> Builder<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }

    pub fn broker(mut self, b: Broker<SW, SR>) -> Self {
        self.broker = Some(b);
        self
    }

    pub fn coo(mut self, c: Coordinator) -> Self {
        self.coo = Some(c);
        self
    }

    pub fn slave(mut self, s: Slave) -> Self {
        self.salve = Some(s);
        self
    }

    fn validate(&self) -> Result<()> {
        if self.salve.is_none() && self.broker.is_none() && self.coo.is_none() {
            return Err(anyhow!("Need at least one module"));
        }
        if self.salve.is_some() && (self.broker.is_some() || self.coo.is_some()) {
            return Err(anyhow!("Only need slave module or [coo, broker] module"));
        }

        Ok(())
    }

    pub fn build(self) -> LogicNode<SW, SR> {
        let _ = &self.validate().expect("LoginNode validate failed");
        LogicNode {
            id: self.id,
            coo: self.coo,
            broker: self.broker,
            slave: self.salve,
            coo_leader_client: None,
            start_smart_client: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
pub struct Slave;

impl Slave {
    pub async fn run(&self) -> Result<()> {
        Ok(())
    }
}

pub struct LogicNode<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    id: u32,
    coo: Option<Coordinator>,
    broker: Option<Broker<SW, SR>>,
    slave: Option<Slave>,

    coo_leader_client: Option<SmartClient>,
    start_smart_client: Arc<AtomicBool>,
}

impl<SW, SR> LogicNode<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    pub async fn run(&mut self, coo_leader: Arc<Mutex<String>>) -> Result<()> {
        self.start_modules().await;
        loop {
            let coo = self.coo.clone();
            let broker = self.broker.clone();
            // 表示当前的 Coo 节点是否是 Leader

            let (tx_watcher, rx_watcher) = watch::channel(false);
            self.broker_interact_with_external_coo(coo_leader.clone(), rx_watcher)
                .await;

            // TODO: 检查当前 coo 是否是 leader,是则走内存
            // if broker.as_ref().is_some() && coo.as_ref().is_some() {
            //     let (tx_watcher, rx_watcher) = watch::channel(false);
            //     self.start_watcher(tx_watcher);
            //     let coo = coo.unwrap().clone();

            //     if coo.is_leader().await {
            //         self.broker_interact_with_local_coo(coo_leader.clone(), rx_watcher.clone())
            //             .await
            //     } else {
            //         self.broker_interact_with_external_coo(coo_leader.clone(), rx_watcher)
            //             .await;
            //     }
            // } else if broker.as_ref().is_some() {
            //     // 该 LogicNode 仅有 Broker, 无 Coo 模块
            //     let (_, rx_watcher) = watch::channel(false);
            //     self.broker_interact_with_external_coo(coo_leader.clone(), rx_watcher)
            //         .await;
            // }
        }
        Ok(())
    }

    async fn start_modules(&self) -> Vec<JoinHandle<()>> {
        let mut handles = vec![];

        let coo = self.coo.clone();
        if coo.is_some() {
            let coo_handle = tokio::spawn(async move {
                if let Err(e) = coo.unwrap().run().await {
                    error!("Coo run err: {:?}", e);
                    std::process::exit(1);
                }
            });
            handles.push(coo_handle);
        }

        let broker = self.broker.clone();
        if broker.is_some() {
            let broker_handle = tokio::spawn(async move {
                if let Err(e) = broker.unwrap().run().await {
                    error!("Broker run err: {:?}", e);
                    std::process::exit(1);
                }
            });
            handles.push(broker_handle);
        }

        let salve = self.slave.clone();
        if salve.is_some() {
            let slave_handle = tokio::spawn(async move {
                if let Err(e) = salve.unwrap().run().await {
                    error!("Slave run err: {:?}", e);
                    std::process::exit(1);
                }
            });
            handles.push(slave_handle);
        }
        // 等待所有监听服务启动
        time::sleep(Duration::from_secs(2)).await;
        handles
    }

    fn start_watcher(&self, tx_watcher: watch::Sender<bool>) {
        // 循环检查当前 Coo 是否已经变更为 Leader，是则停掉网络传输状态
        if self.coo.is_none() {
            return;
        }
        let coo = self.coo.as_ref().unwrap().clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(2));
            loop {
                ticker.tick().await;
                if coo.is_leader().await {
                    // 通知所有等待者，本 LogicNode 的 Coo 已经变更为 leader
                    let _ = tx_watcher.send(true);
                }
                // Coo 变更为非 Leader 时，由 coo.start_leader_checker() 进行检测并返回 NotLeader 信息，由 Broker 进行主动切换。
            }
        });
    }

    async fn broker_interact_with_local_coo(
        &self,
        coo_leader: Arc<Mutex<String>>,
        mut rx_watcher: watch::Receiver<bool>,
    ) {
        if self.broker.is_none()
            || self.coo.is_none()
            || !self.coo.as_ref().unwrap().is_leader().await
        {
            return;
        }
        let broker = self.broker.as_ref().unwrap().clone();
        let broker_id = broker.get_id();
        let coo = self.coo.as_ref().unwrap().clone();
        let coo_id = coo.id;
        info!(
            "Broker[{}] interact with Local-Coo[{}:{}]",
            broker_id, coo_id, coo.conf.coo_addr
        );
        let broker = broker.clone();
        let mut state_recv = broker.get_state_reciever("logic_node".to_string());
        let mut pull_recv = coo.broker_pull(broker.get_id()).await.unwrap();
        loop {
            select! {
                state = state_recv.recv() => {
                    if state.is_none(){
                        continue;
                    }
                    info!("Broker[{}]->Local-Coo[{}]: Report BrokerState",broker_id, coo_id);
                    coo.apply_broker_report(state.unwrap());
                }

                topic_list = pull_recv.recv() => {
                    if topic_list.is_none(){
                        continue;
                    }
                    match topic_list.unwrap() {
                        Ok(tl) =>  {
                            info!("Broker[{}]->Local-Coo[{}]: Pull TopicList",broker_id, coo_id);
                            broker.apply_topic_infos(tl);
                        }
                        Err(ref status) => {
                            // leader 发生了切换，跳出该 loop 下次执行 coo.is_leader() 的 else 分支
                            change_with_status(status, coo_leader.clone()).await;
                            break;
                        },
                    }
                }

                // TODO: 这里可能有问题？
                changed = rx_watcher.changed() => {
                    // 该 LogicNode 的 Coo 已不是
                    if changed.is_ok() && !*rx_watcher.borrow() {
                        warn!("Coo-Leader has been change to External");
                        break;
                    }
                }
            }
        }
    }

    async fn broker_interact_with_external_coo(
        &mut self,
        coo_leader: Arc<Mutex<String>>,
        mut watcher: watch::Receiver<bool>,
    ) {
        if self.broker.is_none() {
            return;
        }
        if coo_leader.lock().await.is_empty() {
            tokio::time::sleep(Duration::from_secs(3)).await;
            return;
        }
        let broker = self.broker.as_ref().unwrap().clone();
        let broker_id = broker.get_id();
        info!(
            "Broker[{}] interact with External-Coo[{}]",
            broker_id,
            coo_leader.lock().await,
        );

        // 如果当前 ln 中的 coo 不是leader，需要先获取 coo:leader 节点并链接，然后汇报状态
        // 使用 grpc 链接 coo:leader, 并进行交互
        if self.coo_leader_client.is_none() {
            let coo_client = SmartClient::new(random_str(6), vec![coo_leader.lock().await.clone()]);
            self.coo_leader_client = Some(coo_client.clone());
        }
        let refresh_coo_client = self.coo_leader_client.as_ref().unwrap().clone();

        if self
            .start_smart_client
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            tokio::spawn(async move {
                refresh_coo_client
                    .refresh_coo_endpoints(|chan| async {
                        BrokerCooServiceClient::new(chan)
                            .list(Request::new(grpcx::commonsvc::CooListReq {
                                id: broker_id.to_string(),
                            }))
                            .await
                    })
                    .await;
            });
        }

        let stop = CancellationToken::new();
        let _stop = stop.clone();
        let _broker = broker.clone();
        tokio::spawn(async move {
            select! {
                changed = watcher.changed() => {
                    if changed.is_ok() && *watcher.borrow() {
                        _broker.unleash_state_reciever("logic_node");
                        _stop.cancel();
                        warn!("Coo-Leader has been change to Local(Report Handle)");
                    }
                }
            }
        });

        // Report  BrokerState
        let report_broker = broker.clone();
        let report_coo_client = self.coo_leader_client.as_ref().unwrap().clone();
        let _coo_leader_call = coo_leader.clone();
        let _stop = stop.clone();
        let report_handle = tokio::spawn(async move {
            let report_resp_strm = report_coo_client
                .open_bistream(
                    || {
                        let report_recver =
                            report_broker.get_state_reciever("logic_node".to_string());
                        ReceiverStream::new(report_recver)
                    },
                    |chan, request, addr| async {
                        *_coo_leader_call.lock().await = addr;
                        BrokerCooServiceClient::new(chan).report(request).await
                    },
                    |res| {
                        let _coo_leader_resp = _coo_leader_call.clone();
                        async move {
                            info!(
                                "Broker[{broker_id}]->External-Coo[{}]: Report BrokerState",
                                _coo_leader_resp.lock().await,
                            );
                            Ok(())
                        }
                    },
                    _stop,
                )
                .await;
            error!("broker-report: 自动调用完成?");
            time::sleep(Duration::from_secs(2)).await;
        });

        // Pull  Broker Partition
        let pull_broker = broker.clone();
        let pull_coo_client = self.coo_leader_client.as_ref().unwrap().clone();
        let _coo_leader_call = coo_leader.clone();
        let _coo_leader_resp = coo_leader.clone();
        let _stop = stop.clone();
        let pull_handle = tokio::spawn(async move {
            // Pull Broker Partition
            let pull_resp_strm = pull_coo_client
                .open_sstream(
                    brokercoosvc::PullReq {
                        topics: vec![],
                        partition_ids: vec![],
                        broker_id,
                    },
                    |chan, request, addr| async {
                        *_coo_leader_call.lock().await = addr;
                        BrokerCooServiceClient::new(chan).pull(request).await
                    },
                    |res| async {
                        pull_broker.apply_topic_infos(res);
                        info!(
                            "Broker[{broker_id}]->External-Coo[{}]: Pull TopicList",
                            _coo_leader_resp.lock().await,
                        );
                        Ok(())
                    },
                    _stop,
                )
                .await;
            error!("pull TopicList: 自动调用完成?");
        });

        // SyncConsumerGroup
        let sync_consumergroup_broker = broker.clone();
        let sync_consumergroup_coo_client = self.coo_leader_client.as_ref().unwrap().clone();
        let _coo_leader_sync_consumergroup = coo_leader.clone();
        let _stop = stop.clone();
        let sync_consumergroup_handle = tokio::spawn(async move {
            // Pull Broker Partition
            let sync_resp = sync_consumergroup_coo_client
                .open_sstream(
                    brokercoosvc::SyncConsumerAssignmentsReq { broker_id },
                    |chan, request, addr| async {
                        *_coo_leader_sync_consumergroup.lock().await = addr;
                        BrokerCooServiceClient::new(chan)
                            .sync_consumer_assignments(request)
                            .await
                    },
                    |res| async {
                        let _ = sync_consumergroup_broker.apply_consumergroup(res).await;
                        info!(
                            "Broker[{broker_id}]->External-Coo[{}]: SyncConsumerGroup",
                            _coo_leader_sync_consumergroup.lock().await,
                        );
                        Ok(())
                    },
                    _stop,
                )
                .await;
            error!("SyncConsumerGroup 自动调用完成？");
        });

        if let Err(e) = tokio::try_join!(report_handle, pull_handle, sync_consumergroup_handle) {
            error!("tokio try_join error: {:?}", e);
        }
    }
}

async fn change_with_status(status: &Status, coo_leader_addr: Arc<Mutex<String>>) {
    if let Some(new_leader) = extract_leader_address(status) {
        if new_leader.is_empty() || new_leader == "unknown" {
            return;
        }
        warn!("Coo-Leader has been changed to: {}", new_leader);
        let mut new_coo_leader_addr_lock = coo_leader_addr.lock().await;
        if new_coo_leader_addr_lock.is_empty() {
            *new_coo_leader_addr_lock = new_leader;
        }
    }
}
