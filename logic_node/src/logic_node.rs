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

/// 通信模式枚举
#[derive(Debug, Clone, Copy, PartialEq)]
enum CommunicationMode {
    /// 本地通信（broker 与同 logic_node 内的 coo 通信）
    Local,
    /// 外部通信（broker 与外部 coo 通过 gRPC 通信）
    External,
}

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

        // 检查当前 logic_node 的配置
        let has_broker = self.broker.is_some();
        let has_coo = self.coo.is_some();

        if !has_broker {
            // 只有 coo 或 slave，不需要 broker-coo 通信
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }

        if has_coo {
            // 有 broker 和 coo，需要动态判断通信方式
            self.handle_broker_coo_communication(coo_leader).await;
        } else {
            // 只有 broker，使用外部 coo 通信
            self.handle_broker_external_communication(coo_leader).await;
        }
    }

    /// 处理 broker 与 coo 的通信，根据 coo 的 leader 状态动态切换通信方式
    async fn handle_broker_coo_communication(&mut self, coo_leader: Arc<Mutex<String>>) {
        let coo = self.coo.as_ref().unwrap().clone();
        let broker = self.broker.as_ref().unwrap().clone();
        let broker_id = broker.get_id();
        let coo_id = coo.id;

        info!(
            "Starting broker-coo communication for Broker[{}] and Coo[{}]",
            broker_id, coo_id
        );

        // 创建状态监听器
        let (tx_watcher, mut rx_watcher) = watch::channel(false);
        self.start_watcher(tx_watcher);

        // 初始化通信状态：None 表示尚未启动任何会话，首次将根据实际 leader 状态启动
        let mut current_communication_mode: Option<CommunicationMode> = None;

        loop {
            // 检查当前 coo 是否为 leader
            let is_leader = coo.is_leader().await;
            let target_mode = if is_leader {
                CommunicationMode::Local
            } else {
                CommunicationMode::External
            };

            // 如果未启动或通信模式需要切换
            if current_communication_mode != Some(target_mode) {
                info!(
                    "Broker[{}] switching communication mode: {:?} -> {:?}",
                    broker_id,
                    current_communication_mode,
                    Some(target_mode)
                );

                // 停止当前的通信模式
                self.stop_current_communication().await;

                // 切换到新的通信模式
                current_communication_mode = Some(target_mode);

                match target_mode {
                    CommunicationMode::Local => {
                        // 启动本地通信
                        self.start_local_communication(coo_leader.clone(), rx_watcher.clone())
                            .await;
                    }
                    CommunicationMode::External => {
                        // 启动外部通信
                        self.start_external_communication(coo_leader.clone(), rx_watcher.clone())
                            .await;
                    }
                }
            }

            // 不再轮询睡眠，依赖内部会话在切换时返回
        }
    }

    /// 处理只有 broker 的情况，使用外部 coo 通信
    async fn handle_broker_external_communication(&mut self, coo_leader: Arc<Mutex<String>>) {
        let broker = self.broker.as_ref().unwrap().clone();
        let broker_id = broker.get_id();

        info!("Starting external communication for Broker[{}]", broker_id);

        // 创建状态监听器（始终为 false，因为没有本地 coo）
        let (_, rx_watcher) = watch::channel(false);

        loop {
            self.start_external_communication(coo_leader.clone(), rx_watcher.clone())
                .await;
            // 外部通信返回后再继续尝试重启会话
        }
    }

    /// 启动本地通信
    async fn start_local_communication(
        &self,
        coo_leader: Arc<Mutex<String>>,
        mut rx_watcher: watch::Receiver<bool>,
    ) {
        if self.broker.is_none() || self.coo.is_none() {
            return;
        }

        let broker = self.broker.as_ref().unwrap().clone();
        let broker_id = broker.get_id();
        let coo = self.coo.as_ref().unwrap().clone();
        let coo_id = coo.id;

        info!(
            "Broker[{}] starting LOCAL communication with Coo[{}]",
            broker_id, coo_id
        );

        // 获取内部通信通道
        let mut state_recv = broker.get_state_reciever("logic_node".to_string());
        let mut pull_recv = match coo.broker_pull(broker.get_id()).await {
            Ok(rx) => rx,
            Err(e) => {
                error!("Failed to get broker pull receiver: {:?}", e);
                return;
            }
        };

        loop {
            select! {
                state = state_recv.recv() => {
                    if state.is_none() {
                        continue;
                    }
                    info!("Broker[{}]->Local-Coo[{}]: Report BrokerState", broker_id, coo_id);
                    coo.apply_broker_report(state.unwrap());
                }

                topic_list = pull_recv.recv() => {
                    if topic_list.is_none() {
                        continue;
                    }
                    match topic_list.unwrap() {
                        Ok(tl) => {
                            info!("Broker[{}]->Local-Coo[{}]: Pull TopicList", broker_id, coo_id);
                            broker.apply_topic_infos(tl);
                        }
                        Err(ref status) => {
                            // leader 发生了切换，需要切换到外部通信
                            change_with_status(status, coo_leader.clone()).await;
                            warn!("Local Coo is no longer leader, switching to external communication");
                            break;
                        },
                    }
                }

                changed = rx_watcher.changed() => {
                    if changed.is_ok() && !*rx_watcher.borrow() {
                        warn!("Coo-Leader has been changed to External");
                        break;
                    }
                }
            }

            // 检查 coo 是否仍然是 leader
            if !coo.is_leader().await {
                warn!("Local Coo is no longer leader, switching to external communication");
                break;
            }
        }

        // 清理资源
        broker.unleash_state_reciever("logic_node");
    }

    /// 启动外部通信
    async fn start_external_communication(
        &mut self,
        coo_leader: Arc<Mutex<String>>,
        mut watcher: watch::Receiver<bool>,
    ) {
        if self.broker.is_none() {
            return;
        }

        // 等待 coo leader 地址可用
        if coo_leader.lock().await.is_empty() {
            tokio::time::sleep(Duration::from_secs(3)).await;
            return;
        }

        let broker = self.broker.as_ref().unwrap().clone();
        let broker_id = broker.get_id();
        info!("Broker[{}] starting EXTERNAL communication", broker_id);

        // 初始化或更新 gRPC 客户端
        if self.coo_leader_client.is_none() {
            let coo_client = SmartClient::new(random_str(6), vec![coo_leader.lock().await.clone()]);
            self.coo_leader_client = Some(coo_client.clone());
        }
        let refresh_coo_client = self.coo_leader_client.as_ref().unwrap().clone();

        // 启动智能客户端刷新
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

        // 监听切换到本地通信的信号
        tokio::spawn(async move {
            select! {
                changed = watcher.changed() => {
                    if changed.is_ok() && *watcher.borrow() {
                        _broker.unleash_state_reciever("logic_node");
                        _stop.cancel();
                        warn!("Coo-Leader has been changed to Local(Report Handle)");
                    }
                }
            }
        });

        // Report BrokerState
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

        // Pull Broker Partition
        let pull_broker = broker.clone();
        let pull_coo_client = self.coo_leader_client.as_ref().unwrap().clone();
        let _coo_leader_call = coo_leader.clone();
        let _coo_leader_resp = coo_leader.clone();
        let _stop = stop.clone();
        let pull_handle = tokio::spawn(async move {
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

    /// 停止当前的通信模式
    async fn stop_current_communication(&self) {
        if let Some(broker) = &self.broker {
            broker.unleash_state_reciever("logic_node");
        }
        // 这里可以添加其他清理逻辑
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
            let mut ticker = interval(Duration::from_secs(1)); // 更频繁的检查
            // 初始化时立即发送一次当前状态，避免首次判断延迟
            let init_state = coo.is_leader().await;
            let _ = tx_watcher.send(init_state);
            let mut last_leader_state = init_state;

            loop {
                ticker.tick().await;
                let current_leader_state = coo.is_leader().await;

                // 只在状态发生变化时发送通知
                if current_leader_state != last_leader_state {
                    info!(
                        "Coo leader state changed: {} -> {}",
                        last_leader_state, current_leader_state
                    );
                    let _ = tx_watcher.send(current_leader_state);
                    last_leader_state = current_leader_state;
                }
            }
        });
    }
}

async fn change_with_status(status: &Status, coo_leader_addr: Arc<Mutex<String>>) {
    if let Some(new_leader) = extract_leader_address(status) {
        if new_leader.is_empty() || new_leader == "unknown" {
            return;
        }
        let mut addr = coo_leader_addr.lock().await;
        if *addr != new_leader {
            warn!("Coo-Leader changed: {} -> {}", *addr, new_leader);
            *addr = new_leader;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_communication_mode_equality() {
        assert_eq!(CommunicationMode::Local, CommunicationMode::Local);
        assert_eq!(CommunicationMode::External, CommunicationMode::External);
        assert_ne!(CommunicationMode::Local, CommunicationMode::External);
    }

    #[test]
    fn test_communication_mode_debug() {
        assert_eq!(format!("{:?}", CommunicationMode::Local), "Local");
        assert_eq!(format!("{:?}", CommunicationMode::External), "External");
    }
}
