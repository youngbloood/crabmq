use anyhow::{Result, anyhow};
use broker::Broker;
use coo::coo::Coordinator;
use dashmap::DashMap;
use grpcx::{
    brokercoosvc::{self, broker_coo_service_client::BrokerCooServiceClient},
    commonsvc::CooListResp,
    smart_client::SmartClient,
};
use log::error;
use std::{sync::Arc, time::Duration};
use storagev2::Storage;
use tokio::{select, time::interval};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;

pub struct Builder<T>
where
    T: Storage + 'static,
{
    id: u32,
    broker: Option<Broker<T>>,
    coo: Option<Coordinator>,
    salve: Option<Slave>,
}

impl<T> Default for Builder<T>
where
    T: Storage + 'static,
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

impl<T> Builder<T>
where
    T: Storage + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }

    pub fn broker(mut self, b: Broker<T>) -> Self {
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

    pub fn build(self) -> LogicNode<T> {
        let _ = &self.validate().expect("LoginNode validate failed");
        LogicNode {
            id: self.id,
            coo: self.coo,
            broker: self.broker,
            slave: self.salve,
            coo_leader_client: None,
            coo_addrs: Arc::default(),
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

pub struct LogicNode<T>
where
    T: Storage + 'static,
{
    id: u32,
    coo: Option<Coordinator>,
    broker: Option<Broker<T>>,
    slave: Option<Slave>,

    coo_leader_client: Option<SmartClient>,
    // coo_client: Option<Arc<Mutex<BrokerCooServiceClient<Channel>>>>,
    coo_addrs: Arc<DashMap<u64, String>>,
}

impl<T> LogicNode<T>
where
    T: Storage + 'static,
{
    fn apply_coo_list(&self, coo_list: CooListResp) {
        todo!()
    }

    pub async fn run(&mut self, coo_leader_addr: String) -> Result<()> {
        let mut handles = vec![];

        let coo = self.coo.clone();
        if coo.is_some() {
            let coo_handle = tokio::spawn(async move {
                if let Err(e) = coo.unwrap().run().await {
                    error!("Coo run err: {:?}", e);
                }
            });
            handles.push(coo_handle);
        }

        let broker = self.broker.clone();
        if broker.is_some() {
            let broker_handle = tokio::spawn(async move {
                if let Err(e) = broker.unwrap().run().await {
                    error!("Coo run err: {:?}", e);
                }
            });
            handles.push(broker_handle);
        }

        let salve = self.slave.clone();
        if salve.is_some() {
            let slave_handle = tokio::spawn(async move {
                if let Err(e) = salve.unwrap().run().await {
                    error!("Coo run err: {:?}", e);
                }
            });
            handles.push(slave_handle);
        }

        let coo = self.coo.clone();
        let broker = self.broker.clone();
        if broker.as_ref().is_some() && coo.as_ref().is_some() {
            let broker = broker.as_ref().unwrap();
            let coo = coo.as_ref().unwrap();
            let broker_id = broker.get_id();

            if coo.is_leader().await {
                let broker = broker.clone();
                let mut state_recv = broker.get_state_reciever("logic_node".to_string());
                let mut pull_recv = coo.broker_pull(broker.get_id()).unwrap();
                loop {
                    select! {
                        state = state_recv.recv() => {
                            if state.is_none(){
                                continue;
                            }
                            coo.broker_report(state.unwrap());
                        }

                        topic_list = pull_recv.recv() => {
                            if topic_list.is_none(){
                                continue;
                            }
                            broker.apply_topic_infos(topic_list.unwrap());
                        }
                    }
                }
            } else {
                // 如果当前 ln 中的 coo 不是leader，需要先获取 coo:leader 节点并链接，然后汇报状态
                // 使用 grpc 链接 coo:leader, 并进行交互
                let coo_client = SmartClient::new(vec![coo_leader_addr.clone()]);
                self.coo_leader_client = Some(coo_client.clone());

                // Report  BrokerState
                let report_broker = broker.clone();
                let report_coo_client = coo_client.clone();
                let report_handle = tokio::spawn(async move {
                    let report_resp_strm = report_coo_client
                        .open_bistream(
                            || {
                                let report_recver =
                                    report_broker.get_state_reciever("logic_node".to_string());
                                ReceiverStream::new(report_recver)
                            },
                            |chan, request| async {
                                BrokerCooServiceClient::new(chan).report(request).await
                            },
                        )
                        .await;

                    let _ = report_resp_strm;
                });
                handles.push(report_handle);

                // Pull  Broker Partition
                let pull_broker = broker.clone();
                let pull_coo_client = coo_client.clone();
                let pull_handle = tokio::spawn(async move {
                    // Pull Broker Partition
                    let pull_resp_strm = pull_coo_client
                        .open_sstream(
                            brokercoosvc::PullReq { id: broker_id },
                            |chan, request| async {
                                BrokerCooServiceClient::new(chan).pull(request).await
                            },
                        )
                        .await;

                    if let Ok(mut strm) = pull_resp_strm {
                        while let Some(Ok(tl)) = strm.next().await {
                            pull_broker.apply_topic_infos(tl);
                        }
                    }
                });
                handles.push(pull_handle);
            }
        }
        if let Err(e) = futures::future::try_join_all(handles).await {
            error!("LogicNode run err: {:?}", e);
            return Err(anyhow!(e));
        }
        Ok(())
    }
}

// async fn connect_to_coo(coo_addr: &mut String) -> Result<(BrokerCooServiceClient<Channel>)> {
//     let mut reconnect_ticker = interval(Duration::from_secs(3));
//     loop {
//         // 2.如果当前 ln 中的 coo 不是leader，需要先获取 coo:leader 节点并链接，然后汇报状态
//         match BrokerCooServiceClient::connect(format!("http://{}", coo_addr)).await {
//             Ok(coo_client) => return Ok(coo_client),
//             Err(e) => {
//                 error!("connect to [{}] failed: {:?}", coo_addr, e);
//                 reconnect_ticker.tick().await;
//             }
//         }
//     }
// }
