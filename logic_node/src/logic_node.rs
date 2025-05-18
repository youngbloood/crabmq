use anyhow::Result;
use broker::Broker;
use coo::coo::Coordinator;
use dashmap::DashMap;
use grpcx::{
    brokercoosvc::broker_coo_service_client::BrokerCooServiceClient, commonsvc::CooListResp,
};
use log::error;
use std::{sync::Arc, time::Duration};
use storagev2::Storage;
use tokio::{select, sync::Mutex, time::interval};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

pub struct LogicNode<T>
where
    T: Storage + 'static,
{
    broker: Option<Arc<Broker<T>>>,
    coo: Option<Arc<Coordinator>>,
    coo_client: Option<Arc<Mutex<BrokerCooServiceClient<Channel>>>>,

    coo_addrs: Arc<DashMap<u64, String>>,
}

impl<T> LogicNode<T>
where
    T: Storage + 'static,
{
    pub fn new() -> Self {
        Self {
            broker: None,
            coo: None,
            coo_client: None,
            coo_addrs: Arc::default(),
        }
    }

    pub fn with_broker(&mut self, b: Broker<T>) -> &mut Self {
        self.broker = Some(Arc::new(b));
        self
    }

    pub fn with_coo(&mut self, coo: Coordinator) -> &mut Self {
        self.coo = Some(Arc::new(coo));
        self
    }

    fn apply_coo_list(&self, coo_list: CooListResp) {
        todo!()
    }
}

pub async fn start_logic_node<T: Storage>(mut ln: LogicNode<T>, coo_addr: &mut String) {
    let broker = ln.broker.clone();
    loop {
        if broker.as_ref().is_some() {
            let broker = broker.as_ref().unwrap();
            let mut state_recv = broker.get_state_reciever();

            if ln.coo.is_some() {
                // 1. 如果当前 ln 中的 coo 是leader，直接向其汇报状态
                let coo = ln.coo.as_ref().unwrap();
                if coo.is_leader().await {
                    let broker = ln.broker.as_ref().unwrap().clone();
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
                                broker.apply_topic_info(topic_list.unwrap());
                            }
                        }
                    }
                }
            } else {
                // 如果当前 ln 中的 coo 不是leader，需要先获取 coo:leader 节点并链接，然后汇报状态
                // 使用 grpc 链接 coo:leader, 并进行交互
                if let Ok(coo_client) = connect_to_coo(coo_addr).await {
                    let coo_client = Arc::new(Mutex::new(coo_client));
                    let coo_client_report = coo_client.clone();
                    let coo_client_pull = coo_client.clone();
                    ln.coo_client = Some(coo_client);

                    let id = broker.get_id();
                    let report_broker = broker.clone();
                    let report_handle = tokio::spawn(async move {
                        let strm = ReceiverStream::new(state_recv);
                        match coo_client_report.lock().await.report(strm).await {
                            Ok(resp) => {
                                let mut resp_strm = resp.into_inner();
                                while let Ok(Some(r)) = resp_strm.message().await {
                                    let _ = report_broker;
                                    todo!("处理 r = {:?}", r);
                                }
                            }
                            Err(_) => todo!(),
                        }
                    });

                    let pull_broker = broker.clone();
                    let pull_handle = tokio::spawn(async move {
                        match coo_client_pull
                            .lock()
                            .await
                            .pull(tonic::Request::new(grpcx::brokercoosvc::PullReq { id }))
                            .await
                        {
                            Ok(resp) => {
                                let mut resp_strm = resp.into_inner();
                                while let Ok(Some(tl)) = resp_strm.message().await {
                                    pull_broker.apply_topic_info(tl);
                                }
                            }
                            Err(_) => todo!(),
                        }
                    });

                    tokio::try_join!(report_handle, pull_handle);

                    // 收到错误，重置 coo_addr，并进入下一次重连
                }
            }
        }
    }
}

async fn connect_to_coo(coo_addr: &mut String) -> Result<(BrokerCooServiceClient<Channel>)> {
    let mut reconnect_ticker = interval(Duration::from_secs(3));
    loop {
        // 2.如果当前 ln 中的 coo 不是leader，需要先获取 coo:leader 节点并链接，然后汇报状态
        match BrokerCooServiceClient::connect(format!("http://{}", coo_addr)).await {
            Ok(coo_client) => return Ok(coo_client),
            Err(e) => {
                error!("connect to [{}] failed: {:?}", coo_addr, e);
                reconnect_ticker.tick().await;
            }
        }
    }
}
