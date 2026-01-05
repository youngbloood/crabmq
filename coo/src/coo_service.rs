use crate::coo::Coordinator;

pub struct CoordinatorService {
    pub coo_addr: String,

    coo: Coordinator,
}

impl CoordinatorService {
    /// 启动 Coordinator 服务
    pub async fn run(&self) -> Result<()> {
        if self.coo_addr.is_empty() || self.coo_grpc_addr.is_empty() {
            return Err(anyhow!("must both have coo_addr and coo_grpc_addr"));
        }
        // 定期检查本节点从: 主 -> 非主，并发送 NotLeader 消息，用户通知 broker 和 client 变更链接逻辑
        // self.start_main_loop();

        self.loop_handle_command();

        let handle = tokio::net::TcpListener::bind(&self.coo_addr)
            .await
            .map_err(|e| anyhow!("Coo bind addr {} err: {:?}", &self.conf.coo_addr, e))?;

        loop {
            let h = handle.accept().await;
            if let Ok((stream, addr)) = h {
                let (rh, wh) = stream.into_split();
                let _addr = addr.ip().to_string().clone();
                self.conns.insert(addr.ip().to_string(), Conn::new(wh));
                let conns = self.conns.clone();
                let cmd_tx = self.cmd_tx.clone();
                let coo = self.coo.clone();

                tokio::spawn(async move {
                    let mut head = [0; 5];
                    loop {
                        match rh.read_exact(&mut head).await {
                            Ok(size) => {
                                if size != head.len() {
                                    conns.remove(_addr);
                                    break;
                                }

                                // 根据 类型和长度解析，然后

                                coo.send_command(Command {
                                    addr: (),
                                    index: (),
                                    endecoder: (),
                                })
                                .await?;
                            }
                            Err(e) => todo!(),
                        }
                    }
                });
            }
        }

        Ok(())
    }

    fn loop_handle_command(&self) {
        let mut coo = self.coo.clone();
        tokio::spawn(async move {
            loop {
                let cmd = coo.get_command().await;
                if let Ok(cmd) = cmd {
                    coo.handle_command(cmd).await;
                }
            }
        });
    }
}
