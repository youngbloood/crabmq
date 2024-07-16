use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use clap::arg;
use clap::Parser;
use common::global;
use core::conn::Conn;
use futures::executor::block_on;
use inquire::Text;
use protocol::message::Message;
use protocol::v1::ProtocolBodyV1;
use protocol::v1::ProtocolBodysV1;
use protocol::v1::ProtocolHeadV1;
use protocol::ProtocolBodys;
use protocol::ProtocolHead;
use std::env::args;
use tokio::fs;
use tokio::net::TcpSocket;
use tokio::select;
use tokio::signal;
use tokio::sync::mpsc;

const DEFAULT_NAME: &str = "default";

#[derive(Parser, Debug, Default)]
#[command(version,about,long_about = None)]
struct HeadArgs {
    /// The action the this reqest.
    #[arg(long)]
    action: String,

    /// Topic name.
    #[arg(long, short, default_value_t = DEFAULT_NAME.to_string())]
    topic_name: String,

    /// Channel name.
    #[arg(long, short, default_value_t = DEFAULT_NAME.to_string())]
    channel_name: String,

    /// It mean this topic is ephemeral.
    #[arg(long, default_value_t = false)]
    topic_ephemeral: bool,

    /// It mean this channel is ephemeral.
    #[arg(long, default_value_t = false)]
    channel_ephemeral: bool,

    /// Weather the packet contain the heartbeat.
    #[arg(long, default_value_t = false)]
    heartbeat: bool,

    /// The protocol version.
    #[arg(long, default_value_t = 1)]
    protocol_version: u8,
}

#[derive(Parser, Debug)]
struct MsgArgs {
    #[arg(long, default_value_t = true)]
    ack: bool,

    #[arg(long, default_value_t = 0)]
    defer: u64,

    #[arg(long, short, default_value_t = true)]
    persist: bool,

    #[arg(long, short, default_value_t = false)]
    delete: bool,

    #[arg(long, short, default_value_t = false)]
    not_ready: bool,

    #[arg(long, short)]
    id: Option<String>,

    #[arg(long, short)]
    body: Option<String>,
    // 指定body的文件
    #[arg(long)]
    body_file: Option<String>,
}

#[derive(Debug)]
struct Args {
    head: Option<HeadArgs>,
    bodys: Vec<MsgArgs>,
}

impl Args {
    fn new() -> Self {
        Self {
            head: None,
            bodys: vec![],
        }
    }

    fn reset(&mut self) {
        self.head = None;
        self.bodys = vec![];
    }

    fn builder(&self) -> Result<Message> {
        if self.head.is_none() {
            return Err(anyhow!("not found head in message"));
        }
        let mut head = ProtocolHeadV1::new();
        head.set_topic(self.head.as_ref().unwrap().topic_name.as_str())?;
        head.set_channel(self.head.as_ref().unwrap().channel_name.as_str())?;
        head.set_action(action_to_u8(self.head.as_ref().unwrap().action.as_str()))
            .set_topic_ephemeral(self.head.as_ref().unwrap().topic_ephemeral)
            .set_channel_ephemeral(self.head.as_ref().unwrap().channel_ephemeral)
            .set_version(self.head.as_ref().unwrap().protocol_version)
            .expect("init head failed");

        let mut bodys = ProtocolBodysV1::new();
        self.bodys.iter().for_each(|msg: &MsgArgs| {
            let mut body = ProtocolBodyV1::new();
            body.with_ack(msg.ack)
                .with_delete(msg.delete)
                .with_persist(msg.persist)
                .with_notready(msg.not_ready)
                .with_defer_time_offset(msg.defer);

            if let Some(body_str) = msg.body.as_ref() {
                body.with_body(Bytes::copy_from_slice(body_str.as_bytes()))
                    .expect("set body err");
            }
            if let Some(body_file) = msg.body_file.as_ref() {
                let content = block_on(fs::read(body_file)).expect("read {filename} err");
                body.with_body(Bytes::copy_from_slice(&content))
                    .expect("set body err");
            }

            if let Some(id) = msg.id.as_ref() {
                body.with_id(id).expect("set id err");
            }

            bodys.push(body);
        });

        let msg = Message::with(ProtocolHead::V1(head), ProtocolBodys::V1(bodys))?;
        msg.validate(u8::MAX as u64, u64::MAX)?;
        // msg.post_fill();
        Ok(msg)
    }

    fn push_body(&mut self, body: MsgArgs) -> Result<()> {
        if self.head.is_none() {
            return Err(anyhow!("can't set body when not set head"));
        }
        self.bodys.push(body);
        Ok(())
    }

    fn set_head(&mut self, head: HeadArgs) -> Result<()> {
        self.head = Some(head);
        Ok(())
    }
}

fn action_to_u8(action: &str) -> u8 {
    let mut a = 0;
    match action.to_lowercase().as_str() {
        "fin" => a = 1,
        "rdy" => a = 2,
        "req" => a = 3,
        "pub" => a = 4,
        "nop" => a = 5,
        "touch" => a = 6,
        "sub" => a = 7,
        "cls" => a = 8,
        "auth" => a = 9,
        _ => a = u8::MIN,
    }

    a
}

#[tokio::main]
async fn main() -> Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    let mut args = args();
    if args.len() < 2 {
        panic!("must specify the target of the crabmqd");
    }
    let first = args.nth(1);
    let target = first.as_ref().unwrap().as_str();
    println!("connect to {target}");

    let socket = TcpSocket::new_v4()?;
    let stream = socket.connect(target.parse().unwrap()).await?;

    let (input_tx, mut input_rx) = mpsc::channel(1);
    let (loop_tx, mut loop_rx) = mpsc::channel(1);

    tokio::spawn(async move {
        loop {
            select! {
                _ = global::CANCEL_TOKEN.cancelled() => {
                    return;
                }

                _ = loop_rx.recv() =>{
                    let input = Text::new("#=").with_help_message("\\? help").prompt();
                    match input {
                        Ok(r) => match input_tx.send(r).await {
                            Ok(_) => {}
                            Err(_) => {
                                global::cancel();
                                return;
                            },
                        },
                        Err(e) => {
                            println!("err: {e}");
                            global::cancel();
                            return;
                        }
                    }
                }
            }
        }
    });

    let _ = loop_tx.send(1).await;
    let mut conn = Conn::new(stream);
    let mut args = Args::new();
    loop {
        select! {
            input = input_rx.recv() => {
                if let Some(cmd) = input {
                    let mut cmds = cmd.split_whitespace();
                    let cmds_clone = cmds.clone();
                    let first = cmds.next();
                    if let Some(v) = first {
                        match v {
                            "\\?" => {
                                println!("TODO: print help info");
                            }

                            "\\q" => {
                                global::cancel();
                                break;
                            }

                            "head" => {
                                match HeadArgs::try_parse_from(cmds_clone){
                                    Ok(head) => {
                                        println!("set head:{head:?}");
                                        let _ = args.set_head(head);
                                    }
                                    Err(e) => {
                                        eprintln!("parse head command err: {e}");
                                    }
                                }
                            }

                            "msg" => {
                                match MsgArgs::try_parse_from(cmds_clone){
                                    Ok(body) =>{
                                        println!("push body:{body:?}");
                                        if let Err(e) = args.push_body(body) {
                                            eprintln!("{e}");
                                        }
                                    }
                                    Err(e) =>{
                                        eprintln!("parse msg command err: {e:?}");
                                    }
                                }
                            }

                            "send" => {
                                println!("send args: {args:?}");
                                match args.builder(){
                                    Ok(msg) => {
                                        let bts = &msg.as_bytes();
                                        println!("send bts: {bts:?}");
                                        if let Err(e) = conn.write(&msg.as_bytes(), 30).await
                                        {
                                            eprintln!("write err: {e}");
                                        }
                                    },
                                    Err(e) => {
                                        eprintln!("builder err: {e}");
                                    },
                                }
                                args.reset();
                            }

                            _ => {
                                println!("black branch");
                                use std::process::Command as std_command;
                                let output = std_command::new("/bin/bash")
                                    .arg("-c")
                                    .arg(cmd)
                                    .output()
                                    .expect("failed");
                                let stdout =
                                    String::from_utf8(output.stdout).expect("convert to string failed");
                                println!("{stdout}");
                            }
                        }
                    }
                    let _= loop_tx.send(1).await;
                }
            }

            resp = conn.read_parse(30) =>{
                match resp {
                    Ok(msg) => {
                        println!("recieve message: {msg:?}\n");
                    }
                    Err(e) => {
                        eprintln!("recieve err: {e}\n");
                        if e.to_string().contains("eof"){
                            break;
                        }
                    }
                }
            }

            _ = global::CANCEL_TOKEN.cancelled() => {
                break;
            }

            sig = signal::ctrl_c() => {
                eprintln!("recieve signal: {:?}", sig);
                global::cancel();
                break;
             }
        }
    }

    Ok(())
}
