use anyhow::Result;
use bytes::Bytes;
use clap::arg;
use clap::builder::ValueParser;
use clap::command;
use clap::Arg;
use clap::ArgAction;
use clap::Command;
use clap::Parser;
use clap::Subcommand;
use common::global;
use core::message::Message;
use core::message::ProtocolBody;
use core::message::ProtocolBodys;
use core::message::ProtocolHead;
use futures::executor::block_on;
use inquire::Text;
use std::collections::HashMap;
use std::env::args;
use std::fmt::Write;
use std::os;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::select;
// use clap::{arg, command, Parser};

#[derive(Parser, Debug)]
#[command(version,about,long_about = None)]
struct Args {
    #[arg(short = 'd', long)]
    target: String,
    /// The action the this reqest.
    #[arg(long)]
    action: String,

    /// Topic name.
    #[arg(long, short)]
    topic_name: String,

    /// Channel name.
    #[arg(long, short)]
    channel_name: String,

    /// It mean this topic is ephemeral.
    #[arg(long)]
    topic_ephemeral: bool,

    /// It mean this channel is ephemeral.
    #[arg(long)]
    channel_ephemeral: bool,

    /// Weather the packet contain the heartbeat.
    #[arg(long)]
    heartbeat: bool,

    /// The protocol version.
    #[arg(long)]
    protocol_version: u8,

    /// 暂时不知道如何解析对象列表： Vec<MessageArgs>
    #[command(subcommand)]
    messages: AddMsg,
}

#[derive(Subcommand, Debug)]
enum AddMsg {
    Msg(MessageArgs),
}

#[derive(Parser, Debug)]
struct MessageArgs {
    #[arg(long)]
    ack: bool,
    #[arg(long)]
    defer: u64,
    #[arg(long, short)]
    persist: bool,
    #[arg(long, short)]
    delete: bool,
    #[arg(long, short)]
    ready: bool,
    #[arg(long, short)]
    id: String,
    #[arg(long, short)]
    body: Vec<u8>,
    // 指定body的文件
    #[arg(long)]
    body_file: String,
}

impl Args {
    fn builder(&self) -> Result<Message> {
        let mut head = ProtocolHead::new();
        head.set_topic(&self.topic_name.as_str())?;
        head.set_channel(&self.channel_name.as_str())?;
        head.set_action(action_to_u8(self.action.as_str()))
            .set_topic_ephemeral(self.topic_ephemeral)
            .set_channel_ephemeral(self.channel_ephemeral)
            .set_version(self.protocol_version)
            .expect("init head failed");

        let mut body = ProtocolBody::new();
        match &self.messages {
            AddMsg::Msg(msg) => {
                body.with_ack(msg.ack)
                    .with_delete(msg.delete)
                    .with_persist(msg.persist)
                    .with_ready(msg.ready)
                    .set_defer_time(msg.defer);
                if msg.body.len() != 0 {
                    body.set_body(Bytes::copy_from_slice(&msg.body))?;
                } else if msg.body_file.len() != 0 {
                    let filename = msg.body_file.to_string();
                    let content = block_on(fs::read(filename))?;
                    body.set_body(Bytes::copy_from_slice(&content))?;
                }
                body.set_body(Bytes::copy_from_slice(&msg.body))?;
                body.set_id(&msg.id.as_str())?;
            }
        }
        let mut bodys = ProtocolBodys::new();
        bodys.push(body);

        let mut msg = Message::with(head, bodys);
        msg.post_fill();
        Ok(msg)
    }
}

fn action_to_u8(action: &str) -> u8 {
    let a: u8;
    match action {
        "FIN" | "fin" => a = 1,
        "PUB" | "pub" => a = 2,
        _ => a = u8::MAX,
    }

    a
}

// #[tokio::main]
// async fn main() -> Result<()> {
//     let args = Args::parse();

//     let addr = args.target.parse().unwrap();
//     let socket = TcpSocket::new_v4()?;
//     let mut stream = socket.connect(addr).await?;

//     let bts = args.builder().unwrap().as_bytes();
//     select! {
//         result = stream.write(&bts) => {
//             println!("result = {result:?}")
//         }
//         _ =  global::CANCEL_TOKEN.cancelled() =>{
//             panic!("cancelled")
//         }
//     }

//     Ok(())
// }

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = args();
    if args.len() < 2 {
        panic!("must specify the target of the Tsuixuq");
    }
    let first = args.nth(1);
    let target = first.as_ref().unwrap().as_str();

    let socket = TcpSocket::new_v4()?;
    let mut stream = socket.connect(target.parse().unwrap()).await?;
    let (reader, writer) = stream.split();

    let mut support_cmds = HashMap::new();

    let mut head_cmd = Command::new("head")
        .arg(arg!(--action).required(true))
        .arg(arg!(  --topic-name <TOPIC_NAME>))
        .arg(arg!(  --channel-name <CHANNEL_NAME>))
        .arg(arg!(  --topic-ephemeral <TOPIC_EPHEMERAL>).action(ArgAction::SetFalse))
        .arg(arg!(  --channel-ephemeral  <CHANNEL_EPHEMERAL>).action(ArgAction::SetFalse))
        .arg(arg!(--heartbeat).action(ArgAction::SetFalse))
        .arg(arg!(  --protocol-version  <PROTOCOL_VERSION>));
    let mut msg_cmd = Command::new("msg")
        // .arg(Arg::new("ack").action(ArgAction::SetFalse))
        // .arg(Arg::new("defer").action(ArgAction::SetFalse))
        // .arg(Arg::new("persist").action(ArgAction::SetFalse))
        // .arg(Arg::new("delete").action(ArgAction::SetFalse))
        // .arg(Arg::new("ready").action(ArgAction::SetTrue))
        .arg(Arg::new("id"))
        .arg(Arg::new("body"))
        .arg(Arg::new("body-file"));

    support_cmds.insert("head", head_cmd.clone());
    support_cmds.insert("msg", msg_cmd.clone());

    loop {
        let input = Text::new("#=").with_help_message("\\? help").prompt();
        match input {
            Ok(cmd) => {
                println!("cmd = {cmd}");

                let mut cmds = cmd.split_whitespace();
                let first = cmds.next();
                if let Some(v) = first {
                    if v == "\\?" {
                        head_cmd.print_help();
                        msg_cmd.print_help();
                        continue;
                    }
                    match support_cmds.get(v) {
                        Some(cmd) => {
                            println!("ccccc = {cmd:?}")
                        }
                        None => {
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
            }
            Err(_) => panic!("An error happened when asking for your name, try again later."),
        }
    }
}
