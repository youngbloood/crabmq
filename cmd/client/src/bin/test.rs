use clap::{arg, ArgAction, Command};

fn main() {
    Command::new("")
        .arg(arg!(--action).required(true))
        .arg(arg!(--topic--name <TOPIC_NAME>));
    // .arg(arg!( --channel-name <CHANNEL_NAME>))
    // .arg(arg!( --topic-ephemeral <TOPIC_EPHEMERAL>).action(ArgAction::SetFalse))
    // .arg(arg!(--channel-ephemeral  <CHANNEL_EPHEMERAL>).action(ArgAction::SetFalse))
    // .arg(arg!(--heartbeat).action(ArgAction::SetFalse))
    // .arg(arg!(--protocol-version  <PROTOCOL_VERSION>));
}
