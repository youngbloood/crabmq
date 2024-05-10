use anyhow::{anyhow, Result};
use clap::Parser;
use common::global;
use core::{tsuixuq::TsuixuqOption, tsuixuqd::Tsuixuqd};
use tokio::{select, signal};

#[derive(Parser, Debug)]
pub struct Config {
    #[arg(short = 'c', long = "config", default_value = "")]
    filename: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 解析是否有配置文件
    let cfg = Config::parse();
    let opt = TsuixuqOption::from_config(cfg.filename.as_str())?;
    // 初始化日志subcriber
    opt.init_log()?;

    let mut daemon = Tsuixuqd::new(opt);
    select! {
        result =  daemon.serve() => {
            if let Err(err) = result{
                return Err(anyhow!(err));
            }
        }

        sig = signal::ctrl_c() => {
            eprintln!("recieve signal: {:?}", sig);
            global::cancel()
        }
    }

    Ok(())
}
