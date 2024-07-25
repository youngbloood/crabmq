use anyhow::{anyhow, Result};
use clap::Parser;
use common::global::{self, Guard};
use core::config::Config;
use core::crabd::CrabMQD;
use tokio::{select, signal};

#[derive(Parser, Debug)]
pub struct ConfigFile {
    #[arg(short = 'c', long = "config", default_value = "")]
    filename: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 解析是否有配置文件
    let cfg = ConfigFile::parse();
    let opt = Config::from_config(cfg.filename.as_str())?;
    // 初始化日志subcriber
    opt.init_log()?;

    let mut daemon = CrabMQD::new(Guard::new(opt))?;
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
