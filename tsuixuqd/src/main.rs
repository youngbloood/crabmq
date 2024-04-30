use anyhow::{anyhow, Result};
use common::global;
use core::{tsuixuq::TsuixuqOption, tsuixuqd::Tsuixuqd};
use tokio::{select, signal};
use tracing::Level;

#[tokio::main]
async fn main() -> Result<()> {
    let subcriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subcriber)?;

    let opt = TsuixuqOption::new();
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
