use env_logger;
use log::debug;
use structopt::StructOpt;

mod error;

use error::Result;

fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::from_args();

    debug!("Generating rainbow table with pepper value = '{}'", args.pepper);

    println!("{:?}", args);

    Ok(())
}

#[derive(Debug, StructOpt)]
struct Cli {
    /// Value to use for the pepper.
    #[structopt(name = "PEPPER")]
    pepper: String,
}
