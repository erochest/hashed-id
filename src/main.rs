use env_logger;
use log::debug;
use structopt::StructOpt;

mod error;
mod hash;
mod par;

use error::Result;

fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::from_args();

    let pepper = args.pepper;
    let max_id = 10u64.pow(args.digits);

    debug!("Generating rainbow table with pepper value = '{}'", &pepper);

    if args.serial {
        for (id, digest) in hash::Hasher::new(&pepper, 0..max_id) {
            par::output_range(id, digest);
        }
    } else {
        par::par_hasher(max_id, &pepper);
    }

    Ok(())
}

#[derive(Debug, StructOpt)]
struct Cli {
    /// Number of digits.
    #[structopt(name = "DIGITS", short = "n", long = "digits", default_value = "10")]
    digits: u32,

    /// Run serially?
    #[structopt(short = "s", long = "serial")]
    serial: bool,

    /// Value to use for the pepper.
    #[structopt(name = "PEPPER")]
    pepper: String,
}
