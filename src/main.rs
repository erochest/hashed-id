use env_logger;
use log::debug;
use structopt::StructOpt;
use rayon::prelude::*;
use ring::digest::{Context, Digest, SHA256};
use data_encoding::HEXUPPER;

mod error;

use error::Result;

fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::from_args();

    debug!("Generating rainbow table with pepper value = '{}'", args.pepper);

    let max_id = 10u64.pow(args.digits);
    let data = (0..max_id)
        .map(|n| (n, format!("{:10}+{}", n, &args.pepper)))
        .collect::<Vec<_>>();

    data
        .par_iter()
        .map(|(n, input)| {
            let mut context = Context::new(&SHA256);
            context.update(input.as_bytes());
            let digest = context.finish();
            let hash = HEXUPPER.encode(digest.as_ref());
            (n, hash)
        })
        .for_each(|(n, hash)| println!("{}\t{}", n, hash));

    Ok(())
}

#[derive(Debug, StructOpt)]
struct Cli {
    /// Number of digits.
    #[structopt(name = "DIGITS", short = "n", long = "digits", default_value = "10")]
    digits: u32,

    /// Value to use for the pepper.
    #[structopt(name = "PEPPER")]
    pepper: String,
}

fn sha256_digest(n: u64, pepper: &str) -> Digest {
    let mut context = Context::new(&SHA256);
    let data = format!("{:10}+{}", n, pepper);
    context.update(&data.as_bytes());
    context.finish()
}

fn hash_id<R: AsRef<str>>(n: u64, pepper: R) -> (u64, String) {
    let digest = sha256_digest(n, pepper.as_ref());
    let hash = HEXUPPER.encode(digest.as_ref());
    (n, hash)
}
