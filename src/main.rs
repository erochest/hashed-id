use env_logger;
use log::debug;
use std::char;
use std::fmt::Write;
use structopt::StructOpt;
use rayon::prelude::*;
use ring::digest::{Context, Digest, SHA256};
use data_encoding::HEXUPPER;

mod error;

use error::Result;

fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::from_args();

    let pepper = args.pepper.as_bytes();
    let max_id = 10u64.pow(args.digits);
    let thread_count = rayon::current_num_threads();
    let chunk_size = max_id as usize / thread_count;
    let data: Box<[u64]> = (0..max_id).collect();

    debug!("Generating rainbow table with pepper value = '{}'", args.pepper);
    debug!("Using chunk size of {}", chunk_size);

    data
        .par_chunks(chunk_size)
        .map(|ids| {
            let mut buffer = String::with_capacity(10);
            let mut output = String::new();

            for id in ids {
                let mut context = Context::new(&SHA256);

                buffer.clear();
                for i in 0..11 {
                    buffer.push(char::from_digit((id / 10u64.pow(i) % 10) as u32, 10).unwrap_or_default());
                }

                context.update(buffer.as_bytes());
                context.update(b"+");
                context.update(&pepper);

                let digest = context.finish();
                let hash = HEXUPPER.encode(digest.as_ref());

                writeln!(&mut output, "{}\t{}", id, hash).unwrap();
            }

            output
        })
        .for_each(|output| print!("{}", output));

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
