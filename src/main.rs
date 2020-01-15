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
            print!("{}", par::format_range(id, &digest));
        }
    } else {
        par::par_hasher(max_id, &pepper, args.chunk_count, args.output_buffer_size);
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

    /// Number of chunks if running in parallel. 0 = number of CPUs.
    #[structopt(short = "c", long = "chunk-count", default_value = "0")]
    chunk_count: usize,

    /// Number of lines to buffer when running in parallel.
    #[structopt(short = "b", long = "buffer-size", default_value = "1024")]
    output_buffer_size: usize,

    /// Value to use for the pepper.
    #[structopt(name = "PEPPER")]
    pepper: String,
}
