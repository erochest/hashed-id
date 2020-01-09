use data_encoding::HEXUPPER;
use env_logger;
use log::debug;
use rayon::prelude::*;
use ring::digest::{Context, SHA256};
use std::fmt::Write;
use std::ops::Range;
use structopt::StructOpt;

mod error;

use error::Result;

fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::from_args();

    let pepper = args.pepper.as_bytes();
    let max_id = 10u64.pow(args.digits);
    let thread_count = rayon::current_num_threads() as u64;
    let mut chunks = partition_chunks(max_id, thread_count);

    debug!(
        "Generating rainbow table with pepper value = '{}'",
        args.pepper
    );
    debug!(
        "Using {} chunks of size {}",
        chunks.len(),
        chunks.get(0).map(|r| r.end - r.start).unwrap_or_default()
    );

    chunks
        .par_iter_mut()
        .map(|chunk| {
            let mut buffer = String::with_capacity(10);
            let mut output = String::new();

            for id in chunk {
                let mut context = Context::new(&SHA256);

                buffer.clear();
                write!(&mut buffer, "{:10}", id).unwrap();

                context.update(buffer.as_bytes());
                context.update(b"+");
                context.update(&pepper);

                let digest = context.finish();
                let hash = HEXUPPER.encode(digest.as_ref());

                // TODO: Don't actually care about order, just that they don't get mixed together.
                // Maybe create a separate thread with a channel for handling output.
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

/// Use a collection of ranges to implement the data to be processed broken into jobs to match
/// the number of cores in a memory-friendly way.
fn partition_chunks(max_value: u64, chunk_count: u64) -> Vec<Range<u64>> {
    let mut chunks = Vec::with_capacity(chunk_count as usize);
    let chunk_size = max_value / chunk_count;
    let remainder = max_value % chunk_count;

    for i in 0..chunk_count {
        chunks.push((i * chunk_size)..((i + 1) * chunk_size));
    }

    if remainder != 0 {
        chunks.push((chunk_count * chunk_size)..max_value);
    }

    chunks
}

#[cfg(test)]
mod test {
    mod partition_chunks {
        use super::super::*;
        use spectral::prelude::*;

        #[test]
        fn test_partitions_evenly() {
            let chunks = partition_chunks(12, 3);
            assert_that(&chunks).has_length(3);
        }

        #[test]
        fn test_partitions_returns_ranges() {
            let chunks = partition_chunks(12, 3);
            assert_that(&chunks[0]).is_equal_to(&(0..4));
            assert_that(&chunks[1]).is_equal_to(&(4..8));
            assert_that(&chunks[2]).is_equal_to(&(8..12));
        }

        #[test]
        fn test_partitions_oddly() {
            let chunks = partition_chunks(13, 3);
            assert_that(&chunks).has_length(4);
        }

        #[test]
        fn test_partition_return_right_last_range() {
            let chunks = partition_chunks(13, 3);
            assert_that(&chunks[3]).is_equal_to(&(12..13));
        }
    }
}
