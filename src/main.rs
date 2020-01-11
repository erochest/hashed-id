use env_logger;
use log::debug;
use rayon::prelude::*;
use std::ops::Range;
use structopt::StructOpt;

mod error;
mod hash;

use error::Result;

fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::from_args();

    let pepper = args.pepper.as_bytes();
    let max_id = 10u64.pow(args.digits);

    debug!(
        "Generating rainbow table with pepper value = '{}'",
        args.pepper
    );

    if args.serial {
        for (id, hash) in hash::Hasher::new(pepper, 0..max_id) {
            println!("{}\t{}", id, hash);
        }

    } else {
        let thread_count = rayon::current_num_threads() as u64;
        let chunks = partition_chunks(max_id, thread_count);

        debug!(
            "Using {} chunks of size {}",
            chunks.len(),
            chunks.get(0).map(|r| r.end - r.start).unwrap_or_default()
        );

        chunks
            .into_par_iter()
            .flat_map(|chunk| {
                let hasher = hash::Hasher::new(pepper, chunk);
                hasher.par_bridge()
            })
        .for_each(|(id, hash)| println!("{}\t{}", id, hash));
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
