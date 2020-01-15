use crossbeam::{channel, thread};
use ring::digest::Digest;
use std::ops::Range;
use crate::hash::Hasher;
use log::debug;
use data_encoding::HEXUPPER;

pub fn par_hasher(max_id: u64, pepper: &str) {
    let thread_count = num_cpus::get() as u64;
    let chunks = partition_chunks(max_id, thread_count);

    debug!(
        "Using {} chunks of size {}",
        chunks.len(),
        chunks.get(0).map(|r| r.end - r.start).unwrap_or_default()
    );

    let (tx, rx) = channel::unbounded();

    thread::scope(|s| {
        for chunk in chunks {
            let tx = tx.clone();
            s.spawn(move |_| {
                let start = chunk.start;
                let end = chunk.end;
                debug!("processing chunk {} - {}", start, end);
                let hasher = Hasher::new(&pepper, chunk);
                for (id, digest) in hasher {
                    tx.send(Job::Output(id, digest)).unwrap();
                }
                tx.send(Job::Done).unwrap();
                debug!("done with chunk {} - {}", start, end);
            });
        }
    }).unwrap();

    let mut counter = thread_count;
    while counter > 0 {
        if let Some(message) = rx.recv().ok() {
            match message {
                Job::Output(id, digest) => output_range(id, digest),
                Job::Done => {
                    counter -= 1;
                }
            }
        }
    }

    debug!("done");
}

pub fn output_range(n: u64, digest: Digest) {
    let hash = HEXUPPER.encode(digest.as_ref());
    println!("{}\t{}", n, hash);
}

enum Job {
    Output(u64, Digest),
    Done,
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
