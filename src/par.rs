use crossbeam::{channel, thread};
use ring::digest::Digest;
use std::ops::Range;
use crate::hash::Hasher;
use log::debug;
use data_encoding::HEXUPPER;
use crossbeam_channel::{Receiver, Sender};

pub fn par_hasher(max_id: u64, pepper: &str, job_count: usize, output_chunk_size: usize) {
    let worker_count = num_cpus::get();
    let job_count = if job_count == 0 {
        worker_count
    } else {
        job_count
    };
    let chunks = partition_jobs(max_id, job_count);

    debug!(
        "Using {} chunks of size {}",
        chunks.len(),
        chunks.get(0).map(|r| r.end - r.start).unwrap_or_default()
    );

    let (input_tx, input_rx) = channel::unbounded();
    let (output_tx, output_rx) = channel::unbounded();

    spawn_chunks(input_rx, output_tx, pepper, chunks, output_chunk_size);
    process_output(output_rx, job_count);

    debug!("done");
}

fn spawn_chunks(rx: Receiver<Job>, tx: Sender<Output>, pepper: &str, jobs: Vec<Range<u64>>, output_chunk_size: usize) {
    // TODO: these should read from an input channel. one thread per CPU, but possibly more chunks
    thread::scope(|s| {
        for job in jobs {
            let tx = tx.clone();
            s.spawn(move |_| {
                let start = job.start;
                let end = job.end;
                let hasher = Hasher::new(&pepper, job);
                let mut buffer = Vec::with_capacity(output_chunk_size);

                debug!("processing chunk {} - {}", start, end);
                for output in hasher {
                    buffer.push(output);
                    if buffer.len() >= 1024 {
                        tx.send(Output::BulkOutput(buffer_to_output(&buffer))).unwrap();
                        buffer.clear();
                    }
                }
                tx.send(Output::BulkOutput(buffer_to_output(&buffer))).unwrap();
                tx.send(Output::Done).unwrap();
                debug!("done with chunk {} - {}", start, end);
            });
        }
    }).unwrap();
}

fn process_output(rx: Receiver<Output>, chunk_count: usize) {
    let mut counter = chunk_count as u64;
    while counter > 0 {
        if let Some(message) = rx.recv().ok() {
            match message {
                Output::BulkOutput(output) => {
                    print!("{}", output);
                },
                Output::Done => {
                    counter -= 1;
                },
            }
        }
    }
}

pub fn buffer_to_output(buffer: &Vec<(u64, Digest)>) -> String {
    let mut output = String::with_capacity(buffer.len() * (10 + 64 + 2));

    for (id, digest) in buffer {
        output += &format_range(*id, digest);
    }

    output
}

pub fn format_range(n: u64, digest: &Digest) -> String {
    let hash = HEXUPPER.encode(digest.as_ref());
    format!("{}\t{}\n", n, hash)
}

enum Job {
    JobRange(Range<u64>),
    Done,
}

enum Output {
    BulkOutput(String),
    Done,
}

/// Use a collection of ranges to implement the data to be processed broken into jobs to match
/// the number of cores in a memory-friendly way.
fn partition_jobs(max_value: u64, job_count: usize) -> Vec<Range<u64>> {
    let mut chunks = Vec::with_capacity(job_count);
    let job_count = job_count as u64;
    let job_size = max_value / job_count;
    let remainder = max_value % job_count;

    for i in 0..job_count {
        chunks.push((i * job_size)..((i + 1) * job_size));
    }

    if remainder != 0 {
        chunks.push((job_count * job_size)..max_value);
    }

    chunks
}

#[cfg(test)]
mod test {
    mod partition_jobs {
        use super::super::*;
        use spectral::prelude::*;

        #[test]
        fn test_partitions_evenly() {
            let chunks = partition_jobs(12, 3);
            assert_that(&chunks).has_length(3);
        }

        #[test]
        fn test_partitions_returns_ranges() {
            let chunks = partition_jobs(12, 3);
            assert_that(&chunks[0]).is_equal_to(&(0..4));
            assert_that(&chunks[1]).is_equal_to(&(4..8));
            assert_that(&chunks[2]).is_equal_to(&(8..12));
        }

        #[test]
        fn test_partitions_oddly() {
            let chunks = partition_jobs(13, 3);
            assert_that(&chunks).has_length(4);
        }

        #[test]
        fn test_partition_return_right_last_range() {
            let chunks = partition_jobs(13, 3);
            assert_that(&chunks[3]).is_equal_to(&(12..13));
        }
    }
}
