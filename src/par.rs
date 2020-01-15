use crate::hash::Hasher;
use crossbeam::{channel, thread};
use crossbeam_channel::{Receiver, Sender};
use data_encoding::HEXUPPER;
use log::debug;
use ring::digest::Digest;
use std::borrow::Cow;
use std::ops::Range;

pub fn par_hasher(max_id: u64, pepper: &str, job_count: usize, output_chunk_size: usize) {
    let worker_count = num_cpus::get();
    let job_count = if job_count == 0 {
        worker_count
    } else {
        job_count
    };
    let jobs = partition_jobs(max_id, job_count);
    let pepper = Cow::from(String::from(pepper));

    debug!(
        "Using {} chunks of size {}",
        jobs.len(),
        jobs.get(0).map(|r| r.end - r.start).unwrap_or_default()
    );

    let (input_tx, input_rx) = channel::unbounded();
    let (output_tx, output_rx) = channel::unbounded();

    thread::scope(|s| {
        process_output(&s, output_rx, worker_count);
        spawn_chunks(
            &s,
            input_rx,
            output_tx,
            &pepper,
            worker_count,
            output_chunk_size,
        );
        queue_jobs(input_tx, jobs, worker_count);
    })
    .unwrap();

    debug!("done");
}

fn process_output(s: &thread::Scope, rx: Receiver<Output>, worker_count: usize) {
    s.spawn(move |_| {
        let mut counter = worker_count as u64;
        while counter > 0 {
            if let Some(message) = rx.recv().ok() {
                match message {
                    Output::BulkOutput(output) => {
                        print!("{}", output);
                    }
                    Output::Done => {
                        counter -= 1;
                    }
                }
            }
        }
    });
}

fn spawn_chunks<'a>(
    s: &thread::Scope<'a>,
    rx: Receiver<Job>,
    tx: Sender<Output>,
    pepper: &Cow<'a, str>,
    worker_count: usize,
    output_chunk_size: usize,
) {
    for n in 0..worker_count {
        let rx = rx.clone();
        let tx = tx.clone();
        let pepper = pepper.clone();

        debug!("spawning worker {}", n);
        s.spawn(move |_| {
            let mut buffer = Vec::with_capacity(output_chunk_size);

            while let Ok(Job::JobRange(job)) = rx.recv() {
                let start = job.start;
                let end = job.end;
                let hasher = Hasher::new(pepper.as_ref(), job);
                buffer.clear();

                debug!("processing chunk {} - {} on worker {}", start, end, n);
                for output in hasher {
                    buffer.push(output);
                    if buffer.len() >= output_chunk_size {
                        tx.send(Output::BulkOutput(buffer_to_output(&buffer)))
                            .unwrap();
                        buffer.clear();
                    }
                }
                tx.send(Output::BulkOutput(buffer_to_output(&buffer)))
                    .unwrap();
                debug!("done with chunk {} - {} on worker {}", start, end, n);
            }

            tx.send(Output::Done).unwrap();
            debug!("worker {} done. exiting.", n);
        });
    }
}

fn queue_jobs(input_tx: Sender<Job>, jobs: Vec<Range<u64>>, worker_count: usize) {
    debug!("queuing {} jobs", jobs.len());
    for job in jobs {
        input_tx.send(Job::JobRange(job)).unwrap();
    }
    for _ in 0..worker_count {
        input_tx.send(Job::Done).unwrap();
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
