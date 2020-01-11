use data_encoding::HEXUPPER;
use ring::digest::{Context, SHA256};
use std::borrow::Cow;
use std::fmt::Write;
use std::iter::Iterator;
use std::ops::Range;

pub struct Hasher<'a> {
    pepper: Cow<'a, [u8]>,
    chunk: Range<u64>,
    buffer: String,
}

impl<'a> Hasher<'a> {
    pub fn new(pepper: &'a [u8], chunk: Range<u64>) -> Self {
        Hasher {
            pepper: Cow::from(pepper),
            chunk,
            buffer: String::with_capacity(10),
        }
    }
}

impl<'a> Iterator for Hasher<'a> {
    type Item = (u64, String);

    fn next(&mut self) -> Option<(u64, String)> {
        self.chunk.next().map(|id| {
            let mut context = Context::new(&SHA256);

            self.buffer.clear();
            write!(&mut self.buffer, "{:10}", id).unwrap();

            context.update(self.buffer.as_bytes());
            context.update(b"+");
            context.update(&self.pepper);

            let digest = context.finish();
            let hash = HEXUPPER.encode(digest.as_ref());

            (id, hash)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunk.size_hint()
    }
}
