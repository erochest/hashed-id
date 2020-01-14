use ring::digest::{digest, Digest, SHA256};
use std::iter::Iterator;
use std::ops::Range;

pub struct Hasher {
    chunk: Range<u64>,
    buffer: Vec<u8>,
}

impl Hasher {
    pub fn new(pepper: &String, chunk: Range<u64>) -> Self {
        let mut buffer = Vec::with_capacity(11 + pepper.len());
        buffer.extend("0000000000+".as_bytes());
        buffer.extend(pepper.as_bytes());
        Hasher { chunk, buffer }
    }
}

impl Iterator for Hasher {
    type Item = (u64, Digest);

    fn next(&mut self) -> Option<(u64, Digest)> {
        self.chunk.next().map(|id| {
            format_id(&mut self.buffer, id, 10);
            let digest = digest(&SHA256, &self.buffer);
            (id, digest)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunk.size_hint()
    }
}

fn format_id(buffer: &mut Vec<u8>, n: u64, width: usize) {
    (0..width).for_each(|i| {
        // Adding the code point for '0' to the code digit's value
        let code = 48 + ((n / 10u64.pow(i as u32)) % 10) as u8;
        buffer[width - i - 1] = code;
    });
}

#[cfg(test)]
mod test {
    use super::*;
    use spectral::prelude::*;

    fn format_buffer() -> String {
        let mut buffer = Vec::from("00000+yaddayadda".as_bytes());
        format_id(&mut buffer, 42, 5);
        let data = String::from_utf8(buffer).unwrap();
        data
    }

    #[test]
    fn test_format_id_does_not_change_buffer_size() {
        let mut buffer = Vec::from("00000+yaddayadda".as_bytes());
        format_id(&mut buffer, 42, 5);
        assert_that(&buffer).has_length(16);
    }

    #[test]
    fn test_format_id_does_not_change_suffix() {
        let buffer = format_buffer();
        assert_that(&buffer).ends_with("+yaddayadda");
    }

    #[test]
    fn test_format_id_inserts_the_number() {
        let buffer = format_buffer();
        assert_that(&buffer).starts_with("00042+");
    }
}
