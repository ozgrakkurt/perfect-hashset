use std::mem;

use anyhow::{anyhow, Result};

use crate::buffer::Buffer;

pub struct HashSet {
    offsets: Buffer<usize>,
    data: Buffer<u8>,
}

impl HashSet {
    pub fn new<'input, Keys>(keys: Keys, len: usize, total_size: usize)
    where
        Keys: Iterator<Item = &'input [u8]>
    {
        let mut offsets = Buffer::new(len);
        let mut data = Buffer::new(total_size);

        let mut offset = 0;
        let mut i = 0;
        for key in keys {
            let next_offset = offset+key.len();
            data.as_mut()[offset..next_offset].copy_from_slice(key);
            offsets.as_mut()[i] = offset;

            offset = next_offset;
            i += 1;
        }

        assert_eq!(i, len);
        assert_eq!(offset, total_size);

        todo!()
    }

    pub fn insert(&mut self, key: &[u8]) -> bool {
        todo!()
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        todo!()
    }
}
