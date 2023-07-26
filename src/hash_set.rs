use std::{collections::BTreeMap, mem};

use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use sbbf_rs_safe::Filter;
use wyhash::wyhash;

use crate::buffer::Buffer;

pub struct HashSet {
    seed: u64,
    data: Data,
    hashes: Vec<u64>,
    offsets: Vec<usize>,
    filter: Filter,
}

impl HashSet {
    pub fn new<'input, Keys>(
        keys: Keys,
        len: usize,
        total_size: usize,
        max_num_tries: usize,
    ) -> Option<Self>
    where
        Keys: Iterator<Item = &'input [u8]>,
    {
        let data = Data::new(keys, len, total_size);
        let mut rand_seed = 0;
        let mut tuples = BTreeMap::<u64, usize>::new();
        let mut filter = Filter::new(8, len);

        'tries: for _ in 0..max_num_tries {
            let seed: u64 = wyhash::wyrng(&mut rand_seed);

            for (offset, key) in data.iter() {
                let hash = wyhash(key, seed);
                if tuples.insert(hash, offset).is_some() {
                    tuples.clear();
                    continue 'tries;
                }
                filter.insert_hash(hash);
            }

            let mut tuples = tuples.into_iter().collect::<Vec<_>>();

            tuples.sort_unstable_by_key(|v| v.0);

            let mut hashes = Vec::with_capacity(tuples.len());
            let mut offsets = Vec::with_capacity(tuples.len());

            for (hash, offset) in tuples.into_iter() {
                hashes.push(hash);
                offsets.push(offset);
            }

            return Some(Self {
                seed,
                data,
                hashes,
                offsets,
                filter,
            });
        }

        None
    }

    pub fn compress(self) -> CompressedHashSet {
        let compressed = compress_prepend_size(self.data.data.as_slice());
        let mut buf = Buffer::new(compressed.len());
        buf.as_mut().copy_from_slice(compressed.as_slice());

        CompressedHashSet {
            seed: self.seed,
            data: Data { data: buf },
            hashes: self.hashes,
            offsets: self.offsets,
            filter: self.filter,
        }
    }
}

struct Data {
    data: Buffer<u8>,
}

impl Data {
    fn new<'input, Items>(items: Items, len: usize, total_size: usize) -> Self
    where
        Items: Iterator<Item = &'input [u8]>,
    {
        let mut data = Buffer::new(total_size + len * mem::size_of::<usize>());

        let mut offset = 0;
        let mut i = 0;
        for item in items {
            let next_offset = offset + item.len() + mem::size_of::<usize>();
            data.as_mut()[offset..offset + mem::size_of::<usize>()]
                .copy_from_slice(item.len().to_ne_bytes().as_slice());
            data.as_mut()[offset + mem::size_of::<usize>()..next_offset].copy_from_slice(item);

            offset = next_offset;
            i += 1;
        }

        assert_eq!(i, len);
        assert_eq!(offset, total_size + len * mem::size_of::<usize>());

        Self { data }
    }

    fn iter(&self) -> DataIter {
        DataIter {
            data: self.data.as_slice(),
            offset: 0,
        }
    }
}

fn get_with_offset(buf: &[u8], offset: usize) -> Option<&[u8]> {
    let base = offset + mem::size_of::<usize>();
    let size = buf.get(offset..base)?;
    let size = usize::from_ne_bytes(size.try_into().unwrap());

    buf.get(base..base + size)
}

struct DataIter<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for DataIter<'a> {
    type Item = (usize, &'a [u8]);

    fn next(&mut self) -> Option<(usize, &'a [u8])> {
        let base = self.offset + mem::size_of::<usize>();
        let size = self.data.get(self.offset..base)?;
        let size = usize::from_ne_bytes(size.try_into().unwrap());

        let offset = self.offset;

        self.offset = base + size;

        Some((offset, self.data.get(base..base + size)?))
    }
}

pub struct CompressedHashSet {
    seed: u64,
    data: Data,
    hashes: Vec<u64>,
    offsets: Vec<usize>,
    filter: Filter,
}

impl CompressedHashSet {
    pub fn contains(&self, key: &[u8]) -> bool {
        let hash = wyhash(key, self.seed);

        if !self.filter.contains_hash(hash) {
            return false;
        }

        let offset = match self.hashes.binary_search(&hash) {
            Ok(idx) => self.offsets[idx],
            Err(_) => return false,
        };

        let data = decompress_size_prepended(self.data.data.as_slice()).unwrap();

        let res = match get_with_offset(data.as_slice(), offset) {
            Some(res) => res,
            None => return false,
        };
        res == key
    }
}
