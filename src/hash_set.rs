use std::mem;

use bit_vec::BitVec;
use rand::Rng;

use crate::buffer::Buffer;

pub struct HashSet {
    seed: u64,
    data: Data,
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
        let mut data = Data::new(keys, len, total_size);

        let mut occupancy_vec = BitVec::from_elem(len, false);

        let mut rng = rand::thread_rng();
        'tries: for _ in 0..max_num_tries {
            let seed: u64 = rng.gen();

            for key in data.iter() {
                let idx = key_to_index(key, seed, len);

                if occupancy_vec.get(idx).unwrap() {
                    occupancy_vec.clear();
                    continue 'tries;
                }

                occupancy_vec.set(idx, true);
            }

            for i in 0..len {
                let key = unsafe { data.get_unchecked(i) };
                let idx = key_to_index(key, seed, len);
                data.swap(i, idx);
            }

            return Some(Self { seed, data });
        }

        None
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let index = key_to_index(key, self.seed, self.data.len());
        // This is safe because we do fast modulo operation with self.data.len(), it will never reach outside the buffer.
        let k = unsafe { self.data.get_unchecked(index) };
        k == key
    }
}

fn key_to_index(key: &[u8], seed: u64, len: usize) -> usize {
    // This assumes usize fits in u64, which might not be true for some systems.
    // TODO: Add a static assertion for this.
    fastrange_rs::fastrange_64(wyhash::wyhash(key, seed), len as u64) as usize
}

struct Data {
    offsets: Buffer<usize>,
    data: Buffer<u8>,
}

impl Data {
    fn new<'input, Items>(items: Items, len: usize, total_size: usize) -> Self
    where
        Items: Iterator<Item = &'input [u8]>,
    {
        let mut offsets: Buffer<usize> = Buffer::new(len);
        let mut data = Buffer::new(total_size + len * mem::size_of::<usize>());

        let mut offset = 0;
        let mut i = 0;
        for item in items {
            let next_offset = offset + item.len() + mem::size_of::<usize>();
            data.as_mut()[offset..offset + mem::size_of::<usize>()]
                .copy_from_slice(item.len().to_ne_bytes().as_slice());
            data.as_mut()[offset + mem::size_of::<usize>()..next_offset].copy_from_slice(item);

            offsets.as_mut()[i] = offset;

            offset = next_offset;
            i += 1;
        }

        assert_eq!(i, len);
        assert_eq!(offset, total_size + len * mem::size_of::<usize>());

        Self { offsets, data }
    }

    unsafe fn get_unchecked(&self, index: usize) -> &[u8] {
        let offset = *self.offsets.as_slice().get_unchecked(index);
        let base = offset + mem::size_of::<usize>();
        let size = self.data.as_slice().get_unchecked(offset..base);
        let size = usize::from_ne_bytes(size.try_into().unwrap());

        self.data.as_slice().get_unchecked(base..base + size)
    }

    fn swap(&mut self, left: usize, right: usize) {
        self.offsets.as_mut().swap(left, right)
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn iter(&self) -> DataIter {
        DataIter {
            data: self.data.as_slice(),
            offset: 0,
        }
    }
}

struct DataIter<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for DataIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        let base = self.offset + mem::size_of::<usize>();
        let size = self.data.get(self.offset..base)?;
        let size = usize::from_ne_bytes(size.try_into().unwrap());

        self.offset = base + size;

        self.data.get(base..base + size)
    }
}
