use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    marker::PhantomData,
    mem, slice,
};

const ALIGNMENT: usize = 64;

pub(crate) struct Buffer<T> {
    ptr: *mut u8,
    layout: Layout,
    len: usize,
    phantom: PhantomData<T>,
}

impl<T> Buffer<T> {
    pub(crate) fn new(len: usize) -> Self {
        let size = mem::size_of::<T>() * len;

        let padded_size = (size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;

        let layout = Layout::from_size_align(padded_size, ALIGNMENT).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };

        Self {
            layout,
            ptr,
            len,
            phantom: PhantomData,
        }
    }

    pub(crate) fn as_mut(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.ptr as *mut T, self.len) }
    }

    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.ptr as *const T, self.len) }
    }
}

impl<T> Drop for Buffer<T> {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}
