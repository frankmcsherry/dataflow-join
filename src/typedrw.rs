use std::mem;
use core::raw::Slice as RawSlice;
use mmap::MapOption::{MapReadable, MapFd};
use mmap::MemoryMap;
use std::os::unix::prelude::AsRawFd;
use core::ops;
use std::fs::File;
use core::marker::PhantomData;

pub struct TypedMemoryMap<T:Copy> {
    map:    MemoryMap,      // mapped file
    len:    usize,          // in bytes (needed because map extends to full block)
    phn:    PhantomData<T>,
}

impl<T:Copy> TypedMemoryMap<T> {
    pub fn new(filename: String) -> TypedMemoryMap<T> {
        let file = File::open(filename).unwrap();
        let size = file.metadata().unwrap().len() as usize;
        TypedMemoryMap {
            map: MemoryMap::new(size, &[MapReadable, MapFd(file.as_raw_fd())]).unwrap(),
            len: size,
            phn: PhantomData,
        }
    }
}

impl<T:Copy> ops::Index<ops::RangeFull> for TypedMemoryMap<T> {
    type Output = [T];
    #[inline]
    fn index(&self, _index: ops::RangeFull) -> &[T] {
        assert!(self.len <= self.map.len());
        unsafe { mem::transmute(RawSlice {
            data: self.map.data() as *const u8,
            len: self.len / mem::size_of::<T>(),
        })}
    }
}
