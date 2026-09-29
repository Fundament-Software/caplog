#![allow(dead_code)]

use bitfield_struct::bitfield;
use eyre::Result;
#[cfg(not(miri))]
use memmap2::MmapMut;
use portable_atomic::AtomicU128;
use std::collections::HashSet;
use std::fs::File;
#[cfg(not(miri))]
use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::mem;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
#[cfg(not(miri))]
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

const MAX_CHILDREN: usize = 32;
const MAX_NODE_SIZE: usize = MAX_CHILDREN + 1;
const MIN_NODE_SIZE: usize = 2; // It's very easy to forget that a node must contain at least 1 value
const MAX_NODE_BYTES: usize = size_of::<[u64; MAX_NODE_SIZE]>();
// Note that page size is not always 4096 but this works nicely as the chunk size for flushing
const PAGE_SIZE: usize = 4096;
const FLUSH_THRESHOLD: usize = 2048;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("Expected memory mapped size of at least {0} but found {1}")]
    MemMapTooSmall(usize, usize),
    #[error("Memory alignment should be aligned to {0} but is off by {1}")]
    InvalidAlignment(usize, usize),
    #[error("Expected file size of at least {0} but found {1}")]
    FileTooSmall(usize, u64),
    #[error("Invalid Trie state! Was this trie not cleanly flushed? (Try using restore)")]
    DirtyTrieState,
    #[error(
        "Requested key is not available (key bits {key_bits:?} at offset {key_offset:?} not found in node {node:?}"
    )]
    NotFound { key_offset: u8, key_bits: u8, node: u64 },
    #[error("Tried to insert key that already exists and has value {0}")]
    AlreadyExists(u64),
    #[error("Ran out of space in backing storage! Storage size: {0}")]
    OutOfMemory(usize),
    #[error("unknown HashedArrayTrie error")]
    Unknown,
    #[error("Can't delete shared node: {0}")]
    Shared(u64),
    #[error("file lock was poisoned, Trie may be in an inconsistent state")]
    LockError,
    #[error("Cannot increment reference counter beyond {0} without risking overflow")]
    TooManyRefs(u64),
}

#[bitfield(u64)]
struct Flags {
    #[bits(32)]
    mask: u32, // Each bit represents a child that exists, and the total number of children is the number of set bits
    #[bits(8)]
    refcount: u8,
    #[bits(1)]
    leaf: bool,
    #[bits(3)]
    skips: u8, // Stores how many skip levels (0-4) we're using in this node
    #[bits(20)]
    skipbits: u32, // stores the skipped bits of the key
}

// Mask for the refcount field above - unfortunately #[bits()] won't accept a constant.
const REFCOUNT_MASK: u64 = 0xFFu64 << 32;
const REFCOUNT_ONE: u64 = 0x1 << 32;

// We have to atomically load most nodes because we usually don't have exclusive access, and doing a non-atomic read on
// two different threads is undefined behavior. However, when we are trying to modify a node, this means we must remember to
// *write back* whatever we changed. This view ensures that happens on drop.
struct FlagView<'a> {
    word: &'a AtomicU64,
    value: u64,
}

impl<'a> FlagView<'a> {
    // We have to atomically load nodes because in multithreaded contexts, another thread could also be walking the tree
    pub fn new(words: &'a [AtomicU64], offset: usize) -> Self {
        let word = &words[offset];
        Self {
            word,
            value: word.load(Ordering::Acquire),
        }
    }

    pub fn flags(&mut self) -> &mut Flags {
        Flags::from_ref_mut(&mut self.value)
    }
}
impl Drop for FlagView<'_> {
    fn drop(&mut self) {
        self.word.store(self.value, Ordering::Release);
    }
}

impl Flags {
    pub fn load(words: &[AtomicU64], offset: usize) -> Self {
        Self(words[offset].load(Ordering::Acquire))
    }

    // If we have a mutable reference, we can directly write to it because no other thread has access.
    pub fn from_ref_mut(target: &mut u64) -> &mut Flags {
        unsafe { &mut *(target as *mut u64).cast::<Flags>() }
    }

    pub fn offset(&self, index: u8) -> usize {
        (((self.0 & 0xFFFFFFFF) << (32 - index)) as u32).count_ones() as usize
    }
    pub fn count(&self) -> usize {
        (self.mask()).count_ones() as usize
    }
    pub fn exists(&self, index: u8) -> bool {
        (self.mask() & (0b1 << index)) != 0
    }
    pub fn append(&mut self, index: u8) {
        assert!(!self.exists(index));
        self.set_mask(self.mask() | (0b1 << index));
    }

    #[inline]
    fn parity_location(&self) -> u8 {
        // We hide the parity either in the skip count or in the skip bits depending on if skip count is 4 or not
        if (self.0 & (1 << 43)) != 0 { 41 } else { 63 }
    }
    #[inline]
    fn calc_parity(&self) -> u32 {
        // Exclude refcount from parity calculation so we can atomically manage them
        (self.0 & !(1 << self.parity_location()) & !REFCOUNT_MASK).count_ones() & 1
    }
    #[inline]
    fn get_parity(&self) -> bool {
        (self.0 & (1 << self.parity_location())) != 0
    }
    #[inline]
    pub fn check_parity(&self) -> bool {
        self.get_parity() == (self.calc_parity() != 0)
    }
    #[inline(never)]
    pub fn set_parity(&mut self) {
        let bit = 1 << self.parity_location();
        let parity = (self.calc_parity() as u64) << self.parity_location();
        self.0 = (self.0 & !bit) | parity;
    }
    pub fn is_valid(&self) -> bool {
        // refcount should be greater than zero
        self.refcount() > 0 &&
        // parity bit should be valid
        self.check_parity() &&
        // should have a nonzero number of children (otherwise it's degenerate)
        self.count() > 0 &&
        // skipbits should be zero for all skiplevels we aren't using
        if (self.skips() & 0b100) != 0 {
            (self.skips() & 0b010) == 0
        } else {
            (self.skipbits() & !(1 << 19) & !((1 << (self.skips() * 5)) - 1)) == 0
        }
    }
}

pub enum NodeResult {
    InPlace,
    Moved(u64),
    Copied(u64),
    Err(Error),
}

#[derive(Debug)]
#[repr(C)]
struct Header {
    freelist: [AtomicU128; MAX_CHILDREN], // Maintains a freelist for all 32 non-leaf node sizes
    root: AtomicU64,                      // This is the primary root
    clean: AtomicU64,                     // Only a 1 bit value but set to a u64 to ensure alignment
}

impl Header {
    pub(crate) fn tag(off: u64, tag: u64) -> u128 {
        (tag as u128) << 64 | off as u128
    }
    pub(crate) fn untag(v: u128) -> (u64, u64) {
        (v as u64, (v >> 64) as u64)
    }

    pub(crate) fn pop(head: &AtomicU128, words: &[AtomicU64]) -> Option<u64> {
        let mut cur = head.load(Ordering::Acquire);
        loop {
            let (offset, tag) = Self::untag(cur);
            if offset == u64::MAX {
                // doesn't point to anything
                return None;
            }
            let next = words[offset as usize].load(Ordering::Relaxed);
            match head.compare_exchange_weak(
                cur,
                Self::tag(next, tag.wrapping_add(1)),
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(offset),
                Err(actual) => cur = actual,
            }
        }
    }

    pub(crate) fn push(head: &AtomicU128, words: &[AtomicU64], offset: u64) {
        let mut cur = head.load(Ordering::Relaxed);
        loop {
            let (curoffset, tag) = Self::untag(cur);
            words[offset as usize].store(curoffset, Ordering::Relaxed);
            match head.compare_exchange_weak(
                cur,
                Self::tag(offset, tag.wrapping_add(1)),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[derive(Debug)]
pub struct Storage {
    handle: Option<File>,
    #[cfg(miri)]
    mapping: RwLock<Vec<u8>>,
    #[cfg(not(miri))]
    mapping: RwLock<MmapMut>,
    // TODO: Maybe replace with data structure that can maintain efficient merged intervals
    //dirty_pages: HashSet<usize>,
}

#[inline]
fn add_ref(node: usize, words: &[AtomicU64]) -> u64 {
    words[node].fetch_add(REFCOUNT_ONE, Ordering::Relaxed)
}

pub struct StorageView<'a>(RwLockReadGuard<'a, MmapMut>);

pub struct StorageViewMut<'a>(std::sync::RwLockWriteGuard<'a, MmapMut>);

impl StorageView<'_> {
    #[inline]
    fn parts(&self) -> (&Header, &[AtomicU64]) {
        (self.header(), self.words())
    }

    #[inline]
    fn words(&self) -> &[AtomicU64] {
        // Build from a raw pointer because sharing slice references is undefined behavior and will make Miri angy
        let p = unsafe { self.0.as_ptr().add(HEADER_BYTES) } as *const AtomicU64;
        unsafe { std::slice::from_raw_parts(p, (self.0.len() - HEADER_BYTES) / 8) }
    }

    #[inline]
    fn header(&self) -> &Header {
        unsafe { &*(self.0.as_ptr() as *const Header) }
    }

    #[inline]
    fn drop_ref(node: usize, w: &[AtomicU64]) -> bool {
        let prev = w[node].fetch_sub(REFCOUNT_ONE, Ordering::Release);
        if Flags::from(prev).refcount() == 1 {
            // This fence is required because it makes all other decrements from other threads visible in this one.
            std::sync::atomic::fence(Ordering::Acquire);
            true
        } else {
            false
        }
    }

    fn release(&self, offset: usize) -> Result<(), Error> {
        let words = self.words();
        if !Self::drop_ref(offset, words) {
            return Ok(());
        }
        let node = Flags::load(words, offset);

        // Recurse into children
        for i in 1..=node.count() {
            if !node.leaf() {
                self.release(words[offset + i].load(Ordering::Relaxed) as usize)?;
            }
        }
        self.free_block(offset)
    }

    fn free_block(&self, offset: usize) -> Result<(), Error> {
        let (header, words) = self.parts();
        let mut count = Flags::load(words, offset).count();

        // This has to be a loop because the atomic stores are mandatory, another thread could touch these while doing an orphan check
        for i in offset + 1..offset + 1 + count {
            words[i].store(0, Ordering::Release); // TODO: might be valid as Relaxed
        }

        // After setting the known cells to zero, walk forward to see if there were any orphaned cells
        while offset + 1 + count < words.len() && words[offset + 1 + count].load(Ordering::Relaxed) == 0 {
            count += 1;
        }

        // This should never happen unless something is corrupted
        assert_ne!(count, 0);
        if count == 0 {
            return Err(Error::DirtyTrieState.into());
        }

        // Add it to the appropriate freelist (0th index has 1 node, so  we use count - 1)
        Header::push(&header.freelist[count - 1], words, offset as u64);
        Ok(())
    }

    pub fn allocate(&self, count: usize) -> Result<u64, Error> {
        assert!(count > 0);
        assert!(count <= MAX_CHILDREN);
        let (header, words) = self.parts();
        let freelist = &header.freelist;
        for i in (count - 1)..MAX_CHILDREN {
            let word = freelist[i].load(Ordering::Relaxed) as u64;
            if word != u64::MAX {
                let Some(result) = Header::pop(&freelist[i], words) else {
                    continue;
                };

                // i is in terms of the index, which is the true size - 2 (MIN_NODE_SIZE), whereas count is the
                // true size - 1. This is in true word size because we need at least 2 words to put in 1 node.
                let remainder = (i + 2) - (count + 1);

                // If we have more than MIN_NODE_SIZE leftover, assign it to different freelist instead of wasting it
                if remainder >= MIN_NODE_SIZE {
                    // However, now that remainder is in the true word size, we have to subtract MIN_NODE_SIZE
                    // to get to the actual freelist index!
                    Header::push(
                        &header.freelist[remainder - MIN_NODE_SIZE],
                        words,
                        result + count as u64 + 1,
                    );
                }
                return Ok(result);
            }
        }
        Err(Error::OutOfMemory(self.0.len()).into())
    }
}

impl StorageViewMut<'_> {
    #[inline]
    fn parts(&mut self) -> (&mut Header, &mut [u64]) {
        let p = unsafe { self.0.as_mut_ptr().add(HEADER_BYTES) } as *mut AtomicU64;
        let words = unsafe { std::slice::from_raw_parts_mut(p as *mut u64, (self.0.len() - HEADER_BYTES) / 8) };

        (unsafe { &mut *(self.0.as_mut_ptr() as *mut Header) }, words)
    }

    #[inline]
    fn words(&mut self) -> &mut [u64] {
        let p = unsafe { self.0.as_mut_ptr().add(HEADER_BYTES) } as *mut AtomicU64;
        unsafe { std::slice::from_raw_parts_mut(p as *mut u64, (self.0.len() - HEADER_BYTES) / 8) }
    }

    #[inline]
    fn header(&mut self) -> &mut Header {
        unsafe { &mut *(self.0.as_mut_ptr() as *mut Header) }
    }
}

const HEADER_BYTES: usize = size_of::<Header>();

impl Storage {
    pub fn view(&self) -> StorageView<'_> {
        StorageView(self.mapping.read().unwrap())
    }

    pub fn view_mut(&mut self) -> StorageViewMut<'_> {
        StorageViewMut(self.mapping.write().unwrap())
    }

    unsafe fn init_self(&mut self) -> Result<()> {
        {
            let mut map = self.mapping.write().map_err(|_| Error::LockError)?;
            // We do this alignment check once, and the rest of the time we do the unguarded direct mutation.
            let (prefix, slice, tail) = unsafe { &mut map[HEADER_BYTES..].align_to_mut::<u64>() };

            if !prefix.is_empty() {
                return Err(Error::InvalidAlignment(size_of::<u64>(), prefix.len()).into());
            } else if !tail.is_empty() {
                return Err(Error::InvalidAlignment(size_of::<u64>(), tail.len()).into());
            }

            // Initialize our canary integer at offset 0, which is always invalid.
            slice[0] = u64::MAX;
            // Initialize the root node as empty, ensuring the parity bit is set
            let mut root = Flags::new().with_refcount(1);
            root.set_parity();
            slice[1] = root.into();
        }

        {
            let mut view = self.view_mut();
            let header = view.header();

            // Initialize freelist and dirty clean value
            header.freelist.iter_mut().for_each(|x| *x.get_mut() = u128::MAX);
            *header.clean.get_mut() = 0;
            *header.root.get_mut() = 1;
        }

        // If init section fails, we will leave a file full of zeros, which is okay because that's considered invalid
        // anyway.

        let map = self.mapping.get_mut().map_err(|_| Error::LockError)?;
        let len = map.len();
        Self::init_section(map, HEADER_BYTES + MAX_NODE_BYTES + size_of::<u64>(), len)?;

        // flush our clean value of 0 so we can detect if we aren't closed properly
        #[cfg(not(miri))]
        self.mapping
            .write()
            .map_err(|_| Error::LockError)?
            .flush_range(0, HEADER_BYTES)?;

        Ok(())
    }

    #[cfg(not(miri))]
    unsafe fn new_inner(handle: Option<File>, mapping: MmapMut) -> Result<Storage> {
        if mapping.len() < HEADER_BYTES {
            return Err(Error::MemMapTooSmall(HEADER_BYTES, mapping.len()).into());
        }

        let mut result = Storage {
            handle,
            mapping: mapping.into(),
            //dirty_pages: HashSet::new(),
        };

        unsafe { result.init_self() }?;
        Ok(result)
    }

    #[cfg(not(miri))]
    pub fn new_file(src: File, new_size: u64) -> Result<Storage> {
        assert_eq!(
            new_size % size_of::<u64>() as u64,
            0,
            "size MUST be a multiple of an unsigned 64-bit integer"
        );

        // Create a new file with enough room for the header, a canary word, and then one maximum size node
        src.set_len((HEADER_BYTES + size_of::<u64>() + MAX_NODE_BYTES) as u64 + new_size)?;
        unsafe {
            let mapping = MmapMut::map_mut(&src)?;
            Self::new_inner(Some(src), mapping)
        }
    }

    #[cfg(not(miri))]
    pub fn new_ref(src: &File, new_size: u64) -> Result<Storage> {
        assert_eq!(
            new_size % size_of::<u64>() as u64,
            0,
            "size MUST be a multiple of an unsigned 64-bit integer"
        );

        // Create a new file with enough room for the header, a canary word, and then one maximum size node
        src.set_len((HEADER_BYTES + size_of::<u64>() + MAX_NODE_BYTES) as u64 + new_size)?;
        unsafe { Self::new_inner(None, MmapMut::map_mut(src)?) }
    }

    #[cfg(not(miri))]
    pub fn new(path: &Path, new_size: u64) -> Result<Storage> {
        let src = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Self::new_file(src, new_size)
    }

    #[cfg(miri)]
    pub fn new(_: &Path, new_size: u64) -> Result<Storage> {
        unsafe {
            let mut aligned = Vec::<u64>::new();
            aligned.resize(
                (HEADER_BYTES + size_of::<u64>() + MAX_NODE_BYTES + new_size as usize) / size_of::<u64>(),
                0,
            );
            let alignlen = aligned.len();
            let aligncap = aligned.capacity();
            let mapping = Vec::from_raw_parts(
                aligned.leak().as_mut_ptr() as *mut u8,
                alignlen * size_of::<u64>(),
                aligncap * size_of::<u64>(),
            );

            let mut result = Storage {
                handle: None,
                mapping: Some(mapping),
                //dirty_pages: HashSet::new(),
            };

            result.init_self()?;
            return Ok(result);
        }
    }

    // Initializes a new section at the given byte offset by adding it to the freelist. We do this in reverse order so
    // the freelist doesn't try to fill up the new section backwards.
    fn init_section(mapping: &mut MmapMut, byte_offset: usize, byte_end: usize) -> Result<()> {
        unsafe {
            let ptr = mapping.as_mut_ptr();
            let unaligned: &mut [u8] = &mut mapping[byte_offset..byte_end];
            let header = &mut *(ptr as *mut Header);
            let (prefix, slice, _) = unaligned.align_to_mut::<u64>();
            if !prefix.is_empty() {
                // If this happens, then there is a high chance that self.mapping itself is not u64 aligned, which
                // is very bad.
                return Err(Error::InvalidAlignment(size_of::<u64>(), prefix.len()).into());
            }

            // First we get the head and add it to the proper freelist, if there is one
            let mut word_offset = (byte_offset - HEADER_BYTES) / size_of::<u64>();
            let headsize = slice.len() % MAX_NODE_SIZE;
            let node_aligned = if headsize >= MIN_NODE_SIZE {
                let target = header.freelist[headsize - MIN_NODE_SIZE].get_mut();
                slice[0] = *target as u64;
                *target = Header::tag(word_offset as u64, 0);
                word_offset += headsize;
                &mut slice[headsize..]
            } else {
                let count = slice.len();
                // 1 extra u64 isn't big enough to put in our freelists so we just ignore it
                &mut slice[..count - headsize]
            };

            assert_eq!(node_aligned.len() % MAX_NODE_SIZE, 0);

            // Then we add the remaining max sized nodes to the freelist in reverse
            for i in (0..node_aligned.len()).step_by(MAX_NODE_SIZE).rev() {
                let target = header.freelist[MAX_NODE_SIZE - MIN_NODE_SIZE].get_mut();
                node_aligned[i] = *target as u64;
                *target = Header::tag((word_offset + i) as u64, 0);
            }
        }
        Ok(())
    }

    //#[cfg(not(miri))]
    pub fn load_file(src: File) -> Result<Storage> {
        let fsize = src.metadata()?.len();
        if fsize < HEADER_BYTES as u64 {
            return Err(Error::FileTooSmall(HEADER_BYTES, fsize).into());
        }

        unsafe {
            let mapping = MmapMut::map_mut(&src)?;
            if mapping.len() < HEADER_BYTES {
                return Err(Error::MemMapTooSmall(HEADER_BYTES, mapping.len()).into());
            }
            let mut storage = Storage {
                mapping: mapping.into(),
                handle: Some(src),
                //dirty_pages: HashSet::new(),
            };

            {
                let slice: &[u8] = &storage.mapping.read().map_err(|_| Error::LockError)?[..HEADER_BYTES];
                let prefix = slice.align_to::<u64>().0;
                if !prefix.is_empty() {
                    return Err(Error::InvalidAlignment(size_of::<u64>(), prefix.len()).into());
                }
            }
            {
                let mut view = storage.view_mut();
                let header = view.header();
                if *header.clean.get_mut() == 0 {
                    return Err(Error::DirtyTrieState.into());
                }

                // Set our clean value to 0 and flush so we can detect if we aren't closed properly
                *header.clean.get_mut() = 0;
            }
            storage
                .mapping
                .read()
                .map_err(|_| Error::LockError)?
                .flush_range(0, HEADER_BYTES)?;
            Ok(storage)
        }
    }

    #[cfg(not(miri))]
    // Load a file - if it doesn't already exist or is invalid, returns an error
    pub fn load(path: &Path) -> Result<Storage> {
        Self::load_file(OpenOptions::new().read(true).write(true).create(false).open(path)?)
    }

    fn scan_valid_nodes(offset: u64, words: &mut [u64], valid: &mut HashSet<u64>) -> bool {
        let total_len = words.len() as u64;
        if offset >= total_len {
            return false;
        }

        let offset = offset as usize;
        // Ensure this node is consistent.
        let node = Flags::from_ref_mut(&mut words[offset]);

        if !node.is_valid() {
            return false;
        }

        if !node.leaf() {
            // Check children for consistency. Remove any that have been corrupted.
            let mut count = 0;
            let mut valid_children = 0;
            let mut children = [0; MAX_CHILDREN];
            let mut mask = node.mask();
            for i in 0..MAX_CHILDREN {
                if (mask & (1 << i)) != 0 {
                    if Self::scan_valid_nodes(words[offset + 1 + count], words, valid) {
                        children[valid_children] = words[offset + 1 + count];
                        valid_children += 1;
                    } else {
                        mask &= !(1 << i);
                    }
                    count += 1;
                }
            }

            if valid_children == 0 {
                return false;
            }

            // If we had some invalid children, we reconstruct the node with the valid ones
            if valid_children != count {
                words[offset + 1..(offset + 1 + count)].fill(0);
                words[offset + 1..(offset + 1 + valid_children)].copy_from_slice(&children[..valid_children]);
            }

            assert_eq!(valid_children as u32, mask.count_ones());

            let node = Flags::from_ref_mut(&mut words[offset]);
            node.set_mask(mask);
            node.set_parity();

            for i in 0..=node.count() {
                valid.insert((offset + i) as u64);
            }
        } else if offset as u64 + node.count() as u64 >= total_len {
            return false;
        } else {
            for i in 0..=node.count() {
                valid.insert((offset + i) as u64);
            }
        }

        true
    }

    unsafe fn restore_inner(storage: &mut Storage) -> Result<()> {
        {
            let slice: &[u8] = &storage.mapping.read().map_err(|_| Error::LockError)?[..HEADER_BYTES];
            let prefix = unsafe { slice.align_to::<u64>().0 };
            if !prefix.is_empty() {
                return Err(Error::InvalidAlignment(size_of::<u64>(), prefix.len()).into());
            }
        }
        // Clean the header out, setting root to 1 if it's invalid and wiping the freelist.
        let mut view = storage.view_mut();
        let (header, words) = view.parts();
        *header.clean.get_mut() = 0;
        if *header.root.get_mut() == 0 || *header.root.get_mut() as usize > words.len() {
            *header.root.get_mut() = 1;
        }
        header.freelist.iter_mut().for_each(|x| *x.get_mut() = u128::MAX);

        let mut validnodes = HashSet::new();
        Self::scan_valid_nodes(*header.root.get_mut(), words, &mut validnodes);

        words[0] = u64::MAX;
        let mut count = 0;
        // Now we reconstruct the freelists by scanning the entire file in reverse.
        for i in (34..words.len() as u64).rev() {
            let valid = validnodes.contains(&i);
            let target = if !valid {
                count += 1;
                i
            } else {
                i + 1
            };

            if valid || count >= MAX_NODE_SIZE {
                assert!(count <= MAX_NODE_SIZE);
                if count >= MIN_NODE_SIZE {
                    let word = header.freelist[count - MIN_NODE_SIZE].get_mut();
                    words[target as usize] = *word as u64;
                    *word = Header::tag(target, 0);
                }

                count = 0;
            }
        }

        Ok(())
    }

    #[cfg(not(miri))]
    // Attempts to reconstruct a file that was not cleanly flushed.
    pub fn restore_file(src: File) -> Result<Storage> {
        // We create a hashmap of all reachable, valid nodes starting from the root. Because all our freelists have at
        // least one 0 following them, no valid node should ever have zero children, and no legitimate offset
        // should ever point at 0, so an invalid section is anything pointing to 0, or pointing to a non-leaf
        // node followed by a zero.

        let fsize = src.metadata()?.len();
        if fsize < HEADER_BYTES as u64 {
            return Err(Error::FileTooSmall(HEADER_BYTES, fsize).into());
        }

        unsafe {
            let mapping = MmapMut::map_mut(&src)?;
            if mapping.len() < HEADER_BYTES {
                return Err(Error::MemMapTooSmall(HEADER_BYTES, mapping.len()).into());
            }
            let mut storage = Storage {
                mapping: mapping.into(),
                handle: Some(src),
                //dirty_pages: HashSet::new(),
            };

            Self::restore_inner(&mut storage)?;
            // Now that we've recovered the file, flush the whole thing, keeping clean at 0 since we haven't closed it
            // yet.
            storage.mapping.write().unwrap().flush()?;
            Ok(storage)
        }
    }

    #[cfg(not(miri))]
    pub fn restore(path: &Path) -> Result<Storage> {
        Self::restore_file(OpenOptions::new().read(true).write(true).create(false).open(path)?)
    }

    #[cfg(miri)]
    pub fn restore(src: Vec<u8>) -> Result<Storage> {
        unsafe {
            if src.len() < HEADER_BYTES {
                return Err(Error::MemMapTooSmall(HEADER_BYTES, src.len()).into());
            }
            let mut storage = Storage {
                mapping: Some(src),
                handle: None,
                //dirty_pages: HashSet::new(),
            };

            Self::restore_inner(&mut storage)?;
            Ok(storage)
        }
    }

    #[cfg(miri)]
    pub fn resize(&mut self) -> Result<()> {
        let mapref = self.mapping.write().map_err(|_| Error::LockError)?;
        unsafe {
            let mapping = std::mem::replace(mapref, Vec::new());
            let maplen = mapping.len();
            let mapcap = mapping.capacity();
            let mut aligned = Vec::<u64>::from_raw_parts(
                mapping.leak().as_mut_ptr() as *mut u64,
                maplen / size_of::<u64>(),
                mapcap / size_of::<u64>(),
            );
            aligned.resize(aligned.len() * 2, 0);
            let alignlen = aligned.len();
            let aligncap = aligned.capacity();
            let mut replace = Vec::from_raw_parts(
                aligned.leak().as_mut_ptr() as *mut u8,
                alignlen * size_of::<u64>(),
                aligncap * size_of::<u64>(),
            );
            mem::swap(mapref, &mut replace);

            Self::init_section(&mut mapref, maplen, self.mapping.as_ref().unwrap_unchecked().len())?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(miri))]
    pub fn resize(&self) -> Result<()> {
        let handle = self.handle.as_ref().ok_or(Error::OutOfMemory(0))?;
        let mut map = self.mapping.write().map_err(|_| Error::LockError)?;
        map.flush()?;
        let old_size = map.len();
        let new_size = old_size * 2;
        handle.set_len(new_size as u64)?; // growing a mapped file is fine on Linux
        unsafe {
            if map
                .remap(new_size, memmap2::RemapOptions::new().may_move(true))
                .is_err()
            {
                *map = MmapMut::map_mut(handle)?; // old mapping still alive here; dropped by the assignment
            }
        }
        let new_size = map.len();
        Self::init_section(&mut map, old_size, new_size)
    }

    #[cfg(not(target_os = "linux"))]
    //#[cfg(not(miri))]
    pub fn resize(&self) -> Result<()> {
        let handle = self.handle.as_ref().ok_or(Error::OutOfMemory(0))?;
        let mut map = self.mapping.write().unwrap();
        map.flush()?;
        let old_size = map.len();
        let new_size = old_size * 2;

        // On windows, MmapMut turns a length 0 map into an empty handle, so we can use it as a placeholder.
        drop(mem::replace(&mut *map, unsafe {
            memmap2::MmapOptions::new().len(0).map_mut(handle)?
        }));

        let result: std::prelude::v1::Result<(), std::io::Error> = handle.set_len(new_size as u64);
        // Swap the new handle back in even if we failed to resize it so the placeholder doesn't leak (unless creating the handle fails)
        *map = unsafe { MmapMut::map_mut(handle)? };
        result?;
        let new_size = map.len();
        Self::init_section(&mut map, old_size, new_size)
    }

    pub fn flush(&self) -> Result<()> {
        // If a poison error happens, the trie may be in an inconsistent state, but we still have to try flushing it to disk anyway
        // so we can rescue any valid data and then reconstruct the trie.
        #[cfg(not(miri))]
        self.mapping
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .flush()?;
        Ok(())
    }

    /*pub fn flush_async(&self) -> Result<()> {
        // TODO: is this even possible or does this hold a RwLock over an await point?
            self.mapping.read().unwrap_or_else(std::sync::PoisonError::into_inner).flush_async()?;
        Ok(())
    }*/
}

#[derive(Debug)]
pub struct HashedArrayTrie<K>
where
    K: num::PrimInt + num::cast::AsPrimitive<u8>,
{
    pub storage: Arc<Storage>,
    offset: u64,
    owner: bool,
    phantomkey: PhantomData<K>,
}

impl<K> HashedArrayTrie<K>
where
    K: num::PrimInt + num::cast::AsPrimitive<u8>,
{
    pub fn new(storage: &Arc<Storage>, root: u64) -> HashedArrayTrie<K> {
        {
            let store = storage.view();
            let (header, words) = store.parts();
            assert_eq!(
                root,
                header.root.load(Ordering::Relaxed),
                "new() opens the header's root; use duplicate() for other handles"
            );
            add_ref(root as usize, words);
        }
        HashedArrayTrie {
            storage: storage.clone(),
            offset: root,
            owner: true,
            phantomkey: PhantomData,
        }
    }

    // Doesn't increment children refcounts, only valid when the prior node will be immediately deleted, hence being a "move"
    #[inline]
    fn move_node(from: usize, to: usize, count: usize, words: &[AtomicU64]) {
        // Can't use copy_within because this must be done atomically.
        for i in 0..=count {
            words[to + i].store(words[from + i].load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    // Does a proper clone of a node, incrementing child refcounts if it isn't a leaf.
    #[inline]
    fn clone_node(from: usize, to: usize, count: usize, words: &[AtomicU64]) {
        Self::move_node(from, to, count, words);
        let leaf = {
            let mut f = FlagView::new(words, to);
            f.flags().set_refcount(1);
            f.flags().set_parity();
            f.flags().leaf()
        };

        // If this isn't a leaf node, fixes the children's refcounts
        if !leaf {
            for i in (1 + to)..=(count + to) {
                add_ref(words[i].load(Ordering::Relaxed) as usize, words);
            }
        }
    }

    #[inline]
    fn append(offset: usize, words: &[AtomicU64], index: u8, value: u64, count: usize) {
        let mut f = Flags::load(words, offset);
        let indice = f.offset(index);
        f.append(index);
        f.set_parity();

        for i in (indice..count).rev() {
            // No need for any special handling here because we only call this on nodes that aren't shared with any other thread.
            words[offset + 2 + i].store(words[offset + 1 + i].load(Ordering::Relaxed), Ordering::Relaxed);
        }
        words[offset + 1 + indice].store(value, Ordering::Relaxed);
        // Only store the new value into the parent *after* we've finished setting up the child
        words[offset].store(f.into(), Ordering::Relaxed);
    }

    #[inline]
    fn remove(offset: usize, words: &[AtomicU64], index: u8, count: usize) -> u64 {
        let mut f = Flags::load(words, offset);
        let indice = f.offset(index);
        f.set_mask(f.mask() & !(0b1 << index));
        f.set_parity();

        let value = words[offset + 1 + indice].load(Ordering::Relaxed);
        for i in indice..count - 1 {
            // No need for any special handling here because we only call this on nodes that aren't shared with any other thread.
            words[offset + 1 + i].store(words[offset + 2 + i].load(Ordering::Relaxed), Ordering::Relaxed);
        }

        words[offset + count].store(0, Ordering::Relaxed);
        words[offset].store(f.into(), Ordering::Relaxed);

        value
    }

    pub fn verify_trie(root: u64, bits: usize, words: &[AtomicU64]) {
        assert_ne!(root, 0);
        assert_ne!(root, u64::MAX);
        let flags = Flags::load(words, root as usize);
        if bits > 5 {
            let count = flags.count();
            for i in 1..=count {
                Self::verify_trie(words[root as usize + i].load(Ordering::Relaxed), bits - 5, words);
            }
        }
    }

    fn insert_entry(
        offset: usize,
        store: &StorageView<'_>,
        count: usize,
        index: u8,
        value: u64,
        mutable: bool,
    ) -> NodeResult {
        if count == MAX_CHILDREN {
            // This can only happen if the key already exists and somehow a previous check failed
            return NodeResult::Err(
                Error::AlreadyExists(store.words()[offset + index as usize + 1].load(Ordering::Relaxed)).into(),
            );
        }
        // If we are mutable and we have space, we just append the child and return nothing
        if mutable
            && offset + count + 1 < store.words().len()
            && store.words()[offset + count + 1].load(Ordering::Relaxed) == 0
        {
            Self::append(offset, store.words(), index, value, count);
            NodeResult::InPlace
        } else {
            // Otherwise, we must clone ourselves
            let n = match store.allocate(count + 1) {
                Ok(x) => x,
                Err(e) => return NodeResult::Err(e),
            };

            // If we have mutable access, just move
            if mutable {
                let words = store.words();
                Self::move_node(offset, n as usize, count, words);
                Self::append(n as usize, words, index, value, count);
                if let Err(e) = store.free_block(offset) {
                    NodeResult::Err(e)
                } else {
                    NodeResult::Moved(n)
                }
            } else {
                // Otherwise, we have to copy
                let words = store.words();
                Self::clone_node(offset, n as usize, count, words);
                Self::append(n as usize, words, index, value, count);
                NodeResult::Copied(n)
            }
        }
    }

    fn insert_node(
        offset: usize,
        store: &StorageView<'_>,
        bits: usize,
        key: K,
        value: u64,
        mutable: bool,
    ) -> NodeResult {
        let node = Flags::load(store.words(), offset);
        let count = node.count();

        //let mutable = mutable && node.refcount() == 1;
        if bits <= 5 {
            let index: u8 = key.as_() & (0b11111 >> (5 - bits));
            // Check if the child exists already
            return if node.exists(index) {
                NodeResult::Err(
                    Error::AlreadyExists(store.words()[offset + node.offset(index) + 1].load(Ordering::Relaxed)).into(),
                )
            } else {
                Self::insert_entry(offset, store, count, index, value, mutable)
            };
        }

        let sbits = bits - 5;
        let index: u8 = key.shr(sbits).as_() & 0b11111;

        // Check if the child exists already
        if !node.exists(index) {
            // Create a new child node that is empty and recurse into it. We force it to be mutable, so it shouldn't
            // return anything.
            let child = match store.allocate(1) {
                Ok(v) => v,
                Err(e) => return NodeResult::Err(e),
            };

            // Setup new empty node
            let mut fnew = Flags::new().with_refcount(1).with_leaf(bits <= 10);
            fnew.set_parity();
            store.words()[child as usize].store(fnew.into(), Ordering::Release);

            let n = match Self::insert_node(child as usize, store, bits - 5, key, value, true) {
                NodeResult::InPlace => child,
                NodeResult::Moved(n) => n,
                NodeResult::Copied(_) => unreachable!("A new child should never need to be copied!"),
                NodeResult::Err(e) => {
                    Self::cleanup(child, store);
                    return NodeResult::Err(e);
                }
            };

            return match Self::insert_entry(offset, store, count, index, n, mutable) {
                NodeResult::Err(e) => {
                    Self::cleanup(n, store);
                    NodeResult::Err(e)
                }
                x => x,
            };
        }

        // If it does exist, just recurse into it
        let child_offset = offset + node.offset(index) + 1;
        let child = store.words()[child_offset].load(Ordering::Acquire);
        let child_mutable = mutable && Flags::load(store.words(), child as usize).refcount() == 1;

        match Self::insert_node(child as usize, store, sbits, key, value, child_mutable) {
            NodeResult::InPlace => NodeResult::InPlace,
            NodeResult::Moved(n) => {
                // Should have only happened if we are mutable
                debug_assert!(mutable);
                store.words()[child_offset].store(n, Ordering::Relaxed);
                NodeResult::InPlace
            }
            NodeResult::Copied(n) if mutable => {
                let words = store.words();
                words[child_offset].store(n, Ordering::Relaxed);
                if let Err(e) = store.release(child as usize) {
                    NodeResult::Err(e)
                } else {
                    NodeResult::InPlace
                }
            }
            // If it was copied, we have to clone ourselves
            NodeResult::Copied(n) => {
                let clone = match store.allocate(count) {
                    Ok(c) => c,
                    Err(e) => {
                        Self::cleanup(n, store); // ITEM 5
                        return NodeResult::Err(e);
                    }
                };

                let words = store.words();
                Self::clone_node(offset, clone as usize, count, words);
                words[clone as usize + (child_offset - offset)].store(n, Ordering::Release);
                if let Err(e) = store.release(child as usize) {
                    NodeResult::Err(e)
                } else {
                    NodeResult::Copied(clone)
                }
            }
            x => x,
        }
    }

    // A particular reference either has one or two active refs depending on if it owns the file reference.
    #[inline]
    fn active_refs(&self) -> u8 {
        1 + self.owner as u8
    }

    // Inserts a new key and value on an exclusive reference to a particular root - call duplicate() to clone
    // a root.
    pub fn insert(&mut self, key: K, value: u64) -> Result<(), Error> {
        let store = self.storage.view();
        let offset: usize = self.offset as usize;
        let bits = size_of::<K>() * 8;
        let mutable = Flags::load(store.words(), offset).refcount() == self.active_refs();

        match Self::insert_node(offset, &store, bits, key, value, mutable) {
            NodeResult::InPlace => Ok(()),
            NodeResult::Moved(n) => {
                if self.owner {
                    store.header().root.store(n, Ordering::Release);
                }
                self.offset = n;
                Ok(())
            }
            NodeResult::Copied(n) => {
                let (header, words) = store.parts();
                if self.owner {
                    // Increment new reference before releasing the old one
                    add_ref(n as usize, words);
                    header.root.store(n, Ordering::Release);
                    store.release(offset)?;
                }
                store.release(offset)?;
                self.offset = n;
                Ok(())
            }
            NodeResult::Err(e) => Err(e),
        }
    }

    pub fn duplicate(&self) -> Result<Self, Error> {
        const MAX_ROOT_REFS: u64 = 127;
        let offset = self.offset;

        let view = self.storage.view();
        let prev = Flags::from(add_ref(offset as usize, view.words())).refcount() as u64;
        if prev >= MAX_ROOT_REFS {
            view.words()[offset as usize].fetch_sub(REFCOUNT_ONE, Ordering::Relaxed);
            return Err(Error::TooManyRefs(prev));
        }

        Ok(Self {
            storage: self.storage.clone(),
            offset,
            owner: false,
            phantomkey: PhantomData,
        })
    }

    pub fn get(&self, key: K) -> Result<u64, Error> {
        let store = self.storage.view();
        let mut offset: usize = self.offset as usize;
        let mut bits = size_of::<K>() * 8;
        let words = store.words();

        while bits > 5 {
            let node = Flags::load(words, offset);
            let index: u8 = key.shr(bits - 5).as_() & 0b11111;

            // Check if the node is set
            if !node.exists(index) {
                return Err(Error::NotFound {
                    key_offset: bits as u8,
                    key_bits: index,
                    node: offset as u64,
                }
                .into());
            }

            offset = words[offset + node.offset(index) + 1].load(Ordering::Relaxed) as usize;
            bits -= 5;
        }

        let node = Flags::load(words, offset);
        let index: u8 = key.as_() & (0b11111 >> (5 - bits));

        // Check if the node is set
        if !node.exists(index) {
            return Err(Error::NotFound {
                key_offset: bits as u8,
                key_bits: index,
                node: offset as u64,
            }
            .into());
        }

        Ok(words[offset + node.offset(index) + 1].load(Ordering::Relaxed))
    }

    fn delete_node(offset: usize, store: &StorageView<'_>, bits: usize, key: K, held: u8) -> Result<u64, Error> {
        let node = Flags::load(store.words(), offset);
        if node.refcount() > held {
            return Err(Error::Shared(offset as u64));
        }

        if bits > 5 {
            let index: u8 = key.shr(bits - 5).as_() & 0b11111;

            // Check if the node is set
            if !node.exists(index) {
                return Err(Error::NotFound {
                    key_offset: bits as u8,
                    key_bits: index,
                    node: offset as u64,
                }
                .into());
            }

            let child_offset = offset + node.offset(index) + 1;
            let count = node.count();
            let child = store.words()[child_offset].load(Ordering::Relaxed) as usize;
            let value = Self::delete_node(child, store, bits - 5, key, 1)?;

            if Flags::load(store.words(), child).count() == 0 {
                let check = Self::remove(offset, store.words(), index, count);
                assert_eq!(check, child as u64);
                store.release(child)?;
            }

            Ok(value)
        } else {
            let index: u8 = key.as_() & (0b11111 >> (5 - bits));

            // Check if the node is set
            if !node.exists(index) {
                return Err(Error::NotFound {
                    key_offset: bits as u8,
                    key_bits: index,
                    node: offset as u64,
                }
                .into());
            }

            let count = node.count();
            let words = store.words();
            let value = Self::remove(offset, words, index, count);
            Ok(value)
        }
    }

    pub fn delete(&mut self, key: K) -> Result<u64, Error> {
        let store = self.storage.view();

        let offset: usize = self.offset as usize;
        let bits = size_of::<K>() * 8;
        Self::delete_node(offset, &store, bits, key, self.active_refs())
    }

    #[inline]
    fn cleanup(node: u64, store: &StorageView<'_>) {
        let result = store.release(node as usize);
        debug_assert!(result.is_ok(), "cleanup failed for {node}");
    }
}

impl<K> Drop for HashedArrayTrie<K>
where
    K: num::PrimInt + num::cast::AsPrimitive<u8>,
{
    fn drop(&mut self) {
        let v = self.storage.view();
        let offset = self.offset as usize;
        let released = v.release(offset);
        debug_assert!(released.is_ok(), "failed to release root {offset}");
    }
}

//#[cfg(not(miri))]
impl Drop for Storage {
    fn drop(&mut self) {
        // Set our clean close bit to 1 and flush.
        self.view_mut().header().clean.store(1, Ordering::Release);
        // We have to ignore any errors here because we're already in the process of closing everything.
        let _ = self.flush();
    }
}

#[cfg(miri)]
impl Drop for Storage {
    fn drop(&mut self) {
        let (header, _) = self.parts_mut();
        header.clean = 1;

        if let Some(mapref) = &mut self.mapping {
            unsafe {
                let mapping = std::mem::take(mapref);
                let maplen = mapping.len();
                let mapcap = mapping.capacity();
                let _ = Vec::<u64>::from_raw_parts(
                    mapping.leak().as_mut_ptr() as *mut u64,
                    maplen / size_of::<u64>(),
                    mapcap / size_of::<u64>(),
                );
            }
        }
    }
}

#[test]
fn test_offsets() {
    let mut a = Flags::new().with_refcount(1);

    assert_eq!(a.count(), 0);
    assert!(!a.exists(0));
    assert!(!a.exists(1));
    assert!(!a.exists(31));
    assert_eq!(a.offset(0), 0);
    assert_eq!(a.offset(1), 0);
    assert_eq!(a.offset(31), 0);

    a.append(1);
    assert_eq!(a.count(), 1);
    assert!(!a.exists(0));
    assert!(a.exists(1));
    assert!(!a.exists(2));
    assert!(!a.exists(31));
    assert_eq!(a.offset(0), 0);
    assert_eq!(a.offset(1), 0);
    assert_eq!(a.offset(2), 1);
    assert_eq!(a.offset(31), 1);

    a.append(0);
    assert_eq!(a.count(), 2);
    assert!(a.exists(0));
    assert!(a.exists(1));
    assert!(!a.exists(2));
    assert!(!a.exists(31));
    assert_eq!(a.offset(0), 0);
    assert_eq!(a.offset(1), 1);
    assert_eq!(a.offset(2), 2);
    assert_eq!(a.offset(31), 2);

    a.append(31);
    assert_eq!(a.count(), 3);
    assert!(a.exists(0));
    assert!(a.exists(1));
    assert!(!a.exists(2));
    assert!(a.exists(31));
    assert_eq!(a.offset(0), 0);
    assert_eq!(a.offset(1), 1);
    assert_eq!(a.offset(2), 2);
    assert_eq!(a.offset(31), 2);

    a.append(2);
    assert_eq!(a.count(), 4);
    assert!(a.exists(0));
    assert!(a.exists(1));
    assert!(a.exists(2));
    assert!(a.exists(31));
    assert_eq!(a.offset(0), 0);
    assert_eq!(a.offset(1), 1);
    assert_eq!(a.offset(2), 2);
    assert_eq!(a.offset(31), 3);

    for i in 3..=30 {
        a.append(i);
    }

    assert_eq!(a.count(), MAX_CHILDREN);

    for i in 0..=31 {
        assert!(a.exists(i));
        assert_eq!(a.offset(i), i.into());
    }
}

#[test]
fn test_parity() {
    let mut a = Flags::new().with_refcount(0);

    assert_eq!(a.0, 0);
    assert!(!a.get_parity());
    assert_eq!(a.calc_parity(), 0);
    assert!(a.check_parity());
    a.set_parity();
    assert_eq!(a.0, 0);
    assert!(!a.get_parity());
    assert_eq!(a.calc_parity(), 0);
    assert!(a.check_parity());

    a.set_refcount(1);
    assert_eq!(a.calc_parity(), 0, "refcount is excluded from parity");
    a.set_refcount(0);
    a.set_mask(1);
    assert_ne!(a.0, 0);
    assert!(!a.get_parity());
    assert_eq!(a.calc_parity(), 1);
    assert!(!a.check_parity());
    a.set_parity();
    assert!(a.get_parity());
    assert_eq!(a.calc_parity(), 1);
    assert!(a.check_parity());

    a.0 = 0;
    assert!(!a.get_parity());
    a.set_skips(1);
    assert!(!a.get_parity());
    a.set_skips(4);
    assert!(!a.get_parity());
    a.set_skips(5);
    assert!(a.get_parity());
    a.set_skips(6);
    assert!(!a.get_parity());
    a.set_skips(7);
    assert!(a.get_parity());
    a.set_skips(1);
    assert!(!a.get_parity());
    a.set_skipbits(1 << 19);
    assert!(a.get_parity());
    a.set_skipbits(1 << 18);
    assert!(!a.get_parity());
}

#[cfg(test)]
use rand::seq::SliceRandom;
#[cfg(not(miri))]
#[cfg(test)]
use tempfile::tempfile;

#[cfg(test)]
#[inline]
fn get_next_u128<T>(rng: &mut T) -> u128
where
    T: rand::Rng,
{
    (rng.next_u64() as u128) | rng.next_u64() as u128
}

#[cfg(not(miri))]
#[test]
fn test_storage_new() -> Result<()> {
    let _ = Storage::new_file(tempfile()?, 64)?;
    Ok(())
}

#[cfg(miri)]
#[test]
fn test_storage_new() -> Result<()> {
    let _ = Storage::new(Path::new(""), 64)?;
    Ok(())
}

#[test]
fn test_out_of_storage() -> Result<()> {
    #[cfg(not(miri))]
    let fileref = tempfile()?;
    #[cfg(not(miri))]
    let store = Storage::new_ref(&fileref, 32)?;
    #[cfg(miri)]
    let store = Storage::new(Path::new(""), 32)?;

    store.view().allocate(1)?;
    let e = store.view().allocate(32).expect_err("Should have run out of memory?!");
    assert_eq!(e, Error::OutOfMemory(832)); // header grew by 32 * 8 bytes (128-bit tagged pointers)
    Ok(())
}

#[test]
fn test_storage_allocate() -> Result<()> {
    #[cfg(not(miri))]
    let store = Storage::new_file(tempfile()?, 32)?;
    #[cfg(miri)]
    let store = Storage::new(Path::new(""), 32)?;

    for i in 1..=32 {
        loop {
            // Don't hold view() past this statement or it deadlocks
            let result = store.view().allocate(i);
            match result {
                Ok(_) => break,
                Err(Error::OutOfMemory(_)) => store.resize(),
                Err(err) => Err(err.into()),
            }?
        }
    }
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_storage_load() -> Result<()> {
    let fileref = tempfile()?;

    {
        let store = Storage::new_ref(&fileref, 64)?;
        store.view().allocate(1)?;
        store.view().allocate(1)?;
    }

    {
        let store = Storage::load_file(fileref)?;
        for i in 1..=32 {
            loop {
                // Don't hold view() past this statement or it deadlocks
                let result = store.view().allocate(i);
                match result {
                    Ok(_) => break,
                    Err(Error::OutOfMemory(_)) => store.resize(),
                    Err(err) => Err(err.into()),
                }?
            }
        }
    }

    Ok(())
}

#[test]
fn test_empty() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 32)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 32)?);

    // We will eventually put a 128-bit FullLogID struct in here
    let _: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);
    Ok(())
}

#[test]
fn test_deep_near_miss() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 1024)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 1024)?);

    let mut trie: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);
    trie.insert(1, 1)?;
    trie.insert(2, 2)?;
    assert_eq!(trie.get(1).expect("Failed to get key"), 1);
    assert_eq!(trie.get(2).expect("Failed to get key"), 2);
    Ok(())
}

#[test]
fn test_duplicate() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 1024)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 1024)?);

    let mut trie: HashedArrayTrie<u16> = HashedArrayTrie::new(&storage, 1);
    trie.insert(1, 1)?;
    assert_eq!(trie.get(1).expect("Failed to get key"), 1);
    let e = trie.insert(1, 2).expect_err("Should have been an error!");
    assert_eq!(e, Error::AlreadyExists(1));
    assert_eq!(trie.get(1).expect("Failed to get key"), 1);
    Ok(())
}

#[test]
fn test_delete() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 1024)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 1024)?);

    let mut trie: HashedArrayTrie<u16> = HashedArrayTrie::new(&storage, 1);
    trie.insert(1, 1)?;
    assert_eq!(trie.get(1).expect("Failed to get key"), 1);
    assert_eq!(trie.delete(1).expect("Failed to delete key"), 1);
    let e = trie.get(1).expect_err("Should have been an error!");
    assert_eq!(
        e,
        Error::NotFound {
            key_bits: 0, // This should fail immediately, due to the root being empty
            key_offset: 16,
            node: 1
        }
    );
    Ok(())
}

#[test]
fn test_near_miss() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 1024)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 1024)?);

    let mut trie: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);
    trie.insert(1, 1)?;
    trie.insert(1 << 126, 2)?;
    assert_eq!(trie.get(1).expect("Failed to get key"), 1);
    assert_eq!(trie.get(1 << 126).expect("Failed to get key"), 2);
    Ok(())
}

#[test]
fn test_fill_leaf() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 1 << 13)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 1 << 13)?);

    let mut trie: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);
    for i in 0..32 {
        trie.insert(i, i as u64)?;
    }

    for i in 0..32 {
        assert_eq!(trie.get(i).expect("Failed to get key"), i as u64);
    }
    Ok(())
}

#[test]
fn test_fill_node() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 1 << 13)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 1 << 13)?);

    let mut trie: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);

    for i in 0..32 {
        trie.insert(i << 6, i as u64 + 10000)?;
        HashedArrayTrie::<u128>::verify_trie(1, 128, trie.storage.view().words());
    }

    for i in 0..32 {
        assert_eq!(trie.get(i << 6).expect("Failed to get key"), i as u64 + 10000);
    }

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_fill_trie() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 1 << 22)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 1 << 16)?);

    #[cfg(not(miri))]
    const MAX_COUNT: u16 = u16::MAX;
    #[cfg(miri)]
    const MAX_COUNT: u8 = u8::MAX;

    // Fill a 16-bit trie with with every single possible key (8-bit for miri tests)
    #[cfg(not(miri))]
    let mut trie: HashedArrayTrie<u16> = HashedArrayTrie::new(&storage, 1);
    #[cfg(miri)]
    let mut trie: HashedArrayTrie<u8> = HashedArrayTrie::new(&storage, 1);
    #[cfg(not(miri))]
    let mut v: Vec<u16> = (0..=MAX_COUNT).collect();
    #[cfg(miri)]
    let mut v: Vec<u8> = (0..=MAX_COUNT).collect();

    //let mut rng = StdRng::seed_from_u64(42);
    let mut rng = rand::rng();
    v.shuffle(&mut rng);
    for i in &v {
        trie.insert(*i, *i as u64).expect("Insertion failure!");
    }

    // Verify they all exist
    for i in 0..=MAX_COUNT {
        assert_eq!(trie.get(i).expect("Failed to get key"), i as u64);
    }

    v.shuffle(&mut rng);
    // Remove and re-insert every single possible value
    for i in &v {
        trie.delete(*i)?;
        trie.insert(*i, *i as u64)?;
    }

    // Verify all values are correct
    for i in 0..=MAX_COUNT {
        assert_eq!(trie.get(i).expect("Failed to get key"), i as u64);
    }

    v.shuffle(&mut rng);
    // Remove all of them in a random order
    for i in &v {
        assert_eq!(trie.delete(*i)?, *i as u64);
    }

    v.shuffle(&mut rng);
    // Re-insert them in a different order
    for i in &v {
        trie.insert(*i, *i as u64).expect("Insertion failure!");
    }

    // Verify all values are correct
    for i in 0..=MAX_COUNT {
        assert_eq!(trie.get(i).expect("Failed to get key"), i as u64);
    }

    // Remove them all again
    for i in 0..=MAX_COUNT {
        assert_eq!(trie.delete(i)?, i as u64);
    }

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_fill_random() -> Result<()> {
    #[cfg(not(miri))]
    let storage = Arc::new(Storage::new_file(tempfile()?, 32)?);
    #[cfg(miri)]
    let storage = Arc::new(Storage::new(Path::new(""), 32)?);

    let mut trie: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);

    let mut rng = rand::rng();
    let mut track: Vec<u128> = Vec::new();

    #[cfg(not(miri))]
    const MAX_COUNT: usize = 0xFFFF;
    #[cfg(miri)]
    const MAX_COUNT: usize = 64;

    // Fill trie with random noise
    for _ in 0..=MAX_COUNT {
        let key = get_next_u128(&mut rng);
        track.push(key);
        while let Err(e) = trie.insert(key, key as u64) {
            match e {
                Error::OutOfMemory(_) => storage.resize(),
                err => Err(err.into()),
            }?;
        }
    }

    storage.flush()?;

    // Remove and re-insert every key
    for i in &track {
        assert_eq!(trie.delete(*i)?, *i as u64);
        trie.insert(*i, *i as u64)?;
    }

    storage.flush()?;

    // Remove everything first, then re-insert every key, ensuring no additional space is used
    for i in &track {
        assert_eq!(trie.delete(*i)?, *i as u64);
    }

    storage.flush()?;

    for i in &track {
        trie.insert(*i, *i as u64)?;
    }

    storage.flush()?;

    // Verify all values are correct
    for i in &track {
        assert_eq!(trie.get(*i).expect("Failed to get key"), *i as u64);
    }

    storage.flush()?;

    //delete everything one more time
    for i in track {
        assert_eq!(trie.delete(i)?, i as u64);
    }

    storage.flush()?;

    Ok(())
}

// This is too expensive for miri
#[test]
#[cfg_attr(miri, ignore)]
fn test_allocations() -> Result<()> {
    for sz in 1..=512 {
        #[cfg(not(miri))]
        let storage = Arc::new(Storage::new_file(tempfile()?, sz * size_of::<u64>() as u64)?);
        #[cfg(miri)]
        let storage = Arc::new(Storage::new(Path::new(""), sz * size_of::<u64>() as u64)?);

        let mut trie: HashedArrayTrie<u8> = HashedArrayTrie::new(&storage, 1);

        for i in 0..=255 {
            while let Err(e) = trie.insert(i, i as u64) {
                match e {
                    Error::OutOfMemory(_) => storage.resize(),
                    err => Err(err.into()),
                }?;
            }
        }
        for i in 0..=255 {
            assert_eq!(trie.get(i).expect("Failed to get key"), i as u64);
        }
    }
    Ok(())
}

#[cfg(not(miri))]
#[test]
fn test_storage_restore_simple() -> Result<()> {
    let fileref = tempfile()?;

    {
        let storage = Arc::new(Storage::new_ref(&fileref, 296)?);
        let mut trie: HashedArrayTrie<u8> = HashedArrayTrie::new(&storage, 1);

        trie.insert(1, 1).expect("Insertion failure!");
        assert_eq!(trie.get(1).expect("Failed to get key"), 1);
    }

    {
        let storage = Arc::new(Storage::restore_file(fileref)?);
        let trie: HashedArrayTrie<u8> = HashedArrayTrie::new(&storage, 1);
        assert_eq!(trie.get(1).expect("Failed to get key"), 1);
    }

    Ok(())
}

#[cfg(not(miri))]
#[test]
fn test_storage_restore() -> Result<()> {
    let fileref = tempfile()?;

    {
        let storage = Arc::new(Storage::new_ref(&fileref, 1 << 16)?);
        let mut trie: HashedArrayTrie<u8> = HashedArrayTrie::new(&storage, 1);
        // Fill an 8-bit trie with with every single possible key
        let mut v: Vec<u8> = (0..=u8::MAX).collect();

        //let mut rng = StdRng::seed_from_u64(42);
        let mut rng = rand::rng();
        v.shuffle(&mut rng);
        for i in &v {
            trie.insert(*i, *i as u64).expect("Insertion failure!");
        }

        for i in 0..=u8::MAX {
            assert_eq!(trie.get(i).expect("Failed to get key"), i as u64);
        }
    }

    {
        let storage = Arc::new(Storage::restore_file(fileref)?);
        let trie: HashedArrayTrie<u8> = HashedArrayTrie::new(&storage, 1);

        for i in 0..=u8::MAX {
            assert_eq!(trie.get(i).expect("Failed to get key"), i as u64);
        }
    }

    Ok(())
}

// TODO: replace AI generated tests

#[cfg(test)]
fn expected_refcounts(roots: &[u64], words: &[AtomicU64]) -> std::collections::HashMap<u64, u32> {
    let mut expected = std::collections::HashMap::new();
    let mut stack = Vec::new();
    let visit = |node: u64, expected: &mut std::collections::HashMap<u64, u32>, stack: &mut Vec<u64>| {
        let e = expected.entry(node).or_insert(0);
        if *e == 0 {
            stack.push(node);
        }
        *e += 1;
    };
    for &r in roots {
        visit(r, &mut expected, &mut stack);
    }
    while let Some(n) = stack.pop() {
        let f = Flags::load(words, n as usize);
        if !f.leaf() {
            for i in 1..=f.count() {
                visit(words[n as usize + i].load(Ordering::Relaxed), &mut expected, &mut stack);
            }
        }
    }
    expected
}

#[cfg(test)]
fn verify_refcounts(roots: &[u64], words: &[AtomicU64]) {
    for (node, want) in expected_refcounts(roots, words) {
        let f = Flags::load(words, node as usize);
        assert!(f.check_parity(), "bad parity on node {node}");
        assert_eq!(f.refcount() as u32, want, "refcount mismatch on node {node}");
    }
}

/// Every word must be the canary, part of a reachable node, part of a free block, or zeroed slack.
/// Anything else is a leaked node.
#[cfg(test)]
fn find_leaks(roots: &[u64], storage: &Storage) -> Vec<usize> {
    let view = storage.view();
    let (header, words) = view.parts();
    let mut owned = vec![false; words.len()];
    owned[0] = true;
    for (i, head) in header.freelist.iter().enumerate() {
        let mut b = head.load(Ordering::Relaxed) as u64;
        while b != u64::MAX {
            owned[b as usize..(b as usize + i + 2).min(words.len())].fill(true);
            b = words[b as usize].load(Ordering::Relaxed);
        }
    }
    for &node in expected_refcounts(roots, words).keys() {
        let count = Flags::load(words, node as usize).count();
        owned[node as usize..=node as usize + count].fill(true);
    }
    (1..words.len())
        .filter(|&w| !owned[w] && words[w].load(Ordering::Relaxed) != 0)
        .collect()
}

/// Checks refcounts and leaks for the header's root plus the roots of the live handles passed in.
/// With no handles, this is exactly the shutdown invariant: counts match what is reachable from the header.
#[cfg(test)]
fn check(handles: &[u64], storage: &Arc<Storage>) {
    let roots = {
        let s = storage.view();
        let mut roots = vec![s.header().root.load(Ordering::Relaxed)];
        roots.extend_from_slice(handles);
        verify_refcounts(&roots, s.words());
        roots
    };
    assert_eq!(find_leaks(&roots, storage), Vec::<usize>::new(), "leaked words");
}

#[cfg(test)]
use rand::RngExt;

#[cfg(not(miri))]
#[test]
fn test_fork_isolation() -> Result<()> {
    let storage = Arc::new(Storage::new_file(tempfile()?, 1 << 20)?);
    check(&[], &storage); // fresh file: the header's root has refcount 1

    let mut a: HashedArrayTrie<u16> = HashedArrayTrie::new(&storage, 1);
    for k in 0..512u16 {
        a.insert(k * 37, k as u64)?;
    }
    assert_eq!(
        a.offset, 1,
        "the owner writes in place while nobody else holds its root"
    );
    let mut b = a.duplicate()?;
    check(&[a.offset, b.offset], &storage);

    // Deletes are refused on shared paths, and the refusal changes nothing.
    let e = a.delete(0).expect_err("root is shared right after a fork");
    assert!(matches!(e, Error::Shared { .. }));
    assert_eq!(b.get(0)?, 0);

    // Interleave writes on both versions. Each write copies only the path it touches.
    for k in 0..512u16 {
        b.insert(k * 37 + 1, 1)?;
        a.insert(k * 37 + 2, 2)?;
        if k % 64 == 0 {
            check(&[a.offset, b.offset], &storage);
        }
    }
    check(&[a.offset, b.offset], &storage);
    assert_eq!(
        storage.view().header().root.load(Ordering::Relaxed),
        a.offset,
        "the owner publishes its root; the fork doesn't"
    );
    for k in 0..512u16 {
        assert_eq!(a.get(k * 37)?, k as u64);
        assert_eq!(b.get(k * 37)?, k as u64);
        assert!(
            a.get(k * 37 + 1).is_err() && b.get(k * 37 + 2).is_err(),
            "write leaked across versions"
        );
    }

    drop(b);
    check(&[a.offset], &storage);
    // Unshared again, so deletes work.
    assert_eq!(a.delete(0)?, 0);
    drop(a);
    // Shutdown invariant: only the header holds a reference, and everything it reaches is intact.
    check(&[], &storage);
    let root = storage.view().header().root.load(Ordering::Relaxed);
    let a: HashedArrayTrie<u16> = HashedArrayTrie::new(&storage, root);
    assert_eq!(a.get(37)?, 1);
    assert!(a.get(0).is_err());
    Ok(())
}

#[cfg(not(miri))]
#[test]
fn test_fork_random_u128() -> Result<()> {
    let storage = Arc::new(Storage::new_file(tempfile()?, 1 << 12)?);
    let mut rng = rand::rng();
    // versions[0] starts as the header's owner; it may be dropped like any other handle.
    let mut versions: Vec<(HashedArrayTrie<u128>, Vec<u128>)> = vec![(HashedArrayTrie::new(&storage, 1), vec![])];
    let roots = |v: &Vec<(HashedArrayTrie<u128>, Vec<u128>)>| v.iter().map(|x| x.0.offset).collect::<Vec<_>>();
    for step in 0..4000 {
        let i = rng.random_range(0..versions.len());
        match rng.random_range(0..10) {
            0 if versions.len() < 8 => {
                let fork = (versions[i].0.duplicate()?, versions[i].1.clone());
                versions.push(fork);
            }
            1 if versions.len() > 1 => {
                versions.swap_remove(i); // Drop releases the handle's reference
            }
            _ => {
                let key = get_next_u128(&mut rng);
                loop {
                    match versions[i].0.insert(key, key as u64) {
                        Ok(()) => break,
                        Err(e) => match e {
                            Error::OutOfMemory(_) => {
                                // ITEM 5: a failed insert must not leak or skew refcounts.
                                check(&roots(&versions), &storage);
                                storage.resize()?;
                            }
                            err => return Err(err.into()),
                        },
                    }
                }
                versions[i].1.push(key);
            }
        }
        if step % 500 == 0 {
            check(&roots(&versions), &storage);
        }
    }
    for (t, keys) in &versions {
        for k in keys {
            assert_eq!(t.get(*k)?, *k as u64);
        }
    }
    check(&roots(&versions), &storage);
    drop(versions);
    check(&[], &storage);
    Ok(())
}

/// The owner's root moves (Rewrite::Moved) only after it has been copied out of the reserved area at offset 1.
/// A move must keep BOTH references (header + handle) and repoint the header.
#[cfg(not(miri))]
#[test]
fn test_owner_root_moves() -> Result<()> {
    let storage = Arc::new(Storage::new_file(tempfile()?, 1 << 16)?);
    let mut a: HashedArrayTrie<u16> = HashedArrayTrie::new(&storage, 1);
    a.insert(0, 0)?;
    let b = a.duplicate()?;
    a.insert(1 << 11, 1)?; // shared root: copied into an ordinary block with no reserved slack
    drop(b);
    check(&[a.offset], &storage);

    let mut moves = 0;
    for top in 2..32u16 {
        let before = a.offset;
        a.insert(top << 11, top as u64)?; // new top-level index on an exclusively owned root
        if a.offset != before {
            moves += 1;
        }
        check(&[a.offset], &storage);
        assert_eq!(
            storage.view().header().root.load(Ordering::Relaxed),
            a.offset,
            "a move must repoint the header"
        );
    }
    assert!(
        moves > 0,
        "expected the owner's root to outgrow its block at least once"
    );
    drop(a);
    check(&[], &storage);
    Ok(())
}

#[cfg(not(miri))]
#[test]
fn test_threads_duplicate_insert_drop() -> Result<()> {
    let storage = Arc::new(Storage::new_file(tempfile()?, 1 << 24)?);
    let mut owner: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);
    for k in 0..2000u128 {
        owner.insert(k, k as u64)?;
    }
    for _round in 0..20 {
        let handles: Vec<_> = (0..8).map(|_| owner.duplicate()).collect::<Result<_, _>>()?;
        std::thread::scope(|s| {
            for (t, mut h) in handles.into_iter().enumerate() {
                s.spawn(move || {
                    for k in 0..500u128 {
                        let key = ((t as u128 + 1) << 64) | k;
                        h.insert(key, k as u64).unwrap();
                        assert_eq!(h.get(key).unwrap(), k as u64);
                    }
                    for k in 0..2000u128 {
                        assert_eq!(h.get(k).unwrap(), k as u64); // shared, never-copied subtrees
                    }
                    // h dropped here, concurrently with the other threads' drops and inserts
                });
            }
        });
        check(&[owner.offset], &storage);
        // owner keeps writing in place between rounds
        owner.insert(1_000_000 + _round as u128, 7)?;
    }
    drop(owner);
    check(&[], &storage);
    Ok(())
}
