use crate::log_capnp::log_entry;
use crate::murmur3::{murmur3_aligned, murmur3_aligned_inner, murmur3_finalize};
use crate::offset_io::{OffsetRead, OffsetWrite, ReadSessionBuf};
use crate::ring_buf_writer::RingBufWriter;

use super::hashed_array_trie::{HashedArrayStorage, HashedArrayTrie, HashedArrayTrieError};

use super::sorted_map::SortedMap;
use capnp::message::{self, Allocator, ReaderOptions, TypedReader};
use eyre::eyre;
use eyre::Result;
use std::alloc;
use std::cell::RefCell;

#[cfg(miri)]
use crate::fakefile::FakeFile;
use std::collections::VecDeque;
#[cfg(not(miri))]
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::mem::size_of;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::sync::RwLock;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CapLogError {
    #[error("value {v:?} is out of bounds: [{minimum:?}-{maximum:?}]")]
    OutOfBounds { v: u64, minimum: u64, maximum: u64 },
    #[error("Expected schema hash {0} but found {1}")]
    InvalidSchema(u64, u64),
    #[error("Data ID {0} doesn't match Trie ID {1} ({2}, {3})")]
    Mismatch(u128, u128, u64, u64),
    #[error("Data failed integrity check (got {0} but expected {1})")]
    CorruptData(u128, u128),
    #[error("the data for key `{0}` is not available")]
    NotFound(String),
    #[error("Tried to allocate a buffer for {0} bytes but only had {1} left")]
    NotEnoughBuffer(usize, usize),
    #[error("File I/O error encountered or filesystem in bad state")]
    FileError,
    #[error("File was not closed properly")]
    DirtyFile,
    #[error("unknown caplog error")]
    Unknown,
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum HeaderFlags {
    Clean = 0b001, // If this isn't set, we didn't do a clean shutdown
}

#[repr(C)]
#[derive(Clone, Debug, Copy, Default)]
struct HeaderStart {
    flags: u64,
}

impl HeaderStart {
    #[inline]
    pub fn new(bytes: &[u8]) -> Result<HeaderStart> {
        if bytes.len() < size_of::<HeaderStart>() {
            return Err(CapLogError::OutOfBounds {
                v: bytes.len() as u64,
                minimum: size_of::<HeaderStart>() as u64,
                maximum: u64::MAX,
            }
            .into());
        }
        Ok(HeaderStart {
            flags: u64::from_le_bytes(bytes[0..8].try_into()?),
        })
    }

    #[inline]
    pub fn write_at<T>(&self, target: &mut T, offset: u64) -> Result<()>
    where
        T: OffsetWrite,
    {
        target.write_all_at(&u64::to_le_bytes(self.flags), offset)?;
        Ok(())
    }
}

/// Maximum size of a data file. When this is reached, both the header file and data file are finalized,
/// the in-memory vectors cleared, and a new data and header file are created
pub const MAX_FILE_SIZE: u64 = 2_u64.pow(28);
pub const MAX_OPEN_FILES: usize = 10;
#[cfg(not(miri))]
pub const MAX_BUFFER_SIZE: usize = 2_usize.pow(22);
#[cfg(miri)]
pub const MAX_BUFFER_SIZE: usize = 2_usize.pow(11);

#[cfg(not(miri))]
type FileType = File;
#[cfg(miri)]
type FileType = FakeFile;

struct FileManagement {
    max_open_files: usize,
    files: RwLock<SortedMap<u128, Option<FileType>>>,
    prefix: PathBuf,
}

impl FileManagement {
    #[cfg(not(miri))]
    // Opens a completely new handle to a file in append mode, strictly for appending to an older archive
    pub fn get_append_file(&self, id: u128) -> Option<FileType> {
        let p = self.get_path(id);
        OpenOptions::new().append(true).open(p).ok()
    }

    #[cfg(miri)]
    pub fn get_append_file(&mut self, id: u128) -> Option<FileType> {
        // For Miri, this has to work a bit different, because the fake file doesn't really exist. So instead
        // we rely on ensuring that it gets put into our archive and then cloning the archival copy, and setting
        // the position to the end.
        let mut file = self.get_data_file(id)?;
        file.seek(std::io::SeekFrom::End(0)).ok()?;
        Some(file)
    }

    // Creates or clones an existing file suitable for normal reads and writes.
    pub fn get_data_file(&mut self, id: u128) -> Option<FileType> {
        let k = {
            let files = self.files.read().ok()?;
            let e = files.range(..=id).last()?;
            if let Some(x) = e.1.as_ref() {
                return x.try_clone().ok();
            }
            e.0
        };

        let mut files = self.files.write().ok()?;
        let p = self.get_path(id);
        let v = files.get_mut(&k)?;

        #[cfg(not(miri))]
        // This must NEVER use append(true) because it will subtly break write_at
        if let Ok(f) = OpenOptions::new().read(true).write(true).open(p) {
            let result = f.try_clone().ok();
            *v = Some(f);
            result
        } else {
            None
        }

        #[cfg(miri)]
        {
            let result = FakeFile::new();
            *v = result.try_clone().ok();
            Some(result)
        }
    }

    pub fn get_path(&self, id: u128) -> PathBuf {
        let mut path = self.prefix.to_path_buf().into_os_string();
        path.push("_");
        path.push(id.to_string());
        path.into()
    }

    pub fn new_file(&self, id: u128) -> Result<FileType> {
        let header = HeaderStart { flags: 0 };

        #[cfg(miri)]
        let mut file = FakeFile::new();

        #[cfg(not(miri))]
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.get_path(id).as_path())?;

        header.write_at(&mut file, 0)?;
        file.flush()?;
        Ok(file)
    }

    fn check_entry(pattern: &str, entry: std::io::Result<std::fs::DirEntry>) -> Option<u128> {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let s = name.as_os_str().to_str()?;
        if s.starts_with(pattern) {
            let (_, n) = s.split_once('_')?;
            u128::from_str_radix(n, 10).ok() // this isn't radix 16 because we only have base 10 to_string
        } else {
            None
        }
    }

    #[cfg(miri)]
    pub fn find_latest_file(&self) -> Option<u128> {
        None
    }

    #[cfg(not(miri))]
    pub fn find_latest_file(&self) -> Option<u128> {
        let pattern = self.prefix.file_name()?.to_str()?;
        let mut highest = None; // 0 is a valid return value here, so this must be a proper Option

        for entry in std::fs::read_dir(self.prefix.parent()?).ok()? {
            if let Some(x) = Self::check_entry(pattern, entry) {
                highest = highest.map(|h| std::cmp::max(h, x))
            }
        }

        highest
    }
}

struct StagingAlloc {
    staging: Box<[u64]>,
    staging_used: bool,
    hash: u128,
    hash_len: usize,
}

impl StagingAlloc {
    pub fn consume_hash(&mut self) -> u128 {
        let hash = murmur3_finalize(self.hash_len, self.hash);
        self.hash = 0;
        self.hash_len = 0;
        hash
    }
}

/// A high-performance append-only log
pub struct CapLog<const BUFFER_SIZE: usize> {
    max_file_size: u64,
    trie: HashedArrayTrie<u128>,
    //schemas: HashMap<u64, Vec<u8>>,
    staging: StagingAlloc,
    pub data_file: Arc<RingBufWriter<FileType, BUFFER_SIZE>>,
    flush_error: AtomicBool, // This is set if there is a failure writing data to disk, which forces all pending writes to return errors
    last_flush: u64,
    current_id: u128,
    archive: FileManagement,
    options: ReaderOptions,
    pending: VecDeque<(u64, SyncSender<bool>)>,
}

unsafe impl capnp::message::Allocator for StagingAlloc {
    fn allocate_segment(&mut self, minimum_size: u32) -> (*mut u8, u32) {
        if self.staging_used || minimum_size as usize > self.staging.len() {
            let layout = alloc::Layout::from_size_align(minimum_size as usize * size_of::<u64>(), 8).unwrap();
            unsafe { (alloc::alloc_zeroed(layout), minimum_size) }
        } else {
            self.staging_used = true;
            (self.staging.as_mut_ptr() as *mut u8, self.staging.len() as u32)
        }
    }

    unsafe fn deallocate_segment(&mut self, ptr: *mut u8, word_size: u32, words_used: u32) {
        let slice = &*std::ptr::slice_from_raw_parts(ptr as *const u64, words_used as usize);
        self.hash = murmur3_aligned_inner(slice, self.hash, self.hash_len);
        self.hash_len += slice.len();
        if self.staging.as_ptr() as *const u8 == ptr {
            self.staging[..words_used as usize].fill(0);
            self.staging_used = false;
        } else {
            unsafe {
                alloc::dealloc(
                    ptr,
                    alloc::Layout::from_size_align(word_size as usize * size_of::<u64>(), 8).unwrap(),
                );
            }
        }
    }
}

#[test]
fn alloc_test() -> Result<()> {
    let mut alloc = StagingAlloc {
        staging: vec![0_u64; 10].into_boxed_slice(),
        staging_used: false,
        hash: 0,
        hash_len: 0,
    };

    unsafe {
        let (ptr, len) = alloc.allocate_segment(1);
        alloc.deallocate_segment(ptr, len, 1);
    }
    Ok(())
}

impl<const BUFFER_SIZE: usize> CapLog<BUFFER_SIZE> {
    #[cfg(not(miri))]
    pub fn new(
        max_file_size: u64,
        trie_file: &Path,
        data_prefix: &Path,
        max_open_files: usize,
        check_consistency: bool,
    ) -> Result<Self> {
        let trie_storage = if let Ok(file) = OpenOptions::new().read(true).write(true).create(false).open(trie_file) {
            HashedArrayStorage::load_file(file)?
        } else {
            HashedArrayStorage::new(trie_file, 2_u64.pow(16))?
        };

        Self::new_storage(
            max_file_size,
            trie_storage,
            data_prefix,
            max_open_files,
            check_consistency,
        )
    }

    pub fn new_storage(
        max_file_size: u64,
        trie_storage: HashedArrayStorage,
        data_prefix: &Path,
        max_open_files: usize,
        check_consistency: bool,
    ) -> Result<Self> {
        let storage = Rc::new(RefCell::new(trie_storage));
        let archive = FileManagement {
            prefix: data_prefix.to_path_buf(),
            max_open_files,
            files: RwLock::new(SortedMap::new()),
        };

        #[cfg(not(miri))]
        let (mut file, id) = if let Some(id) = archive.find_latest_file() {
            let data_path = archive.get_path(id);
            if let Ok(mut file) = OpenOptions::new().read(true).write(true).create(false).open(data_path) {
                // Always check for the clean close flag, if that's not set a recovery pass should be done
                let mut headerstartbuf = [0_u8; size_of::<HeaderStart>()];
                file.read_at(&mut headerstartbuf, 0)?;
                if (HeaderStart::new(&headerstartbuf)?.flags & HeaderFlags::Clean as u64) == 0 {
                    return Err(CapLogError::DirtyFile.into());
                }

                if check_consistency {
                    // TODO: Go through file and check all the hashes

                    // We can assume that the data is a struct of some kind
                    //let value_reader: capnp::dynamic_value::Reader<'_> = payload.into();
                    //let struct_reader = value_reader.downcast::<dynamic_struct::Reader<'_>>();
                    //let payload_size = struct_reader.total_size()?;
                }

                file.seek(std::io::SeekFrom::End(0))?;

                (file, id)
            } else {
                return Err(CapLogError::FileError.into());
            }
        } else {
            (archive.new_file(0)?, 0)
        };

        #[cfg(miri)]
        let (mut file, id) = (archive.new_file(0)?, 0);

        let file_clone = file.try_clone()?;
        let position = file.stream_position()?;
        let log = Self {
            max_file_size,
            trie: HashedArrayTrie::new(&storage, 1),
            data_file: Arc::new(RingBufWriter::new(file, position)),
            staging: StagingAlloc {
                staging: vec![0_u64; BUFFER_SIZE / size_of::<u64>()].into_boxed_slice(),
                staging_used: false,
                hash: 0,
                hash_len: 0,
            },
            flush_error: AtomicBool::new(false),
            last_flush: 0,
            current_id: id,
            archive,
            options: ReaderOptions {
                traversal_limit_in_words: None,
                nesting_limit: 128,
            },
            pending: VecDeque::new(),
        };

        if let Ok(mut files) = log.archive.files.write() {
            files.insert(id, Some(file_clone));
        }
        Ok(log)
    }

    #[inline]
    pub fn get_log(
        &mut self,
        snowflake: u64,
        machine: u64,
        check_consistency: bool,
        builder: &mut capnp::any_pointer::Builder<'_>,
    ) -> Result<()> {
        let id = ((machine as u128) << 64) | snowflake as u128;
        self.get_internal(id, self.trie.get(id)?, check_consistency, builder)
    }

    #[inline]
    fn get_internal(
        &mut self,
        id: u128,
        offset: u64,
        check_consistency: bool,
        builder: &mut capnp::any_pointer::Builder<'_>,
    ) -> Result<()> {
        // Okay, try to find the file
        let f = self.archive.get_data_file(id).ok_or(CapLogError::FileError)?;

        let mut session: ReadSessionBuf<'_, FileType, 4096> = ReadSessionBuf::new(&f, offset);

        let reader = capnp::serialize_packed::read_message(&mut session, self.options)?;

        let reader = if check_consistency {
            let segments = reader.into_segments();
            let bytes: &[u8] = &segments;
            let words =
                unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const u64, bytes.len() / size_of::<u64>()) };

            let hash = murmur3_aligned(words, 0);

            let mut buf = [0_u8; size_of::<u128>()];
            (&mut session).read_exact(&mut buf)?;
            let expected = u128::from_le_bytes(buf);

            if hash != expected {
                return Err(CapLogError::CorruptData(hash, expected).into());
            }

            capnp::message::Reader::new(segments, self.options)
        } else {
            reader
        };

        let message = TypedReader::<_, log_entry::Owned>::new(reader);
        let entry = message.get()?;
        let machine_id = entry.get_machine_id();
        let snowflake_id = entry.get_snowflake_id();
        let entry_id = ((machine_id as u128) << 64) | snowflake_id as u128;
        if entry_id != id {
            return Err(CapLogError::Mismatch(entry_id, id, (id >> 64) as u64, id as u64).into());
        }

        builder.set_as(entry.get_payload())?;
        Ok(())
    }

    fn reset(&mut self, id: u128) -> Result<()> {
        // We open a new file handle first so that we can swap it into the ring buffer if necessary
        let mut file = self.archive.new_file(id)?;

        if let Ok(mut files) = self.archive.files.write() {
            files.insert(id, file.try_clone().ok());
        }

        if let Ok((mut flusher, state)) = self.data_file.lock_flusher() {
            // Dump current buffer
            flusher.flush_buf::<BUFFER_SIZE>(state)?;
            flusher.get_mut().flush()?;

            //  Write clean header flag
            HeaderStart {
                flags: HeaderFlags::Clean as u64,
            }
            .write_at(flusher.get_mut(), 0)?;

            // Finalize old file
            if let Err(e) = flusher.get_mut().flush() {
                self.flush_error.store(true, Ordering::Release);
                return Err(e.into());
            }

            let position = file.stream_position()?;
            // We swap the file pointers here and then simply drop this one, because we already have a clone in the archive.
            flusher.swap_inner::<BUFFER_SIZE>(state, &mut file, position)?;

            self.trie.storage.borrow_mut().flush()?;
        } else {
            return Err(CapLogError::Unknown.into());
        }

        self.last_flush = 0;
        self.current_id = id;

        Ok(())
    }

    pub fn check_write_size(&mut self, size: usize) -> bool {
        self.data_file.write_position() + size as u64 > self.max_file_size
    }

    #[inline]
    fn trie_insert(&mut self, id: u128, value: u64) -> Result<()> {
        while let Err(e) = self.trie.insert(id, value) {
            match e.downcast::<HashedArrayTrieError>()? {
                HashedArrayTrieError::OutOfMemory(_) => self.trie.storage.borrow_mut().resize(),
                err => Err(err.into()),
            }?;
        }
        Ok(())
    }

    pub fn append(
        &mut self,
        snowflake: u64,
        machine: u64,
        instance: u64,
        schema: u64,
        payload: capnp::any_pointer::Reader<'_>,
        size: usize,
    ) -> Result<Receiver<bool>> {
        // We do a sanity check here in case the files are empty or the error bit is set.
        if self.flush_error.load(Ordering::Relaxed) {
            return Err(eyre!("Log filesystem is in bad state!"));
        }

        let id = ((machine as u128) << 64) | snowflake as u128;

        // Check if we need to update flush status - this MUST be checked before a potential reset or we lose the flush progress!
        self.process_pending();

        // Check if we need to reset to a new file if this isn't an older ID
        if id >= self.current_id && self.check_write_size(size) {
            self.reset(id)?;
        }

        let mut message = message::Builder::new(&mut self.staging);

        let mut builder: log_entry::Builder<'_> = message.init_root();
        builder.set_snowflake_id(snowflake);
        builder.set_machine_id(machine);
        builder.set_instance_id(instance);
        builder.set_schema(schema);
        builder.get_payload().set_as(payload)?;

        if id < self.current_id {
            let mut handle = self.archive.get_append_file(id).ok_or(CapLogError::FileError)?;
            let position = handle.stream_position()?;
            assert_ne!(position, 0);

            {
                let message = message; // Move message into inner scope so it is dropped
                capnp::serialize_packed::write_message(&mut handle, &message)?;
            }

            self.trie_insert(id, position)?;
            self.trie.insert(id, position)?;
            handle.write_all(&self.staging.consume_hash().to_le_bytes())?;
        } else {
            let position = self.data_file.write_position();
            let handle = &mut self.data_file.as_ref();
            {
                let message = message; // Move message into inner scope so it is dropped
                capnp::serialize_packed::write_message(handle, &message)?;
            }

            self.trie_insert(id, position)?;

            // Reborrow because write_message consumes it's handle for no reason
            let handle = &mut self.data_file.as_ref();
            handle.write_all(&self.staging.consume_hash().to_le_bytes())?;
        }

        let (sender, receiver) = sync_channel(1);

        self.pending.push_back((self.data_file.write_position(), sender));
        Ok(receiver)
    }

    // Every time this is called, write all data that is ready to be flushed to disk
    pub fn flush(&mut self) -> Result<()> {
        let handle = &mut self.data_file.as_ref();
        Ok(handle.flush()?)
    }

    pub fn process_pending(&mut self) -> usize {
        let cur_flush = self.data_file.flush_position();
        let mut count = 0;
        if self.last_flush < cur_flush {
            while !self.pending.is_empty() {
                if self.pending.front().unwrap().0 <= cur_flush {
                    // We only get an error if the receiver has stopped existing, which we don't care about
                    let _ = self.pending.pop_front().unwrap().1.send(true);
                    count += 1;
                } else {
                    break;
                }
            }

            self.last_flush = cur_flush;
        }

        count
    }
}

impl<const BUFFER_SIZE: usize> Drop for CapLog<BUFFER_SIZE> {
    fn drop(&mut self) {
        self.process_pending();
        let _ = self.trie.storage.borrow_mut().flush();
        let handle = &mut self.data_file.as_ref();
        let _ = handle.flush();
    }
}
