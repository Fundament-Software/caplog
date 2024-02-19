use crate::log_capnp::{log_entry, log_sink};
use crate::offset_io::{OffsetRead, OffsetWrite, ReadSessionBuf};
use crate::ring_buf_writer::RingBufWriter;

use super::hashed_array_trie::{HashedArrayStorage, HashedArrayTrie};
use super::murmur3::murmur3_stream;
use super::sorted_map::SortedMap;
use capnp::message::{ReaderOptions, TypedReader};
use capnp::{data, message};
use eyre::eyre;
use eyre::Result;
use std::alloc;
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Seek, Write};
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
    #[error("Data ID {0} doesn't match Trie ID {1}")]
    Mismatch(u128, u128),
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
const MAX_FILE_SIZE: u64 = 2_u64.pow(28);
const MAX_BUFFER_SIZE: usize = 2_usize.pow(22);
const MAX_HEADER_SIZE: usize = MAX_BUFFER_SIZE / 32;
const MAX_OPEN_FILES: usize = 10;

struct FileManagement {
    max_open_files: usize,
    files: RwLock<SortedMap<u128, Option<File>>>,
    prefix: PathBuf,
}

impl FileManagement {
    pub fn get_data_file(&mut self, id: u128) -> Option<File> {
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
        if let Some(f) = OpenOptions::new().read(true).write(true).open(p).ok() {
            let result = f.try_clone().ok();
            *v = Some(f);
            result
        } else {
            None
        }
    }

    pub fn get_path(&self, id: u128) -> PathBuf {
        let mut path = self.prefix.to_path_buf().into_os_string();
        path.push("_");
        path.push(id.to_string());
        return path.into();
    }

    pub fn new_file(&self, id: u128) -> Result<File> {
        let header = HeaderStart { flags: 0 };
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.get_path(id).as_path())?;

        header.write_at(&mut file, 0);
        file.flush()?;
        Ok(file)
    }

    pub fn find_latest_file(&self) -> Option<u128> {
        // Go through the directory prefix is in to find the highest valued file with the given prefix

        // Convert to a number and return without opening the file
        None
    }
}

struct StagingAlloc {
    staging: Box<[u64]>,
    staging_used: bool,
}

/// A high-performance append-only log
pub struct CapLog {
    max_file_size: u64,
    trie: HashedArrayTrie<u128>,
    //schemas: HashMap<u64, Vec<u8>>,
    staging: StagingAlloc,
    pub data_file: Arc<RingBufWriter<File, MAX_BUFFER_SIZE>>,
    data_position: u64,
    flush_error: AtomicBool, // This is set if there is a failure writing data to disk, which forces all pending writes to return errors
    last_flush: u64,
    current_id: u128,
    archive: FileManagement,
    options: ReaderOptions,
    pending: VecDeque<(u64, SyncSender<bool>)>,
}

unsafe impl<'a> capnp::message::Allocator for StagingAlloc {
    fn allocate_segment(&mut self, minimum_size: u32) -> (*mut u8, u32) {
        assert!(minimum_size as usize <= self.staging.len());
        if self.staging_used || minimum_size as usize > self.staging.len() {
            let layout = alloc::Layout::from_size_align(minimum_size as usize * size_of::<u64>(), 8).unwrap();
            // if this happens in release mode, we can't crash, we have to just handle it anyway
            unsafe { (alloc::alloc_zeroed(layout), minimum_size) }
        } else {
            self.staging_used = true;
            (self.staging.as_mut_ptr() as *mut u8, self.staging.len() as u32)
        }
    }

    unsafe fn deallocate_segment(&mut self, ptr: *mut u8, word_size: u32, words_used: u32) {
        if ptr == self.staging.as_mut_ptr() as *mut u8 {
            (*std::ptr::slice_from_raw_parts_mut(ptr as *mut u64, words_used as usize)).fill(0);
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

impl CapLog {
    pub fn new(
        max_file_size: u64,
        trie_file: &Path,
        data_prefix: &Path,
        max_open_files: usize,
        check_consistency: bool,
    ) -> Result<Self> {
        let trie_storage = if let Some(file) = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(trie_file)
            .ok()
        {
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

        let (mut file, id) = if let Some(id) = archive.find_latest_file() {
            let data_path = archive.get_path(id);
            if let Some(mut file) = OpenOptions::new().read(true).write(true).open(data_path).ok() {
                // Always check for the clean close flag, if that's not set a recovery pass should be done
                let mut headerstartbuf = [0_u8; size_of::<HeaderStart>()];
                file.read_at(&mut headerstartbuf, 0);
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

        let file_clone = file.try_clone()?;

        let mut log = Self {
            max_file_size,
            trie: HashedArrayTrie::new(&storage, 1),
            data_position: file.stream_position()?,
            data_file: Arc::new(RingBufWriter::new(file)),
            staging: StagingAlloc {
                staging: vec![0_u64; MAX_BUFFER_SIZE].into_boxed_slice(),
                staging_used: false,
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

        let session: ReadSessionBuf<'_, File, 4096> = ReadSessionBuf::new(&f, offset);

        let reader = capnp::serialize_packed::read_message(session, self.options)?;
        let message = TypedReader::<_, log_entry::Owned>::new(reader);
        let entry = message.get()?;
        let entry_id = ((entry.get_machine_id() as u128) << 64) | entry.get_snowflake_id() as u128;
        if entry_id != id {
            return Err(CapLogError::Mismatch(entry_id, id).into());
        }

        if check_consistency {
            // TODO
        }

        builder.set_as(entry.get_payload())?;
        Ok(())
    }

    fn reset(&mut self, id: u128) -> Result<()> {
        // We open a new file handle first so that we can swap it into the ring buffer if necessary
        let mut file = self.archive.new_file(self.current_id)?;
        let position = file.stream_position()?;

        if let Ok(mut files) = self.archive.files.write() {
            files.insert(id, file.try_clone().ok());
        }

        if let Ok((mut flusher, state)) = self.data_file.lock_flusher() {
            // Dump current buffer
            flusher.flush_buf::<MAX_BUFFER_SIZE>(state)?;
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

            // We swap the file pointers here and then simply drop this one, because we already have a clone in the archive.
            flusher.swap_inner::<MAX_BUFFER_SIZE>(state, &mut file)?;

            self.trie.storage.borrow_mut().flush()?;
        } else {
            return Err(CapLogError::Unknown.into());
        }

        self.last_flush = 0;
        self.current_id = id;
        self.data_position = position;

        Ok(())
    }

    pub fn check_write_size(&mut self, size: usize) -> bool {
        self.data_file.write_position() + size as u64 > self.max_file_size
    }

    pub fn append(
        &mut self,
        snowflake: u64,
        machine: u64,
        schema: u64,
        payload: capnp::any_pointer::Reader<'_>,
        size: usize,
    ) -> Result<Receiver<bool>> {
        // We do a sanity check here in case the files are empty or the error bit is set.
        if self.flush_error.load(Ordering::Relaxed) {
            return Err(eyre!("Log filesystem is in bad state!"));
        }

        // TODO: Check to see if this is an old insert, which requires us to redirect it to an older file
        let id = ((machine as u128) << 64) | snowflake as u128;

        // Check if we need to update flush status - this MUST be checked before a potential reset or we lose the flush progress!
        self.process_pending();

        // Check if we need to reset to a new file
        if self.check_write_size(size) {
            self.reset(id)?;
        }

        let mut message = message::Builder::new(&mut self.staging);

        let mut builder: log_entry::Builder<'_> = message.init_root();
        builder.set_snowflake_id(snowflake);
        builder.set_machine_id(machine);
        builder.set_schema(schema);
        builder.get_payload().set_as(payload)?;

        let position = self.data_file.write_position();
        let bypass_arc = unsafe {
            let extremely_unsafe: *const RingBufWriter<File, MAX_BUFFER_SIZE> = &*self.data_file.as_ref();
            &mut *(extremely_unsafe as *mut RingBufWriter<File, MAX_BUFFER_SIZE>)
        };
        capnp::serialize_packed::write_message(&mut *bypass_arc, &message)?;
        self.trie.insert(id, position)?;

        // Asyncronously calculate the hash on the packed data stream and write it into our reserved slot
        //let hash = murmur3_stream(data_file.view(start, end - start)?, end - start, 0)?;
        let hash = 0_u128;
        bypass_arc.write(&hash.to_le_bytes())?;

        let (sender, receiver) = sync_channel(1);

        self.pending.push_back((self.data_file.write_position(), sender));
        Ok(receiver)
    }

    // Every time this is called, write all data that is ready to be flushed to disk
    pub fn flush(&mut self) -> Result<()> {
        Ok(self.data_file.flush_atomic()?)
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

impl Drop for CapLog {
    fn drop(&mut self) {
        self.process_pending();
        let _ = self.trie.storage.borrow_mut().flush();
        let _ = self.data_file.flush_atomic();
    }
}
