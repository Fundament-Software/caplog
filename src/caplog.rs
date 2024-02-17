use crate::log_capnp::{log_entry, log_sink};
use crate::ring_buf_writer::RingBufWriter;

use super::hashed_array_trie::{HashedArrayStorage, HashedArrayTrie};
use super::murmur3::murmur3_stream;
use super::sorted_map::SortedMap;
use capnp::message::ReaderOptions;
use capnp::{data, message};
use eyre::eyre;
use eyre::Result;
use std::alloc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::io::{Cursor, Seek, Write};
use std::mem::size_of;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CapLogError {
    #[error("value {v:?} is out of bounds: [{minimum:?}-{maximum:?}]")]
    OutOfBounds { v: u64, minimum: u64, maximum: u64 },
    #[error("Length of data {0} exceeds data stream length by {1}")]
    InvalidBlock(u64, u64),
    #[error("Expected schema hash {0} but found {1}")]
    InvalidSchema(u64, u64),
    #[error("Data failed integrity check (got {0} but expected {1})")]
    CorruptData(u128, u128),
    #[error("the data for key `{0}` is not available")]
    NotFound(String),
    #[error("Tried to allocate a buffer for {0} bytes but only had {1} left")]
    NotEnoughBuffer(usize, usize),
    #[error("File I/O error encountered or filesystem in bad state")]
    FileError,
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
    pub fn write<T>(&self, target: &mut T) -> Result<()>
    where
        T: std::io::Write,
    {
        target.write_all(&u64::to_le_bytes(self.flags))?;
        Ok(())
    }
}

/// Maximum size of a data file. When this is reached, both the header file and data file are finalized,
/// the in-memory vectors cleared, and a new data and header file are created
const MAX_FILE_SIZE: u64 = 2_u64.pow(28);
const MAX_BUFFER_SIZE: usize = 2_usize.pow(22);
const MAX_HEADER_SIZE: usize = MAX_BUFFER_SIZE / 32;
const MAX_OPEN_FILES: usize = 10;

#[derive(Debug)]
struct FileManagement {
    max_open_files: usize,
    files: SortedMap<u128, Option<RingBufWriter<File, 4096>>>,
}

impl FileManagement {
    pub fn get_data_file(&self, offset: u64) -> Result<(BufReader<File>, usize)> {
        // BufReader::new(file);
        Err(CapLogError::Unknown.into())
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
    data_prefix: PathBuf,
    data_file: Option<RingBufWriter<File, MAX_BUFFER_SIZE>>,
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
        let mut log = Self {
            max_file_size,
            trie: HashedArrayTrie::new(&storage, 1),
            data_prefix: data_prefix.to_path_buf(),
            data_file: None,
            data_position: 0,
            staging: StagingAlloc {
                staging: vec![0_u64; MAX_BUFFER_SIZE].into_boxed_slice(),
                staging_used: false,
            },
            flush_error: AtomicBool::new(false),
            last_flush: 0,
            current_id: 0,
            archive: FileManagement {
                max_open_files,
                files: SortedMap::new(),
            },
            options: ReaderOptions {
                traversal_limit_in_words: None,
                nesting_limit: 128,
            },
            pending: VecDeque::new(),
        };

        if let Some(id) = Self::find_latest_file(&data_prefix) {
            let data_path = Self::assemble_path(data_prefix, id);
            if let Some(mut file) = OpenOptions::new().read(true).write(true).open(data_path).ok() {
                if check_consistency {
                    // TODO: Go through file and check all the hashes

                    // We can assume that the data is a struct of some kind
                    //let value_reader: capnp::dynamic_value::Reader<'_> = payload.into();
                    //let struct_reader = value_reader.downcast::<dynamic_struct::Reader<'_>>();
                    //let payload_size = struct_reader.total_size()?;
                }

                file.seek(std::io::SeekFrom::End(0))?;
                log.data_position = file.stream_position()?;
                log.data_file = Some(RingBufWriter::new(file));

                return Ok(log);
            }
        }

        log.reset(0);

        Ok(log)
    }

    fn assemble_path(prefix: &Path, id: u128) -> PathBuf {
        let mut path = prefix.to_path_buf().into_os_string();
        path.push("_");
        path.push(id.to_string());
        return path.into();
    }

    fn find_latest_file(prefix: &Path) -> Option<u128> {
        // Go through the directory prefix is in to find the highest valued file with the given prefix

        // Convert to a number and return without opening the file
        None
    }

    #[inline]
    fn get_internal<R, F1, F2>(
        &self,
        offset: u64,
        length: u64,
        hash: Option<u128>,
        callback: F1,
        file_callback: F2,
    ) -> Result<R>
    where
        F1: FnOnce(&Self, &[u8]) -> Result<R>,
        F2: FnOnce(&Self, BufReader<File>, u64) -> Result<R>,
    {
        if offset > self.data_position {
            return Err(CapLogError::OutOfBounds {
                v: offset,
                minimum: 0,
                maximum: self.data_position,
            }
            .into());
        }

        // Are we querying our current in-progress file that's still in memory?
        /*if offset >= self.base_data_offset {
            let data_offset: usize = (offset - self.base_data_offset) as usize;
            let slice = &self.data[data_offset..(data_offset + length as usize)];

            // If we requested an integrity check, hash the data and compare with the expected value
            if let Some(expected) = hash {
                let result = fastmurmur3::hash(slice);
                if expected != result {
                    return Err(CapLogError::CorruptData(result, expected).into());
                }
            }

            return Ok(callback(&self, slice)?);
        }*/

        // Okay, try to find the file
        let (mut reader, data_offset) = self.archive.get_data_file(offset)?;

        // Seek to the message location
        reader.seek_relative(data_offset as i64)?;

        // If we requested an integrity check, hash the data (using a slightly different codepath for
        // files) and compare it with the expected value
        if let Some(expected) = hash {
            let result = murmur3_stream(&mut reader, length as usize, 0)?;
            if expected != result {
                return Err(CapLogError::CorruptData(result, expected).into());
            }
            // Reset reader by the negative length
            reader.seek_relative(-(length as i64))?;
        }

        return Ok(file_callback(&self, reader, length)?);
    }

    #[inline]
    pub fn get_message(
        &self,
        offset: u64,
        length: u64,
        hash: Option<u128>,
    ) -> Result<message::Reader<capnp::serialize::OwnedSegments>> {
        self.get_internal(
            offset,
            length,
            hash,
            |s: &Self, slice: &[u8]| Ok(capnp::serialize_packed::read_message(Cursor::new(slice), s.options)?),
            |s: &Self, reader: BufReader<File>, _: u64| Ok(capnp::serialize_packed::read_message(reader, s.options)?),
        )
    }

    pub fn get_packed<F1, F2>(
        &self,
        offset: u64,
        length: u64,
        callback: F1,
        file_callback: F2,
        hash: Option<u128>,
    ) -> Result<()>
    where
        F1: FnOnce(&[u8]) -> Result<()>,
        F2: FnOnce(BufReader<File>, u64) -> Result<()>,
    {
        self.get_internal(
            offset,
            length,
            hash,
            |s: &CapLog, slice: &[u8]| Ok(callback(slice)?),
            |s: &CapLog, reader: BufReader<File>, len: u64| Ok(file_callback(reader, len)?),
        )
    }

    fn reset(&mut self, id: u128) -> Result<()> {
        // We open a new file handle first so that we can swap it into the ring buffer if necessary
        let mut headerstartbuf: [u8; size_of::<HeaderStart>()] = [0; size_of::<HeaderStart>()];
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(Self::assemble_path(&self.data_prefix, self.current_id).as_path())?;

        file.write(&mut headerstartbuf)?;
        file.flush()?;
        let position = file.stream_position()?;

        if let Some(data_file) = self.data_file.as_mut() {
            if let Ok((mut flusher, state)) = data_file.lock_flusher() {
                // Dump current buffer
                flusher.flush_buf::<MAX_BUFFER_SIZE>(state)?;
                flusher.get_mut().flush()?;

                //  Write clean header flag
                flusher.get_ref().seek(std::io::SeekFrom::Start(0))?;
                HeaderStart {
                    flags: HeaderFlags::Clean as u64,
                }
                .write(flusher.get_mut())?;

                // Finalize old file
                if let Err(e) = flusher.get_mut().flush() {
                    self.flush_error.store(true, Ordering::Release);
                    return Err(e.into());
                }

                // Move the old file into the archive because there will likely be some stragglers to write
                flusher.swap_inner::<MAX_BUFFER_SIZE>(state, &mut file)?;
                self.archive
                    .files
                    .insert(self.current_id, Some(RingBufWriter::new(file)));

                self.trie.storage.borrow_mut().flush()?;
            } else {
                return Err(CapLogError::Unknown.into());
            }
        } else {
            self.data_file = Some(RingBufWriter::new(file));
        }

        self.last_flush = 0;
        self.current_id = id;
        self.data_position = position;

        Ok(())
    }

    pub fn check_write_size(&mut self, size: usize) -> Result<bool> {
        if let Some(data_file) = self.data_file.as_mut() {
            Ok(data_file.write_position() + size as u64 > self.max_file_size)
        } else {
            Ok(false)
        }
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
        if self.data_file.is_some() {
            if self.check_write_size(size)? {
                self.reset(id)?;
            }
        }

        if let Some(data_file) = self.data_file.as_mut() {
            let mut message = message::Builder::new(&mut self.staging);

            let mut builder: log_entry::Builder<'_> = message.init_root();
            builder.set_snowflake_id(snowflake);
            builder.set_machine_id(machine);
            builder.set_schema(schema);
            builder.get_payload().set_as(payload)?;

            let position = data_file.write_position();
            capnp::serialize_packed::write_message(&mut *data_file, &message)?;
            self.trie.insert(id, position)?;

            // Asyncronously calculate the hash on the packed data stream and write it into our reserved slot
            //let hash = murmur3_stream(data_file.view(start, end - start)?, end - start, 0)?;
            let hash = 0_u128;
            data_file.write(&hash.to_le_bytes())?;

            let (sender, receiver) = sync_channel(1);

            self.pending.push_back((data_file.write_position(), sender));
            Ok(receiver)
        } else {
            Err(CapLogError::FileError.into())
        }
    }

    // Every time this is called, write all data that is ready to be flushed to disk
    pub fn flush(&mut self) -> Result<()> {
        if let Some(data_file) = self.data_file.as_mut() {
            data_file.flush()?;
        }
        Ok(())
    }

    pub fn process_pending(&mut self) {
        if let Some(data_file) = self.data_file.as_ref() {
            let cur_flush = data_file.flush_position();
            if self.last_flush < cur_flush {
                while !self.pending.is_empty() {
                    if self.pending.front().unwrap().0 <= cur_flush {
                        // We only get an error if the receiver has stopped existing, which we don't care about
                        let _ = self.pending.pop_front().unwrap().1.send(true);
                    } else {
                        break;
                    }
                }

                self.last_flush = cur_flush;
            }
        }
    }
}

impl Drop for CapLog {
    fn drop(&mut self) {
        self.process_pending();
        let _ = self.trie.storage.borrow_mut().flush();
        if let Some(data_file) = self.data_file.as_mut() {
            let _ = data_file.flush();
        }
    }
}
