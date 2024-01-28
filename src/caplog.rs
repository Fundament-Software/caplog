use crate::atomic_ring_buffer::AtomicRingBuffer;

use super::murmur3::murmur3_stream;
use super::sorted_map::SortedMap;
use capnp::io::Write;
use capnp::message;
use capnp::message::ReaderOptions;
use eyre::eyre;
use eyre::Result;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::io::Seek;
use std::io::{BufReader, Read};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    #[error("unknown caplog error")]
    Unknown,
}

#[derive(Clone, Debug, Copy, Default)]
pub struct HeaderBlock {
    id: u64,
    schema: u64,
    data: u64,
    length: u64,
    hash: u128,
}

impl HeaderBlock {
    const BYTESIZE: usize = (u64::BITS * 6) as usize / 8;

    #[inline]
    pub fn new(bytes: &[u8]) -> Result<HeaderBlock> {
        if bytes.len() < Self::BYTESIZE as usize {
            return Err(CapLogError::OutOfBounds {
                v: bytes.len() as u64,
                minimum: Self::BYTESIZE as u64,
                maximum: u64::MAX,
            }
            .into());
        }
        Ok(HeaderBlock {
            id: u64::from_le_bytes(bytes[0..8].try_into()?),
            schema: u64::from_le_bytes(bytes[8..16].try_into()?),
            data: u64::from_le_bytes(bytes[16..24].try_into()?),
            length: u64::from_le_bytes(bytes[24..32].try_into()?),
            hash: u128::from_le_bytes(bytes[32..48].try_into()?),
        })
    }

    #[inline]
    pub fn write<T>(&self, target: &mut T) -> Result<()>
    where
        T: std::io::Write,
    {
        target.write_all(&u64::to_le_bytes(self.id))?;
        target.write_all(&u64::to_le_bytes(self.schema))?;
        target.write_all(&u64::to_le_bytes(self.data))?;
        target.write_all(&u64::to_le_bytes(self.length))?;
        target.write_all(&u128::to_le_bytes(self.hash))?;
        Ok(())
    }

    #[inline]
    pub fn write_bytes(&self, target: &mut [u8]) {
        target[0..8].copy_from_slice(&u64::to_le_bytes(self.id));
        target[8..16].copy_from_slice(&u64::to_le_bytes(self.id));
        target[16..24].copy_from_slice(&u64::to_le_bytes(self.id));
        target[24..32].copy_from_slice(&u64::to_le_bytes(self.id));
        target[32..48].copy_from_slice(&u64::to_le_bytes(self.id));
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum HeaderFlags {
    Sorted = 0b001,
    Finalized = 0b010, // We can still add things to a "finalized" file, we just have then recalculate the start section
    ValidHash = 0b100, // Hash is never valid if we are actively adding things
}

#[derive(Clone, Debug, Copy, Default)]
struct HeaderStart {
    blocks: u64, // number of header blocks NOT including this one, so should be (file size / sizeof(HeaderBlock)) - 1
    begin: u64,
    end: u64,
    flags: u64,
    digest: u128, // Hash of the entire rest of the file
}

impl HeaderStart {
    const BYTESIZE: usize = (u64::BITS * 6) as usize / 8;

    #[inline]
    pub fn new(bytes: &[u8]) -> Result<HeaderStart> {
        if bytes.len() < Self::BYTESIZE {
            return Err(CapLogError::OutOfBounds {
                v: bytes.len() as u64,
                minimum: Self::BYTESIZE as u64,
                maximum: u64::MAX,
            }
            .into());
        }
        Ok(HeaderStart {
            blocks: u64::from_le_bytes(bytes[0..8].try_into()?),
            begin: u64::from_le_bytes(bytes[8..16].try_into()?),
            end: u64::from_le_bytes(bytes[16..24].try_into()?),
            flags: u64::from_le_bytes(bytes[24..32].try_into()?),
            digest: u128::from_le_bytes(bytes[32..48].try_into()?),
        })
    }

    #[inline]
    pub fn write<T>(&self, target: &mut T) -> Result<()>
    where
        T: std::io::Write,
    {
        target.write_all(&u64::to_le_bytes(self.blocks))?;
        target.write_all(&u64::to_le_bytes(self.begin))?;
        target.write_all(&u64::to_le_bytes(self.end))?;
        target.write_all(&u64::to_le_bytes(self.flags))?;
        target.write_all(&u128::to_le_bytes(self.digest))?;
        Ok(())
    }
}

// Ensure headerblock and headerstart are the same size
const _: fn() = || {
    let _ = std::mem::transmute::<HeaderBlock, HeaderStart>;
};

/// Maximum size of a data file. When this is reached, both the header file and data file are finalized,
/// the in-memory vectors cleared, and a new data and header file are created
const MAX_DATA_SIZE: u64 = 2_u64.pow(28);
const MAX_BUFFER_SIZE: usize = 2_usize.pow(22);
const MAX_HEADER_SIZE: usize = MAX_BUFFER_SIZE / 32;
const FILE_KEEPALIVE: u64 = 10000; // milliseconds of inactivity to keep a file open for lookup operations.
const FILE_SORTDELAY: u64 = FILE_KEEPALIVE; // milliseconds of inactivity before sorting a header block file.

#[derive(Debug)]
struct FileManagement {
    keepalive: u64,
    sortdelay: u64,
    files: SortedMap<u64, Option<File>>,
}

impl FileManagement {
    pub fn get_data_file(&self, offset: u64) -> Result<(BufReader<File>, usize)> {
        // BufReader::new(file);
        Err(CapLogError::Unknown.into())
    }
}

/// A high-performance append-only log
#[derive(Debug)]
pub struct CapLog {
    header_prefix: PathBuf,
    data_prefix: PathBuf,
    max_data_size: u64,
    header_file: Option<File>, // These CAN be empty if filesystem errors occur, such as the disk being full or a read-only filesystem due a degraded RAID state.
    data_file: Option<File>,
    //schemas: HashMap<u64, Vec<u8>>,
    header_count: u64,
    data_offset: u64,
    headers: AtomicRingBuffer<u8, MAX_BUFFER_SIZE>, // This stores already serialized bytes ready to be dumped into a file.
    data: AtomicRingBuffer<u8, MAX_BUFFER_SIZE>,
    finalizing: AtomicBool,
    error_flag: AtomicBool, // This is set if there is a failure writing data to disk, which forces all pending writes to return errors
    pending_count: AtomicU64,
    archive: FileManagement,
    options: ReaderOptions,
}

impl CapLog {
    pub fn new(
        max_data_size: u64,
        header_prefix: &Path,
        data_prefix: &Path,
        keepalive: u64,
        sortdelay: u64,
        check_consistency: bool,
    ) -> Result<CapLog> {
        let mut log = CapLog {
            header_prefix: header_prefix.to_path_buf(),
            data_prefix: data_prefix.to_path_buf(),
            max_data_size,
            header_file: None,
            data_file: None,
            header_count: 0,
            data_offset: 0,
            headers: AtomicRingBuffer::new(),
            data: AtomicRingBuffer::new(),
            finalizing: AtomicBool::new(false),
            error_flag: AtomicBool::new(false),
            pending_count: AtomicU64::new(0),
            archive: FileManagement {
                keepalive,
                sortdelay,
                files: SortedMap::new(),
            },
            options: ReaderOptions {
                traversal_limit_in_words: None,
                nesting_limit: 128,
            },
        };

        if let Some(id) = Self::find_latest_file(&log.header_prefix) {
            let data_path = Self::assemble_path(data_prefix, id);
            let header_path = Self::assemble_path(header_prefix, id);
            let mut headerstartbuf: [u8; HeaderStart::BYTESIZE] = [0; HeaderStart::BYTESIZE];
            let headerlength = {
                let mut file = File::open(&header_path)?;
                file.read_exact(&mut headerstartbuf)?;
                file.seek(std::io::SeekFrom::End(0))?
            };

            let header_start = HeaderStart::new(&headerstartbuf)?;

            // We try to be conservative here - if the reported block count is less than the calculated block
            // count, we use the reported count. Otherwise, we use the calculated block count.
            log.header_count = std::cmp::min(
                header_start.blocks,
                (headerlength - HeaderStart::BYTESIZE as u64) / HeaderBlock::BYTESIZE as u64,
            );
            log.data_file = Some(OpenOptions::new().append(true).open(data_path)?);
            log.header_file = Some(OpenOptions::new().append(true).open(header_path)?);
        } else {
            log.header_file = Some(Self::init_header_file(header_prefix, 0)?);
            log.data_file = Some(File::create(Self::assemble_path(data_prefix, 0))?);
        }

        Ok(log)
    }
    fn assemble_path(prefix: &Path, id: u64) -> PathBuf {
        let mut path = prefix.to_path_buf().into_os_string();
        path.push("_");
        path.push(id.to_string());
        return path.into();
    }
    fn init_header_file(prefix: &Path, id: u64) -> Result<File> {
        let mut f = File::create(Self::assemble_path(prefix, id))?;
        f.write_all(&[0; HeaderStart::BYTESIZE])?;
        return Ok(f);
    }

    fn find_latest_file(prefix: &Path) -> Option<u64> {
        // Go through the directory prefix is in to find the highest valued file with the given prefix

        // Convert to a number and return without opening the file
        None
    }
    pub fn get_header(&self, id: u64) -> Result<HeaderBlock> {
        Err(CapLogError::Unknown.into())
    }
    pub fn get(
        &self,
        id: u64,
        schema: Option<u64>,
        integrity_check: bool,
    ) -> Result<message::Reader<capnp::serialize::OwnedSegments>> {
        let header = self.get_header(id)?;
        if let Some(schema_id) = schema {
            if schema_id != header.schema {
                return Err(CapLogError::InvalidSchema(schema_id, header.schema).into());
            }
        }
        self.get_message(
            header.data,
            header.length,
            if integrity_check {
                Some(header.hash)
            } else {
                None
            },
        )
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
        F1: FnOnce(&CapLog, &[u8]) -> Result<R>,
        F2: FnOnce(&CapLog, BufReader<File>, u64) -> Result<R>,
    {
        if offset > self.data_offset {
            return Err(CapLogError::OutOfBounds {
                v: offset,
                minimum: 0,
                maximum: self.data_offset,
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
            |s: &CapLog, slice: &[u8]| {
                Ok(capnp::serialize_packed::read_message(
                    Cursor::new(slice),
                    s.options,
                )?)
            },
            |s: &CapLog, reader: BufReader<File>, _: u64| {
                Ok(capnp::serialize_packed::read_message(reader, s.options)?)
            },
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

    pub fn reset() {
        // Set global lock

        // Finalize old file and open a new one

        // after setting up the new files, dump everything in our buffers into them

        // Now that our ring buffers are empty, we can reset them
    }
    pub fn append(&mut self, id: u64, machine_id: u64, schema: u64, data: &[u8]) -> Result<()> {
        // We do a sanity check here in case the files are empty or the error bit is set.
        if self.data_file.is_none()
            || self.header_file.is_none()
            || self.error_flag.load(Ordering::Relaxed)
        {
            return Err(eyre!("Log filesystem is in bad state!"));
        }

        // TODO: Check to see if this is an old insert, which requires us to redirect it to an older file

        // First we insert the data into the ring buffer
        let position = self.data.append(data)?;
        let mut block: [u8; HeaderBlock::BYTESIZE] = [0; HeaderBlock::BYTESIZE];

        HeaderBlock {
            id,
            schema,
            data: position as u64,
            length: data.len() as u64,
            hash: fastmurmur3::hash(data),
        }
        .write_bytes(&mut block);

        {
            // Increment pending so the error handler knows when we have bailed out
            self.pending_count.fetch_add(1, Ordering::Release);

            let end = position + data.len();

            // Now we wait until our data has been flushed to disk
            while self.data.read_location() < end {
                if self.error_flag.load(Ordering::Relaxed) {
                    self.pending_count.fetch_sub(1, Ordering::Release);
                    return Err(eyre!("File flush error!"));
                }

                // TODO: replace with a full async yield, since flushing to disk will almost certainly require yielding.
                std::hint::spin_loop();
            }
            self.pending_count.fetch_sub(1, Ordering::Release);
        }

        // Then we insert into the header ring buffer. In the extremely rare case this fails, we just bail out and abandon our written data.
        let end = self.headers.append(&block)? + HeaderBlock::BYTESIZE;

        self.pending_count.fetch_add(1, Ordering::Release);

        // Now we wait until our header block has been flushed to disk
        while self.headers.read_location() < end {
            if self.error_flag.load(Ordering::Relaxed) {
                self.pending_count.fetch_sub(1, Ordering::Release);
                return Err(eyre!("File flush error!"));
            }

            // TODO: replace with a full async yield, since flushing to disk will almost certainly require yielding.
            std::hint::spin_loop();
        }

        self.pending_count.fetch_sub(1, Ordering::Release);
        Ok(())
    }

    // Every time this is called, write all data that is ready to be flushed to disk
    pub fn flush(&mut self) -> Result<()> {
        if let Some(file) = &mut self.data_file {
            if let Err(e) = self.data.process(|x| Ok(file.write_all(x)?)) {
                self.error_flag.swap(true, Ordering::Relaxed);

                // Spin until all pending writes have finished failing
                while self.pending_count.load(Ordering::Relaxed) > 0 {
                    std::hint::spin_loop();
                }

                return Err(e);
            }
        } else {
            return Err(eyre!("Invalid data file!"));
        }

        if let Some(file) = &mut self.header_file {
            if let Err(e) = self.headers.process(|x| Ok(file.write_all(x)?)) {
                self.error_flag.swap(true, Ordering::Relaxed);

                // Spin until all pending writes have finished failing
                while self.pending_count.load(Ordering::Relaxed) > 0 {
                    std::hint::spin_loop();
                }

                return Err(e);
            }
        } else {
            return Err(eyre!("Invalid header file!"));
        }

        Ok(())
    }
}
