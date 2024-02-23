use crate::offset_io::{OffsetRead, OffsetWrite};
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

// Defines a simple fake file because the mocks library is too heavy handed for this.
struct FakeFileInner {
    inner: Vec<u8>,
    position: u64,
}

impl FakeFileInner {
    fn write_inner(&mut self, buf: &[u8], position: Option<u64>) -> usize {
        let pos = position.unwrap_or(self.position) as usize;
        let end = buf.len() + pos;
        if end > self.inner.len() {
            self.inner.resize(end, 0);
        }
        self.inner.as_mut_slice()[pos..end].copy_from_slice(buf);
        if position.is_none() {
            self.position = end as u64;
        }
        buf.len()
    }

    fn read_inner(&mut self, buf: &mut [u8], position: Option<u64>) -> usize {
        let pos = position.unwrap_or(self.position) as usize;
        let end = std::cmp::min(buf.len() + pos, self.inner.len());
        let len = end - pos;
        buf[..len].copy_from_slice(&self.inner[pos..end]);
        if position.is_none() {
            self.position = end as u64;
        }
        len
    }
}

pub struct FakeFile {
    inner: Arc<Mutex<FakeFileInner>>,
}

impl FakeFile {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeFileInner {
                inner: Vec::new(),
                position: 0,
            })),
        }
    }
    pub fn try_clone(&self) -> Result<Self> {
        Ok(FakeFile {
            inner: self.inner.clone(),
        })
    }
}

impl Write for FakeFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if let Ok(mut this) = self.inner.lock() {
            Ok(this.write_inner(buf, None))
        } else {
            Err(Error::new(ErrorKind::PermissionDenied, "couldn't acquire lock"))
        }
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl OffsetWrite for FakeFile {
    fn write_at(&mut self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        if let Ok(mut this) = self.inner.lock() {
            Ok(this.write_inner(buf, Some(offset)))
        } else {
            Err(Error::new(ErrorKind::PermissionDenied, "couldn't acquire lock"))
        }
    }

    fn flush_all(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for FakeFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Ok(mut this) = self.inner.lock() {
            Ok(this.read_inner(buf, None))
        } else {
            Err(Error::new(ErrorKind::PermissionDenied, "couldn't acquire lock"))
        }
    }
}

impl OffsetRead for FakeFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        if let Ok(mut this) = self.inner.lock() {
            Ok(this.read_inner(buf, Some(offset)))
        } else {
            Err(Error::new(ErrorKind::PermissionDenied, "couldn't acquire lock"))
        }
    }
}

impl Seek for FakeFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        if let Ok(mut this) = self.inner.lock() {
            let n = match pos {
                SeekFrom::Start(n) => n as i64,
                SeekFrom::Current(n) => this.position as i64 + n,
                SeekFrom::End(n) => this.inner.len() as i64 + n,
            };

            if n.is_negative() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "tried to seek before start of file",
                ));
            }

            this.position = n as u64;
            if this.position > this.inner.len() as u64 {
                this.inner.resize(n as usize, 0);
            }

            Ok(this.position)
        } else {
            Err(Error::new(ErrorKind::PermissionDenied, "couldn't acquire lock"))
        }
    }
}
