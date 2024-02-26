use std::cmp::min;
use std::fs::File;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::mem::MaybeUninit;

pub trait OffsetRead {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    fn read_all_at(&mut self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to read whole buffer"));
                }
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }

                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl OffsetRead for &[u8] {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        Ok(self.get(offset as usize..).map_or(0, |r| {
            let n = min(r.len(), buf.len());
            buf[..n].copy_from_slice(&r[..n]);
            n
        }))
    }
}

impl OffsetRead for File {
    /// Uses `std::os::unix::fs::FileExt::read_at()` (aka `pread()`) on unix
    /// and `std::os::windows::fs::FileExt::seek_read()` on windows.
    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::prelude::FileExt;
        FileExt::read_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::prelude::FileExt;
        FileExt::seek_read(self, buf, offset)
    }
}

#[cfg(unix)]
impl<R: ?Sized + std::os::unix::fs::FileExt> OffsetRead for BufReader<R> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::prelude::FileExt;
        FileExt::read_at(self.get_ref(), buf, offset)
    }
}

#[cfg(windows)]
impl<R: ?Sized + std::os::windows::fs::FileExt> OffsetRead for BufReader<R> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::prelude::FileExt;
        FileExt::seek_read(self.get_ref(), buf, offset)
    }
}

pub trait OffsetWrite {
    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize>;
    fn flush_all(&mut self) -> io::Result<()>;
    fn write_all_at(&mut self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write_at(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer"));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }

                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl OffsetWrite for &mut [u8] {
    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
        Ok(self.get_mut(offset as usize..).map_or(0, |r| {
            let n = min(r.len(), buf.len());
            r[..n].copy_from_slice(&buf[..n]);
            n
        }))
    }
    fn flush_all(&mut self) -> io::Result<()> {
        self.flush()
    }
}

impl OffsetWrite for File {
    /// For convenience, we also expose write_at (for File), because
    /// code that needs to read_at might want to write_at.
    ///
    /// Uses `std::os::unix::prelude::FileExt::write_at` and
    /// `std::os::windows::prelude::FileExt::seek_write`.
    #[cfg(unix)]
    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::prelude::FileExt;
        FileExt::write_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::prelude::FileExt;
        FileExt::seek_write(self, buf, offset)
    }

    fn flush_all(&mut self) -> io::Result<()> {
        self.flush()
    }
}

pub struct ReadSessionBuf<'a, R: OffsetRead, const SIZE: usize> {
    offset: u64,
    inner: &'a R,
    buf: MaybeUninit<[u8; SIZE]>,
    pos: usize,
    filled: usize,
}

impl<'a, R: OffsetRead, const SIZE: usize> ReadSessionBuf<'a, R, SIZE> {
    pub fn new(wrap: &'a R, offset: u64) -> Self {
        Self {
            inner: wrap,
            offset,
            buf: MaybeUninit::uninit(),
            pos: 0,
            filled: 0,
        }
    }

    #[inline]
    pub fn get_position(&self) -> u64 {
        self.pos as u64
    }

    #[inline]
    pub fn buffer(&self) -> &[u8] {
        // SAFETY: self.pos and self.cap are valid, and self.cap => self.pos, and
        // that region is initialized because those are all invariants of this type.
        unsafe { self.buf.assume_init_ref().get_unchecked(self.pos..self.filled) }
    }

    #[inline]
    pub fn discard_buffer(&mut self) {
        self.pos = 0;
        self.filled = 0;
    }

    /// If there are `amt` bytes available in the buffer, pass a slice containing those bytes to
    /// `visitor` and return true. If there are not enough bytes available, return false.
    #[inline]
    pub fn consume_with<V>(&mut self, amt: usize, mut visitor: V) -> bool
    where
        V: FnMut(&[u8]),
    {
        if let Some(claimed) = self.buffer().get(..amt) {
            visitor(claimed);
            // If the indexing into self.buffer() succeeds, amt must be a valid increment.
            self.pos += amt;
            true
        } else {
            false
        }
    }
}

impl<'a, R: OffsetRead, const SIZE: usize> Read for &'_ mut ReadSessionBuf<'a, R, SIZE> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.pos == self.filled && buf.len() >= SIZE {
            self.discard_buffer();
            return self.inner.read_at(buf, self.offset).map(|x| {
                self.offset += x as u64;
                x
            });
        }
        let mut rem = self.fill_buf()?;
        let nread = rem.read(buf)?;
        self.consume(nread);
        Ok(nread)
    }

    // Small read_exacts from a BufReader are extremely common when used with a deserializer.
    // The default implementation calls read in a loop, which results in surprisingly poor code
    // generation for the common path where the buffer has enough bytes to fill the passed-in
    // buffer.
    fn read_exact(&mut self, mut buf: &mut [u8]) -> io::Result<()> {
        if self.consume_with(buf.len(), |claimed| buf.copy_from_slice(claimed)) {
            return Ok(());
        }

        while !buf.is_empty() {
            match self.read(buf) {
                Ok(0) => break,
                Ok(n) => {
                    buf = &mut buf[n..];
                }
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        } else {
            Ok(())
        }
    }
}

impl<'a, R: OffsetRead, const SIZE: usize> BufRead for &'_ mut ReadSessionBuf<'a, R, SIZE> {
    #[inline]
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // If we've reached the end of our internal buffer then we need to fetch
        // some more data from the reader.
        // Branch using `>=` instead of the more correct `==`
        // to tell the compiler that the pos..cap slice is always valid.
        if self.pos >= self.filled {
            debug_assert!(self.pos == self.filled);

            unsafe {
                self.filled = self.inner.read_at(self.buf.assume_init_mut(), self.offset).map(|x| {
                    self.offset += x as u64;
                    x
                })?;
            }

            self.pos = 0;
        }
        Ok(self.buffer())
    }

    #[inline]
    fn consume(&mut self, amt: usize) {
        self.pos = std::cmp::min(self.pos + amt, self.filled);
    }
}
