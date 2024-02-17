use std::cell::UnsafeCell;
use std::io::{IoSlice, Read, Result, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{LockResult, Mutex, MutexGuard};

pub struct RingBufFlusher<W: ?Sized + Write> {
    panicked: bool,
    inner: W,
}

pub struct RingBufState {
    buf: UnsafeCell<Box<[u8]>>,
    start: AtomicUsize,
    marker: AtomicUsize,
    flush_head: AtomicU64,
    write_head: AtomicU64,
}
/// This is a ring-buffered writer with an atomic write marker that allows
/// one thread to write and one thread to flush to disk.
pub struct RingBufWriter<W: ?Sized + Write, const POW2_SIZE: usize> {
    state: RingBufState,
    flusher: Mutex<RingBufFlusher<W>>,
}

impl<W: Write + Seek, const POW2_SIZE: usize> RingBufWriter<W, POW2_SIZE> {
    pub fn new(mut inner: W) -> RingBufWriter<W, POW2_SIZE> {
        RingBufWriter {
            state: RingBufState {
                buf: UnsafeCell::new(vec![0_u8; POW2_SIZE].into_boxed_slice()),
                start: AtomicUsize::new(0),
                marker: AtomicUsize::new(0),
                flush_head: AtomicU64::new(0),
                write_head: AtomicU64::new(inner.stream_position().unwrap()),
            },
            flusher: Mutex::new(RingBufFlusher { inner, panicked: false }),
        }
    }
}

impl<W: Write + Seek> RingBufFlusher<W> {
    pub fn swap_inner<const SIZE: usize>(&mut self, state: &mut RingBufState, other: &mut W) -> Result<()> {
        let position = other.stream_position()?;
        std::mem::swap(&mut self.inner, other);
        state
            .write_head
            .store(state.len::<SIZE>() as u64 + position, Ordering::Release);
        state.flush_head.store(0, Ordering::Release);
        Ok(())
    }
}

impl RingBufState {
    #[inline]
    pub fn len<const SIZE: usize>(&self) -> usize {
        let start = self.start.load(Ordering::Acquire);
        let end = self.marker.load(Ordering::Acquire);
        if start > end {
            SIZE - start + end
        } else {
            end - start
        }
    }
}
fn mutex_lock_err() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, "Could not acquire mutex!")
}

impl<W: ?Sized + Write> RingBufFlusher<W> {
    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Flush all data up to the commit marker into our writer. We ignore any data that
    /// hasn't been committed to, because we allow writers to manipulate data they've
    /// written but not committed.
    #[must_use]
    pub fn flush_buf<const SIZE: usize>(&mut self, state: &mut RingBufState) -> Result<()> {
        let mut start = state.start.load(Ordering::Acquire);
        let marker = state.marker.load(Ordering::Acquire);
        let buf = state.buf.get_mut();
        while start != marker {
            self.panicked = true;
            let r = self.inner.write(if start > marker {
                &buf[start..]
            } else {
                &buf[start..marker]
            });
            self.panicked = false;

            match r {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write the buffered data",
                    ));
                }
                Ok(n) => start = (start + n) % SIZE,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        state.start.store(start, Ordering::Release);
        Ok(())
    }
}

impl<W: ?Sized + Write, const SIZE: usize> RingBufWriter<W, SIZE> {
    pub fn flush_position(&self) -> u64 {
        self.state.flush_head.load(Ordering::Relaxed)
    }

    pub fn write_position(&self) -> u64 {
        self.state.write_head.load(Ordering::Relaxed)
    }

    // Ensure this function does not get inlined into `write`, so that it
    // remains inlineable and its common path remains as short as possible.
    // If this function ends up being called frequently relative to `write`,
    // it's likely a sign that the client is using an improperly sized buffer
    // or their write patterns are somewhat pathological.
    #[cold]
    #[inline(never)]
    fn write_cold(&mut self, buf: &[u8]) -> Result<usize> {
        if buf.len() > self.spare_capacity() {
            if let Ok((mut flusher, state)) = self.lock_flusher() {
                flusher.flush_buf::<SIZE>(state)?;
            } else {
                return Err(mutex_lock_err());
            }
        }

        // Why not len > capacity? To avoid a needless trip through the buffer when the input
        // exactly fills it. We'd just need to flush it to the underlying writer anyway.
        if buf.len() >= SIZE {
            if let Ok(mut flusher) = self.flusher.lock() {
                flusher.panicked = true;
                let r = flusher.get_mut().write(buf);
                flusher.panicked = false;
                r
            } else {
                Err(mutex_lock_err())
            }
        } else {
            // Write to the buffer. In this case, we write to the buffer even if it fills it
            // exactly. Doing otherwise would mean flushing the buffer, then writing this
            // input to the inner writer, which in many cases would be a worse strategy.

            // SAFETY: There was either enough spare capacity already, or there wasn't and we
            // flushed the buffer to ensure that there is. In the latter case, we know that there
            // is because flushing ensured that our entire buffer is spare capacity, and we entered
            // this block because the input buffer length is less than that capacity. In either
            // case, it's safe to write the input buffer to our buffer.
            unsafe {
                self.write_to_buffer_unchecked(buf);
            }

            Ok(buf.len())
        }
    }

    // Ensure this function does not get inlined into `write_all`, so that it
    // remains inlineable and its common path remains as short as possible.
    // If this function ends up being called frequently relative to `write_all`,
    // it's likely a sign that the client is using an improperly sized buffer
    // or their write patterns are somewhat pathological.
    #[cold]
    #[inline(never)]
    fn write_all_cold(&mut self, buf: &[u8]) -> Result<()> {
        // Normally, `write_all` just calls `write` in a loop. We can do better
        // by calling `self.get_mut().write_all()` directly, which avoids
        // round trips through the buffer in the event of a series of partial
        // writes in some circumstances.

        if buf.len() > self.spare_capacity() {
            if let Ok((mut flusher, state)) = self.lock_flusher() {
                flusher.flush_buf::<SIZE>(state)?;
            } else {
                return Err(mutex_lock_err());
            }
        }

        // Why not len > capacity? To avoid a needless trip through the buffer when the input
        // exactly fills it. We'd just need to flush it to the underlying writer anyway.
        if buf.len() >= SIZE {
            if let Ok(mut flusher) = self.flusher.lock() {
                flusher.panicked = true;
                let r = flusher.get_mut().write_all(buf);
                flusher.panicked = false;
                r
            } else {
                Err(mutex_lock_err())
            }
        } else {
            // Write to the buffer. In this case, we write to the buffer even if it fills it
            // exactly. Doing otherwise would mean flushing the buffer, then writing this
            // input to the inner writer, which in many cases would be a worse strategy.

            // SAFETY: There was either enough spare capacity already, or there wasn't and we
            // flushed the buffer to ensure that there is. In the latter case, we know that there
            // is because flushing ensured that our entire buffer is spare capacity, and we entered
            // this block because the input buffer length is less than that capacity. In either
            // case, it's safe to write the input buffer to our buffer.
            unsafe {
                self.write_to_buffer_unchecked(buf);
            }

            Ok(())
        }
    }

    #[inline]
    fn spare_capacity(&self) -> usize {
        SIZE - self.state.len::<SIZE>()
    }
    /*
    #[inline]
    fn check_reserved_range(&self, start: usize, length: usize) -> bool {
        let marker = self.marker.load(Ordering::Acquire);

        if self.end > marker {
            !(start < marker || start >= self.end || start + length > self.end)
        } else {
            let end = start + length;
            let end_bounded = end % SIZE;

            !(start < marker && start >= self.end)
                || (end_bounded < marker && end_bounded > self.end)
                || (start < self.end && end > self.end)
                || (start >= marker && end_bounded > self.end)
        }
    }*/

    /// Buffer some data without flushing it, regardless of the size of the
    /// data. Writes as much as possible without exceeding capacity. Returns
    /// the number of bytes written.
    pub(super) fn write_to_buf(&mut self, buf: &[u8]) -> usize {
        let available = self.spare_capacity();
        let amt_to_buffer = available.min(buf.len());

        // SAFETY: `amt_to_buffer` is <= buffer's spare capacity by construction.
        unsafe {
            self.write_to_buffer_unchecked(&buf[..amt_to_buffer]);
        }

        amt_to_buffer
    }

    // SAFETY: Requires `buf.len() <= self.buf.capacity() - self.buf.len()`,
    // i.e., that input buffer length is less than or equal to spare capacity.
    #[inline]
    unsafe fn write_to_buffer_unchecked(&mut self, src: &[u8]) {
        debug_assert!(src.len() < self.spare_capacity());

        let start = self.state.marker.load(Ordering::Acquire);
        let end = start + src.len();

        if end <= SIZE {
            self.state.buf.get_mut()[start..end].copy_from_slice(src);
        } else {
            self.state.buf.get_mut()[start..].copy_from_slice(&src[..(SIZE - start)]);
            self.state.buf.get_mut()[..(end % SIZE)].copy_from_slice(&src[(SIZE - start)..]);
        }

        self.state.marker.store(end, Ordering::Release);
    }

    pub fn lock_flusher(
        &mut self,
    ) -> std::result::Result<
        (MutexGuard<'_, RingBufFlusher<W>>, &mut RingBufState),
        std::sync::PoisonError<MutexGuard<'_, RingBufFlusher<W>>>,
    > {
        self.flusher.lock().map(|x| (x, &mut self.state))
    }
}

impl<W: ?Sized + Write + Seek, const SIZE: usize> Write for RingBufWriter<W, SIZE> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        // Use < instead of <= to avoid a needless trip through the buffer in some cases.
        // See `write_cold` for details.
        if buf.len() < self.spare_capacity() {
            // SAFETY: safe by above conditional.
            unsafe {
                self.write_to_buffer_unchecked(buf);
            }

            Ok(buf.len())
        } else {
            self.write_cold(buf)
        }
        .map(|x| {
            self.state.write_head.fetch_add(x as u64, Ordering::AcqRel);
            x
        })
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        // Use < instead of <= to avoid a needless trip through the buffer in some cases.
        // See `write_all_cold` for details.
        if buf.len() < self.spare_capacity() {
            // SAFETY: safe by above conditional.
            unsafe {
                self.write_to_buffer_unchecked(buf);
            }

            Ok(())
        } else {
            self.write_all_cold(buf)
        }
        .map(|_| {
            self.state.write_head.fetch_add(buf.len() as u64, Ordering::AcqRel);
        })
    }

    fn flush(&mut self) -> Result<()> {
        if let Ok((mut flusher, state)) = self.lock_flusher() {
            flusher.flush_buf::<SIZE>(state)?;
            let position = flusher.inner.stream_position()?;
            flusher.get_mut().flush()?;
            state.flush_head.store(position, Ordering::Release);
            Ok(())
        } else {
            Err(mutex_lock_err())
        }
    }
}

impl<W: ?Sized + Write, const SIZE: usize> std::fmt::Debug for RingBufWriter<W, SIZE>
where
    W: std::fmt::Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("RingBufWriter")
            .field("buffer", &format_args!("{}/{}", self.state.len::<SIZE>(), SIZE))
            .finish()
    }
}

impl<W: ?Sized + Write + Seek, const SIZE: usize> Seek for RingBufWriter<W, SIZE> {
    /// Seek to the offset, in bytes, in the underlying writer.
    ///
    /// Seeking always writes out the internal buffer before seeking.
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        if let Ok((mut flusher, state)) = self.lock_flusher() {
            flusher.flush_buf::<SIZE>(state)?;
            let result = flusher.get_mut().seek(pos)?;
            state
                .write_head
                .store(state.len::<SIZE>() as u64 + result, Ordering::Release);
            Ok(result)
        } else {
            Err(mutex_lock_err())
        }
    }

    fn stream_position(&mut self) -> Result<u64> {
        if let Ok(mut flusher) = self.flusher.lock() {
            flusher.inner.stream_position()
        } else {
            Err(mutex_lock_err())
        }
    }
}

impl<W: ?Sized + Write, const SIZE: usize> Drop for RingBufWriter<W, SIZE> {
    fn drop(&mut self) {
        if let Ok((mut flusher, state)) = self.lock_flusher() {
            if !flusher.panicked {
                // dtors should not panic, so we ignore a failed flush
                let _r = flusher.flush_buf::<SIZE>(state);
            }
        }
    }
}
