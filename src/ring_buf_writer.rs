use crate::offset_io::OffsetWrite;

use std::cell::UnsafeCell;
use std::io::{Result, Write};

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

pub struct RingBufFlusher<W: ?Sized + OffsetWrite> {
    panicked: bool,
    inner: W,
}

pub struct RingBufState {
    staging: UnsafeCell<Box<[u8]>>,
    start: AtomicUsize,
    marker: AtomicUsize,
    flush_head: AtomicU64,
    write_head: AtomicU64,
}

unsafe impl Sync for RingBufState {}

/// This is a ring-buffered writer with an atomic write marker that allows
/// one thread to write and one thread to flush to disk.
pub struct RingBufWriter<W: ?Sized + OffsetWrite, const POW2_SIZE: usize> {
    state: RingBufState,
    flusher: Mutex<RingBufFlusher<W>>,
}

impl<W: OffsetWrite, const POW2_SIZE: usize> RingBufWriter<W, POW2_SIZE> {
    pub fn new(inner: W, position: u64) -> RingBufWriter<W, POW2_SIZE> {
        RingBufWriter {
            state: RingBufState {
                staging: UnsafeCell::new(vec![0_u8; POW2_SIZE].into_boxed_slice()),
                start: AtomicUsize::new(0),
                marker: AtomicUsize::new(0),
                flush_head: AtomicU64::new(0),
                write_head: AtomicU64::new(position),
            },
            flusher: Mutex::new(RingBufFlusher { panicked: false, inner }),
        }
    }
}

impl<W: OffsetWrite> RingBufFlusher<W> {
    pub fn swap_inner<const SIZE: usize>(&mut self, state: &RingBufState, other: &mut W, position: u64) -> Result<()> {
        std::mem::swap(&mut self.inner, other);
        state.flush_head.store(0, Ordering::Release);
        state.write_head.store(position, Ordering::Release);
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

impl<W: ?Sized + OffsetWrite> RingBufFlusher<W> {
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
    pub fn flush_buf<const SIZE: usize>(&mut self, state: &RingBufState) -> Result<()> {
        let mut start = state.start.load(Ordering::Acquire);
        let mut position = state.write_head.load(Ordering::Acquire);
        let marker = state.marker.load(Ordering::Acquire);
        let staging = unsafe { &*state.staging.get() };

        while start != marker {
            self.panicked = true;
            let r = self.inner.write_at(
                if start > marker {
                    &staging[start..]
                } else {
                    &staging[start..marker]
                },
                position,
            );
            self.panicked = false;

            match r {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write the buffered data",
                    ));
                }
                Ok(n) => {
                    start = (start + n) % SIZE;
                    position += n as u64;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }

        state.write_head.store(position, Ordering::Release);
        state.start.store(start, Ordering::Release);
        Ok(())
    }
}

impl<W: ?Sized + OffsetWrite, const SIZE: usize> RingBufWriter<W, SIZE> {
    pub fn flush_position(&self) -> u64 {
        self.state.flush_head.load(Ordering::Relaxed)
    }

    pub fn write_position(&self) -> u64 {
        self.state.write_head.load(Ordering::Relaxed) + self.state.len::<SIZE>() as u64
    }

    // Ensure this function does not get inlined into `write`, so that it
    // remains inlineable and its common path remains as short as possible.
    // If this function ends up being called frequently relative to `write`,
    // it's likely a sign that the client is using an improperly sized buffer
    // or their write patterns are somewhat pathological.
    #[cold]
    #[inline(never)]
    fn write_cold(&self, buf: &[u8]) -> Result<usize> {
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
            if let Ok((mut flusher, state)) = self.lock_flusher() {
                let position = state.write_head.load(Ordering::Acquire);
                flusher.panicked = true;
                let r = flusher.get_mut().write_at(buf, position);
                flusher.panicked = false;
                if let Ok(n) = r {
                    state.write_head.store(position + n as u64, Ordering::Release);
                }
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
    fn write_all_cold(&self, buf: &[u8]) -> Result<()> {
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
            if let Ok((mut flusher, state)) = self.lock_flusher() {
                let position = state.write_head.load(Ordering::Acquire);
                flusher.panicked = true;
                let r = flusher.get_mut().write_all_at(buf, position);
                flusher.panicked = false;
                if r.is_ok() {
                    state.write_head.store(position + buf.len() as u64, Ordering::Release);
                }
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

    /// Buffer some data without flushing it, regardless of the size of the
    /// data. Writes as much as possible without exceeding capacity. Returns
    /// the number of bytes written.
    pub(super) fn write_to_buf(&self, buf: &[u8]) -> usize {
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
    unsafe fn write_to_buffer_unchecked(&self, src: &[u8]) {
        debug_assert!(src.len() < self.spare_capacity());

        let start = self.state.marker.load(Ordering::Acquire);
        let end = start + src.len();

        // SAFETY: This operation is safe provided that we are accurately keeping track
        // of which sections of the buffer are being written to vs. being flushed.
        let staging = &mut *self.state.staging.get();

        if end <= SIZE {
            staging[start..end].copy_from_slice(src);
        } else {
            staging[start..].copy_from_slice(&src[..(SIZE - start)]);
            staging[..(end % SIZE)].copy_from_slice(&src[(SIZE - start)..]);
        }

        self.state.marker.store(end % SIZE, Ordering::Release);
    }

    pub fn lock_flusher(
        &self,
    ) -> std::result::Result<
        (MutexGuard<'_, RingBufFlusher<W>>, &RingBufState),
        std::sync::PoisonError<MutexGuard<'_, RingBufFlusher<W>>>,
    > {
        self.flusher.lock().map(|x| (x, &self.state))
    }
}

impl<'a, W: ?Sized + OffsetWrite, const SIZE: usize> Write for &'a RingBufWriter<W, SIZE> {
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
    }

    fn flush(&mut self) -> Result<()> {
        if let Ok((mut flusher, state)) = self.lock_flusher() {
            flusher.flush_buf::<SIZE>(state)?;
            let position = state.write_head.load(Ordering::Acquire);
            flusher.get_mut().flush_all()?;
            state.flush_head.store(position, Ordering::Release);
            Ok(())
        } else {
            Err(mutex_lock_err())
        }
    }
}

impl<W: ?Sized + OffsetWrite, const SIZE: usize> std::fmt::Debug for RingBufWriter<W, SIZE>
where
    W: std::fmt::Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("RingBufWriter")
            .field("buffer", &format_args!("{}/{}", self.state.len::<SIZE>(), SIZE))
            .finish()
    }
}

impl<W: ?Sized + OffsetWrite, const SIZE: usize> Drop for RingBufWriter<W, SIZE> {
    fn drop(&mut self) {
        if let Ok((mut flusher, state)) = self.lock_flusher() {
            if !flusher.panicked {
                // dtors should not panic, so we ignore a failed flush
                let _r = flusher.flush_buf::<SIZE>(state);
            }
        }
    }
}
