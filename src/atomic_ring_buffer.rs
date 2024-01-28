use eyre::eyre;
use eyre::Result;
use std::default::Default;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct AtomicRingBuffer<T, const POW2_SIZE: usize>
where
    T: Default + std::marker::Copy,
{
    buf: [T; POW2_SIZE],
    head: AtomicUsize,
    write_marker: AtomicUsize,
    tail: AtomicUsize,
}

impl<T, const SIZE: usize> Default for AtomicRingBuffer<T, SIZE>
where
    T: Default + std::marker::Copy,
{
    #[inline]
    fn default() -> AtomicRingBuffer<T, SIZE> {
        if SIZE != usize::next_power_of_two(SIZE) {
            panic!("Size must be a power of 2!");
        }

        AtomicRingBuffer {
            buf: [T::default(); SIZE],
            head: AtomicUsize::new(0),
            write_marker: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }
}

impl<T, const SIZE: usize> AtomicRingBuffer<T, SIZE>
where
    T: Default + std::marker::Copy,
{
    #[inline]
    pub fn new() -> AtomicRingBuffer<T, SIZE> {
        if SIZE != usize::next_power_of_two(SIZE) {
            panic!("Size must be a power of 2!");
        }

        AtomicRingBuffer::default()
    }

    // Atomically reserves a possibly discontinuous length of memory, then copies whatever is in the byte buffer
    // into the reserved section. Fails if there isn't enough room left. This function will block until it can
    // "commit" the written value, which involves spinning until other write operations before it have completed.
    // This has a forward-progress gaurantee bounded by the number of cores, because we do not allow async yielding
    // between reserving the space and committing the result.
    pub fn append(&mut self, src: &[T]) -> Result<usize> {
        let length = src.len();

        // We do a check first to avoid unnecessarily hammering a filled buffer (atomic ordering doesn't matter here)
        if (self.tail.load(Ordering::Relaxed) + length) > (self.head.load(Ordering::Relaxed) + SIZE)
        {
            return Err(eyre!("Not enough space!"));
        }

        // Now we do the real attempt
        let begin_unbounded = self.tail.fetch_add(length, Ordering::AcqRel);
        let end_unbounded = begin_unbounded + length;

        // If the addition didn't put us past head + SIZE, our reservation succeeded
        if end_unbounded <= self.head.load(Ordering::Acquire) + SIZE {
            // Perform up to two copy operations, in case our memory region got segmented
            let begin = begin_unbounded % SIZE;
            let end = begin + length;
            if end <= SIZE {
                self.buf[begin..end].copy_from_slice(src);
            } else {
                self.buf[begin..].copy_from_slice(&src[..(SIZE - begin)]);
                self.buf[..(end % SIZE)].copy_from_slice(&src[(SIZE - begin)..]);
            }

            // We now need to commit our copy operation. If we finished before a previous write, we spin until it finishes
            while self
                .write_marker
                .compare_exchange_weak(
                    begin_unbounded,
                    end_unbounded,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                std::hint::spin_loop();
            }

            Ok(begin_unbounded)
        } else {
            // Otherwise, we have to reverse our reservation atomically. Even if multiple failures happen, the atomic
            // subtraction will never leave us in a bad state, so no race conditions occur here.
            self.tail.fetch_sub(length, Ordering::Release);
            Err(eyre!("Not enough space!"))
        }
    }

    pub fn process<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(&[T]) -> Result<()>,
    {
        let start_unbounded = self.head.load(Ordering::Acquire);
        let end_unbounded = self.write_marker.load(Ordering::Acquire);
        let start = start_unbounded % SIZE;
        let length = std::cmp::min(end_unbounded - start_unbounded, SIZE - start);

        func(&self.buf[start..(start + length)])?;

        // If and only if the function succeeds, we commit our read
        self.head.fetch_add(length, Ordering::Release);
        Ok(())
    }

    pub fn read_location(&self) -> usize {
        self.head.load(Ordering::Relaxed)
    }
}
