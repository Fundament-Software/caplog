// Based on the fastmurmur3 crate, but modified to work on an aligned u64 buffer

use num::Integer;

#[inline]
pub fn murmur3_aligned_inner(mut data: &[u64], seed: u128, accumulator: usize) -> u128 {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    const C3: u64 = 0x52dc_e729;
    const C4: u64 = 0x3849_5ab5;
    const R1: u32 = 27;
    const R2: u32 = 31;
    const R3: u32 = 33;
    const M: u64 = 5;
    const BLOCK_SIZE: usize = 16 / std::mem::size_of::<u64>();

    let [mut h1, mut h2] = unsafe { std::mem::transmute::<u128, [u64; 2]>(seed) };

    // If offset is set, the last run of murmur only processed half of h1
    if accumulator.is_odd() && data.len() > 0 {
        let k2 = data[0];
        h1 = h1.rotate_left(R1).wrapping_add(h2).wrapping_mul(M).wrapping_add(C3);
        h2 ^= k2.wrapping_mul(C2).rotate_left(R3).wrapping_mul(C1);
        h2 = h2.rotate_left(R2).wrapping_add(h1).wrapping_mul(M).wrapping_add(C4);
        data = &data[1..];
    }

    while data.len() >= BLOCK_SIZE {
        let k1 = data[0];
        let k2 = data[1];
        h1 ^= k1.wrapping_mul(C1).rotate_left(R2).wrapping_mul(C2);
        h1 = h1.rotate_left(R1).wrapping_add(h2).wrapping_mul(M).wrapping_add(C3);
        h2 ^= k2.wrapping_mul(C2).rotate_left(R3).wrapping_mul(C1);
        h2 = h2.rotate_left(R2).wrapping_add(h1).wrapping_mul(M).wrapping_add(C4);

        data = &data[BLOCK_SIZE..];
    }

    if data.len() > 0 {
        assert_eq!(data.len(), 1);
        let k1 = data[0];
        h1 ^= k1.wrapping_mul(C1).rotate_left(R2).wrapping_mul(C2);
    }

    unsafe { std::mem::transmute::<[u64; 2], u128>([h1, h2]) }
}

pub fn murmur3_finalize(mut total_len: usize, seed: u128) -> u128 {
    total_len *= std::mem::size_of::<u64>();
    let [mut h1, mut h2] = unsafe { std::mem::transmute::<u128, [u64; 2]>(seed) };

    h1 ^= total_len as u64;
    h2 ^= total_len as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    unsafe { std::mem::transmute::<[u64; 2], u128>([h1, h2]) }
}

#[inline]
pub fn murmur3_aligned(data: &[u64], seed: u128) -> u128 {
    murmur3_finalize(data.len(), murmur3_aligned_inner(data, seed, 0))
}

trait XorShift {
    fn xor_shr(&self, shift: u32) -> Self;
}

impl XorShift for u64 {
    fn xor_shr(&self, shift: u32) -> Self {
        self ^ (self >> shift)
    }
}

fn fmix64(k: u64) -> u64 {
    const C1: u64 = 0xff51_afd7_ed55_8ccd;
    const C2: u64 = 0xc4ce_b9fe_1a85_ec53;
    const R: u32 = 33;
    k.xor_shr(R).wrapping_mul(C1).xor_shr(R).wrapping_mul(C2).xor_shr(R)
}

#[cfg(test)]
use std::io::Cursor;

#[cfg(test)]
mod test {
    use super::*;
    use rand::{Rng, RngCore};

    static SOURCE: &'static [u8; 40] = b"The quick brown fox jumps over the lazy ";

    #[test]
    fn test_agreement_basic() {
        let aligned = unsafe { std::mem::transmute::<[u8; 40], [u64; 5]>(*SOURCE) };
        let a = murmur3_aligned(&aligned, 0);
        let b = murmur3::murmur3_x64_128(&mut Cursor::new(SOURCE), 0).unwrap();
        assert_eq!(a, b);

        let a = murmur3_aligned(&aligned, 12345 | 12345_u128 << 64);
        let b = murmur3::murmur3_x64_128(&mut Cursor::new(SOURCE), 12345).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_agreement_split() {
        let aligned = unsafe { std::mem::transmute::<[u8; 40], [u64; 5]>(*SOURCE) };
        let a1 = murmur3_aligned_inner(&aligned[0..2], 0, 0);
        let a2 = murmur3_aligned_inner(&aligned[2..], a1, 2);
        let a = murmur3_finalize(aligned.len(), a2);
        let b = murmur3_aligned(&aligned, 0);

        let c = murmur3::murmur3_x64_128(&mut Cursor::new(SOURCE), 0).unwrap();
        assert_eq!(b, c);

        assert_eq!(a, b);
    }

    #[test]
    fn test_agreement_odd_split() {
        let aligned = unsafe { std::mem::transmute::<[u8; 40], [u64; 5]>(*SOURCE) };
        let a1 = murmur3_aligned_inner(&aligned[0..3], 0, 0);
        let a2 = murmur3_aligned_inner(&aligned[3..], a1, 3);
        let a = murmur3_finalize(aligned.len(), a2);
        let b = murmur3_aligned(&aligned, 0);

        let c = murmur3::murmur3_x64_128(&mut Cursor::new(SOURCE), 0).unwrap();
        assert_eq!(b, c);

        assert_eq!(a, b);
    }

    #[test]
    fn test_agreement_fuzzed() {
        let mut rng = rand::thread_rng();

        #[cfg(miri)]
        const MAXCOUNT: usize = 10;
        #[cfg(not(miri))]
        const MAXCOUNT: usize = 10000;

        for i in 0..MAXCOUNT {
            let len: u8 = rng.gen();
            let mut buf: Vec<u64> = vec![0; len as usize];
            let (_, inner, _) = unsafe { buf.align_to_mut::<u8>() };
            rng.fill_bytes(inner);
            let salt: u32 = rng.gen();
            let b = murmur3::murmur3_x64_128(&mut Cursor::new(inner), salt).unwrap();
            let a = murmur3_aligned(&buf[..], salt as u128 | (salt as u128) << 64);
            assert_eq!(
                a,
                b,
                "Failed after {} iterations. salt={} data={}",
                i,
                salt,
                buf.iter().map(|b| format!("{:x}", b)).collect::<String>(),
            );
        }
    }
}
