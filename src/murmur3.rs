// Based on the fastmurmur3 crate, but modified to work on a generic Read trait

use crate::{
    match_fallthrough, match_fallthrough_make_loops, match_fallthrough_make_match,
    match_fallthrough_reverse_branches,
};
use std::io::Read;
use std::ops::Shl;

#[inline]
pub fn murmur3_stream<R>(mut data: R, len: usize, seed: u64) -> Result<u128, std::io::Error>
where
    R: Read,
{
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    const C3: u64 = 0x52dc_e729;
    const C4: u64 = 0x3849_5ab5;
    const R1: u32 = 27;
    const R2: u32 = 31;
    const R3: u32 = 33;
    const M: u64 = 5;
    const BLOCK_SIZE: usize = 16;
    const HALF_BLOCK_SIZE: usize = BLOCK_SIZE / 2;
    let mut trailing_len = len;

    let mut h1: u64 = seed;
    let mut h2: u64 = seed;
    let mut buf: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];

    while trailing_len >= BLOCK_SIZE {
        data.read_exact(&mut buf);
        trailing_len -= BLOCK_SIZE;

        let k1 = u64::from_le_bytes(unsafe { *(buf.as_ptr() as *const [u8; HALF_BLOCK_SIZE]) });
        let k2 = u64::from_le_bytes(unsafe {
            *(buf.as_ptr().offset(HALF_BLOCK_SIZE as isize) as *const [u8; HALF_BLOCK_SIZE])
        });
        h1 ^= k1.wrapping_mul(C1).rotate_left(R2).wrapping_mul(C2);
        h1 = h1
            .rotate_left(R1)
            .wrapping_add(h2)
            .wrapping_mul(M)
            .wrapping_add(C3);
        h2 ^= k2.wrapping_mul(C2).rotate_left(R3).wrapping_mul(C1);
        h2 = h2
            .rotate_left(R2)
            .wrapping_add(h1)
            .wrapping_mul(M)
            .wrapping_add(C4);
    }

    data.read_exact(&mut buf[..trailing_len])?;
    let mut k1 = 0;
    let mut k2 = 0;
    // this macro saves about 8% on performance compared to a huge if statement.
    match_fallthrough!(trailing_len, {
        15 => k2 ^= (buf[14] as u64).shl(48),
        14 => k2 ^= (buf[13] as u64).shl(40),
        13 => k2 ^= (buf[12] as u64).shl(32),
        12 => k2 ^= (buf[11] as u64).shl(24),
        11 => k2 ^= (buf[10] as u64).shl(16),
        10 => k2 ^= (buf[9] as u64).shl(8),
        9 => {
            k2 ^= buf[8] as u64;
            k2 = k2.wrapping_mul(C2)
                .rotate_left(33)
                .wrapping_mul(C1);
            h2 ^= k2;
        },
        8 => k1 ^= (buf[7] as u64).shl(56),
        7 => k1 ^= (buf[6] as u64).shl(48),
        6 => k1 ^= (buf[5] as u64).shl(40),
        5 => k1 ^= (buf[4] as u64).shl(32),
        4 => k1 ^= (buf[3] as u64).shl(24),
        3 => k1 ^= (buf[2] as u64).shl(16),
        2 => k1 ^= (buf[1] as u64).shl(8),
        1 => k1 ^= buf[0] as u64,
        0 => {
            k1 = k1.wrapping_mul(C1)
                .rotate_left(R2)
                .wrapping_mul(C2);
            h1 ^= k1;
            break;
        },
        _ => unreachable!()
    });

    h1 ^= len as u64;
    h2 ^= len as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    Ok(u128::from_ne_bytes(unsafe {
        *([h1, h2].as_ptr() as *const [u8; 16])
    }))
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
    k.xor_shr(R)
        .wrapping_mul(C1)
        .xor_shr(R)
        .wrapping_mul(C2)
        .xor_shr(R)
}

#[cfg(test)]
use std::io::Cursor;

#[cfg(test)]
mod test {
    use super::*;
    use rand::{Rng, RngCore};

    static SOURCE: &'static [u8] = b"The quick brown fox jumps over the lazy dog";

    #[test]
    fn test_agreement_basic() {
        let a = murmur3_stream(SOURCE, SOURCE.len(), 0).unwrap();
        let b = murmur3::murmur3_x64_128(&mut Cursor::new(SOURCE), 0).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_agreement_fuzzed() {
        let mut rng = rand::thread_rng();

        for i in 0..10000 {
            let len: u8 = rng.gen();
            let mut buf = vec![0; len as usize];
            rng.fill_bytes(&mut buf[..]);
            let salt: u32 = rng.gen();
            let a = murmur3_stream(&buf[..], buf.len(), salt as u64).unwrap();
            let b = murmur3::murmur3_x64_128(&mut Cursor::new(&buf), salt).unwrap();
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
