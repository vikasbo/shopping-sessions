//! Fast fused line + field splitter: one pass over a chunk that only visits the bytes
//! `,` `"` and `\n`, found 64 at a time with SSE2 compares and a bitmask.
//! (Java analogue: what the JDK's vectorized `String.indexOf` intrinsics do internally.)

struct Splitter<'a, const N: usize> {
    data: &'a [u8],
    out: [&'a [u8]; N],
    nf: usize,
    start: usize,
    q: u32,
    bad: bool,
}

impl<'a, const N: usize> Splitter<'a, N> {
    #[inline(always)]
    fn step(&mut self, i: usize, c: u8, f: &mut impl FnMut([&'a [u8]; N])) {
        if c == b'"' {
            self.q += 1;
            return;
        }
        let (d, s) = (self.data, self.start);
        if self.nf < N {
            self.out[self.nf] = if self.q == 0 {
                &d[s..i]
            } else if self.q == 2 && i - s >= 2 && d[s] == b'"' && d[i - 1] == b'"' {
                &d[s + 1..i - 1]
            } else {
                self.bad = true;
                &[]
            };
        }
        self.nf += 1;
        self.start = i + 1;
        self.q = 0;
        if c == b'\n' {
            if self.nf == N && !self.bad {
                f(self.out);
            }
            self.nf = 0;
            self.bad = false;
        }
    }
}

#[inline(always)]
/// Calls `f` with the `N` fields of every well-formed line in `data` (same rules as
/// `parse::fields` + `parse::for_each_line`, which stay as the readable spec and test oracle).
/// Malformed lines are skipped. `SIMD = false` is a plain byte loop, kept for testing.
pub fn for_each_row<'a, const N: usize, const SIMD: bool>(data: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    let mut s = Splitter { data, out: [&[]; N], nf: 0, start: 0, q: 0, bad: false };
    if SIMD {
        let mut base = 0;
        while base + 64 <= data.len() {
            let mut mask = special_mask64(&data[base..base + 64]);
            while mask != 0 {
                let i = base + mask.trailing_zeros() as usize;
                s.step(i, data[i], &mut f);
                mask &= mask - 1; // clear lowest set bit
            }
            base += 64;
        }
        for i in base..data.len() {
            let c = data[i];
            if c == b',' || c == b'"' || c == b'\n' {
                s.step(i, c, &mut f);
            }
        }
    } else {
        for (i, &c) in data.iter().enumerate() {
            if c == b',' || c == b'"' || c == b'\n' {
                s.step(i, c, &mut f);
            }
        }
    }
    // A last line without a trailing \n is still a line (it may end in a comma or quote).
    if s.nf > 0 || s.q > 0 || s.start < data.len() {
        s.step(data.len(), b'\n', &mut f);
    }
}

/// Bit k is set when block[k] is `,` `"` or `\n`. SSE2 is part of every x86_64 CPU.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn special_mask64(block: &[u8]) -> u64 {
    use std::arch::x86_64::*;
    debug_assert!(block.len() >= 64);
    // SAFETY: SSE2 is baseline on x86_64, and the four 16-byte unaligned loads stay inside `block`.
    unsafe {
        let (comma, quote, nl) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'"' as i8), _mm_set1_epi8(b'\n' as i8));
        let mut m = 0u64;
        for k in 0..4 {
            let v = _mm_loadu_si128(block.as_ptr().add(k * 16) as *const __m128i);
            let hit = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(v, comma), _mm_cmpeq_epi8(v, quote)), _mm_cmpeq_epi8(v, nl));
            m |= ((_mm_movemask_epi8(hit) as u32 & 0xFFFF) as u64) << (k * 16);
        }
        m
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn special_mask64(block: &[u8]) -> u64 {
    let mut m = 0u64;
    for (k, &c) in block[..64].iter().enumerate() {
        m |= ((c == b',' || c == b'"' || c == b'\n') as u64) << k;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{fields, for_each_line};

    fn slow<const N: usize>(d: &[u8]) -> Vec<[Vec<u8>; N]> {
        let mut v = Vec::new();
        for_each_line(d, |l| if let Some(r) = fields::<N>(l) { v.push(r.map(|x| x.to_vec())) });
        v
    }
    fn fast<const N: usize, const S: bool>(d: &[u8]) -> Vec<[Vec<u8>; N]> {
        let mut v = Vec::new();
        for_each_row::<N, S>(d, |r| v.push(r.map(|x| x.to_vec())));
        v
    }

    #[test]
    fn matches_slow_splitter_on_random_input() {
        // Small xorshift PRNG so the test needs no extra crates.
        let mut x: u64 = 0x2545F4914F6CDD1D;
        let alphabet = b"ab,\"\n\r 1";
        for len in 0..400 {
            for _ in 0..200 {
                let d: Vec<u8> = (0..len)
                    .map(|_| {
                        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                        alphabet[(x % alphabet.len() as u64) as usize]
                    })
                    .collect();
                assert_eq!(fast::<2, true>(&d), slow::<2>(&d), "{:?}", String::from_utf8_lossy(&d));
                assert_eq!(fast::<2, false>(&d), slow::<2>(&d));
                assert_eq!(fast::<4, true>(&d), slow::<4>(&d));
            }
        }
    }
}
