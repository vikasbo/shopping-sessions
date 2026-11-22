//! Byte-level CSV dialect, timestamp and price parsing, exactly as rules.md says.
//!
//! Everything works on `&[u8]` (a borrowed view into the memory-mapped file, like a
//! Java `ByteBuffer.slice()` that never copies). Nothing here allocates.

use memchr::{memchr, memchr_iter};
use memmap2::Mmap;

pub const DAY_MS: i64 = 86_400_000;
pub const GAP_MS: i64 = 1_800_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;

/// Split one line into exactly `N` fields. `None` means the line is malformed:
/// empty, wrong field count, or a quote anywhere except around a whole field.
/// An empty field (or `""`) comes back as an empty slice, which the callers treat as null.
///
/// `const N: usize` is a compile-time parameter (Java has no equivalent; think of
/// a generic over a number). It lets us return a fixed-size array on the stack.
#[inline]
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out: [&[u8]; N] = [&[]; N];
    let mut start = 0;
    let mut i = 0;
    for pos in memchr_iter(b',', line) {
        if i + 1 >= N {
            return None; // too many fields
        }
        out[i] = unquote(&line[start..pos])?; // `?` = "return None if this is None"
        i += 1;
        start = pos + 1;
    }
    if i + 1 != N {
        return None; // too few fields
    }
    out[i] = unquote(&line[start..])?;
    Some(out)
}

#[inline]
fn unquote(f: &[u8]) -> Option<&[u8]> {
    match memchr(b'"', f) {
        None => Some(f),
        Some(0) if f.len() >= 2 && f[f.len() - 1] == b'"' && memchr(b'"', &f[1..f.len() - 1]).is_none() => {
            Some(&f[1..f.len() - 1])
        }
        _ => None,
    }
}

/// Call `f` for every line in `data`. Only `\n` ends a line (`\r` stays in the field).
/// Text after a final `\n` is not a line; text with no final `\n` is.
#[inline]
pub fn for_each_line<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut pos = 0;
    while pos < data.len() {
        match memchr(b'\n', &data[pos..]) {
            Some(i) => {
                f(&data[pos..pos + i]);
                pos += i + 1;
            }
            None => {
                f(&data[pos..]);
                break;
            }
        }
    }
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<u32> {
    // wrapping_sub turns every non-digit byte into a value >= 10 (unsigned underflow).
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 { Some(a as u32 * 10 + c as u32) } else { None }
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let (m, d) = (m as i64, d as i64);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// F1 / F2 / F3 to epoch milliseconds; `None` for any bad timestamp.
/// An F3 too large for i64 is "outside the window", so returning `None` is equivalent.
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    if b.iter().all(u8::is_ascii_digit) {
        let mut v: i64 = 0;
        for &c in b {
            v = v.checked_mul(10)?.checked_add((c - b'0') as i64)?;
        }
        return Some(v);
    }
    let n = b.len();
    if n != 20 && n != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let (mo, d, h, mi, s) = (d2(b, 5)?, d2(b, 8)?, d2(b, 11)?, d2(b, 14)?, d2(b, 17)?);
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let off_ms: i64 = if n == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if b[22] != b':' {
            return None;
        }
        let (oh, om) = (d2(b, 20)?, d2(b, 23)?);
        if oh > 23 || om > 59 {
            return None;
        }
        sign * ((oh * 60 + om) as i64) * 60_000
    };
    let days = days_from_civil(y as i64, mo, d);
    Some(days * DAY_MS + (h as i64 * 3600 + mi as i64 * 60 + s as i64) * 1000 - off_ms)
}

/// Purchase price: ASCII digits only, value 1..=10,000,000. Leading zeros are fine.
#[inline]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None; // digits only ever grow the value, so stop early (no overflow)
        }
    }
    if v == 0 { None } else { Some(v as u32) }
}

/// A byte range of one file, starting at a line start and ending just after a `\n`
/// (or at end of file). The header line is never inside a chunk.
pub struct Chunk {
    pub file: usize,
    pub start: usize,
    pub end: usize,
}

/// Cut every file into chunks of roughly `target` bytes at line boundaries,
/// so the chunks can be parsed in parallel.
pub fn chunks(maps: &[Mmap], target: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for (file, m) in maps.iter().enumerate() {
        let data = &m[..];
        let Some(h) = memchr(b'\n', data) else { continue }; // header only
        let mut start = h + 1;
        while start < data.len() {
            let mut end = (start + target).min(data.len());
            if end < data.len() {
                end = match memchr(b'\n', &data[end..]) {
                    Some(p) => end + p + 1,
                    None => data.len(),
                };
            }
            out.push(Chunk { file, start, end });
            start = end;
        }
    }
    out
}

/// Header-skipping iteration over a small, whole file (products, campaigns).
pub fn rows<'a, const N: usize>(data: &'a [u8]) -> Vec<[&'a [u8]; N]> {
    let mut out = Vec::new();
    if let Some(h) = memchr(b'\n', data) {
        for_each_line(&data[h + 1..], |l| {
            if let Some(r) = fields::<N>(l) {
                out.push(r);
            }
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(parse_ts(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-04T01:30:00-00:00"), parse_ts(b"2026-10-04T01:30:00Z"));
        for bad in [&b"2026-10-03T23:58:10z"[..], b"2026-10-03 23:58:10Z", b"2026-02-29T00:00:00Z",
                    b"2026-10-03T24:00:00Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
                    b"2026-10-03T23:58:10+0200", b"1791071890000.0", b"-5", b"", b"99999999999999999999999"] {
            assert_eq!(parse_ts(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
    }

    #[test]
    fn csv() {
        assert_eq!(fields::<2>(b"\"u1\",u2"), Some([&b"u1"[..], b"u2"]));
        assert_eq!(fields::<2>(b"\"\",u2"), Some([&b""[..], b"u2"]));
        assert_eq!(fields::<2>(b"u1,u5\""), None);
        assert_eq!(fields::<2>(b"u1,u2,u3"), None);
        assert_eq!(fields::<2>(b"u1"), None);
        assert_eq!(fields::<2>(b""), None);
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"1000\r"), None);
        assert_eq!(parse_price(b"0"), None);
        assert_eq!(parse_price(b"10000001"), None);
    }
}
