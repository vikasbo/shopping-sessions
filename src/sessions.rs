//! Per-person work: sessionize, credit each session to a click, count assists, and
//! aggregate into the two output tables.

use crate::parse::{DAY_MS, GAP_MS, WEEK_MS};

/// A surviving event, already routed to its person's shard.
#[derive(Clone, Copy)]
pub struct PEv<'a> {
    pub person: &'a [u8],
    pub ph: u64, // hash(person)
    pub ts: i64,
    pub price: u32, // 0 unless a valid purchase
    pub cat: u16,
}

/// A valid click, already routed to its person's shard.
#[derive(Clone, Copy)]
pub struct PClick<'a> {
    pub person: &'a [u8],
    pub ph: u64, // hash(person)
    pub id: &'a [u8],
    pub ts: i64,
    pub camp: u32,
}

pub struct URow<'a> {
    pub person: &'a [u8],
    pub day: i32,
    pub sessions: u64,
    pub events: u64,
    pub revenue: u64,
    pub top: Option<u16>,
}

/// One accumulator per rayon work split (roughly one per thread), merged at the end.
/// Like a Java `Collector`'s mutable container in `parallelStream().collect(...)`.
pub struct Acc<'a> {
    pub users: Vec<URow<'a>>,
    /// `[sessions, purchasing, revenue, assisted]` per (campaign, day); campaign index
    /// `ncamp` is `organic`. Flattened as `camp * ndays + day_index`.
    pub camps: Vec<[u64; 4]>,
    // Scratch state for the sliding 7-day click window, reused across people.
    cnt: Vec<u32>,
    active: Vec<u32>,
    pos: Vec<u32>,
    cats: Vec<(u16, u64)>,
}

pub struct Ctx {
    pub ncamp: usize,
    pub day0: i64,
    pub ndays: usize,
}

impl<'a> Acc<'a> {
    pub fn new(ctx: &Ctx) -> Self {
        Acc {
            users: Vec::new(),
            camps: vec![[0; 4]; (ctx.ncamp + 1) * ctx.ndays],
            cnt: vec![0; ctx.ncamp],
            active: Vec::new(),
            pos: vec![0; ctx.ncamp],
            cats: Vec::new(),
        }
    }

    #[inline]
    fn add(&mut self, c: u32) {
        let c = c as usize;
        if self.cnt[c] == 0 {
            self.pos[c] = self.active.len() as u32;
            self.active.push(c as u32);
        }
        self.cnt[c] += 1;
    }

    #[inline]
    fn remove(&mut self, c: u32) {
        let c = c as usize;
        self.cnt[c] -= 1;
        if self.cnt[c] == 0 {
            // O(1) removal from the active list: move the last element into the hole.
            let p = self.pos[c] as usize;
            let last = *self.active.last().unwrap();
            self.active[p] = last;
            self.pos[last as usize] = p as u32;
            self.active.pop();
        }
    }

    fn flush_day(&mut self, person: &'a [u8], day: i64, s: u64, n: u64, rev: u64) {
        // Highest revenue wins; a tie goes to the smaller category id, and ids are
        // assigned in byte-wise name order, so that is the byte-wise smallest name.
        let top = self.cats.iter().copied().min_by_key(|&(c, r)| (std::cmp::Reverse(r), c)).map(|(c, _)| c);
        self.users.push(URow { person, day: day as i32, sessions: s, events: n, revenue: rev, top });
        self.cats.clear();
    }

    /// Process one person: `evs` sorted by ts, `clicks` sorted by (ts, click_id).
    pub fn person(&mut self, ctx: &Ctx, person: &'a [u8], evs: &[(u32, i64, u32, u16)], clicks: &[(u32, i64, &'a [u8], u32)]) {
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut cur_day = i64::MIN;
        let (mut ds, mut dn, mut drev) = (0u64, 0u64, 0u64);
        let mut i = 0;
        while i < evs.len() {
            let start = evs[i].1;
            let day = start.div_euclid(DAY_MS);
            if day != cur_day {
                if cur_day != i64::MIN {
                    self.flush_day(person, cur_day, ds, dn, drev);
                }
                cur_day = day;
                (ds, dn, drev) = (0, 0, 0);
            }
            // Extend the session while the gap to the previous event is <= 30 min.
            let mut j = i;
            let mut last = start;
            let mut rev = 0u64;
            let mut purchased = false;
            while j < evs.len() && evs[j].1 - last <= GAP_MS {
                last = evs[j].1;
                let (price, cat) = (evs[j].2, evs[j].3);
                if price > 0 {
                    purchased = true;
                    rev += price as u64;
                    match self.cats.iter_mut().find(|(c, _)| *c == cat) {
                        Some(e) => e.1 += price as u64,
                        None => self.cats.push((cat, price as u64)),
                    }
                }
                j += 1;
            }
            ds += 1;
            dn += (j - i) as u64;
            drev += rev;

            // Slide the click window to [start - 7 days, start].
            while hi < clicks.len() && clicks[hi].1 <= start {
                self.add(clicks[hi].3);
                hi += 1;
            }
            while lo < hi && clicks[lo].1 < start - WEEK_MS {
                self.remove(clicks[lo].3);
                lo += 1;
            }
            let camp = if hi > lo {
                // Latest ts; among equal ts the smallest click_id, which sorts first.
                let t = clicks[hi - 1].1;
                let mut k = hi - 1;
                while k > lo && clicks[k - 1].1 == t {
                    k -= 1;
                }
                clicks[k].3 as usize
            } else {
                ctx.ncamp // organic
            };
            let di = (day - ctx.day0) as usize;
            let row = &mut self.camps[camp * ctx.ndays + di];
            row[0] += 1;
            row[1] += purchased as u64;
            row[2] += rev;
            for &a in &self.active {
                self.camps[a as usize * ctx.ndays + di][3] += 1;
            }
            i = j;
        }
        if cur_day != i64::MIN {
            self.flush_day(person, cur_day, ds, dn, drev);
        }
        // Reset the window for the next person.
        for &a in &self.active {
            self.cnt[a as usize] = 0;
        }
        self.active.clear();
    }
}
