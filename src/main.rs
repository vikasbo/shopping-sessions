//! Shopping Sessions pipeline.
//!
//! Phases (each one parallel with rayon, Rust's equivalent of a ForkJoinPool + parallel streams):
//!   1. mmap every input file; parse links and build persons (union-find).
//!   2. Parse event and click chunks in parallel. Events go to shards by hash(event_id),
//!      clicks to shards by hash(person).
//!   3. Per event shard: dedup on event_id (smallest ingest_ts), keep the valid kept copies,
//!      route them to shards by hash(person).
//!   4. Per person shard: sort, sessionize, attribute, aggregate.
//!   5. Write Parquet.
//!
//! Reads $DATA_DIR (default /data) and writes $OUTPUT_DIR (default /output).
//! Optional arguments: DATA_DIR OUTPUT_DIR (used by scripts/run_fixtures.py).

mod links;
mod output;
mod parse;
mod rows;
mod sessions;

use ahash::RandomState;
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use parse::{DAY_MS, WEEK_MS, chunks, parse_price, parse_ts};
use rows::for_each_row;
use sessions::{Acc, Ctx, PClick, PEv};

// Global allocator swap: like choosing a JVM GC, but for malloc/free.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Number of hash shards (power of two). Each shard is one unit of parallel work.
const NS: usize = 256;

/// Deterministic hash used only to pick a shard. Fixed seeds so every thread agrees;
/// different from the per-map hashers, so keys within a shard still spread evenly.
fn shard_hasher() -> RandomState {
    RandomState::with_seeds(0x9E37_79B9, 0x85EB_CA6B, 0xC2B2_AE35, 0x27D4_EB2F)
}
#[inline]
fn shard_of(h: u64) -> usize {
    (h >> 32) as usize & (NS - 1)
}

/// A hasher for keys that are already 64-bit hashes: it passes them through unchanged.
/// (Java's HashMap would call `hashCode()` and spread it; here we skip both steps.)
#[derive(Default)]
struct PassThrough(u64);
impl std::hash::Hasher for PassThrough {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("only u64 keys")
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
type U64Map<V> = HashMap<u64, V, std::hash::BuildHasherDefault<PassThrough>>;

/// An event line as parsed, before dedup. Invalid copies are kept (with ok = false)
/// because dedup happens before validation: an invalid earliest copy must win.
/// The hashes and the person are computed while the line is still in the CPU cache,
/// so later phases never have to touch the mapped file again.
struct RawEv<'a> {
    h: u64, // hash(event_id)
    id: &'a [u8],
    person: &'a [u8], // only meaningful when ok
    ph: u64,          // hash(person), only meaningful when ok
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    ok: bool,
}

/// `[a][b]` -> `[b][a]` by moving the inner Vecs (no element is copied).
fn transpose<T>(outer: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut cols: Vec<Vec<Vec<T>>> = (0..NS).map(|_| Vec::with_capacity(outer.len())).collect();
    for row in outer {
        for (i, v) in row.into_iter().enumerate() {
            cols[i].push(v);
        }
    }
    cols
}

/// Person name -> dense index, keyed by the precomputed hash. The name is compared only
/// when two hashes are equal; a true 64-bit collision falls back to a slice-keyed map.
struct Interner<'a> {
    map: U64Map<u32>,
    clash: HashMap<&'a [u8], u32, RandomState>,
    names: Vec<&'a [u8]>,
}

impl<'a> Interner<'a> {
    fn with_capacity(n: usize) -> Self {
        Interner { map: U64Map::with_capacity_and_hasher(n, Default::default()), clash: HashMap::default(), names: Vec::new() }
    }

    #[inline]
    fn intern(&mut self, h: u64, name: &'a [u8]) -> u32 {
        match self.map.entry(h) {
            Entry::Vacant(v) => {
                self.names.push(name);
                *v.insert(self.names.len() as u32 - 1)
            }
            Entry::Occupied(o) => {
                let i = *o.get();
                // Pointer equality first: the same person's events usually share one slice.
                let n = self.names[i as usize];
                if std::ptr::eq(n, name) || n == name {
                    i
                } else {
                    let names = &mut self.names;
                    *self.clash.entry(name).or_insert_with(|| {
                        names.push(name);
                        names.len() as u32 - 1
                    })
                }
            }
        }
    }

    #[inline]
    fn get(&self, h: u64, name: &[u8]) -> Option<u32> {
        let &i = self.map.get(&h)?;
        let n = self.names[i as usize];
        if std::ptr::eq(n, name) || n == name { Some(i) } else { self.clash.get(name).copied() }
    }
}

fn map_dir(dir: &Path) -> Vec<Mmap> {
    let mut paths: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd.filter_map(|e| e.ok().map(|e| e.path())).collect(),
        Err(_) => return Vec::new(),
    };
    paths.retain(|p| p.extension().is_some_and(|e| e == "csv"));
    paths.sort();
    paths.into_iter().filter_map(|p| map_file(&p)).collect()
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    if f.metadata().ok()?.len() == 0 {
        return None;
    }
    // SAFETY: the input is mounted read-only and nobody modifies it while we run.
    let m = unsafe { Mmap::map(&f) }.ok()?;
    #[cfg(unix)]
    let _ = m.advise(memmap2::Advice::Sequential);
    Some(m)
}

fn manifest_ts(json: &str, key: &str) -> i64 {
    let at = json.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &json[at + key.len() + 2..];
    let q1 = rest.find('"').unwrap() + 1;
    let q2 = q1 + rest[q1..].find('"').unwrap();
    parse_ts(rest[q1..q2].as_bytes()).expect("manifest timestamp")
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let (data, out) = if args.len() >= 3 {
        (PathBuf::from(&args[1]), PathBuf::from(&args[2]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into())),
        )
    };
    if let Some(n) = std::env::var("NCPU").ok().and_then(|v| v.parse::<usize>().ok()) {
        rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
    }
    let verbose = std::env::var("VERBOSE").is_ok();
    let lap = |what: &str| {
        if verbose {
            // utime/stime from /proc/self/stat (fields 14 and 15, in clock ticks of 10 ms).
            let st = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
            let f: Vec<&str> = st.rsplit(')').next().unwrap_or("").split_whitespace().collect();
            let tick = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 100.0;
            eprintln!("{:>8.3}s {what:<14} cpu user {:>7.2}s sys {:>6.2}s", t0.elapsed().as_secs_f64(), tick(11), tick(12));
        }
    };

    // ---- small inputs
    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let (ws, we) = (manifest_ts(&manifest, "window_start"), manifest_ts(&manifest, "window_end"));
    let products_map = std::fs::read(data.join("products.csv")).expect("products.csv");
    let campaigns_map = std::fs::read(data.join("campaigns.csv")).expect("campaigns.csv");

    // Categories get ids in byte-wise name order, so "smaller id" == "smaller name".
    let prod_rows = parse::rows::<2>(&products_map);
    let mut cat_set: BTreeSet<Vec<u8>> = prod_rows.iter().map(|r| r[1].to_vec()).collect();
    cat_set.insert(b"unknown".to_vec());
    let cat_names: Vec<Vec<u8>> = cat_set.into_iter().collect();
    let cat_id = |name: &[u8]| cat_names.binary_search_by(|c| c.as_slice().cmp(name)).unwrap() as u16;
    let unknown = cat_id(b"unknown");
    let products: HashMap<&[u8], u16, RandomState> = prod_rows.iter().map(|r| (r[0], cat_id(r[1]))).collect();

    let camp_rows = parse::rows::<2>(&campaigns_map);
    let campaigns: HashMap<&[u8], u32, RandomState> =
        camp_rows.iter().enumerate().map(|(i, r)| (r[0], i as u32)).collect();
    let ncamp = camp_rows.len();

    // ---- mmap the big inputs
    let ev_maps = map_dir(&data.join("events"));
    let cl_maps = map_dir(&data.join("clicks"));
    let ln_maps = map_dir(&data.join("links"));
    let total: usize = ev_maps.iter().chain(&cl_maps).map(|m| m.len()).sum();
    let threads = rayon::current_num_threads();
    let target = (total / (threads * 16)).clamp(1 << 20, 32 << 20);
    lap("mapped");

    // ---- phase 1: links -> persons
    let ln_chunks = chunks(&ln_maps, target);
    let pairs: Vec<Vec<(&[u8], &[u8])>> = ln_chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            for_each_row::<2, true>(&ln_maps[c.file][c.start..c.end], |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    v.push((a, b));
                }
            });
            v
        })
        .collect();
    let persons = links::Persons::build(pairs.iter().flatten().copied());
    drop(pairs);
    lap("persons");

    // ---- phase 2: parse events (shard by event_id) and clicks (shard by person)
    let sh = shard_hasher();
    let ev_chunks = chunks(&ev_maps, target);
    let raw: Vec<Vec<Vec<RawEv>>> = ev_chunks
        .par_iter()
        .map(|c| {
            // Pre-size every shard's Vec from the chunk size (lines are ~80+ bytes), so the hot
            // loop never reallocates. Untouched capacity is never paged in, so overshoot is cheap.
            let per = (c.end - c.start) / 60 / NS + 16;
            let mut out: Vec<Vec<RawEv>> = (0..NS).map(|_| Vec::with_capacity(per)).collect();
            for_each_row::<7, true>(&ev_maps[c.file][c.start..c.end], |[id, user, ts, ing, ty, prod, price]| {
                if id.is_empty() {
                    return;
                }
                let Some(ingest) = parse_ts(ing) else { return };
                let h = sh.hash_one(id);
                let mut ev = RawEv { h, id, person: &[], ph: 0, ingest, ts: 0, price: 0, cat: 0, ok: false };
                // `'v: { ... break 'v; }` is a labeled block: an early exit, like a
                // `do { ... break; } while (false)` in Java.
                'v: {
                    if user.is_empty() || prod.is_empty() {
                        break 'v;
                    }
                    match parse_ts(ts) {
                        Some(t) if t >= ws && t < we => ev.ts = t,
                        _ => break 'v,
                    }
                    match ty {
                        b"view" | b"cart" => {}
                        b"purchase" => match parse_price(price) {
                            Some(p) => {
                                ev.price = p;
                                ev.cat = products.get(prod).copied().unwrap_or(unknown);
                            }
                            None => break 'v,
                        },
                        _ => break 'v,
                    }
                    if ingest - ev.ts > DAY_MS {
                        break 'v;
                    }
                    ev.ok = true;
                    ev.person = persons.name_of(user);
                    ev.ph = sh.hash_one(ev.person);
                }
                out[shard_of(h)].push(ev);
            });
            out
        })
        .collect();
    lap("events parsed");

    let cl_chunks = chunks(&cl_maps, target);
    let clicks: Vec<Vec<Vec<PClick>>> = cl_chunks
        .par_iter()
        .map(|c| {
            let per = (c.end - c.start) / 40 / NS + 16;
            let mut out: Vec<Vec<PClick>> = (0..NS).map(|_| Vec::with_capacity(per)).collect();
            for_each_row::<4, true>(&cl_maps[c.file][c.start..c.end], |[id, user, camp, ts]| {
                // Copies of a click are identical, so duplicates cannot change the credited
                // click (same ts, same id) or the set of assisting campaigns: no dedup needed.
                if id.is_empty() || user.is_empty() {
                    return;
                }
                let Some(&camp) = campaigns.get(camp) else { return };
                let Some(t) = parse_ts(ts) else { return };
                if t < ws - WEEK_MS || t >= we {
                    return;
                }
                let person = persons.name_of(user);
                let ph = sh.hash_one(person);
                out[shard_of(ph)].push(PClick { person, ph, id, ts: t, camp });
            });
            out
        })
        .collect();
    lap("clicks parsed");

    // ---- phase 3: dedup events per event_id shard, route survivors to person shards
    // Each shard task takes ownership of its column and frees it when done, so the raw
    // records shrink while the routed ones grow (peak memory ~ one copy, not two).
    let routed: Vec<Vec<Vec<PEv>>> = transpose(raw)
        .into_par_iter()
        .map(|col| {
            let n: usize = col.iter().map(Vec::len).sum();
            // Keyed by the precomputed hash. A different event_id with the same 64-bit hash
            // (astronomically rare, but handled exactly) goes to the slice-keyed fallback map.
            let mut best: U64Map<&RawEv> = U64Map::with_capacity_and_hasher(n, Default::default());
            let mut clash: HashMap<&[u8], &RawEv, RandomState> = HashMap::default();
            for v in &col {
                for e in v {
                    match best.entry(e.h) {
                        Entry::Vacant(v) => {
                            v.insert(e);
                        }
                        Entry::Occupied(mut o) => {
                            let b = o.get_mut();
                            if b.id == e.id {
                                if e.ingest < b.ingest {
                                    *b = e;
                                }
                            } else {
                                clash.entry(e.id)
                                    .and_modify(|b| {
                                        if e.ingest < b.ingest {
                                            *b = e;
                                        }
                                    })
                                    .or_insert(e);
                            }
                        }
                    }
                }
            }
            let per = n / NS + 16;
            let mut out: Vec<Vec<PEv>> = (0..NS).map(|_| Vec::with_capacity(per)).collect();
            for e in best.values().chain(clash.values()).filter(|e| e.ok) {
                out[shard_of(e.ph)].push(PEv { person: e.person, ph: e.ph, ts: e.ts, price: e.price, cat: e.cat });
            }
            out
        })
        .collect();
    lap("dedup");

    // ---- phase 4: per person shard
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day0 + 1) as usize;
    let ctx = Ctx { ncamp, day0, ndays };
    let accs: Vec<Acc> = transpose(routed)
        .into_par_iter()
        .zip(transpose(clicks))
        .with_min_len((NS / (threads * 2)).max(1))
        .fold(
            || Acc::new(&ctx),
            |mut acc, (ev_col, cl_col)| {
                let n: usize = ev_col.iter().map(Vec::len).sum();
                let mut people = Interner::with_capacity(n / 8);
                let mut evs: Vec<(u32, i64, u32, u16)> = Vec::with_capacity(n);
                for v in ev_col {
                    for e in v {
                        evs.push((people.intern(e.ph, e.person), e.ts, e.price, e.cat));
                    }
                }
                let mut cls: Vec<(u32, i64, &[u8], u32)> = Vec::new();
                for v in cl_col {
                    for k in v {
                        if let Some(p) = people.get(k.ph, k.person) {
                            cls.push((p, k.ts, k.id, k.camp));
                        }
                    }
                }
                let names = people.names;
                evs.sort_unstable_by_key(|e| (e.0, e.1));
                cls.sort_unstable_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
                let (mut i, mut k) = (0, 0);
                while i < evs.len() {
                    let p = evs[i].0;
                    let mut j = i;
                    while j < evs.len() && evs[j].0 == p {
                        j += 1;
                    }
                    while k < cls.len() && cls[k].0 < p {
                        k += 1;
                    }
                    let mut m = k;
                    while m < cls.len() && cls[m].0 == p {
                        m += 1;
                    }
                    acc.person(&ctx, names[p as usize], &evs[i..j], &cls[k..m]);
                    i = j;
                    k = m;
                }
                acc
            },
        )
        .collect();
    lap("sessions");

    // ---- phase 5: output
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");

    let mut camps = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for a in &accs {
        for (t, s) in camps.iter_mut().zip(&a.camps) {
            for x in 0..4 {
                t[x] += s[x];
            }
        }
    }
    let mut crows = Vec::new();
    for c in 0..=ncamp {
        let (id, ch): (&[u8], &[u8]) = if c == ncamp { (b"organic", b"organic") } else { (camp_rows[c][0], camp_rows[c][1]) };
        for d in 0..ndays {
            let v = camps[c * ndays + d];
            if v[0] > 0 || v[3] > 0 {
                crows.push(output::CRow { campaign: id, channel: ch, day: (day0 + d as i64) as i32, v });
            }
        }
    }

    let user_parts: Vec<&Acc> = accs.iter().filter(|a| !a.users.is_empty()).collect();
    rayon::join(
        || output::write_campaigns(&cd.join("part-0.parquet"), &crows),
        || {
            if user_parts.is_empty() {
                output::write_users(&ud.join("part-0.parquet"), &[], &cat_names);
            }
            user_parts.par_iter().enumerate().for_each(|(i, a)| {
                output::write_users(&ud.join(format!("part-{i}.parquet")), &a.users, &cat_names);
            });
        },
    );
    lap("written");
}
