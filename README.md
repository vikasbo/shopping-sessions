# Shopping Sessions: Rust pipeline

A single static Rust binary that reads `/data`, applies `rules.md`, and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet.

Verified against: all 36 fixtures, the published 1M-event `dev-answer` (exact match), and the
Python reference on 29 further generator seeds (3 of them at 1M events, one window holding
Feb 29 2028). A 20M-event run passes the self-checks in `rules.md` (equal session and revenue
totals across the two tables, assists >= credited non-organic sessions, no duplicate keys).

## Layout

| Path | What it is |
| --- | --- |
| `src/main.rs` | Orchestration: mmap, the parallel phases, output |
| `src/parse.rs` | CSV dialect (readable spec version), F1/F2/F3 timestamps, prices, chunking (+ unit tests) |
| `src/rows.rs` | Fast fused line/field splitter (SSE2 bitmask scan), fuzz-tested against `parse.rs` |
| `src/links.rs` | Union-find over linked IDs; person = byte-wise smallest ID |
| `src/sessions.rs` | Sessionize, click credit, 7-day assist window, aggregation |
| `src/output.rs` | Parquet writers (arrow-rs) |
| `reference/reference.py` | Slow, literal Python version of `rules.md`: the correctness oracle |
| `scripts/` | Fixture runner, output diff, scorer-like `docker run`, seed difftest |
| `fixtures/` | The 36 edge cases |

## How it works

1. **mmap** every CSV (zero-copy: all IDs are `&[u8]` slices into the mapped files).
2. **Links** are parsed in parallel, then a union-find whose root is always the smallest ID.
3. **Events and clicks** are cut into line-aligned chunks and parsed in parallel (rayon).
   The splitter finds `,` `"` `\n` 64 bytes at a time with SSE2 and jumps from one to the next.
   Each event line is fully validated at parse time, but invalid copies are *kept* with
   `ok = false`, because dedup happens before validation. The 64-bit hashes of the event ID and
   of the person are computed here, while the line is in cache. Events are sharded by
   `hash(event_id)`; clicks go straight to shards by `hash(person)`.
4. **Dedup** per event shard on the precomputed hash (the strings are compared only when two
   hashes are equal; a true 64-bit collision falls back to a string-keyed map, so the result
   is exact). The smallest `ingest_ts` wins; valid survivors are re-sharded by person.
   Every phase takes ownership of its shard and frees it when done, which keeps peak memory
   to about one copy of the data.
5. **Per person shard**: sort events by `(person, ts)` and clicks by `(person, ts, click_id)`,
   cut sessions (gap > 30 min), and walk the clicks with two pointers. The latest click at or
   before the start (ties: smallest id) gets the credit; a counted set of campaigns in the
   `[start - 7d, start]` window gives the assists in O(1) per click.
6. **Parquet** output, one `user_daily` file per worker, written in parallel.

Click duplicates are not removed on purpose: copies are identical, so they cannot change the
credited click or the set of assisting campaigns.

## Commands

```sh
make test          # native build + unit tests + 36 fixtures (no Docker)
make fixtures      # Docker image + 36 fixtures with the scorer's docker flags
make run-dev       # Docker run on dev/ (make dev first), timed
make check-dev     # diff out/ against dev-answer/
make difftest SEEDS="2 3 4"   # Rust vs Python reference on fresh seeds (needs `generate` on PATH)
```

Requirements: Rust 1.85+ (`rustup`), Python 3.10+ with `polars` and `pyarrow`, Docker with buildx.

`VERBOSE=1` prints per-phase wall time and CPU (user/sys) to stderr.

## Performance notes

- 1M events (dev set): about 0.4 s on 2 cores. 20M events (2 GB CSV): about 10 s on 2 cores,
  peak RSS about 5 GB, of which about 2 GB is the memory-mapped input. Those numbers come from
  a sandbox VM with very expensive page faults; a normal VM such as the scorer's should be faster.
- The Docker build uses `-C target-cpu=x86-64-v3` (AVX2). Pass
  `--build-arg TARGET_CPU=cascadelake` to target the scorer's AVX-512 exactly; that image would
  not run on a CPU without AVX-512. `NCPU` sets the thread count (the scorer sets it).
