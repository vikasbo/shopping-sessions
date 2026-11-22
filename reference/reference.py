#!/usr/bin/env python3
"""
Slow, literal reference implementation of rules.md. It is the correctness oracle
for the Rust pipeline: readability over speed. Every step is numbered as in rules.md.

Usage: reference.py [DATA_DIR OUTPUT_DIR]   (or env DATA_DIR / OUTPUT_DIR)
Needs pyarrow for the Parquet output.
"""
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
DIGITS = b"0123456789"


# ---------------------------------------------------------------- CSV dialect
def parse_line(line, ncols):
    """Return a list of values (bytes, or None for empty) or None if malformed."""
    if line == b"":
        return None
    fields = line.split(b",")
    if len(fields) != ncols:
        return None
    out = []
    for f in fields:
        if b'"' in f:
            if len(f) < 2 or f[0:1] != b'"' or f[-1:] != b'"' or b'"' in f[1:-1]:
                return None
            f = f[1:-1]
        out.append(f if f != b"" else None)
    return out


def read_rows(path, ncols):
    with open(path, "rb") as fh:
        data = fh.read()
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()  # text after the final \n is not a line
    for line in lines[1:]:  # first line is the header
        row = parse_line(line, ncols)
        if row is not None:
            yield row


def read_dir(data, sub, ncols):
    for path in sorted(glob.glob(os.path.join(data, sub, "*.csv"))):
        yield from read_rows(path, ncols)


# ---------------------------------------------------------------- timestamps
def is_digits(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def days_in_month(y, m):
    if m == 2:
        leap = (y % 4 == 0 and y % 100 != 0) or y % 400 == 0
        return 29 if leap else 28
    return 30 if m in (4, 6, 9, 11) else 31


def parse_ts(b):
    """Epoch ms (int, possibly huge for F3) or None if bad."""
    if b is None:
        return None
    if is_digits(b):  # F3
        return int(b)
    if len(b) not in (20, 25):
        return None
    d = b[:19]
    if not (d[4:5] == b"-" and d[7:8] == b"-" and d[10:11] == b"T" and d[13:14] == b":" and d[16:17] == b":"):
        return None
    parts = [d[0:4], d[5:7], d[8:10], d[11:13], d[14:16], d[17:19]]
    if not all(len(p) and is_digits(p) for p in parts):
        return None
    y, mo, da, h, mi, s = (int(p) for p in parts)
    if not (1 <= mo <= 12 and 1 <= da <= days_in_month(y, mo) and h <= 23 and mi <= 59 and s <= 59):
        return None
    if y < 1:
        return None
    zone = b[19:]
    if zone == b"Z":
        off = 0
    elif len(zone) == 6 and zone[0:1] in (b"+", b"-") and zone[3:4] == b":" and is_digits(zone[1:3]) and is_digits(zone[4:6]):
        oh, om = int(zone[1:3]), int(zone[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000 * (1 if zone[0:1] == b"+" else -1)
    else:
        return None
    base = datetime(y, mo, da, h, mi, s, tzinfo=timezone.utc)
    return int(base.timestamp()) * 1000 - off


# ---------------------------------------------------------------- union-find
class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        p = self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:  # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # keep the byte-wise smallest ID as the root, so the root is the name
            if rb < ra:
                ra, rb = rb, ra
            self.parent[rb] = ra


def main():
    data = sys.argv[1] if len(sys.argv) > 2 else os.environ.get("DATA_DIR", "/data")
    out = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("OUTPUT_DIR", "/output")

    with open(os.path.join(data, "manifest.json")) as fh:
        man = json.load(fh)
    w_start = parse_ts(man["window_start"].encode())
    w_end = parse_ts(man["window_end"].encode())

    products = {r[0]: r[1] for r in read_rows(os.path.join(data, "products.csv"), 2)}
    campaigns = {r[0]: r[1] for r in read_rows(os.path.join(data, "campaigns.csv"), 2)}

    # Linked IDs
    uf = UnionFind()
    for a, b in read_dir(data, "links", 2):
        if a is None or b is None:
            continue
        uf.union(a, b)

    # Events 1-2: parse, dedup on event_id keeping smallest ingest_ts
    kept = {}
    for row in read_dir(data, "events", 7):
        eid = row[0]
        if eid is None:
            continue
        ing = parse_ts(row[3])
        if ing is None:  # rules say always valid F1; be defensive
            continue
        cur = kept.get(eid)
        if cur is None or ing < cur[0]:
            kept[eid] = (ing, row)

    # Events 3-4: validate, drop late
    per_person = defaultdict(list)  # person -> [(ts, price or None, category or None)]
    for ing, (eid, user, ts_b, _, etype, product, price_b) in kept.values():
        if user is None or product is None:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (w_start <= ts < w_end):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        price = None
        if etype == b"purchase":
            if price_b is None or not is_digits(price_b):
                continue
            price = int(price_b)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > DAY_MS:
            continue
        cat = products.get(product, b"unknown") if price is not None else None
        per_person[uf.find(user)].append((ts, price, cat))

    # Clicks
    clicks_seen = {}
    for cid, user, camp, ts_b in read_dir(data, "clicks", 4):
        if cid is None or cid in clicks_seen:
            continue
        clicks_seen[cid] = (user, camp, ts_b)
    person_clicks = defaultdict(list)
    for cid, (user, camp, ts_b) in clicks_seen.items():
        if user is None or camp not in campaigns:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (w_start - WEEK_MS <= ts < w_end):
            continue
        person_clicks[uf.find(user)].append((ts, cid, camp))

    user_daily = {}  # (person, day) -> [sessions, events, revenue, {cat: rev}]
    camp_daily = {}  # (camp, day) -> [sessions, purchasing, revenue, assisted]

    for person, evs in per_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if not sessions or e[0] - sessions[-1][-1][0] > GAP_MS:
                sessions.append([e])
            else:
                sessions[-1].append(e)
        clicks = person_clicks.get(person, [])
        for sess in sessions:
            start = sess[0][0]
            day = start // DAY_MS
            rev = sum(e[1] for e in sess if e[1] is not None)
            purchased = any(e[1] is not None for e in sess)
            u = user_daily.setdefault((person, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(sess)
            u[2] += rev
            for e in sess:
                if e[1] is not None:
                    u[3][e[2]] += e[1]

            qual = [c for c in clicks if start - WEEK_MS <= c[0] <= start]
            if qual:
                # latest ts, then smallest click_id (byte-wise)
                best = sorted(qual, key=lambda c: (-c[0], c[1]))[0]
                camp = best[2]
            else:
                camp = b"organic"
            c = camp_daily.setdefault((camp, day), [0, 0, 0, 0])
            c[0] += 1
            c[1] += 1 if purchased else 0
            c[2] += rev
            for a in {q[2] for q in qual}:
                camp_daily.setdefault((a, day), [0, 0, 0, 0])[3] += 1

    def date(day):
        return datetime.fromtimestamp(day * 86400, tz=timezone.utc).date()

    rows_u = []
    for (person, day), (s, n, rev, cats) in user_daily.items():
        top = None
        if cats:
            top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0][0].decode()
        rows_u.append((person.decode(), date(day), s, n, rev, top))
    rows_c = []
    for (camp, day), (s, p, rev, a) in camp_daily.items():
        ch = b"organic" if camp == b"organic" else campaigns[camp]
        rows_c.append((camp.decode(), ch.decode(), date(day), s, p, rev, a))

    write(out, "user_daily", ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
          [pa.string(), pa.date32(), pa.int64(), pa.int64(), pa.int64(), pa.string()], rows_u)
    write(out, "campaign_daily", ["campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                                  "revenue_cents", "assisted_sessions"],
          [pa.string(), pa.string(), pa.date32(), pa.int64(), pa.int64(), pa.int64(), pa.int64()], rows_c)


def write(out, name, cols, types, rows):
    os.makedirs(os.path.join(out, name), exist_ok=True)
    arrays = [pa.array([r[i] for r in rows], type=t) for i, t in enumerate(types)]
    pq.write_table(pa.table(arrays, names=cols), os.path.join(out, name, "part-0.parquet"))


if __name__ == "__main__":
    main()
