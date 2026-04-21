# Column Store

A disk-based column-oriented storage engine for HDB resale flat data (SC4023 project).

## Project Structure

```
column-store/
  storage/          core storage engine (schema, pages, dicts, writers/readers, store)
  query/            zone maps, month index, execution engine
  data/             binary column files, dictionaries, month index JSON (committed for reproducibility)
  main.py           program entry (documented in Entry point section)
```

## Storage Layer (`storage/`)

Bottom-up layout: **`schema.py`** (types and column list) → **`page.py`** (4096-byte on-disk page) → **`dictionary.py`** (string → id) → **`column_writer.py`** / **`column_reader.py`** (append and random access per column) → **`store.py`** (orchestrates load, dictionaries, and `get_value` for queries).

### `schema.py` — Column definitions

Defines `DType`, the four storage types:

| DType   | Bytes | Struct | Used for |
|---------|-------|--------|----------|
| INT8    | 1     | `b`    | Dict IDs for town, flat_type, storey_range, flat_model |
| INT16   | 2     | `h`    | Year, lease_date, dict IDs for block, street_name |
| INT32   | 4     | `i`    | Large integers (reserved) |
| FLOAT32 | 4     | `f`    | floor_area, resale_price |

Also defines `ColumnDef` (name + dtype + dict_encoded flag) and the full `COLUMNS` list:

| Column       | DType   | Dict encoded |
|--------------|---------|--------------|
| year         | INT16   |              |
| month_num    | INT8    |              |
| town         | INT8    | yes (~26)    |
| flat_type    | INT8    | yes (~7)     |
| block        | INT16   | yes (~500+)  |
| street_name  | INT16   | yes (~500+)  |
| storey_range | INT8    | yes (~17)    |
| flat_model   | INT8    | yes (~15)    |
| floor_area   | FLOAT32 |              |
| lease_date   | INT16   |              |
| resale_price | FLOAT32 |              |

---

### `page.py` — Fixed-size binary page

The unit of disk I/O. Every read and write is exactly **4096 bytes**.

Page layout:
```
Bytes 0–3   : page_id       (uint32)
Bytes 4–5   : num_records   (uint16)
Byte  6     : dtype_id      (uint8, matches DType.value)
Byte  7     : padding
Bytes 8–4095: packed values (zero-padded to fill the page)
```

Capacity per page by type:
```
INT8    → 4088 ÷ 1 = 4088 values/page
INT16   → 4088 ÷ 2 = 2044 values/page
INT32   → 4088 ÷ 4 = 1022 values/page
FLOAT32 → 4088 ÷ 4 = 1022 values/page
```

Key methods:
- `append(value)` — add a value to the in-memory records list
- `is_full` — true when records reach capacity
- `serialize() -> bytes` — pack into exactly 4096 bytes for disk write
- `deserialize(bytes) -> Page` — unpack 4096 bytes back into a Page (used by reader)

---

### `dictionary.py` — String-to-integer encoding

String columns cannot be stored in fixed-size integer pages. Dictionary encoding
maps each unique string to a compact integer ID before storage.

```
encode("PASIR RIS") → 6   (used when writing to disk)
decode(6) → "PASIR RIS"   (used when outputting query results)
```

Two modes:
- **Closed-set** (town, flat_type, storey_range, flat_model): pre-populated from a
  known list so IDs are stable regardless of CSV row order.
- **Open-set** (block, street_name): starts empty, auto-assigns IDs as new strings
  appear in the CSV.

Persisted to `data/dict_<name>.json` as a JSON array where index == ID:
```json
["ANG MO KIO", "BEDOK", "BISHAN", ..., "YISHUN"]
```

---

### `column_writer.py` — Write path

One `ColumnWriter` per column. Buffers values in a `Page` in memory and flushes
to disk in 4096-byte blocks when the page is full.

Write flow per value:
```
raw CSV string
  → _encode():
      dict column  → dictionary.encode("PASIR RIS") = 6
      FLOAT32      → float("430000.0") = 430000.0
      int column   → int("2022") = 2022
  → cur_page.append(encoded)
  → if cur_page.is_full:
        cur_page.serialize() → write 4096 bytes to col_<name>.bin
        start new Page
```

Result on disk — e.g. `col_town.bin` with ~259k rows at 1 byte/value ≈ 64 blocks:
```
Block 0:  [8-byte header][4088 town IDs packed as INT8]
Block 1:  [8-byte header][4088 town IDs packed as INT8]
...
Block 63: [8-byte header][partial block, zero-padded to 4096 bytes]
```

---

### `column_reader.py` — Read path

The counterpart to the writer. It fetches specific rows from the binary `.bin` files without loading the entire file into memory.

Read path:
* **Block index:** `row_index // capacity`, where `capacity` is `4088 // dtype` bytes (same idea as the writer’s page capacity).
* **Single-block buffer:** Keeps the last-read 4096-byte page in memory so consecutive rows in the same block avoid extra disk reads.
* **Decode:** Offset within the page (`8-byte header + index * dtype size`), then `struct.unpack` using `DType`’s format.


---

### `store.py` — Orchestrator

Bridge between storage and query: owns build from CSV and the open-for-query lifecycle.
* **Build (`build_from_csv`):** Reads `ResalePricesSingapore.csv`, splits the CSV `month` field into `year` / `month_num` to align with `COLUMNS`, streams each row through 12 `ColumnWriter`s, updates the on-disk month index (`MonthIndex`), and saves dictionaries JSON. (Run when you need to regenerate `data/`.)
* **Query (`open_for_queries`):** Loads dictionaries and `MonthIndex`, opens one `ColumnReader` per column, exposes `get_value(column_name, row_index)` on the `ColumnStore` class.


---

## Query Layer (`query/`)

### `zone_map.py` — Per-block statistics

`ColumnWriter` updates a `ZoneMap` on each page flush: **numeric** columns record per-block `(min, max)`; **dictionary-encoded** columns record a bitmask of which dictionary IDs appear in the block. `should_scan_block(block_num, target_min_area, target_town_ids)` returns whether a block could still contain a matching row for those constraints.


---

### `index.py` — Month index (`MonthIndex`)

Maps `(year, month)` to the set of block numbers that contain at least one row from that month (built during CSV ingest, loaded in `open_for_queries`).

* For each `x`, `query.py` unions the blocks for the months in the rolling window and only considers rows whose block id `row_index // 4088` appears in that union (matching how blocks are tagged during `build_from_csv`).
* Rows that pass this stage are filtered by `year`, `month_num`, town, and price limits as required by the assignment.


---

### `query.py` — Execution engine

Implements the assignment logic: towns, price-per-m² cap, rolling windows from August 2015, and `(x, y)` pairs.

* **Scanning:** For each `x` (1–8), it collects candidate blocks from the month index for the corresponding months, then scans those rows after filters (town, window, price/sqm).
* **Scorecard:** Keeps the best (minimum) price per m² per `(x, y)` when a row qualifies; `y` runs over valid floor-area bands as required by the brief.
* **Output:** Writes `ScanResult_<MatricNum>.csv` with the required columns and ordering, or `No result` when nothing qualifies.

---

## Entry point (`main.py`)

`main.py` calls `run_query()` from `query.query`, which opens the column store with `open_for_queries` and runs the scan and CSV export. To rebuild `data/` from `ResalePricesSingapore.csv`, use `ColumnStore.build_from_csv` (see `storage/store.py`).
