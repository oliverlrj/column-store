# Column Store

A disk-based column-oriented storage engine for HDB resale flat data (SC4023 project).

## Project Structure

```
column-store/
  storage/          core storage engine
  query/            filtering and aggregation (in progress)
  data/             generated binary column files (git-ignored)
  constants.py      known value lists (towns, flat types, etc.)
  main.py           entry point
  ColumnStore.py    friend's original in-memory reference implementation
```

## Storage Layer (`storage/`)

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

Result on disk — e.g. `col_town.bin` with ~190k rows at 1 byte/value ≈ 47 blocks:
```
Block 0:  [8-byte header][4088 town IDs packed as INT8]
Block 1:  [8-byte header][4088 town IDs packed as INT8]
...
Block 46: [8-byte header][partial block, zero-padded to 4096 bytes]
```

---

### `storage/column_reader.py` — Read path

The counterpart to the writer. It fetches specific rows from the binary `.bin` files without loading the entire file into memory.

Read flow and optimization:
* **Byte-Offset Math:** Calculates exactly which 4096-byte block a requested row lives in (`row_index // capacity`).
* **Disk Buffering:** Keeps the most recently read 4096-byte block in memory. If the query asks for row 10,000 and then row 10,001, the reader grabs the second value directly from RAM instead of hitting the hard drive twice.
* **Deserialization:** Locates the exact byte offset within the buffered block (`8-byte header + offset`), slices the raw bytes, and uses `struct.unpack` to convert them back to native Python types based on `DType`.


---

### `storage/store.py` - The Orchestrator
The bridge between the Storage layer and the Query layer. It manages the entire lifecycle of the database.
* **Build Phase:** Reads the raw `ResalePricesSingapore.csv`, parses the dates, delegates data to the 12 `ColumnWriters`, and saves the Dictionaries and Inverted Index to disk.
* **Query Phase:** Loads the Dictionaries and Indexes back into memory, initializes the `ColumnReaders`, and exposes a clean `get_value(column_name, row_index)` API for the execution engine to use.


---

## Query Layer (`query/`)

### `zone_map.py` — Data Skipping Index

A vital optimization to prevent the `ColumnReader` from doing unnecessary disk I/O. The Zone Map acts as a metadata cheat sheet for every 4096-byte block on disk.

* **Numeric Columns (e.g., floor_area):** Stores the `(min, max)` values of the block. If a query looks for flats >= 120sqm, and a block's max is 90sqm, the entire block is skipped.
* **Dictionary Columns (e.g., town):** Stores a bitmask representing which dictionary IDs are present in the block. Uses lightning-fast bitwise `&` operations to check if target towns exist in the block before reading it.


---

### `index.py` — Inverted Month Index

A high-level index that maps a specific Year and Month (e.g., `2015-08`) to a set of specific block numbers. 
* During the query phase, the engine asks the index for the blocks corresponding to the target 8-month window. 
* Any block not returned by the index is instantly bypassed, allowing the engine to skip hundreds of thousands of irrelevant rows without executing a single disk read.


---

### `query.py` — The Execution Engine

The core logic that solves the target analysis problem using a "Scorecard Matrix" approach.

* **Single-Pass Architecture:** Instead of querying the database multiple times for different `(x, y)` combinations, the engine scans the valid blocks exactly once.
* **Cascading Logic:** As a valid flat is found, its `Price Per Square Meter` is calculated and automatically cascaded down to update the minimum price for every valid `y` (area) parameter it satisfies.
* **Output:** Formats the final results and generates the target `ScanResult_<MatricNum>.csv` file.

---
## What's Next (Completed)
- `storage/column_reader.py` — read values back from disk by row position, with a 1-block buffer to avoid redundant disk reads
- `storage/store.py` — orchestrator: one writer/reader per column, loads CSV, exposes. `get_value(col, row)` API
- `query/zone_map.py` — per-block metadata (bitmask for towns, min/max for area) to skip irrelevant blocks
- `query/index.py` — month inverted index mapping month values to block numbers
- `query/query.py` — filtering pipeline and aggregation (min price/sqm, etc.)
- `main.py` — CLI entry point, matric number parsing, (x, y) loop, CSV output
