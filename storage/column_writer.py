"""
column_writer.py — Writes one column's values into 4096-byte pages on disk.

Each column gets its own binary file (col_<name>.bin) composed of sequential
4096-byte pages. Values are buffered in a Page in memory and flushed to disk
when the page is full.
"""

import os
from storage.page import Page
from storage.schema import ColumnDef, DType
from storage.dictionary import Dictionary
from query.zone_map import ZoneMap 


class ColumnWriter:
    """
    Writes values for a single column to a binary file on disk.

    Usage:
        writer = ColumnWriter(col_def, data_dir="data", dictionary=town_dict)
        for raw_value in csv_rows:
            writer.append(raw_value)
        writer.close()
    """

    def __init__(self, col_def: ColumnDef, data_dir: str = "data",
                 dictionary: Dictionary = None):
        self.col_def    = col_def
        self.dictionary = dictionary
        self.page_count = 0
        self.zone_map = ZoneMap(col_def.name)

        os.makedirs(data_dir, exist_ok=True)
        self._file     = open(col_def.bin_path(data_dir), "wb")
        self._cur_page = Page(page_id=0, dtype=col_def.dtype)

    def append(self, raw_value: str) -> None:
        """Encode raw_value and write it into the current page, flushing to disk when full."""
        encoded = self._encode(raw_value)

        if self._cur_page.is_full:
            self._flush_page()

        self._cur_page.append(encoded)

    def close(self) -> None:
        """Flush any remaining records and close the file."""
        if self._cur_page.records:
            self._flush_page()
        self._file.close()

    def _encode(self, raw_value: str):
        """Convert a raw CSV string to the stored type."""
        if self.dictionary is not None:
            return self.dictionary.encode(raw_value)
        if self.col_def.dtype == DType.FLOAT32:
            return float(raw_value)
        return int(raw_value)

    def _flush_page(self) -> None:
        """Serialize the current page to exactly 4096 bytes and write to disk."""

        # Calculate and store Zone Map statistics before flushing
        if self._cur_page.records:
            if self.dictionary is None:
                # Numeric column (like floor_area): Store min and max
                min_val = min(self._cur_page.records)
                max_val = max(self._cur_page.records)
                self.zone_map.add_area_stats(min_val, max_val)
            else:
                # Dictionary column (like town): Calculate and store bitmask
                bitmask = 0
                for val in self._cur_page.records:
                    bitmask |= (1 << val)
                self.zone_map.add_town_bitmask(bitmask)
                
        self._file.write(self._cur_page.serialize())
        self.page_count += 1
        self._cur_page = Page(page_id=self.page_count, dtype=self.col_def.dtype)
