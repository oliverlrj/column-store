"""
schema.py — Column definitions for the column store.

Each column has:
  - name:         logical name used for filenames (col_<name>.bin)
  - dtype:        storage type (INT8, INT16, INT32, FLOAT32)
  - dict_encoded: if True, strings are mapped to integer IDs before storage
"""

from enum import Enum


class DType(Enum):
    INT8    = 1   # 1 byte  signed — small codes: dict IDs for town, flat_type, etc.
    INT16   = 2   # 2 bytes signed — years, large dict IDs (block, street_name)
    INT32   = 3   # 4 bytes signed — large integers (enum value ≠ byte size here)
    FLOAT32 = 4   # 4 bytes float  — floor_area, resale_price

    def byte_size(self) -> int:
        return {DType.INT8: 1, DType.INT16: 2, DType.INT32: 4, DType.FLOAT32: 4}[self]

    def struct_fmt(self) -> str:
        """Return struct format character for one value (little-endian)."""
        return {DType.INT8: 'b', DType.INT16: 'h', DType.INT32: 'i', DType.FLOAT32: 'f'}[self]


class ColumnDef:
    def __init__(self, name: str, dtype: DType, dict_encoded: bool = False):
        self.name         = name
        self.dtype        = dtype
        self.dict_encoded = dict_encoded

    def bin_path(self, data_dir: str = "data") -> str:
        return f"{data_dir}/col_{self.name}.bin"

    def dict_path(self, data_dir: str = "data") -> str:
        return f"{data_dir}/dict_{self.name}.json"


# ---------------------------------------------------------------------------
# All columns in the order they appear logically.
# month is split into year + month_num for efficient range filtering.
# ---------------------------------------------------------------------------
COLUMNS = [
    ColumnDef("year",         DType.INT16),
    ColumnDef("month_num",    DType.INT8),

    # String columns — dictionary encoded to save space
    ColumnDef("town",         DType.INT8,  dict_encoded=True),  # ~26 unique towns
    ColumnDef("flat_type",    DType.INT8,  dict_encoded=True),  # ~7 types
    ColumnDef("block",        DType.INT16, dict_encoded=True),  # hundreds of blocks
    ColumnDef("street_name",  DType.INT16, dict_encoded=True),  # many streets
    ColumnDef("storey_range", DType.INT8,  dict_encoded=True),  # ~17 ranges
    ColumnDef("flat_model",   DType.INT8,  dict_encoded=True),  # ~15 models

    # Numeric columns — floor_area and resale_price stored as 32-bit floats
    ColumnDef("floor_area",   DType.FLOAT32),  # sqm, e.g. 126.0
    ColumnDef("lease_date",   DType.INT16),    # year, 1966–2024
    ColumnDef("resale_price", DType.FLOAT32),  # dollars, e.g. 430000.0
]

# Convenient lookup by name
COLUMN_MAP = {col.name: col for col in COLUMNS}
