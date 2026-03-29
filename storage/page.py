"""
page.py — Fixed-size binary page (4096 bytes).

Layout:
  Bytes 0-3  : page_id      (unsigned 32-bit int)
  Bytes 4-5  : num_records  (unsigned 16-bit int)
  Byte  6    : dtype_id     (unsigned 8-bit int, matches DType.value)
  Byte  7    : padding/reserved
  Bytes 8-4095 : packed data values (zero-padded to fill the page)
"""

import struct
from storage.schema import DType

PAGE_SIZE   = 4096
HEADER_SIZE = 8
DATA_SIZE   = PAGE_SIZE - HEADER_SIZE  # 4088 bytes available for values

# Header layout (little-endian): page_id (uint32), num_records (uint16), dtype_id (uint8), 1 pad byte
_HEADER_FMT = '<IHBx'


class Page:
    """
    In-memory representation of one 4096-byte page.

    Attributes:
      page_id     : sequential page number within a column file (0-based)
      dtype       : DType of the values stored
      records     : list of values stored on this page (int or float)
    """

    def __init__(self, page_id: int, dtype: DType, records: list = None):
        self.page_id = page_id
        self.dtype   = dtype
        self.records = records if records is not None else []

    @property
    def capacity(self) -> int:
        """Maximum number of values this page can hold."""
        return DATA_SIZE // self.dtype.byte_size()

    @property
    def is_full(self) -> bool:
        return len(self.records) >= self.capacity

    def append(self, value):
        """Add one value (int or float) to this page. Raises if full."""
        if self.is_full:
            raise OverflowError(f"Page {self.page_id} is full ({self.capacity} records)")
        self.records.append(value)

    def get(self, index: int):
        """Retrieve a value by its position within this page (0-based)."""
        return self.records[index]

    # ------------------------------------------------------------------
    # Serialisation: Page → bytes  (for writing to disk)
    # ------------------------------------------------------------------
    def serialize(self) -> bytes:
        """
        Pack this page into exactly PAGE_SIZE (4096) bytes.
        Header is 8 bytes; data section is zero-padded to DATA_SIZE.
        """
        num_records = len(self.records)
        header = struct.pack(_HEADER_FMT, self.page_id, num_records, self.dtype.value)

        # Pack all record values tightly using the column's struct format
        fmt_char = self.dtype.struct_fmt()
        data = struct.pack(f'<{num_records}{fmt_char}', *self.records)

        # Zero-pad the data section to DATA_SIZE bytes
        data = data.ljust(DATA_SIZE, b'\x00')

        return header + data

    # ------------------------------------------------------------------
    # Deserialisation: bytes → Page  (for reading from disk)
    # ------------------------------------------------------------------
    @staticmethod
    def deserialize(raw: bytes) -> 'Page':
        """
        Parse a PAGE_SIZE byte string back into a Page object.
        """
        if len(raw) != PAGE_SIZE:
            raise ValueError(f"Expected {PAGE_SIZE} bytes, got {len(raw)}")

        page_id, num_records, dtype_id = struct.unpack(_HEADER_FMT, raw[:HEADER_SIZE])
        dtype = DType(dtype_id)

        fmt_char = dtype.struct_fmt()
        values = list(struct.unpack_from(f'<{num_records}{fmt_char}', raw, HEADER_SIZE))

        return Page(page_id, dtype, values)

    def __repr__(self):
        return (f"Page(id={self.page_id}, dtype={self.dtype.name}, "
                f"records={len(self.records)}/{self.capacity})")
