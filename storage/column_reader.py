import struct
import os
from storage.schema import ColumnDef

class ColumnReader:
    """
    Reads values for a single column from a binary file on disk.
    Uses a 1-block (4096 byte) memory buffer to minimize disk I/O.
    """
    def __init__(self, col_def: ColumnDef, data_dir: str = "data"):
        self.col_def = col_def
        self.dtype_size = col_def.dtype.byte_size()
        self.capacity = 4088 // self.dtype_size  # 4088 bytes available after 8-byte header
        
        # Build the exact unpack format string (e.g., '<f' for FLOAT32, '<b' for INT8)
        # The '<' ensures standard little-endian byte order.
        self.unpack_fmt = f"<{col_def.dtype.struct_fmt()}"
        
        # Buffer variables
        self.current_block_num = -1
        self.block_buffer = b""
        
        # Open the file in 'rb' (read binary) mode
        self._file = open(col_def.bin_path(data_dir), "rb")

    def get_value(self, row_index: int):
        """Fetches the value at the given row index, hitting disk only if necessary."""
        # 1. Figure out which block this row lives in
        block_num = row_index // self.capacity
        
        # 2. If the block isn't in our memory buffer, read it from disk
        if block_num != self.current_block_num:
            self._file.seek(block_num * 4096)
            self.block_buffer = self._file.read(4096)
            self.current_block_num = block_num
            
        # 3. Calculate the exact byte position inside the buffer
        index_in_block = row_index % self.capacity
        start_byte = 8 + (index_in_block * self.dtype_size)
        end_byte = start_byte + self.dtype_size
        
        # 4. Extract the raw bytes
        raw_bytes = self.block_buffer[start_byte:end_byte]
        
        # 5. Unpack the bytes back into a native Python integer or float.
        # struct.unpack always returns a tuple, so we grab the first element [0].
        unpacked_value = struct.unpack(self.unpack_fmt, raw_bytes)[0]
        
        return unpacked_value

    def close(self):
        """Closes the file pointer."""
        self._file.close()