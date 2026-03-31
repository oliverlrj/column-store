class ZoneMap:
    def __init__(self, column_name):
        self.column_name = column_name
        
        # We will store one entry per block. 
        # Index 0 = Block 0 metadata, Index 1 = Block 1 metadata, etc.
        self.area_min_max = [] 
        self.town_bitmasks = []

    def add_area_stats(self, min_val, max_val):
        """Called by the ColumnWriter just before it flushes a block to disk"""
        self.area_min_max.append((min_val, max_val))

    def add_town_bitmask(self, bitmask):
        """Called by the ColumnWriter to store the town presence bitmask"""
        self.town_bitmasks.append(bitmask)

    def should_scan_block(self, block_num, target_min_area, target_town_ids):
        """
        The query engine will call this before reading a block from disk.
        Returns True if the block MIGHT have matching data.
        Returns False if we can safely SKIP the block entirely.
        """
        # 1. Check Floor Area
        if self.area_min_max:
            block_min, block_max = self.area_min_max[block_num]
            if block_max < target_min_area:
                return False # Entire block is too small, skip it!

        # 2. Check Towns (using bitwise AND)
        if self.town_bitmasks:
            block_mask = self.town_bitmasks[block_num]
            # Create a mask of the towns the user actually wants
            target_mask = 0
            for t_id in target_town_ids:
                target_mask |= (1 << t_id)
                
            # If the bitwise AND is 0, none of the target towns are in this block
            if (block_mask & target_mask) == 0:
                return False # Skip it!
                
        return True # The block passed the checks, tell the reader to fetch it