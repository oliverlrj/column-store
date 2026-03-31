import json
import os

class MonthIndex:
    """
    An inverted index mapping a specific 'YYYY-MM' to the block numbers
    where those records appear. This allows the query engine to instantly 
    skip reading disk blocks that don't contain the target timeframe.
    """
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.index_path = os.path.join(data_dir, "month_index.json")
        
        # Dictionary mapping string "YYYY-MM" to a set of unique block numbers
        # Example: {"2015-08": {12, 13, 14}}
        self.mapping = {}

    def add_record(self, year: int, month_num: int, block_num: int):
        """Tracks that a specific year/month exists in a specific block."""
        if year == 0 or month_num == 0:
            return # Skip invalid dates
            
        key = f"{year}-{month_num:02d}"
        if key not in self.mapping:
            self.mapping[key] = set()
        self.mapping[key].add(block_num)

    def get_blocks(self, year: int, month_num: int) -> set:
        """Returns the set of block numbers containing the target year and month."""
        key = f"{year}-{month_num:02d}"
        return self.mapping.get(key, set())

    def save(self):
        """Saves the index to disk as a JSON file."""
        # Convert sets to lists because JSON cannot serialize Python sets
        serializable_mapping = {k: list(v) for k, v in self.mapping.items()}
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.index_path, "w", encoding="utf-8") as f:
            json.dump(serializable_mapping, f)

    def load(self):
        """Loads the index from disk."""
        if os.path.exists(self.index_path):
            with open(self.index_path, "r", encoding="utf-8") as f:
                loaded_mapping = json.load(f)
                # Convert lists back to sets for lightning-fast lookups
                self.mapping = {k: set(v) for k, v in loaded_mapping.items()}