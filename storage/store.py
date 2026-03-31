import csv
import os
from storage.schema import COLUMNS
from storage.column_writer import ColumnWriter
from storage.column_reader import ColumnReader
from storage.dictionary import Dictionary
from query.index import MonthIndex

class ColumnStore:
    """
    The orchestrator for the disk-based column store.
    Manages loading the CSV into binary pages and opening readers for querying.
    """
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.readers = {}
        self.dictionaries = {}
        self.total_rows = 0

    def build_from_csv(self, csv_path):
        """Reads the CSV and uses ColumnWriters to generate the binary files."""
        print("Initializing dictionaries and writers...")
        
        writers = []
        for col_def in COLUMNS:
            dict_obj = None
            if col_def.dict_encoded:
                dict_obj = Dictionary()
                self.dictionaries[col_def.name] = dict_obj

            writer = ColumnWriter(col_def, self.data_dir, dict_obj)
            writers.append(writer)

        print(f"Reading {csv_path} and writing binary pages...")
        with open(csv_path, 'r', encoding='utf-8') as file:
            # Initialize the Inverted Index
            month_index = MonthIndex(self.data_dir)
            reader = csv.reader(file)
            header = next(reader)

            header_map = {name: idx for idx, name in enumerate(header)}

            def parse_month_field(s: str):
                try:
                    mon_abbr, yy = s.split('-')
                    mon_abbr = mon_abbr.strip().lower()
                    month_map = {
                        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
                    }
                    month_num = month_map.get(mon_abbr[:3], 0)
                    year = int(yy)
                    if year < 100:
                        year = 2000 + year
                    return year, month_num
                except Exception:
                    return 0, 0

            for row in reader:
                ordered_values = []

                # Intercept the 'month' column and split it into year and month_num
                month_raw = row[header_map.get('month')]
                yr, mn = parse_month_field(month_raw)
                ordered_values.append(str(yr))
                ordered_values.append(str(mn))

                # Map the rest of the CSV columns to match the schema.py order
                ordered_values.append(row[header_map.get('town')])
                ordered_values.append(row[header_map.get('flat_type')])
                ordered_values.append(row[header_map.get('block')])
                ordered_values.append(row[header_map.get('street_name')])
                ordered_values.append(row[header_map.get('storey_range')])
                ordered_values.append(row[header_map.get('flat_model')])
                ordered_values.append(row[header_map.get('floor_area_sqm')])
                ordered_values.append(row[header_map.get('lease_commence_date')])
                ordered_values.append(row[header_map.get('resale_price')])

                # Now the data perfectly matches the 12 writers!
                for i, value in enumerate(ordered_values):
                    writers[i].append(value)

                self.total_rows += 1
                # Add to inverted index
                block_num = self.total_rows // 4088
                month_index.add_record(yr, mn, block_num)

        for writer in writers:
            writer.close()

        # Save dictionaries to disk
        for col_def in COLUMNS:
            if col_def.dict_encoded and col_def.name in self.dictionaries:
                path = col_def.dict_path(self.data_dir)
                os.makedirs(self.data_dir, exist_ok=True)
                self.dictionaries[col_def.name].save(path)

        # Save the index to disk
        month_index.save()   

        print(f"Success! {self.total_rows} rows written to disk-based column store.")
        

    def open_for_queries(self, total_rows):
        """Initializes the ColumnReaders so we can query the data."""
        self.total_rows = total_rows
        
        # Your fixed logic to load the dictionaries safely
        for col_def in COLUMNS:
            if col_def.dict_encoded:
                dict_path = col_def.dict_path(self.data_dir)
                try:
                    dict_obj = Dictionary()
                    dict_obj.load(dict_path)
                    self.dictionaries[col_def.name] = dict_obj
                except FileNotFoundError:
                    pass

            self.readers[col_def.name] = ColumnReader(col_def, self.data_dir)

        # Load the index for querying
        self.month_index = MonthIndex(self.data_dir)
        self.month_index.load()
            
            
    def get_value(self, column_name, row_index):
        """Fetches a specific value from disk using the ColumnReader."""
        if column_name not in self.readers:
            raise ValueError(f"Column {column_name} not found.")
            
        raw_val = self.readers[column_name].get_value(row_index)
        
        if column_name in self.dictionaries:
            return self.dictionaries[column_name].decode(raw_val)
            
        return raw_val

    def close(self):
        """Close all open file pointers."""
        for reader in self.readers.values():
            reader.close()


def main():
    store = ColumnStore()
    csv_file = 'ResalePricesSingapore.csv'

    # STEP 1: Build the binary database (Only needs to be run once!)
    # store.build_from_csv(csv_file) 
    
    # STEP 2: Open the database for querying
    # You'll need to pass the actual total rows. (e.g., 259236)
    store.open_for_queries(259236) 
    
    # Verify it works by grabbing data from the disk
    print("Testing disk read for Row 1:")
    print(f"Town: {store.get_value('town', 1)}")
    print(f"Price: {store.get_value('resale_price', 1)}")

    store.close()

    # --- QUERY LOGIC ---
    # Input: U2340985K (Jonathan's Matric No.)
    # Target Year: 2015
    # Commencing Month: 8 (August)
    # Matched Towns: CLEMENTI, CHOA CHU KANG, HOUGANG, BEDOK, YISHUN, WOODLANDS, JURONG WEST
    # 
    # Your teammate's breakdown of the matriculation number in the comments is 100% accurate!
    # The next step is to build the 'Scorecard' loop in query.py using store.get_value() 
    # and the ZoneMap to find the absolute minimum prices.

if __name__ == "__main__":
    main()