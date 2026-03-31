from storage.store import ColumnStore
import math
import csv

# --- JONATHAN'S MATRICULATION RULES (U2340985K) ---
TARGET_YEAR = 2015
COMMENCING_MONTH = 8
TARGET_TOWNS = {
    "CLEMENTI", "CHOA CHU KANG", "HOUGANG", 
    "BEDOK", "YISHUN", "WOODLANDS", "JURONG WEST"
}
MAX_PRICE_PER_SQM = 4725

def run_query():
    store = ColumnStore()
    
    # We must pass the exact number of rows your database built.
    # Replace 259236 with your actual row count if it differs!
    TOTAL_ROWS = 259236 
    store.open_for_queries(TOTAL_ROWS)

    # The Scorecard: A dictionary to hold the best flat for each (x, y) pair
    # Key: (x, y) tuple -> Value: Dictionary of flat details
    scorecard = {}

    # --- Using the inverted index ---
    print("Consulting the Inverted Index...")
    valid_blocks = set()
    # We want data from August (Month 1) up to March of next year (Month 8)
    for i in range(8):
        # Calculate wrapping months (e.g., Month 13 becomes Jan of next year)
        target_month = COMMENCING_MONTH + i
        target_year = TARGET_YEAR
        if target_month > 12:
            target_month -= 12
            target_year += 1
            
        # Ask the index which blocks hold these months
        blocks = store.month_index.get_blocks(target_year, target_month)
        valid_blocks.update(blocks)
        
    print(f"Index optimization: Only scanning {len(valid_blocks)} blocks instead of reading the whole file!")
    # -----------------------------------

    for i in range(TOTAL_ROWS):
        # To skip data blocks that don't contain our target months, we can check the block number first before reading any columns.
        # If this row belongs to a block that isn't in our valid list, skip it instantly!
        if (i // 4088) not in valid_blocks:
            continue
            
        # 1. Filter by Year (Fastest check first)
        year = store.get_value('year', i)

    print("Scanning 259,000+ rows... (This will take a few seconds)")

    # The "One-Pass" Scan: We read each row once and apply all filters in sequence. If a row fails any filter, we skip to the next one immediately.
    for i in range(TOTAL_ROWS):
        
        # 1. Filter by Year (Fastest check first)
        year = store.get_value('year', i)
        if year != TARGET_YEAR:
            continue
            
        # 2. Filter by Town
        town = store.get_value('town', i)
        if town not in TARGET_TOWNS:
            continue
            
        # 3. Filter by Month to calculate 'x' (Timeframe)
        month_num = store.get_value('month_num', i)
        months_passed = month_num - COMMENCING_MONTH
        if months_passed < 0:
            continue # Sale happened before August
            
        x = months_passed + 1 # e.g., August = Month 1, Sept = Month 2
        if x < 1 or x > 8:
            continue # Sale is outside our 8-month window
            
        # 4. Calculate Price per Square Meter
        area = store.get_value('floor_area', i)
        price = store.get_value('resale_price', i)
        price_per_sqm = price / area
        
        # Must be at most 4725
        if price_per_sqm > MAX_PRICE_PER_SQM:
            continue

        # 5. Update the Scorecard for 'y' (Area Requirement)
        # If a flat is 95 sqm, it satisfies the requirement for y=80, 81, ..., 95.
        max_y = min(150, int(math.floor(area)))
        
        for y in range(80, max_y + 1):
            key = (x, y)
            
            # If this (x,y) slot is empty OR we found a cheaper flat, save it!
            if key not in scorecard or price_per_sqm < scorecard[key]['price_per_sqm']:
                scorecard[key] = {
                    'price_per_sqm': price_per_sqm,
                    'year': year,
                    'month': month_num,
                    'town': town,
                    'block': store.get_value('block', i),
                    'floor_area': area,
                    'flat_model': store.get_value('flat_model', i),
                    'lease_date': store.get_value('lease_date', i)
                }

    store.close()
    
    # --- GENERATE THE CSV OUTPUT ---
    output_filename = "ScanResult_U2340985K.csv"
    print(f"Scan complete! Generating {output_filename}...")
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Write the required header from the assignment brief [cite: 92]
        writer.writerow(["(x, y)", "Year", "Month", "Town", "Block", "Floor_Area", "Flat_Model", "Lease_Commence_Date", "Price_Per_Square_Meter"])
        
        # Sort the results by x (increasing), then by y (increasing) [cite: 79, 80]
        sorted_keys = sorted(scorecard.keys())
        
        if not sorted_keys:
            # Assignment states: "If there is no qualified data in your target range, please take 'No result' as the query result." [cite: 78]
            writer.writerow(["No result"])
        else:
            for key in sorted_keys:
                flat = scorecard[key]
                
                # Format the month to strictly be "MM" (e.g., 08 instead of 8) [cite: 82]
                month_str = f"{flat['month']:02d}"
                
                # Format (x, y) pair with a space [cite: 93]
                xy_str = f"({key[0]}, {key[1]})"
                
                # Round price_per_sqm to nearest integer [cite: 87]
                rounded_price = round(flat['price_per_sqm'])
                
                writer.writerow([
                    xy_str, 
                    flat['year'], 
                    month_str, 
                    flat['town'], 
                    flat['block'], 
                    flat['floor_area'], 
                    flat['flat_model'], 
                    flat['lease_date'], 
                    rounded_price
                ])
                
    print("Done! Check your folder for the output file.")

if __name__ == "__main__":
    run_query()