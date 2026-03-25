#Q1. For our project, can we use Python?
#A1: You can use Python, just that you need to reveal the column-oriented 
#    design procedures in the code and report.

# Python Pandas is NOT column-oriented
# Simple SQL-Implementation is NOT column-oriented


# Packages
import csv

class ColumnStore:

    # Create a column store with an empty dictionary
    def __init__(self):
        self.columns = {}
        self.header = [] # Keeps track of order of columns (for inserting rows) 

    # Adds a new column: column_name(key) : empty list(value)
    def add_column(self, column_name):
        self.columns[column_name] = []
        self.header.append(column_name)

    # Insert an entry [list] into the column store
    def insert_row(self, row):
        for i, value in enumerate(row):
            column_name = self.header[i]
            self.columns[column_name].append(value)


def main():

    
    
    # CSV file -> Column Store
    column_store = ColumnStore()

    with open('ResalePricesSingapore.csv', 'r') as file:
        reader = csv.reader(file)
        
        # Add empty columns to the column store
        Header = next(reader)
        for column_name in Header:
            column_store.add_column(column_name)

        # Insert rows into the column store
        for row in reader:
            column_store.insert_row(row)

    # Querying, Find the minimun price per square metre of matched flats
    # Input: U2340985K (Jonathan's Matric No.)
    # x months
    # [CLEMENTI, CHOA CHU KANG, HOUGANG, BEDOK, YISHUN, WOODLANDS, JURONG WEST]
    # Target Year: 2015
    # Commencing Month: 8 (August)
    # y>=80, y<=150 
    # Find all possible combinations of (x,y) that gives one record satisfying the above conditions, 
    # and output the corresponding record with the minimum price per square metre that is not higher
    # than 4725


    # x months, [] list of matched towns, y square metres, target year
    # 
    # x is a user-input from 1-8, it means pick whatever you want 
    #
    # Target Year corresponds to last digit of Matric No., 
    # e.g. A5656567B -> 2017, A1234565B -> 2015
    # (Note, 2025 is only provided for querying, not as the target year)
    #
    # commencing month (MM) corresponds to 2nd last digit of Matric No.,
    # "0" -> october, rest is obvious, "1" -> Jan and so on
    #
    # List of towns Depends on ALL digits of Matric No. (Refer to file)
    #
    # square metres y is the range, 80 <= y <= 150





if __name__ == "__main__":
    main()