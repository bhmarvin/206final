import requests
import sqlite3
from datetime import datetime

def create_crashes_table(cursor):
    """Create crashes table in the database if not exists"""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crashes (
            id INTEGER PRIMARY KEY,
            county_id INTEGER,
            CrashDate TEXT,
            Fatals INTEGER,
            Peds INTEGER,
            Persons INTEGER,
            St_Case INTEGER,
            State INTEGER,
            TotalVehicles INTEGER,
            UNIQUE(St_Case) ON CONFLICT IGNORE,
            FOREIGN KEY (county_id) REFERENCES counties(county_id)
        )
    ''')

def create_counties_table(cursor):
    """Create counties table in the database if not exists"""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS counties (
            county_id INTEGER PRIMARY KEY,
            county_name TEXT UNIQUE ON CONFLICT IGNORE
        )
    ''')

def insert_county(cursor, county_name):
    """Insert county into counties table and return its ID"""
    cursor.execute('''
        INSERT OR IGNORE INTO counties (county_name) VALUES (?)
    ''', (county_name,))
    cursor.execute('''
        SELECT county_id FROM counties WHERE county_name = ?
    ''', (county_name,))
    return cursor.fetchone()[0]

def insert_crash_data(cursor, entry):
    """Insert crash data into the database"""
    county_id = insert_county(cursor, entry["CountyName"])
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO crashes VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            county_id,
            datetime.utcfromtimestamp(int(entry["CrashDate"][6:16])),
            entry["Fatals"],
            entry["Peds"],
            entry["Persons"],
            entry["St_Case"],
            entry["State"],
            entry["TotalVehicles"],
        ))
    except sqlite3.Error as e:
        print("SQLite error:", e)

def fetch_api_data(start_date, end_date, start_index):
    """Fetch API data from NHTSA DOT API"""
    url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCaseList"
    params = {
        "states": "26",  # Change the state code to Michigan's code
        "fromYear": start_date.year,
        "toYear": end_date.year,
        "minNumOfVehicles": "1",
        "maxNumOfVehicles": "6",
        "format": "json",
    }

    # Make the API request
    response = requests.get(url, params=params)
    return response.json()["Results"]

def update_start_index(end_index):
    """Update the start index in the file"""
    with open('start_index.txt', 'w') as file:
        file.write(str(end_index))

def fetch_and_insert_crashes(cursor, start_date, end_date):
    """Fetch and insert crashes data into the database"""
    start_index = int(open('start_index.txt', 'r').read().strip())
    api_data = fetch_api_data(start_date, end_date, start_index)
    end_index = start_index + 25

    if end_index >= len(api_data[0]):
        print("Done fetching data for this time period")
        return
    # INSERTS THE NEXT 25
    for entry in api_data[0][start_index:end_index]:
        insert_crash_data(cursor, entry)

    update_start_index(end_index)
    conn.commit()

# SQLite database connection
conn = sqlite3.connect('proj_data.db')
cursor = conn.cursor()

# Create counties table in the database if not exists
create_counties_table(cursor)

# Create crashes table in the database if not exists
create_crashes_table(cursor)
conn.commit()

# Set the time period for the entire timeframe
start_date_initial = datetime(2020, 1, 1)
end_date_initial = datetime(2021, 1, 1)

# Fetch and insert the next 25 records for Michigan
fetch_and_insert_crashes(cursor, start_date_initial, end_date_initial)

# Close the database connection
conn.close()
