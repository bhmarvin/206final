import requests
import sqlite3
from datetime import datetime

# SQLite database connection
conn = sqlite3.connect('proj_data.db')
cursor = conn.cursor()

# Create crashes table in the database
cursor.execute('''
    CREATE TABLE IF NOT EXISTS crashes (
        id INTEGER PRIMARY KEY,
        CountyName TEXT,
        CrashDate TEXT,
        Fatals INTEGER,
        Peds INTEGER,
        Persons INTEGER,
        St_Case INTEGER,
        State INTEGER,
        StateName TEXT,
        TotalVehicles INTEGER,
        UNIQUE(St_Case) ON CONFLICT IGNORE
    )
''')
conn.commit()

def fetch_and_insert_crashes(cursor, start_date, end_date, limit=25):
    # Make the API request with parameters for the entire timeframe
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
    api_data = response.json()

    with open('start_index.txt', 'r') as file:
        start_index = int(file.read().strip())
    results = api_data["Results"]
    end_index = start_index + 24
    results = results[0]

    if end_index >= len(results):
        print("Done fetching data for this time period")
        return
    for entry in results[start_index:end_index]:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO crashes VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                entry["CountyName"],
                datetime.utcfromtimestamp(int(entry["CrashDate"][6:16])),
                entry["Fatals"],
                entry["Peds"],
                entry["Persons"],
                entry["St_Case"],
                entry["State"],
                entry["StateName"],
                entry["TotalVehicles"],
            ))
        except sqlite3.Error as e:
            print("SQLite error:", e)
    with open('start_index.txt', 'w') as file:
        file.write(str(end_index+1))
    conn.commit()

# Commit the changes to the database
conn.commit()

# Set the time period for the entire timeframe
start_date_initial = datetime(2014, 1, 1)
end_date_initial = datetime(2015, 12, 31)

# Fetch and insert the next 25 records for Michigan
fetch_and_insert_crashes(cursor, start_date_initial, end_date_initial)

# Close the database connection
conn.close()
