import requests
import sqlite3
from datetime import datetime

# SQLite database connection
conn = sqlite3.connect('proj_data.db')
cursor = conn.cursor()

# Create crashes table in the database
cursor.execute('''
    CREATE TABLE IF NOT EXISTS crash_details (
        id INTEGER PRIMARY KEY,
        drunk INTEGER,
        weekday INTEGER, 
        intersection_type TEXT,  
        FOREIGN KEY (id) REFERENCES crashes(id)
        UNIQUE(id) ON CONFLICT IGNORE
    )
''')
conn.commit()

def get_case_details(state_case, case_year, state):
    print("GETTING DETAILS")
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCaseDetails"
    params = {
        "stateCase": state_case,
        "caseYear": case_year,
        "state": state,
        "format": "json"
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def fetch_and_insert_crashes(cursor, start_id):
    # Make the API request with parameters for the entire timeframe
    end_id = start_id + 25
    for id in range(start_id,end_id):
        cursor.execute("SELECT ST_Case, State, CrashDate FROM crashes WHERE id = ?", (id,))
        data = cursor.fetchone()
        if data is not None:
            st_case, state_code, date = data
            year = date.split('-')[0]
            case_details = get_case_details(st_case,year,state_code)
            results = case_details["Results"][0][0]
            drunk = results['CrashResultSet']['DRUNK_DR']
            intersection_type =results['CrashResultSet']['TYP_INTNAME']
            #7 = saturday, 0 = sunday
            weekDay = results['CrashResultSet']['DAY_WEEK']

            cursor.execute('''
                INSERT OR IGNORE INTO crash_details VALUES (?, ?, ?, ?)
            ''', (id, drunk, weekDay, intersection_type))

    conn.commit()


fetch_and_insert_crashes(cursor,0)
# Commit the changes to the database
conn.commit()

# Close the database connection
conn.close()
