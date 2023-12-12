import requests
import sqlite3

def create_crash_details_table(cursor):
    """Create crash_details table in the database if not exists"""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crash_details (
            id INTEGER PRIMARY KEY,
            drunk INTEGER,
            weekday INTEGER, 
            intersection_type TEXT,
            FOREIGN KEY (id) REFERENCES crashes(id),
            UNIQUE(id) ON CONFLICT IGNORE
        )
    ''')

def get_case_details(state_case, case_year, state):
    """Fetch case details from NHTSA DOT API"""
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

def fetch_and_insert_crash_details(cursor, start_id):
    """Fetch and insert crash details into the database"""
    # Make the API request with parameters for the next 25 crashes
    end_id = start_id + 24
    for id in range(start_id, end_id):
        cursor.execute("SELECT ST_Case, State, CrashDate FROM crashes WHERE id = ?", (id,))
        data = cursor.fetchone()
        
        if data is not None:
            st_case, state_code, date = data
            year = date.split('-')[0]
            case_details = get_case_details(st_case, year, state_code)
            
            try:
                results = case_details["Results"][0][0]
            except IndexError:
                print(f"Case id {id} not found")
                continue

            if results is not None:
                drunk = results['CrashResultSet']['DRUNK_DR']
                intersection_type = results['CrashResultSet']['TYP_INTNAME']
                # 7 = Saturday, 0 = Sunday
                weekDay = results['CrashResultSet']['DAY_WEEK']

                cursor.execute('''
                    INSERT OR IGNORE INTO crash_details VALUES (?, ?, ?, ?)
                ''', (id, drunk, weekDay, intersection_type))

    with open('details_index.txt', 'w') as file:
        file.write(str(end_id))
    conn.commit()

# SQLite database connection
conn = sqlite3.connect('proj_data.db')
cursor = conn.cursor()

# Create crash_details table in the database if it doesn't exists
create_crash_details_table(cursor)
conn.commit()

# Read the start index from the file
with open('details_index.txt', 'r') as file:
    start_index = int(file.read().strip())

# Fetch and insert the next 25 crash details
fetch_and_insert_crash_details(cursor, start_index)
conn.commit()
conn.close()
