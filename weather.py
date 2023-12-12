from datetime import datetime, timedelta
from meteostat import Point, Daily
import sqlite3
import pandas as pd  # Import pandas

def convert_to_farenheit(temp):
    return (temp * (9/5)) + 32

# Function to fetch and insert data into the database
def fetch_and_insert_data(cursor, start_date, end_date):
    # Create Point for Detroit, MI
    detroit = Point(42.3314, -83.0458, 183)  # Latitude, Longitude, Elevation

    # Get daily data for the specified date range
    data = Daily(detroit, start_date, end_date)
    data = data.fetch()

    # Insert unique Meteostat data into the database
    for index, row in data.iterrows():
        date_value = index.date()
        temperature_avg = convert_to_farenheit(row['tavg'])
        temperature_min = convert_to_farenheit(row['tmin'])
        temperature_max = convert_to_farenheit(row['tmax'])

        cursor.execute('''
            INSERT OR IGNORE INTO daily_data_meteostat VALUES (NULL, ?, ?, ?, ?)
        ''', (date_value, temperature_avg, temperature_min, temperature_max))

    # Commit the changes to the database
    conn.commit()

    # Write the last end date to a file for the next run
    with open('last_end_date.txt', 'w') as file:
        file.write(end_date.strftime('%Y-%m-%d'))

# SQLite database connection
conn = sqlite3.connect('proj_data.db')
cursor = conn.cursor()

# Create daily data table in the database if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_data_meteostat (
        id INTEGER PRIMARY KEY,
        date TEXT,
        temperature_avg REAL,
        temperature_min REAL,
        temperature_max REAL,
        UNIQUE(date) ON CONFLICT IGNORE
    )
''')

conn.commit()

# Read the last end date from the file or use an initial date
try:
    with open('last_end_date.txt', 'r') as file:
        last_end_date_str = file.read()
    last_end_date = datetime.strptime(last_end_date_str, '%Y-%m-%d')
except FileNotFoundError:
    # If the file is not found, use an initial start date
    last_end_date = datetime(2020, 1, 1)

# Set time period for the current fetch
start_date_current = last_end_date + timedelta(days=1)
end_date_current = start_date_current + timedelta(days=24)  # Fetching 25 days in the current fetch

# Fetch and insert data for the current period
fetch_and_insert_data(cursor, start_date_current, end_date_current)

# Close the database connection
conn.close()
