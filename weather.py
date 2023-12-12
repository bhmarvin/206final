from datetime import datetime, timedelta
from meteostat import Point, Daily
import sqlite3
import pandas as pd

def convert_to_fahrenheit(temp):
    """Convert temperature from Celsius to Fahrenheit"""
    return (temp * 9/5) + 32

def create_database_table(cursor):
    """Create daily data table in the database if not exists"""
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

def fetch_and_insert_data(cursor, start_date, end_date):
    """Fetch and insert Meteostat data into the database"""
    # Create Point for Detroit, MI
    detroit = Point(42.3314, -83.0458, 183)  # Latitude, Longitude, Elevation

    # Get daily data for the specified date range
    data = Daily(detroit, start_date, end_date).fetch()

    # Insert unique Meteostat data into the database
    for index, row in data.iterrows():
        date_value = index.date()
        temperature_avg = convert_to_fahrenheit(row['tavg'])
        temperature_min = convert_to_fahrenheit(row['tmin'])
        temperature_max = convert_to_fahrenheit(row['tmax'])
        cursor.execute('''
            INSERT OR IGNORE INTO daily_data_meteostat VALUES (NULL, ?, ?, ?, ?)
        ''', (date_value, temperature_avg, temperature_min, temperature_max))

def write_last_end_date(end_date):
    """Write the last end date to a file for the next run"""
    with open('last_end_date.txt', 'w') as file:
        file.write(end_date.strftime('%Y-%m-%d'))

def read_last_end_date():
    """Read the last end date from the file or use an initial date"""
    try:
        with open('last_end_date.txt', 'r') as file:
            last_end_date_str = file.read()
        return datetime.strptime(last_end_date_str, '%Y-%m-%d')
    except FileNotFoundError:
        # If the file is not found, use an initial start date
        return datetime(2020, 1, 1)

def main():
    # SQLite database connection
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

    # Create daily data table in the database if not exists
    create_database_table(cursor)
    conn.commit()

    # Read the last end date from the file or use an initial date
    last_end_date = read_last_end_date()

    # Set time period for the current fetch
    start_date_current = last_end_date + timedelta(days=1)
    end_date_current = start_date_current + timedelta(days=24)  # Fetching 25 days in the current fetch

    # Fetch and insert data for the current period
    fetch_and_insert_data(cursor, start_date_current, end_date_current)
    conn.commit()

    # Write the last end date to a file for the next run
    write_last_end_date(end_date_current)
    conn.close()

if __name__ == "__main__":
    main()
