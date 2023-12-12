import sqlite3
import matplotlib.pyplot as plt
import numpy as np

def make_scatter_plot():
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

    # Fetch data for the analysis
    cursor.execute('''
        SELECT
            daily_data_meteostat.date,
            daily_data_meteostat.temperature_avg,
            COUNT(crashes.id) as num_crashes
        FROM
            daily_data_meteostat
        LEFT JOIN
            crashes
        ON
            strftime('%Y-%m-%d', daily_data_meteostat.date) = strftime('%Y-%m-%d', crashes.CrashDate)
        GROUP BY
            daily_data_meteostat.date
    ''')
    data = cursor.fetchall()

    # Close the database connection
    conn.close()

    # Extract data for plotting
    dates = [entry[0] for entry in data]
    temperatures = [entry[1] for entry in data]
    num_crashes = [entry[2] if entry[2] else 0 for entry in data]  # Replace None with 0 if no crashes

    # Scatter plot: Temperature vs Number of Crashes
    plt.figure(figsize=(10, 6))
    plt.scatter(temperatures, num_crashes, color='red', alpha=0.5)
    plt.title('Temperature vs Number of Crashes')
    plt.xlabel('Temperature (Average)')
    plt.ylabel('Number of Crashes')
    plt.grid(True)
    plt.show()

def make_bar_chart():
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

    # Fetch data for the analysis
    cursor.execute('''
        SELECT
            CASE
                WHEN temperature_avg BETWEEN 0 AND 10 THEN '0-10'
                WHEN temperature_avg BETWEEN 11 AND 20 THEN '11-20'
                WHEN temperature_avg BETWEEN 21 AND 30 THEN '21-30'
                WHEN temperature_avg BETWEEN 31 AND 40 THEN '31-40'
                WHEN temperature_avg BETWEEN 41 AND 50 THEN '41-50'
                WHEN temperature_avg BETWEEN 51 AND 60 THEN '51-60'
                WHEN temperature_avg BETWEEN 61 AND 70 THEN '61-70'
                WHEN temperature_avg BETWEEN 71 AND 80 THEN '71-80'
                WHEN temperature_avg BETWEEN 81 AND 90 THEN '81-90'
                -- Add more ranges as needed
                ELSE 'Unknown'
            END as temperature_bin,
            COUNT(crashes.id) as num_fatal_crashes,
            COUNT(DISTINCT strftime('%Y-%m-%d', daily_data_meteostat.date)) as num_days
        FROM
            daily_data_meteostat
        LEFT JOIN
            crashes
        ON
            strftime('%Y-%m-%d', daily_data_meteostat.date) = strftime('%Y-%m-%d', crashes.CrashDate)
        WHERE
            crashes.Fatals > 0  -- Consider only fatal crashes
        GROUP BY
            temperature_bin
        ORDER BY
            temperature_bin;
    ''')
    data = cursor.fetchall()

    # Close the database connection
    conn.close()

    # Extract data for plotting
    temperature_bins = [entry[0] for entry in data]
    num_fatal_crashes = [entry[1] for entry in data]
    num_days = [entry[2] for entry in data]

    # Calculate average fatal crashes per day
    average_fatal_crashes_per_day = np.array(num_fatal_crashes) / np.array(num_days)

    # Bar chart: Temperature Bins vs Average Fatal Crashes per Day
    plt.figure(figsize=(12, 8))
    bars = plt.bar(temperature_bins, average_fatal_crashes_per_day, color='skyblue', alpha=0.7, edgecolor='black', linewidth=1.5)

    # Add data labels on each bar
    for bar, label in zip(bars, average_fatal_crashes_per_day):
        plt.text(bar.get_x() + bar.get_width() / 2 - 0.1, bar.get_height() + 0.01, f'{label:.2f}', ha='center', va='bottom', color='black')

    plt.title('Temperature Bins vs Average Fatal Crashes per Day', fontsize=16)
    plt.xlabel('Temperature Bins', fontsize=14)
    plt.ylabel('Average Fatal Crashes per Day', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.show()


make_bar_chart()
make_scatter_plot() 