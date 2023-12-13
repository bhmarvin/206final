import sqlite3
import matplotlib.pyplot as plt
import numpy as np

def fetch_temperature_vs_crashes_data():
    """Fetch data for Temperature vs Number of Crashes analysis"""
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

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
            daily_data_meteostat.date, daily_data_meteostat.temperature_avg
    ''')

    data = cursor.fetchall()
    conn.close()
    return data



def make_scatter_plot(data):
    """Create scatter plot for Temperature vs Number of Crashes"""
    
    temperatures = [entry[1] for entry in data]
    num_crashes = [entry[2] if entry[2] else 0 for entry in data]  # Replace None with 0 if no crashes
    
    # Scatter plot: Temperature vs Number of Crashes
    plt.figure(figsize=(10, 6))
    plt.scatter(temperatures, num_crashes, color='skyblue', edgecolors='black', alpha=0.7)
    
    # Add title and labels
    plt.title('Scatter Plot: Temperature vs Number of Crashes', fontsize=16)
    plt.xlabel('Temperature (Average)', fontsize=14)
    plt.ylabel('Number of Crashes', fontsize=14)
    
    # Add gridlines
    plt.grid(True, linestyle='--', alpha=0.5)
    
    # Show the plot
    plt.show()

def fetch_temperature_bins_vs_fatal_crashes_data():
    """Fetch data for Temperature Bins vs Average Fatal Crashes per Day analysis"""
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

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
            crashes.Fatals > 0
        GROUP BY
            temperature_bin
        ORDER BY
            temperature_bin;
    ''')

    data = cursor.fetchall()
    conn.close()
    return data

def make_bar_chart(data):
    """Create bar chart for Temperature Bins vs Average Fatal Crashes per Day"""
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

    return temperature_bins,num_fatal_crashes,num_days

def fetch_crash_details_data():
    """Fetch data for crashes by intersection type, involvement of drunk drivers, and counts for each weekday"""
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            crash_details.id,
            crash_details.drunk,
            crash_details.weekday,
            crash_details.type_id
        FROM
            crash_details
    ''')

    data = cursor.fetchall()
    conn.close()
    return data

def analyze_crash_details(data):
    """Analyze crash details and print counts for different categories"""
    # Count crashes by intersection type
    intersection_counts = {}
    for entry in data:
        intersection_type = entry[3]
        if intersection_type in intersection_counts:
            intersection_counts[intersection_type] += 1
        else:
            intersection_counts[intersection_type] = 1

    # Count crashes involving drunk drivers
    drunk_counts = {"Drunk": 0, "Not Drunk": 0}
    for entry in data:
        drunk_status = "Drunk" if entry[1] == 1 else "Not Drunk"
        drunk_counts[drunk_status] += 1

    # Count crashes for each weekday
    weekday_counts = {int(i): 0 for i in range(1,8)}
    for entry in data:
        weekday = entry[2]
        weekday_counts[weekday] += 1

    return intersection_counts, drunk_counts, weekday_counts

def print_crash_details_counts(intersection_counts, drunk_counts, weekday_counts, ):
    """Print counts for crash details"""

    with open('calcs.txt', 'a') as fhand:
        fhand.write("\nCrash Details Counts:")
        fhand.write("---------------------")
        
        # Intersection Type Counts
        conn = sqlite3.connect('proj_data.db')
        cursor = conn.cursor()
        fhand.write("\n1. Intersection Type Counts:\n")
        for intersection_type, count in intersection_counts.items():
            cursor.execute("Select type_name FROM intersection_types where type_id = ?", (intersection_type,))
            fhand.write(f"   - {cursor.fetchone()[0]}: {count}\n")

        # Drunk Driver Involvement Counts
        fhand.write("\n2. Drunk Driver Involvement Counts:\n")
        for drunk_status, count in drunk_counts.items():
            fhand.write(f"   - {drunk_status}: {count}\n")

        # Weekday Counts
        fhand.write("\n3. Weekday Counts(1 corresponds to monday):\n")
        for weekday, count in weekday_counts.items():
            fhand.write(f"   - Weekday {weekday}: {count}\n")
    

def make_drunk_fatalities_comparison_chart():
    """Create a bar chart for the count of deaths based on the number of drunk drivers"""
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            drunk,
            COUNT(crashes.id) as total_fatalities
        FROM
            crash_details
        LEFT JOIN
            crashes
        ON
            crash_details.id = crashes.id
        GROUP BY
            drunk;
    ''')

    data = cursor.fetchall()
    conn.close()

    drunk_counts = [entry[1] if entry[1] else 0 for entry in data]  # Replace None with 0
    labels = [f'{entry[0]} Drunk Drivers' if entry[0] else 'No Drunk Drivers' for entry in data]

    # Bar chart: Count of Drunk Drivers vs Total Fatalities
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(labels, drunk_counts, color='skyblue', alpha=0.7, edgecolor='black', linewidth=1.5)

    # Add data labels on each bar with adjustments for better visibility
    for bar, label in zip(bars, drunk_counts):
        ax.text(bar.get_x() + bar.get_width() / 2 - 0.1, bar.get_height() + 0.01,
                f'{label}', ha='center', va='bottom', color='black', fontsize=10, rotation=45, rotation_mode='anchor')

    plt.title('Count of Drunk Drivers vs Total Fatalities', fontsize=16)
    plt.xlabel('Drunk Driver Count', fontsize=14)
    plt.ylabel('Total Fatalities', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(rotation=45, ha='right', fontsize=12)
    plt.yticks(fontsize=12)
    plt.tight_layout()  # Adjust layout to prevent labels from going out of the window
    plt.show()

def make_intersection_pie():
    """Create a pie chart for intersection types distribution"""
    conn = sqlite3.connect('proj_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            intersection_types.type_name,
            COUNT(crash_details.id) as num_crashes
        FROM
            crash_details
        LEFT JOIN
            intersection_types
        ON
            crash_details.type_id = intersection_types.type_id
        GROUP BY
            crash_details.type_id
    ''')

    data = cursor.fetchall()
    conn.close()

    # Extract data for plotting
    types = [entry[0] if entry[0] else 'Unknown' for entry in data]
    num_crashes = [entry[1] for entry in data]

    # Create a pie chart with improved aesthetics
    plt.figure(figsize=(10, 8))
    explode = [0.1] * len(types)  # Add some separation between slices

    # Use wedgeprops to set properties of wedges (slices)
    wedges, texts, autotexts = plt.pie(num_crashes, labels=None, autopct='%1.1f%%', startangle=140,
                                       colors=plt.cm.Paired.colors, explode=explode,
                                       wedgeprops=dict(width=0.4, edgecolor='w'))

    # Add labels with adjusted distance from the center
    label_distance = 2  # Adjust this value for better spacing
    for i, type_name in enumerate(types):
        angle = wedges[i].theta2 - (wedges[i].theta2 - wedges[i].theta1) / 2
        radius = label_distance * 0.5 * wedges[i].r
        x = radius * np.cos(np.radians(angle))
        y = radius * np.sin(np.radians(angle))
        plt.text(x, y, type_name, ha="center", va="center", fontsize=10, fontweight='bold')

    # Add a title
    plt.title('Distribution of Crash Types by Intersection', fontsize=16)

    # Show the plot
    plt.show()

def main():
    # Fetch data for scatter plot
    temperature_vs_crashes_data = fetch_temperature_vs_crashes_data()

    # Create scatter plot
    make_scatter_plot(temperature_vs_crashes_data)

    # Fetch data for bar chart
    temperature_bins_vs_fatal_crashes_data = fetch_temperature_bins_vs_fatal_crashes_data()

    # Create bar chart
    bins, crashes, days = make_bar_chart(temperature_bins_vs_fatal_crashes_data)
    with open('calcs.txt', 'w')  as fhand:
        # Display calculated values
        fhand.write("\nCalculated Values:")
        fhand.write("------------------")
        fhand.write("\n1. Bar Chart: Temperature Bins vs Average Fatal Crashes per Day\n")
        for num,bin in enumerate(bins):
            fhand.write(f"   - Temperature Bin: {bin}, Average Fatal Crashes per Day: {crashes[num]/days[num]}\n")
        
        # Fetch data for crash details
    crash_details_data = fetch_crash_details_data()

    # Analyze crash details
    intersection_counts, drunk_counts, weekday_counts = analyze_crash_details(crash_details_data)

    # Print counts for crash details
    print_crash_details_counts(intersection_counts, drunk_counts, weekday_counts)

    make_drunk_fatalities_comparison_chart()
    make_intersection_pie()

if __name__ == "__main__":
    main()
