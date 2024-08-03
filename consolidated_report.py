import sqlite3

def create_and_populate_table(cursor, table_name, file_path):
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (entry TEXT PRIMARY KEY)")
    with open(file_path, 'r') as file:
        entries = [(line.strip(),) for line in file]
        cursor.executemany(f"INSERT OR IGNORE INTO {table_name} (entry) VALUES (?)", entries)

def generate_consolidated_report(file1, file2, file3, file4, report_file, db_path):
    conn = sqlite3.connect(db_path)  # Use a persistent database file
    cursor = conn.cursor()
    
    # Create and populate tables
    create_and_populate_table(cursor, 'file1', file1)
    create_and_populate_table(cursor, 'file2', file2)
    create_and_populate_table(cursor, 'file3', file3)
    create_and_populate_table(cursor, 'file4', file4)
    
    processed_entries = set()
    
    # Generate the report
    with open(report_file, 'w') as report:
        report.write('Entry,In File 1,In File 2,In File 3,In File 4\n')
        
        # Check entries from file1
        cursor.execute("SELECT entry FROM file1")
        for row in cursor.fetchall():
            entry = row[0]
            if entry not in processed_entries:
                in_file1 = 'Yes'
                cursor.execute("SELECT 1 FROM file2 WHERE entry = ?", (entry,))
                in_file2 = 'Yes' if cursor.fetchone() else 'No'
                cursor.execute("SELECT 1 FROM file3 WHERE entry = ?", (entry,))
                in_file3 = 'Yes' if cursor.fetchone() else 'No'
                cursor.execute("SELECT 1 FROM file4 WHERE entry = ?", (entry,))
                in_file4 = 'Yes' if cursor.fetchone() else 'No'
                report.write(f'{entry},{in_file1},{in_file2},{in_file3},{in_file4}\n')
                processed_entries.add(entry)
        
        # Check entries from file2
        cursor.execute("SELECT entry FROM file2")
        for row in cursor.fetchall():
            entry = row[0]
            if entry not in processed_entries:
                cursor.execute("SELECT 1 FROM file1 WHERE entry = ?", (entry,))
                in_file1 = 'Yes' if cursor.fetchone() else 'No'
                in_file2 = 'Yes'
                cursor.execute("SELECT 1 FROM file3 WHERE entry = ?", (entry,))
                in_file3 = 'Yes' if cursor.fetchone() else 'No'
                cursor.execute("SELECT 1 FROM file4 WHERE entry = ?", (entry,))
                in_file4 = 'Yes' if cursor.fetchone() else 'No'
                report.write(f'{entry},{in_file1},{in_file2},{in_file3},{in_file4}\n')
                processed_entries.add(entry)
    
    conn.close()
    print(f'Report generated: {report_file}')

# File paths
file1 = 'file1.txt'
file2 = 'file2.txt'
file3 = 'file3.txt'
file4 = 'file4.txt'
report_file = 'consolidated_report.csv'
db_path = 'comparison.db'  # Path to the SQLite database file

# Generate the report for large files
generate_consolidated_report(file1, file2, file3, file4, report_file, db_path)
