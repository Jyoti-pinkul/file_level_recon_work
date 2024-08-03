import sqlite3

def create_and_populate_table(cursor, table_name, file_path):
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (entry TEXT PRIMARY KEY, count INTEGER)")
    entry_counts = {}
    with open(file_path, 'r') as file:
        for line in file:
            entry = line.strip()
            if entry in entry_counts:
                entry_counts[entry] += 1
            else:
                entry_counts[entry] = 1
    entries = [(entry, count) for entry, count in entry_counts.items()]
    cursor.executemany(f"INSERT OR IGNORE INTO {table_name} (entry, count) VALUES (?, ?)", entries)

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
        report.write('Entry,In File 1,Count File 1,In File 2,Count File 2,In File 3,Count File 3,In File 4,Count File 4\n')
        
        # Check entries from file1
        cursor.execute("SELECT entry, count FROM file1")
        for row in cursor.fetchall():
            entry, count1 = row
            if entry not in processed_entries:
                in_file1 = 'Yes'
                count1 = count1
                cursor.execute("SELECT count FROM file2 WHERE entry = ?", (entry,))
                row2 = cursor.fetchone()
                in_file2 = 'Yes' if row2 else 'No'
                count2 = row2[0] if row2 else 0
                cursor.execute("SELECT count FROM file3 WHERE entry = ?", (entry,))
                row3 = cursor.fetchone()
                in_file3 = 'Yes' if row3 else 'No'
                count3 = row3[0] if row3 else 0
                cursor.execute("SELECT count FROM file4 WHERE entry = ?", (entry,))
                row4 = cursor.fetchone()
                in_file4 = 'Yes' if row4 else 'No'
                count4 = row4[0] if row4 else 0
                report.write(f'{entry},{in_file1},{count1},{in_file2},{count2},{in_file3},{count3},{in_file4},{count4}\n')
                processed_entries.add(entry)
        
        # Check entries from file2
        cursor.execute("SELECT entry, count FROM file2")
        for row in cursor.fetchall():
            entry, count2 = row
            if entry not in processed_entries:
                cursor.execute("SELECT count FROM file1 WHERE entry = ?", (entry,))
                row1 = cursor.fetchone()
                in_file1 = 'Yes' if row1 else 'No'
                count1 = row1[0] if row1 else 0
                in_file2 = 'Yes'
                count2 = count2
                cursor.execute("SELECT count FROM file3 WHERE entry = ?", (entry,))
                row3 = cursor.fetchone()
                in_file3 = 'Yes' if row3 else 'No'
                count3 = row3[0] if row3 else 0
                cursor.execute("SELECT count FROM file4 WHERE entry = ?", (entry,))
                row4 = cursor.fetchone()
                in_file4 = 'Yes' if row4 else 'No'
                count4 = row4[0] if row4 else 0
                report.write(f'{entry},{in_file1},{count1},{in_file2},{count2},{in_file3},{count3},{in_file4},{count4}\n')
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
