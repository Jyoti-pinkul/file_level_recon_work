import os
import sqlite3
import pytest

# Helper function to create a mock file with given content
def create_mock_file(file_path, content):
    with open(file_path, 'w') as f:
        f.write(content)

# Test for creating and populating tables
def test_create_and_populate_table():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    file_path = 'file1.txt'
    content = "entry1\nentry2\n"
    create_mock_file(file_path, content)
    
    create_and_populate_table(cursor, 'file1', file_path)
    
    cursor.execute("SELECT entry FROM file1")
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    assert rows[0] == ('entry1',)
    assert rows[1] == ('entry2',)
    
    os.remove(file_path)
    conn.close()

# Test for generating consolidated report
def test_generate_consolidated_report():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    file1_content = "entry1\nentry2\n"
    file2_content = "entry1\nentry3\n"
    file3_content = "entry1\nentry4\n"
    file4_content = "entry2\nentry3\n"
    create_mock_file('file1.txt', file1_content)
    create_mock_file('file2.txt', file2_content)
    create_mock_file('file3.txt', file3_content)
    create_mock_file('file4.txt', file4_content)
    
    generate_consolidated_report('file1.txt', 'file2.txt', 'file3.txt', 'file4.txt', 'consolidated_report.csv', ':memory:')
    
    with open('consolidated_report.csv', 'r') as report:
        lines = report.readlines()
        assert len(lines) == 4  # Header + 3 unique entries
        assert lines[0].strip() == 'Entry,In File 1,In File 2,In File 3,In File 4'
        assert lines[1].strip() == 'entry1,Yes,Yes,Yes,No'
        assert lines[2].strip() == 'entry2,Yes,No,No,Yes'
        assert lines[3].strip() == 'entry3,No,Yes,No,Yes'
    
    os.remove('file1.txt')
    os.remove('file2.txt')
    os.remove('file3.txt')
    os.remove('file4.txt')
    os.remove('consolidated_report.csv')
    conn.close()

# Test for file not found
def test_file_not_found():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    try:
        create_and_populate_table(cursor, 'file1', 'non_existent_file.txt')
    except FileNotFoundError:
        pass
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file1'")
    assert cursor.fetchone() is None
    
    conn.close()

# This would run the tests when you execute pytest
if __name__ == "__main__":
    pytest.main()
