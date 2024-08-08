import sqlite3
import subprocess

class DatabaseManager:
    def __init__(self, db_name=':memory:'):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def create_file5_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS file5 (
                name TEXT PRIMARY KEY,
                in_file1 TEXT,
                in_file2 TEXT,
                in_file3 TEXT,
                in_file4 TEXT
            )
        ''')
        self.conn.commit()

    def insert_file5_data(self, data):
        self.cursor.executemany('''
            INSERT OR IGNORE INTO file5 (name, in_file1, in_file2, in_file3, in_file4)
            VALUES (?, ?, ?, ?, ?)
        ''', data)
        self.conn.commit()

    def find_missing_rows(self):
        self.cursor.execute('''
            SELECT name
            FROM file5
            WHERE in_file3 = 'No'
              AND in_file4 = 'No'
              AND in_file1 = 'Yes'
              AND in_file2 = 'Yes'
        ''')
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()

class FileProcessor:
    def __init__(self, file1_path):
        self.file1_path = file1_path

    def get_roll_number(self, name):
        # Use grep to find the roll number from file1.txt
        result = subprocess.run(['grep', name, self.file1_path], capture_output=True, text=True)
        
        if result.stdout:
            return result.stdout.strip().split(',')[1]  # Assuming file1.txt has "name,roll_number"
        else:
            return "Roll number not found"

class MissingDataFinder:
    def __init__(self, db_manager, file_processor):
        self.db_manager = db_manager
        self.file_processor = file_processor

    def find_missing_entries(self):
        missing_names = self.db_manager.find_missing_rows()
        
        missing_entries = []
        for (name,) in missing_names:
            roll_number = self.file_processor.get_roll_number(name)
            missing_entries.append((name, roll_number))
        
        return missing_entries

def main():
    # Initialize DatabaseManager and FileProcessor
    db_manager = DatabaseManager()
    file_processor = FileProcessor(file1_path='file1.txt')

    # Create table and insert data
    db_manager.create_file5_table()
    
    # Example data for file5 (Replace this with actual data insertion)
    file5_data = [
        ('John Doe', 'Yes', 'Yes', 'No', 'No'),
        ('Jane Smith', 'Yes', 'No', 'Yes', 'No'),
        ('Alice Brown', 'No', 'Yes', 'Yes', 'Yes'),
        ('Bob White', 'Yes', 'Yes', 'Yes', 'Yes')
    ]
    db_manager.insert_file5_data(file5_data)
    
    # Use MissingDataFinder to find missing entries
    missing_data_finder = MissingDataFinder(db_manager, file_processor)
    missing_entries = missing_data_finder.find_missing_entries()

    # Output the results
    print("Missing entries:")
    for name, roll_number in missing_entries:
        print(f"{name} (Roll Number: {roll_number}) is missing in file3 and file4")

    # Clean up
    db_manager.close()

if __name__ == "__main__":
    main()



import pytest
import sqlite3
from unittest.mock import patch, MagicMock
from your_module import DatabaseManager, FileProcessor, MissingDataFinder  # Replace 'your_module' with your actual module name

@pytest.fixture
def db_manager():
    # Set up an in-memory database
    db = DatabaseManager()
    db.create_file5_table()
    yield db
    db.close()

@pytest.fixture
def file_processor():
    # Mock FileProcessor to simulate grep output
    with patch('your_module.subprocess.run') as mock_run:
        mock_run.return_value = MagicMock(stdout='John Doe,1001\n')
        processor = FileProcessor(file1_path='file1.txt')
        yield processor

def test_insert_file5_data(db_manager):
    data = [
        ('John Doe', 'Yes', 'Yes', 'No', 'No'),
        ('Jane Smith', 'Yes', 'No', 'Yes', 'No')
    ]
    db_manager.insert_file5_data(data)
    
    # Verify the data was inserted correctly
    cursor = db_manager.conn.cursor()
    cursor.execute('SELECT * FROM file5')
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    assert ('John Doe', 'Yes', 'Yes', 'No', 'No') in rows
    assert ('Jane Smith', 'Yes', 'No', 'Yes', 'No') in rows

def test_find_missing_rows(db_manager):
    data = [
        ('John Doe', 'Yes', 'Yes', 'No', 'No'),
        ('Jane Smith', 'Yes', 'No', 'Yes', 'No'),
        ('Alice Brown', 'No', 'Yes', 'Yes', 'Yes')
    ]
    db_manager.insert_file5_data(data)
    
    missing_rows = db_manager.find_missing_rows()
    assert len(missing_rows) == 2
    assert ('John Doe',) in missing_rows
    assert ('Jane Smith',) in missing_rows

@patch('your_module.subprocess.run')
def test_get_roll_number(file_processor, mock_run):
    # Simulate grep returning a specific output
    mock_run.return_value = MagicMock(stdout='John Doe,1001\n')
    
    roll_number = file_processor.get_roll_number('John Doe')
    assert roll_number == '1001'

def test_missing_data_finder(db_manager, file_processor):
    data = [
        ('John Doe', 'Yes', 'Yes', 'No', 'No'),
        ('Jane Smith', 'Yes', 'No', 'Yes', 'No')
    ]
    db_manager.insert_file5_data(data)
    
    # Create a MissingDataFinder instance
    missing_data_finder = MissingDataFinder(db_manager, file_processor)
    
    # Find missing entries
    missing_entries = missing_data_finder.find_missing_entries()
    
    assert len(missing_entries) == 2
    assert ('John Doe', '1001') in missing_entries
    assert ('Jane Smith', 'Roll number not found') in missing_entries

if __name__ == "__main__":
    pytest.main()
