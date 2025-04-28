import sqlite3
import os

# Define database path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Healthcare_ETL_Project'))
db_path = os.path.join(project_root, "db", "healthcare_data.db")

# Connect to the SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Drop existing triggers if they exist
cursor.execute('DROP TRIGGER IF EXISTS trg_after_provider_insert;')
cursor.execute('DROP TRIGGER IF EXISTS trigger_provider_scd2;')
cursor.execute('DROP TRIGGER IF EXISTS trg_provider_scd2;')

# Create PROVIDER_LOG table if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS PROVIDER_LOG (
    Log_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Provider_ID INTEGER,
    Version_ID INTEGER,
    Action TEXT,
    Timestamp TEXT
);
''')

# Optional: insert logging trigger (if needed)
cursor.execute('''
CREATE TRIGGER IF NOT EXISTS trg_after_provider_insert
AFTER INSERT ON PROVIDER
BEGIN
    INSERT INTO PROVIDER_LOG (Provider_ID, Version_ID, Action, Timestamp)
    VALUES (NEW.Provider_ID, NEW.Version_ID, 'INSERT', DATETIME('now'));
END;
''')

# ✅ Create SCD Type 2 Trigger with proper logic (prevents duplicates)
cursor.execute('''
CREATE TRIGGER trg_provider_scd2
BEFORE UPDATE ON PROVIDER
FOR EACH ROW
WHEN OLD.Is_Current = 1 AND OLD.Affiliated_Hospital != NEW.Affiliated_Hospital
BEGIN
    -- Insert new record with updated Affiliated_Hospital
    INSERT INTO PROVIDER (
        Provider_ID, First_Name, Last_Name, Speciality_Id, Speciality_Name,
        Affiliated_Hospital, Valid_From, Valid_To, Is_Current
    )
    VALUES (
        OLD.Provider_ID, OLD.First_Name, OLD.Last_Name, OLD.Speciality_Id,
        OLD.Speciality_Name, NEW.Affiliated_Hospital,
        DATETIME('now'), NULL, 1
    );

    -- Mark old record as inactive
    UPDATE PROVIDER
    SET Valid_To = DATETIME('now'), Is_Current = 0
    WHERE rowid = OLD.rowid;

    -- Prevent original update from being applied
    SELECT RAISE(IGNORE);
END;
''')

# Commit and close connection
conn.commit()
conn.close()

print("✅ SCD Type 2 trigger created successfully. Old triggers dropped and new logic applied.")
