import sqlite3
import os
import sys

def create_database_schema(db_path):
    try:
        # Connect to SQLite
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print("📂 Connected to database.")

        # Create tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS PROVIDER (
            Version_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Provider_ID INTEGER,
            First_Name TEXT,
            Last_Name TEXT,
            Speciality_Id INTEGER,
            Speciality_Name TEXT,
            Affiliated_Hospital TEXT,
            Valid_From TEXT,
            Valid_To TEXT,
            Is_Current INTEGER,
            UNIQUE (Provider_ID, Version_ID)
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS PATIENT (
            Patient_ID INTEGER PRIMARY KEY,
            First_Name TEXT,
            Last_Name TEXT,
            Name TEXT,
            Gender TEXT,
            Age INTEGER
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS DISEASE (
            Disease_ID INTEGER PRIMARY KEY,
            Speciality_Id INTEGER,
            Name TEXT,
            Type TEXT,
            Severity TEXT,
            Transmission_mode TEXT,
            Mortality_Rate REAL
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS LOCATION (
            Location_ID INTEGER PRIMARY KEY,
            Country TEXT,
            State TEXT,
            City TEXT,
            UNIQUE (Country, State, City)
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS EFFECTIVENESS (
            Effectiveness_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Outcome_Status TEXT UNIQUE,
            Effectiveness_Score INTEGER
        );
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS TREATMENT (
            Treatment_ID INTEGER PRIMARY KEY,
            Start_Date TEXT,
            Completion_Date TEXT,
            Outcome_Date TEXT,
            Outcome_Quarter INTEGER,
            Treatment_Duration INTEGER,
            Cost REAL,
            Effectiveness_Score INTEGER,
            Type TEXT,
            Patient_ID INTEGER,
            Provider_ID INTEGER,
            Location_ID INTEGER,
            Disease_ID INTEGER,
            Outcome_Day TEXT,
            Outcome_Weekend_Flag INTEGER,
            Report_Duration INTEGER
        );
        ''')

        conn.commit()
        print("✅ Database schema created successfully.")

    except sqlite3.Error as e:
        print(f"❌ SQLite error occurred: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            print("🔒 Database connection closed.")

if __name__ == "__main__":
    try:
        # Set up paths
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Healthcare_ETL_Project'))
        db_dir = os.path.join(project_root, "db")
        os.makedirs(db_dir, exist_ok=True)
        db_path = os.path.join(db_dir, "healthcare_data.db")

        create_database_schema(db_path)

    except Exception as e:
        print(f"❌ Failed to set up the database: {e}")
        sys.exit(1)
