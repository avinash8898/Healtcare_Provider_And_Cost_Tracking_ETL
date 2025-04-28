import sqlite3

def create_indexes():
    # Connect to healthcare database
    try:
        conn = sqlite3.connect('../Healthcare_ETL_Project/db/healthcare_data.db')
        cursor = conn.cursor()
        print("Successfully connected to the database.")
    except sqlite3.Error as e:
        print(f"Error while connecting to the database: {e}")
        return

    # List of index creation statements with DROP IF EXISTS to avoid conflicts
    index_statements = [
        "DROP INDEX IF EXISTS idx_provider_id;",
        "DROP INDEX IF EXISTS idx_valid_from;",
        "DROP INDEX IF EXISTS idx_treatment_type;",
        "DROP INDEX IF EXISTS idx_outcome_quarter_year;",
        "DROP INDEX IF EXISTS idx_treatment_date;",
        "DROP INDEX IF EXISTS idx_total_cost;",
        "DROP INDEX IF EXISTS idx_effectiveness_score;",
        "DROP INDEX IF EXISTS idx_disease;",
        
        "CREATE INDEX IF NOT EXISTS idx_provider_id ON PROVIDER(Provider_ID);",
        "CREATE INDEX IF NOT EXISTS idx_valid_from ON PROVIDER(Valid_From);",
        
        # For TREATMENT_FACT table
        "CREATE INDEX IF NOT EXISTS idx_treatment_type ON TREATMENT(Type);",
        "CREATE INDEX IF NOT EXISTS idx_outcome_quarter_year ON TREATMENT(Outcome_Quarter);",
        "CREATE INDEX IF NOT EXISTS idx_treatment_date ON TREATMENT(Start_Date, Completion_Date);",  # Added to cover multiple dates
        "CREATE INDEX IF NOT EXISTS idx_total_cost ON TREATMENT(Cost);",
        "CREATE INDEX IF NOT EXISTS idx_effectiveness_score ON TREATMENT(Effectiveness_Score);",
        "CREATE INDEX IF NOT EXISTS idx_disease ON TREATMENT(Disease_ID);"
    ]
    
    # Execute each statement
    try:
        for stmt in index_statements:
            print(f"Executing: {stmt}")
            cursor.execute(stmt)
        
        # Commit changes
        conn.commit()
        print("All indexes created successfully.")
    
    except sqlite3.Error as e:
        print(f"Error while creating indexes: {e}")
        # Optionally rollback if there was a failure
        conn.rollback()
    
    finally:
        # Close connection
        if conn:
            conn.close()
            print("Database connection closed.")

# Run the function
if __name__ == "__main__":
    create_indexes()
