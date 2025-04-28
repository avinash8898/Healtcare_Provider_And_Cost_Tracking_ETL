import pandas as pd
import sqlite3
import os
import sys
from datetime import datetime

def main():
    try:
        # Setting up paths
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Healthcare_ETL_Project'))
        db_dir = os.path.join(project_root, "db")
        processed_csv_path = os.path.join(project_root, "processed", "Healthcare_Dataset.csv")
        db_path = os.path.join(db_dir, "healthcare_data.db")

        if not os.path.exists(processed_csv_path):
            raise FileNotFoundError(f"Processed CSV not found at: {processed_csv_path}")

        # Loading data
        df = pd.read_csv(processed_csv_path)

        print(f"Loaded {len(df)} records from processed CSV.")

        # Transform date columns
        df['treatment_start_date'] = pd.to_datetime(df['treatment_start_date'])
        df['treatment_completion_date'] = pd.to_datetime(df['treatment_completion_date'])
        df['treatment_outcome_date'] = pd.to_datetime(df['treatment_outcome_date'])

        df['treatment_start_date_only'] = df['treatment_start_date'].dt.date
        df['treatment_start_time_only'] = df['treatment_start_date'].dt.time
        df['treatment_end_date_only'] = df['treatment_completion_date'].dt.date
        df['treatment_end_time_only'] = df['treatment_completion_date'].dt.time
        df['treatment_outcome_date_only'] = df['treatment_outcome_date'].dt.date
        df['treatment_outcome_time'] = df['treatment_outcome_date'].dt.time

        df.drop(columns=['treatment_start_date', 'treatment_completion_date', 'treatment_outcome_date'], inplace=True)
        df.rename(columns={
            'treatment_start_date_only': 'treatment_start_date',
            'treatment_start_time_only': 'treatment_start_time',
            'treatment_end_date_only': 'treatment_end_date',
            'treatment_end_time_only': 'treatment_end_time',
            'treatment_outcome_date_only': 'treatment_outcome_date'
        }, inplace=True)

        # Add calculated fields
        df['treatment_outcome_date_dt'] = pd.to_datetime(df['treatment_outcome_date'])
        df['treatment_end_date_dt'] = pd.to_datetime(df['treatment_end_date'])
        df['Outcome_Day'] = df['treatment_outcome_date_dt'].dt.day_name()
        df['Outcome_Weekend_Flag'] = df['Outcome_Day'].isin(['Saturday', 'Sunday']).astype(int)
        df['Report_Duration'] = (df['treatment_outcome_date_dt'] - df['treatment_end_date_dt']).dt.days
        df['Outcome_Quarter'] = df['treatment_outcome_date_dt'].dt.quarter
        df.drop(columns=['treatment_outcome_date_dt', 'treatment_end_date_dt'], inplace=True)

        # Split names
        df[['provider_first_name', 'provider_last_name']] = df['provider_name'].str.split(' ', n=1, expand=True)
        df[['patient_first_name', 'patient_last_name']] = df['patient_name'].str.split(' ', n=1, expand=True)

        # Connect to SQLite
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Connected to database.")

        # Effectiveness table mapping
        effectiveness_mapping = {
            'deceased': 0,
            'worsened': 1,
            'unsuccessful': 2,
            'partially successful': 3,
            'stable': 4,
            'successful': 5
        }

        inserted_patients = 0
        inserted_providers = 0
        inserted_diseases = 0
        inserted_locations = 0
        inserted_treatments = 0

        # Insert Effectiveness data
        for idx, (status, score) in enumerate(effectiveness_mapping.items(), start=1):
            cursor.execute('''
                INSERT OR IGNORE INTO EFFECTIVENESS (Effectiveness_ID, Outcome_Status, Effectiveness_Score)
                VALUES (?, ?, ?)
            ''', (idx, status, score))
        print("Effectiveness scores populated.")

        # Insert records
        for _, row in df.iterrows():
            # PATIENT
            cursor.execute('''
            INSERT OR IGNORE INTO PATIENT (Patient_ID, First_Name, Last_Name, Gender, Age)
            VALUES (?, ?, ?, ?, ?)
            ''', (row['patient_id'], row['patient_first_name'], row['patient_last_name'], row['gender'], row['age']))
            inserted_patients += cursor.rowcount

            # PROVIDER SCD TYPE 2
            cursor.execute('''
            SELECT * FROM PROVIDER
            WHERE Provider_ID = ? AND Is_Current = 1
            ''', (row['provider_id'],))
            existing = cursor.fetchone()

            today = datetime.today().strftime('%Y-%m-%d')

            if existing:
                _, _, old_fname, old_lname, old_spec_id, old_spec_name, old_hosp, _, _, _ = existing
                if (old_fname != row['provider_first_name'] or
                    old_lname != row['provider_last_name'] or
                    old_spec_id != row['speciality_id_x'] or
                    old_spec_name != row['speciality_name'] or
                    old_hosp != row['affiliated_hospital']):
                    cursor.execute('''
                    UPDATE PROVIDER
                    SET Valid_To = ?, Is_Current = 0
                    WHERE Provider_ID = ? AND Is_Current = 1
                    ''', (today, row['provider_id']))

                    cursor.execute('''
                    INSERT INTO PROVIDER (
                        Provider_ID, First_Name, Last_Name, Speciality_Id, Speciality_Name, Affiliated_Hospital,
                        Valid_From, Valid_To, Is_Current
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 1)
                    ''', (row['provider_id'], row['provider_first_name'], row['provider_last_name'],
                          row['speciality_id_x'], row['speciality_name'], row['affiliated_hospital'], today))
                    inserted_providers += 1
            else:
                cursor.execute('''
                INSERT INTO PROVIDER (
                    Provider_ID, First_Name, Last_Name, Speciality_Id, Speciality_Name, Affiliated_Hospital,
                    Valid_From, Valid_To, Is_Current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 1)
                ''', (row['provider_id'], row['provider_first_name'], row['provider_last_name'],
                      row['speciality_id_x'], row['speciality_name'], row['affiliated_hospital'], today))
                inserted_providers += 1

            # DISEASE
            cursor.execute('''
            INSERT OR IGNORE INTO DISEASE (Disease_ID, Speciality_Id, Name, Type, Severity, Transmission_Mode, Mortality_Rate)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (row['disease_id'], row['speciality_id_x'], row['disease_name'],
                  row['disease_type'], row['severity'], row['transmission_mode'], row['mortality_rate']))
            inserted_diseases += cursor.rowcount

            # LOCATION
            cursor.execute('''
            SELECT Location_ID FROM LOCATION WHERE Country = ? AND State = ? AND City = ?
            ''', (row['country'], row['state'], row['city']))
            location_row = cursor.fetchone()

            if location_row:
                location_id = location_row[0]
            else:
                cursor.execute('''
                INSERT INTO LOCATION (Country, State, City)
                VALUES (?, ?, ?)
                ''', (row['country'], row['state'], row['city']))
                location_id = cursor.lastrowid
                inserted_locations += 1

            row['location_id'] = location_id

            # TREATMENT
            effectiveness_score = effectiveness_mapping.get(str(row['treatment_outcome_status']).lower(), None)
            cursor.execute('''
            INSERT OR IGNORE INTO TREATMENT (
                Treatment_ID, Start_Date, Completion_Date, Outcome_Date, Outcome_Quarter, Treatment_Duration, Cost,
                Effectiveness_Score, Type, Patient_ID, Provider_ID, Location_ID, Disease_ID,
                Outcome_Day, Outcome_Weekend_Flag, Report_Duration
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['treatment_id'],
                row['treatment_start_date'].strftime('%Y-%m-%d') if pd.notnull(row['treatment_start_date']) else None,
                row['treatment_end_date'].strftime('%Y-%m-%d') if pd.notnull(row['treatment_end_date']) else None,
                row['treatment_outcome_date'].strftime('%Y-%m-%d') if pd.notnull(row['treatment_outcome_date']) else None,
                row['Outcome_Quarter'],
                row['treatment_duration'],
                row['treatment_cost'],
                effectiveness_score,
                row['treatment_type'],
                row['patient_id'],
                row['provider_id'],
                row['location_id'],
                row['disease_id'],
                row['Outcome_Day'],
                row['Outcome_Weekend_Flag'],
                row['Report_Duration']
            ))
            inserted_treatments += cursor.rowcount

        conn.commit()
        print("All data inserted and committed successfully.")

        # Print record counts
        print(f"Patients inserted: {inserted_patients}")
        print(f"Providers inserted (new versions or new): {inserted_providers}")
        print(f"Diseases inserted: {inserted_diseases}")
        print(f"Locations inserted: {inserted_locations}")
        print(f"Treatments inserted: {inserted_treatments}")

    except FileNotFoundError as fe:
        print(f"{fe}")
        sys.exit(1)

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
