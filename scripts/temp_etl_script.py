#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import os
from pathlib import Path

# Mappings
state_mapping = {
    'Hariyana': 'Washington',
    'Karnataka': 'California',
    'Madhya Pradesh': 'Michigan',
    'Maharashtra': 'Massachusetts',
    'Punjab': 'New Jersey',
    'Rajasthan': 'Florida',
    'Tamil Nadu': 'Texas',
    'Uttar Pradesh': 'New York',
    'West Bengal': 'Illinois'
}

city_mapping = {
    'Faridabad': 'Seattle',
    'Mysore': 'San Francisco',
    'Bangalore': 'Los Angeles',
    'Mangalore': 'San Diego',
    'Gwalior': 'Detroit',
    'Pune': 'Boston',
    'Amritsar': 'Jersey City',
    'Chandigarh': 'Newark',
    'Jaipur': 'Miami',
    'Madurai': 'Houston',
    'Varanasi': 'New York City',
    'Lucknow': 'Buffalo',
    'Darjeeling': 'Chicago'
}

country_mapping = {
    'India': 'United States'
}

hospital_mapping = {
    'AIIMS': 'Mayo Clinic',
    'Kokilaben Hospital': 'Cleveland Clinic',
    'Global Hospitals': 'Johns Hopkins Hospital',
    'Narayana Health': 'Massachusetts General Hospital',
    'Columbia Asia': 'Mount Sinai Hospital',
    'Care Hospitals': 'St. Jude Children\'s Research Hospital',
    'Manipal Hospital': 'UCLA Medical Center',
    'Tata Memorial Hospital': 'Memorial Sloan Kettering Cancer Center',
    'BLK Super Speciality Hospital': 'Cedars-Sinai Medical Center',
    'Wockhardt Hospitals': 'New York-Presbyterian Hospital',
    'Fortis Hospital': 'Washington University in St. Louis Medical Center'
}

def process_file(file_path):
    df = pd.read_csv(file_path)
    
    # Apply mappings
    df['state'] = df['state'].replace(state_mapping)
    df['city'] = df['city'].replace(city_mapping)
    df['country'] = df['country'].replace(country_mapping)
    df['affiliated_hospital'] = df['affiliated_hospital'].map(hospital_mapping).fillna(df['affiliated_hospital'])
    
    # Convert cost and rename column
    df['treatment_cost_usd'] = df['treatment_cost'] / 85
    df['treatment_cost_usd'] = df['treatment_cost_usd'].round(2)
    df.drop(columns=['treatment_cost'], inplace=True)
    df.rename(columns={'treatment_cost_usd': 'treatment_cost'}, inplace=True)
    
    return df

def run_etl():
    base_path = Path(__file__).resolve().parent
    raw_dir = base_path / "raw_data"
    cleaned_file = base_path / "cleaned_data.csv"
    processed_file_log = base_path / "processed_files.txt"

    # Ensure log file exists
    processed_file_log.touch(exist_ok=True)
    with open(processed_file_log, 'r') as f:
        processed_files = set(f.read().splitlines())

    new_rows = []
    for file in raw_dir.glob("*.csv"):
        if file.name not in processed_files:
            print(f"Processing {file.name}")
            cleaned_df = process_file(file)
            new_rows.append(cleaned_df)
            with open(processed_file_log, 'a') as f:
                f.write(f"{file.name}\n")

    if new_rows:
        final_df = pd.concat(new_rows, ignore_index=True)
        if cleaned_file.exists():
            final_df.to_csv(cleaned_file, mode='a', index=False, header=False)
        else:
            final_df.to_csv(cleaned_file, index=False)
        print(f"Appended {len(final_df)} new rows to cleaned_data.csv")
    else:
        print("No new files to process.")

# Run the ETL
if __name__ == "__main__":
    run_etl()

