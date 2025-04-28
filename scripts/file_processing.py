# file_processing.py
import pandas as pd
from pathlib import Path
from mappings import apply_mappings, apply_hospital_mapping, convert_cost
import os

# Function to load and process the file
def load_and_process_file(file_path):
    if file_path.suffix == ".csv":
        df = pd.read_csv(file_path)

        # Apply mappings and transformations
        df = apply_mappings(df)
        df = apply_hospital_mapping(df)
        df = convert_cost(df)  
        return df
    else:
        print(f"Unsupported file format: {file_path.name}")
        return pd.DataFrame()

# Function to mark a file as processed
def mark_file_processed(file_path, metadata_file="processed_files.txt"):
    file_name = file_path.name
    if os.path.exists(metadata_file):
        with open(metadata_file, "r") as f:
            existing = set(f.read().splitlines())
    else:
        existing = set()

    if file_name not in existing:
        with open(metadata_file, "a") as f:
            f.write(f"{file_name}\n")

