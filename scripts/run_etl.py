# run_etl.py
import os
from pathlib import Path
import pandas as pd
from file_processing import load_and_process_file, mark_file_processed

def run_etl():
    print("Starting ETL process...", flush=True)

    base_dir = Path(__file__).resolve().parent.parent

    raw_data_dir = base_dir / "Healthcare_ETL_Project" / "raw_data"
    processed_dir = base_dir / "Healthcare_ETL_Project" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    processed_file = processed_dir / "Healthcare_Dataset.csv"
    processed_metadata_file = base_dir / "processed_files.txt"

    # Read processed files
    processed_files = set()
    if processed_metadata_file.exists():
        with open(processed_metadata_file, "r") as f:
            processed_files = set(f.read().splitlines())

    new_files_processed = []

    for file_path in raw_data_dir.glob("*.csv"):
        if file_path.name in processed_files:
            print(f"Skipping already processed file: {file_path.name}")
            continue

        print(f"Processing file: {file_path.name}")
        df = load_and_process_file(file_path)
        print(f"Loaded {len(df)} records from {file_path.name}")

        if processed_file.exists():
            df.to_csv(processed_file, mode='a', header=False, index=False)
        else:
            df.to_csv(processed_file, index=False)

        new_files_processed.append(file_path.name)
        print(f"File {file_path.name} processed and saved.")

    # Append new processed file names to metadata
    if new_files_processed:
        with open(processed_metadata_file, "a") as f:
            for fname in new_files_processed:
                f.write(fname + "\n")

    print(f"Processed files this run: {new_files_processed}")

if __name__ == "__main__":
    run_etl()
