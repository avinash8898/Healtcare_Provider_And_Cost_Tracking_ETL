# mappings.py
import pandas as pd

# Define mappings
state_mapping = {
    'Haryana': 'Washington',
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

# Function to apply mappings to the data
def apply_mappings(df):
    df['state'] = df['state'].replace(state_mapping)
    df['city'] = df['city'].replace(city_mapping)
    df['country'] = df['country'].replace(country_mapping)
    return df

def apply_hospital_mapping(df):
    df['affiliated_hospital'] = df['affiliated_hospital'].map(hospital_mapping).fillna(df['affiliated_hospital'])
    return df

# Function to convert treatment costs to USD
def convert_cost(df, exchange_rate=85):
    df['treatment_cost'] = df['treatment_cost'] / exchange_rate
    df['treatment_cost'] = df['treatment_cost'].round(2)
    return df
