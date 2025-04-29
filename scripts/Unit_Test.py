
import pandas as pd
from datetime import datetime
import pytest

def transform_outcome_fields(df):
    df['treatment_outcome_date'] = pd.to_datetime(df['Outcome_Date'])
    df['Outcome_Day'] = df['treatment_outcome_date'].dt.day_name()
    df['Outcome_Weekend_Flag'] = df['Outcome_Day'].isin(['Saturday', 'Sunday']).astype(int)
    df['Outcome_Quarter'] = df['treatment_outcome_date'].dt.quarter
    return df[['Outcome_Day', 'Outcome_Weekend_Flag', 'Outcome_Quarter']]

def test_transform_outcome_fields():
    try:
        input_data = {
            'Outcome_Date': ['2023-01-15', '2024-05-30', '2025-12-01']  # Sunday, Thursday, Monday
        }
        df = pd.DataFrame(input_data)

        result = transform_outcome_fields(df)
        print(result)

        expected_days = ['Sunday', 'Thursday', 'Monday']
        expected_flags = [1, 0, 0]
        expected_quarters = [1, 2, 4]

        assert result['Outcome_Day'].tolist() == expected_days, "Mismatch in Outcome_Day"
        assert result['Outcome_Weekend_Flag'].tolist() == expected_flags, "Mismatch in Outcome_Weekend_Flag"
        assert result['Outcome_Quarter'].tolist() == expected_quarters, "Mismatch in Outcome_Quarter"

    except KeyError as ke:
        pytest.fail(f"KeyError occurred: {ke}")
    except AssertionError as ae:
        pytest.fail(f"Assertion failed: {ae}")
    except Exception as e:
        pytest.fail(f"Unexpected error: {e}")
