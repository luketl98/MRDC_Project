from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import numpy as np
import pandas as pd


db_connector = DatabaseConnector()
data_cleaning = DataCleaning()
data_extractor = DataExtractor(header={})

@staticmethod
def clean_date_times_data(df):
    """Cleans the date_times table data."""

    # Drop the first column if it is unnamed
    if 'Unnamed: 0' in df.columns:
        df = df.drop('Unnamed: 0', axis=1)

    # Making null any cells that contain letters, or that are empty
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
    df['month'] = pd.to_numeric(df['month'], errors='coerce')
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['day'] = pd.to_numeric(df['day'], errors='coerce')

    # Retaining only the valid 'time_period' values
    valid_time_periods = ['Morning', 'Midday', 'Evening', 'Late_Hours']
    df['time_period'] = df['time_period'].where(df['time_period'].isin(valid_time_periods))

    # Convert the datetime to just time
    df['timestamp'] = df['timestamp'].dt.time

    # Remove rows where more than half of the cells are null
    df = df.dropna(thresh=df.shape[1] // 2 + 1)

    return df

# Task 8
# Step 1 & 2
json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
raw_date_times_data_df = data_extractor.extract_from_json_url(json_url)
clean_date_times_data_df = clean_date_times_data(raw_date_times_data_df)
db_connector.upload_to_db(clean_date_times_data_df, 'dim_date_times')

