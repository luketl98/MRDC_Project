import pandas as pd

class DataCleaning:
    @staticmethod
    def clean_user_data(df):
        # Replace non-alphabetical characters in specific columns
        columns_to_clean = ['first_name', 'last_name']
        for col in columns_to_clean:
            df[col] = df[col].str.replace('[^a-zA-Z]', '')

        # Standardize phone numbers
        phone_number_columns = ['phone_number']
        for col in phone_number_columns:
            df[col] = df[col].str.replace('[^0-9]', '')
            df[col] = df[col].apply(lambda x: f"{x[:3]}-{x[3:6]}-{x[6:]}")

        # Parse and reformat dates
        date_columns = ['date_of_birth', 'join_date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')

        # Handle any other cleaning tasks here...

        return df
