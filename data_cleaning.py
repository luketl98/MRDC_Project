import pandas as pd
import re

class DataCleaning:

    @staticmethod
    def clean_user_data(user_df):
        # Clean date columns
        date_columns = ['date_of_birth', 'join_date']
        for date_col in date_columns:
            user_df[date_col] = pd.to_datetime(user_df[date_col], errors='coerce')

        # Clean names
        name_columns = ['first_name', 'last_name']
        for name_col in name_columns:
            user_df[name_col] = user_df[name_col].str.replace('[^a-zA-Z\s]', '', regex=True)

        # Clean phone numbers
        def clean_phone_number(phone):
            return re.sub(r'[^\d\+]', '', phone)

        user_df['phone_number'] = user_df['phone_number'].apply(clean_phone_number)

        # Drop rows with NULL values or empty strings
        user_df.dropna(inplace=True)
        user_df.replace('', pd.NA, inplace=True)

        return user_df
