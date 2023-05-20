import numpy as np
import pandas as pd
from dateutil.parser import parse, ParserError
import re
import phonenumbers

class DataCleaning:
    @staticmethod
    def clean_user_data(df):
        df = DataCleaning.clean_alphabetical_columns(df)
        df = DataCleaning.clean_phone_numbers(df)
        df = DataCleaning.clean_dates(df)
        return df

    @staticmethod
    def clean_alphabetical_columns(df):
        columns_to_clean = ['first_name', 'last_name']
        for col in columns_to_clean:
            df[col] = df[col].str.replace('[^a-zA-Z]', '')
        return df

    @staticmethod
    def clean_phone_numbers(df):
        def clean_number(row):
            country_code = row['country_code']
            raw_number = row['phone_number']

            # Skip rows with NaN or empty values
            if pd.isna(raw_number) or raw_number == '':
                return pd.NA

            # Remove extensions (e.g., "x1234")
            raw_number = re.sub(r'x\d+', '', raw_number)

            # Remove "(0)" often used in domestic dialing within the UK and Germany
            raw_number = re.sub(r'\(0\)', '', raw_number)

            # Remove all non-numeric characters
            raw_number = re.sub('[^0-9]', '', raw_number)

            # If the number starts with the country code, remove it
            if country_code == 'DE' and raw_number.startswith('49'):
                raw_number = raw_number[2:]
            elif country_code == 'GB' and raw_number.startswith('44'):
                raw_number = raw_number[2:]
            elif country_code == 'US' and raw_number.startswith('1'):
                raw_number = raw_number[1:]

            # If the number now starts with a 0, remove it
            raw_number = raw_number.lstrip('0')

            # Add the country code back to the number
            if country_code == 'DE':
                raw_number = '49' + raw_number
            elif country_code == 'GB':
                raw_number = '44' + raw_number
            elif country_code == 'US':
                raw_number = '1' + raw_number

            # Parse the raw phone number using phonenumbers library
            try:
                parsed_number = phonenumbers.parse(raw_number, country_code)
            except phonenumbers.phonenumberutil.NumberParseException:
                return pd.NA

            # Check if the number is valid
            if not phonenumbers.is_valid_number(parsed_number):
                return pd.NA

            # Format the number in international format
            formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)

            return formatted_number

        df['phone_number'] = df.apply(clean_number, axis=1)
        return df


    @staticmethod
    def clean_dates(df):
        date_columns = ['date_of_birth', 'join_date']

        @staticmethod
        def try_parsing_date(date_str):
            try:
                return parse(date_str).strftime('%Y-%m-%d')
            except ParserError:
                return np.nan


        def is_valid_date(date):
            if pd.isnull(date):
                return False
            if isinstance(date, str):
                try:
                    parse(date)
                    return True
                except ParserError:
                    return False
            return True

        for col in date_columns:
            df.loc[:, col] = df[col].apply(lambda x: try_parsing_date(x) if isinstance(x, str) else x)
            df = df[df[col].apply(is_valid_date)]
        
        return df

