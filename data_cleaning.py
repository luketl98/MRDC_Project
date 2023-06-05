import numpy as np
import pandas as pd
from dateutil.parser import parse, ParserError
import re
import phonenumbers
from pandas.api.types import is_numeric_dtype

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
    
    def clean_card_data(self, df):
        df = df.copy()  # Add this line to make a copy of the DataFrame

        # Define the list of known card providers
        known_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 
                           'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

        # Remove card_number with invalid length
        df = df[df['card_number'].apply(lambda x: 13 <= len(str(x).replace('.0', '')) <= 19)]

        # Convert expiry_date to datetime and remove invalid dates
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df = df[df['expiry_date'].notna()]
        df = df[df['expiry_date'] > pd.to_datetime('today')]

        # Keep only known card providers
        df = df[df['card_provider'].isin(known_providers)]

        # Convert date_payment_confirmed to datetime and remove invalid dates
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df = df[df['date_payment_confirmed'].notna()]
        df = df[df['date_payment_confirmed'] < pd.to_datetime('today')]

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


    @staticmethod
    def clean_store_data(df):
        # 1. Removing `lat` column and moving `latitude` next to `longitude`
        df = df.drop(columns=['lat'])
        cols = df.columns.tolist()
        cols.insert(cols.index('longitude') + 1, cols.pop(cols.index('latitude')))
        df = df[cols]
        
        # 2. Correcting the encoding issue with the `address` column
        df['address'] = df['address'].str.encode('latin1').str.decode('utf-8', 'ignore')
        
        # 3. Formatting the `opening_date` column
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        
        # 4. Removing rows with totally invalid or all null data
        df = df.dropna(how='all')  # drop rows with all null values
        df = df[~df.applymap(lambda x: len(str(x)) < 2 and not str(x).isnumeric()).all(axis=1)]  # drop rows with invalid data
        
        # 5. Correcting 'eeEurope' to 'Europe' in the `continent` column
        df['continent'] = df['continent'].replace('eeEurope', 'Europe')
        
        # 6. Cleaning the `staff_numbers` column
        df['staff_numbers'] = df['staff_numbers'].replace({'J': '1', 'e': '3'}, regex=True)
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').astype(pd.Int64Dtype())
        
        numeric_cols = ['longitude', 'latitude']  # add any other numeric columns that need cleaning
        nan_threshold = 3
        df = DataCleaning.clean_invalid_data(df, numeric_cols, nan_threshold)
        
        return df
    
    @staticmethod
    def clean_invalid_data(df, numeric_cols, nan_threshold):
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df[df.isnull().sum(axis=1) <= nan_threshold]
        return df


    @staticmethod
    def clean_products_data(df):
        def convert_product_weights(df):
            def to_kg(weight):
                if isinstance(weight, float):
                    return weight  # It's already a float, no conversion needed
                elif 'kg' in weight:
                    numeric_weight = float(re.sub('[^0-9.]', '', weight))
                elif 'g' in weight:
                    numeric_weight = float(re.sub('[^0-9.]', '', weight)) / 1000  # Convert grams to kg
                elif 'x' in weight:
                    quantity, unit_weight = weight.split(' x ')
                    if 'g' in unit_weight:
                        numeric_weight = float(quantity) * (float(re.sub('[^0-9.]', '', unit_weight)) / 1000)  # Convert grams to kg
                    else:
                        numeric_weight = float(quantity) * float(re.sub('[^0-9.]', '', unit_weight))  # If kg, no need to convert
                else:
                    numeric_weight = None  # Handle edge cases that might have been missed

                return numeric_weight

            df['weight'] = df['weight'].apply(to_kg)
            return df
        df = convert_product_weights(df)

        # Format 'date_added' column
        def parse_dates(date):
            try:
                return parse(date).date()
            except:
                return None

        df['date_added'] = df['date_added'].apply(parse_dates)

        # Remove the first column
        df = df.drop(df.columns[[0]], axis=1)

        # Set any 'category' cell to null if it contains numbers
        df.loc[df['category'].str.contains('\d', na=False), 'category'] = None

        # Corrects spelling mistake in 'removed' column ('still_avaliable' to 'still_available')
        df['removed'] = df['removed'].str.replace('Still_avaliable', 'Still_available', regex=False)

        # Ensure 'removed' column contains only 'Removed' or 'Still_available', if not, set it to null
        df.loc[~df['removed'].isin(['Removed', 'Still_available']), 'removed'] = None

        # Make sure 'product_price' contains only valid format like '£10.99', if not, set it to null
        df.loc[df['product_price'].str.contains('[a-zA-Z]', na=False), 'product_price'] = None

        # Remove rows where more than half of the cells are null
        half_len = len(df.columns) / 2
        df = df.dropna(thresh=half_len)
        
        return df