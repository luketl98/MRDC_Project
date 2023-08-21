import numpy as np
import pandas as pd
from dateutil.parser import parse, ParserError
import re
import phonenumbers

class DataCleaning:

# Main functions

    staticmethod
    def clean_card_data(df):
        known_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
        df = df[df['card_number'].str.len().between(13, 19, inclusive='both')]
        df.loc[:, 'expir@y_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce').dt.date
        df = df[df['card_provider'].isin(known_providers)]
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce').dt.date
        df = df.loc[df['date_payment_confirmed'].notna() & (df['date_payment_confirmed'] < pd.to_datetime('today').date())]
        return DataCleaning.clean_invalid_data(df)

    @staticmethod
    def clean_date_times_data(df):
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
        return DataCleaning.clean_invalid_data(df)

    @staticmethod
    def clean_products_data(df):
        df['weight'] = df['weight'].apply(DataCleaning.convert_product_weights)
        df['date_added'] = df['date_added'].apply(DataCleaning.parse_date)
        df.drop(df.columns[0], axis=1, inplace=True)
        df.loc[df['category'].str.contains('\d', na=False), 'category'] = None
        df.replace({'removed': {'Still_avaliable': 'Still_available'}}, inplace=True)
        df.loc[~df['removed'].isin(['Removed', 'Still_available']), 'removed'] = None
        df.loc[df['product_price'].str.contains('[a-zA-Z]', na=False), 'product_price'] = None
        return DataCleaning.clean_invalid_data(df)

    @staticmethod
    def clean_store_data(df):
        df.drop(columns=['lat'], inplace=True)
        df['address'] = df['address'].str.encode('latin1').str.decode('utf-8', 'ignore')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce').dt.date
        df.replace({'continent': {'eeEurope': 'Europe', 'eeAmerica': 'America'}, 'staff_numbers': {'J': '1', 'e': '3'}}, inplace=True)
        df['continent'] = df['continent'].where(df['continent'].isin(['America', 'Europe']))
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').astype(pd.Int64Dtype())
        df = DataCleaning.clean_country_codes(df)
        df = DataCleaning.remove_cells_with_letters(df, ['longitude', 'latitude'])
        return DataCleaning.clean_invalid_data(df)

    @staticmethod
    def clean_user_data(df):
        df = (df.pipe(DataCleaning.clean_alphabetical_columns)
                .pipe(DataCleaning.clean_phone_numbers)
                .assign(date_of_birth = lambda df: df['date_of_birth'].apply(DataCleaning.parse_date),
                        join_date = lambda df: df['join_date'].apply(DataCleaning.parse_date))
                .pipe(DataCleaning.clean_country_codes)) 
        df = df[df['user_uuid'].apply(DataCleaning.is_valid_uuid)]
        return DataCleaning.clean_invalid_data(df)

    @staticmethod
    def clean_orders_data(df):
        # Remove unnecessary columns
        columns_to_drop = ["first_name", "last_name", "1", "level_0"]
        df = df.drop(columns=columns_to_drop, errors="ignore")
        return DataCleaning.clean_invalid_data(df)


# Other helper functions

    @staticmethod
    def is_valid_uuid(val):
        regex = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z', re.I)
        match = regex.match(val)
        return bool(match)

    @staticmethod
    def clean_alphabetical_columns(df):
        df[['first_name', 'last_name']] = df[['first_name', 'last_name']].applymap(lambda x: re.sub('[^a-zA-Z]', '', x))
        return df
    
    @staticmethod
    def clean_phone_numbers(df):
        def clean_number(row):
            if pd.isna(row['phone_number']) or row['phone_number'] == '':
                return pd.NA

            raw_number = re.sub(r'x\d+|\(0\)|[^0-9]', '', row['phone_number'])
            raw_number = raw_number.lstrip('0')

            country_code_dict = {'DE': '49', 'GB': '44', 'US': '1'}
            if raw_number.startswith(country_code_dict.get(row['country_code'], '')):
                raw_number = raw_number[len(country_code_dict.get(row['country_code'], '')):]

            raw_number = country_code_dict.get(row['country_code'], '') + raw_number

            try:
                parsed_number = phonenumbers.parse(raw_number, row['country_code'])
                return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL) if phonenumbers.is_valid_number(parsed_number) else pd.NA
            except phonenumbers.phonenumberutil.NumberParseException:
                return pd.NA

        df['phone_number'] = df.apply(clean_number, axis=1)
        return df

    @staticmethod
    def clean_invalid_data(df):
        threshold = len(df.columns) / 2
        return df.dropna(thresh=threshold)

    @staticmethod
    def convert_product_weights(weight):
        if isinstance(weight, str):
            if 'kg' in weight:
                return float(re.sub('[^0-9.]', '', weight))
            elif 'g' in weight:
                return float(re.sub('[^0-9.]', '', weight)) / 1000
            elif 'ml' in weight:
                return float(re.sub('[^0-9.]', '', weight))
            elif 'x' in weight:
                quantity, unit_weight = weight.split(' x ')
                return float(quantity) * (float(re.sub('[^0-9.]', '', unit_weight)) / 1000 if 'g' in unit_weight else float(re.sub('[^0-9.]', '', unit_weight)))
        else:
            return weight

    @staticmethod
    def parse_date(date_str):
        try:
            return parse(date_str).strftime('%Y-%m-%d') if isinstance(date_str, str) else np.nan
        except ParserError:
            return np.nan
        
    @staticmethod
    def clean_country_codes(df):
        # Convert to upper case and trim spaces if any
        df['country_code'] = df['country_code'].str.upper().str.strip()
        # Replace 'GGB' with 'GB'
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        # Set to null if country_code isn't 'US', 'GB', or 'DE'
        df.loc[~df['country_code'].isin(['US', 'GB', 'DE']), 'country_code'] = None
        return df
    
    
    @staticmethod
    def remove_cells_with_letters(df, columns):
        df[columns] = df[columns].applymap(lambda x: np.nan if isinstance(x, str) and re.search(r'[a-zA-Z]', x) else x)
        return df

