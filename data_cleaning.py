import numpy as np
import pandas as pd
from dateutil.parser import parse, ParserError
import re
import phonenumbers

class DataCleaning:
    @staticmethod
    def clean_user_data(df):
        return df.pipe(DataCleaning.clean_alphabetical_columns).pipe(DataCleaning.clean_phone_numbers).pipe(DataCleaning.clean_dates)

    @staticmethod
    def clean_alphabetical_columns(df):
        df[['first_name', 'last_name']] = df[['first_name', 'last_name']].applymap(lambda x: re.sub('[^a-zA-Z]', '', x))
        return df

    def clean_card_data(self, df):
        known_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

        df = df[df['card_number'].str.len().between(13, 19, inclusive='both')]
        df.loc[:, 'expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df = df.loc[df['expiry_date'].notna() & (df['expiry_date'] > pd.to_datetime('today')) & df['card_provider'].isin(known_providers)]
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df = df.loc[df['date_payment_confirmed'].notna() & (df['date_payment_confirmed'] < pd.to_datetime('today'))]

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
    def clean_dates(df):
        def parse_date(date_str):
            try:
                return parse(date_str).strftime('%Y-%m-%d') if isinstance(date_str, str) else np.nan
            except ParserError:
                return np.nan

        df[['date_of_birth', 'join_date']] = df[['date_of_birth', 'join_date']].applymap(parse_date)
        return df

    @staticmethod
    def clean_store_data(df):
        df.drop(columns=['lat'], inplace=True)
        df['address'] = df['address'].str.encode('latin1').str.decode('utf-8', 'ignore')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        df.dropna(how='all', inplace=True)
        df = df[~df.applymap(lambda x: len(str(x)) < 2 and not str(x).isnumeric()).all(axis=1)]
        df.replace({'continent': {'eeEurope': 'Europe'}, 'staff_numbers': {'J': '1', 'e': '3'}}, inplace=True)
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').astype(pd.Int64Dtype())

        return DataCleaning.clean_invalid_data(df, ['longitude', 'latitude'], 3)

    @staticmethod
    def clean_invalid_data(df, numeric_cols, nan_threshold):
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        return df[df.isnull().sum(axis=1) <= nan_threshold]

    @staticmethod
    def clean_products_data(df):
        df['weight'] = df['weight'].apply(DataCleaning.convert_product_weights)
        df['date_added'] = df['date_added'].apply(DataCleaning.parse_date)
        df.drop(df.columns[0], axis=1, inplace=True)
        df.loc[df['category'].str.contains('\d', na=False), 'category'] = None
        df.replace({'removed': {'Still_avaliable': 'Still_available'}}, inplace=True)
        df.loc[~df['removed'].isin(['Removed', 'Still_available']), 'removed'] = None
        df.loc[df['product_price'].str.contains('[a-zA-Z]', na=False), 'product_price'] = None
        # Remove rows where more than half of the cells are null
        half_len = len(df.columns) / 2
        df = df.dropna(thresh=half_len)

        return df

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