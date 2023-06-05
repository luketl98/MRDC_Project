import pandas as pd
import tabula
import requests
import boto3
from io import StringIO
from urllib.parse import urlparse
from botocore.client import Config
from botocore import UNSIGNED


class DataExtractor:
            
    def __init__(self, header):
        self.header = header

    @staticmethod
    def read_rds_table(database_connector, table_name):
        engine = database_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df

    def retrieve_pdf_data(self, link):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        df_list = tabula.read_pdf(link, pages='all')
        df = pd.concat(df_list, ignore_index=True)
        return df
    
    def list_number_of_stores(self, endpoint):
        response = requests.get(endpoint, headers=self.header)
        print(response.json())  # Print the full response
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, endpoint, num_stores):
        # Initialize an empty list to store the data from each store
        data = []

        for i in range(1, num_stores + 1):
            try:
                response = requests.get(f"{endpoint}/{i}", headers=self.header)
                if response.status_code == 200:
                    data.append(response.json())
                else:
                    print(f"There was an error retrieving data for store number {i}")
            except Exception as e:
                print(f"There was an error: {e}")

        # If no data was collected, return None
        if not data:
            return None

        # Create DataFrame from the collected data
        df = pd.DataFrame(data)
        
        return df


    @staticmethod
    def extract_from_s3(s3_address: str) -> pd.DataFrame:
        bucket_name = s3_address.split('/')[2]
        key = '/'.join(s3_address.split('/')[3:])
        
        s3 = boto3.client('s3')
        csv_obj = s3.get_object(Bucket=bucket_name, Key=key)
        body = csv_obj['Body'].read().decode('utf-8')
        
        data = pd.read_csv(StringIO(body))

        return data

