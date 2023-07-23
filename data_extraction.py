import pandas as pd
import tabula
import requests
import boto3
from io import StringIO

class DataExtractor:            
    def __init__(self, header):
        """Initializes the DataExtractor with the provided request header."""
        self.header = header

    @staticmethod
    def read_rds_table(database_connector, table_name):
        """
        Reads a table from a relational database service (RDS).

        Args:
            database_connector: Instance of the DatabaseConnector class.
            table_name: Name of the table to read.

        Returns:
            DataFrame with the content of the table.
        """
        engine = database_connector.init_db_engine()
        return pd.read_sql_table(table_name, engine)

    def retrieve_pdf_data(self, link):
        """
        Retrieves data from a PDF file.

        Args:
            link: URL of the PDF file.

        Returns:
            DataFrame with the content of the PDF file.
        """
        df_list = tabula.read_pdf(link, pages='all')
        return pd.concat(df_list, ignore_index=True)
    
    def list_number_of_stores(self, endpoint):
        """
        Retrieves the number of stores from a given endpoint.

        Args:
            endpoint: URL of the API endpoint.

        Returns:
            Integer with the number of stores.
        """
        response = requests.get(endpoint, headers=self.header)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, endpoint, num_stores):
        """
        Retrieves data about a certain number of stores from a given endpoint.

        Args:
            endpoint: URL of the API endpoint.
            num_stores: Number of stores to retrieve data for.

        Returns:
            DataFrame with the data about the stores. Returns None if no data could be retrieved.
        """
        data = []
        for i in range(num_stores):
            try:
                response = requests.get(f"{endpoint}/{i}", headers=self.header)
                if response.status_code == 200:
                    data.append(response.json())


            except Exception as e:
                print(f"There was an error: {e}")

        if not data:
            return None

        return pd.DataFrame(data)

    @staticmethod
    def extract_from_s3(s3_address: str) -> pd.DataFrame:
        """
        Extracts data from a file stored in AWS S3.

        Args:
            s3_address: Address of the file in the S3 bucket.

        Returns:
            DataFrame with the content of the file.
        """
        bucket_name, key = s3_address.split('/')[2], '/'.join(s3_address.split('/')[3:])
        s3 = boto3.client('s3')
        csv_obj = s3.get_object(Bucket=bucket_name, Key=key)
        body = csv_obj['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(body))

    def extract_from_json_url(self, json_url):
        """
        Extracts JSON data from a URL and converts it to a DataFrame.

        Args:
            json_url (str): The URL of the JSON data.

        Returns:
            DataFrame: The JSON data as a DataFrame.
        """
        data = requests.get(json_url).json()
        df = pd.DataFrame(data)
        return df