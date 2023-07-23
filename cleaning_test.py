from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import numpy as np
import pandas as pd
from dateutil.parser import parse, ParserError
import re
import phonenumbers
import requests


db_connector = DatabaseConnector()
data_cleaning = DataCleaning()

header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
data_extractor = DataExtractor(header)


def list_number_of_stores(endpoint):
    """
    Retrieves the number of stores from a given endpoint.

    Args:
        endpoint: URL of the API endpoint.

    Returns:
        Integer with the number of stores.
    """
    response = requests.get(endpoint, headers=header)
    print("Status code:", response.status_code)  # add this line to print the status code
    try:
        print(response.json())  # print the response data
        return response.json()['number_stores']
    except KeyError:
        print(f"KeyError: The key 'number_stores' does not exist in the response")
        print("Here is the entire response:", response.json())
        raise


# dim_store_details : Extract, clean and upload store data from API
number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_data_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
num_stores = list_number_of_stores(number_stores_endpoint)
store_data = data_extractor.retrieve_stores_data(store_data_endpoint, num_stores)
print("Number of rows in the original data:", len(store_data)) ## delete this line
print("Total number of stores:", num_stores) ## delete this line
clean_store_data = data_cleaning.clean_store_data(store_data)
db_connector.upload_to_db(clean_store_data, 'dim_store_details')

# upload store data to csv
store_data.to_csv('raw_store_data.csv', index=False)

clean_store_data.to_csv('clean_store_data.csv', index=False)


# Assuming df is your DataFrame and 'column_name' is the name of your column
unique_values = store_data['store_type'].unique()

# Convert the numpy array to a list
unique_values_list = unique_values.tolist()

print(unique_values_list)

# find the index of each unique value from the unique_values list
# and insert it in a new column named 'store_type_id'
store_data['store_type_id'] = store_data['store_type'].apply(lambda x: unique_values_list.index(x))