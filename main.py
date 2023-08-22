from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from sqlalchemy import text

# Instantiate DatabaseConnector and DataExtractor with necessary parameters
db_connector = DatabaseConnector('db_creds.yaml')
local_db_connector = DatabaseConnector('local_creds.yaml')

header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
data_extractor = DataExtractor(header)
data_cleaning = DataCleaning()

# Initialize database engine


####################

# dim_users : Extract and clean user data, then upload to database
user_data_table = 'legacy_users'

if user_data_table in db_connector.list_db_tables():
    raw_user_data_df = DataExtractor.read_rds_table(db_connector, user_data_table)
    cleaned_user_data_df = DataCleaning.clean_user_data(raw_user_data_df)
    local_db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')
else:
    print(f"User data table '{user_data_table}' not found")

# dim_card_details : Extract, clean and upload card data from PDF
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
raw_card_data_df = data_extractor.retrieve_pdf_data(link)
clean_card_data_df = DataCleaning.clean_card_data(raw_card_data_df)
local_db_connector.upload_to_db(clean_card_data_df, 'dim_card_details')

# dim_store_details : Extract, clean and upload store data from API
number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_data_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
num_stores = data_extractor.list_number_of_stores(number_stores_endpoint)
store_data = data_extractor.retrieve_stores_data(store_data_endpoint, num_stores)
clean_store_data = data_cleaning.clean_store_data(store_data)
local_db_connector.upload_to_db(clean_store_data, 'dim_store_details')

# dim_products : Extract, clean and upload product data from S3
s3_address = "s3://data-handling-public/products.csv"
raw_product_data_df = DataExtractor.extract_from_s3(s3_address)
clean_product_data_df = DataCleaning.clean_products_data(raw_product_data_df)
local_db_connector.upload_to_db(clean_product_data_df, 'dim_products')

# orders_table : Extract and clean the orders data
orders_data_table = 'orders_table'
raw_orders_data_df = DataExtractor.read_rds_table(db_connector, orders_data_table)
clean_orders_data_df = DataCleaning.clean_orders_data(raw_orders_data_df)
local_db_connector.upload_to_db(clean_orders_data_df, 'orders_table')

# dim_date_times : Extract and clean the date times data
json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
raw_date_times_data_df = data_extractor.extract_from_json_url(json_url)
clean_date_times_data_df = data_cleaning.clean_date_times_data(raw_date_times_data_df)
local_db_connector.upload_to_db(clean_date_times_data_df, 'dim_date_times')


# Read the SQL file
with open('cast_data_types.sql', 'r') as file:
    sql_file_content = file.read()

# Split the SQL file content by 'ALTER TABLE'
sql_commands = sql_file_content.split('ALTER TABLE')

# Execute each SQL command individually
with local_db_connector.engine.connect() as connection:
    for sql_command in sql_commands[1:]:  # Skip the first split, as it's empty
        # Prepend 'ALTER TABLE' to the command and remove leading/trailing whitespaces
        sql_command = 'ALTER TABLE ' + sql_command.strip()  # Notice the space after 'ALTER TABLE '
        
        # Execute the command
        connection.execute(text(sql_command))


# Close the engine
# self.engine.dispose()

# Close all connections

# connection.close()


"""
so instead of the engine connect close line you can have

Close the engine

engine.dispose()

Or close the connection

connection.close()
"""