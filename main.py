from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector()

# Create the database engine
engine = db_connector.init_db_engine()

# Pass the engine to the list_db_tables method
table_names = db_connector.list_db_tables(engine)

# Replace 'your_user_data_table' with the correct table name containing user data
user_data_table = 'legacy_users'

if user_data_table in table_names:
    raw_user_data_df = DataExtractor.read_rds_table(db_connector, user_data_table)
else:
    print(f"User data table '{user_data_table}' not found")

# Clean the user data
cleaned_user_data_df = DataCleaning.clean_user_data(raw_user_data_df)

# Upload the cleaned user data to the sales_data database
db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')

# Instantiate the classes
header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
data_extractor = DataExtractor(header)

# TASK 4
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
raw_card_data_df = data_extractor.retrieve_pdf_data(link)

data_cleaning = DataCleaning()
clean_card_data_df = data_cleaning.clean_card_data(raw_card_data_df)

db_connector.upload_to_db(clean_card_data_df, 'dim_card_details')

number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_data_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

cleaner = DataCleaning()

# Step 2: Get the number of stores
num_stores = data_extractor.list_number_of_stores(number_stores_endpoint)

# Retrieve and clean store data
store_data = data_extractor.retrieve_stores_data(store_data_endpoint, num_stores)
clean_store_data = cleaner.clean_store_data(store_data)

# Export to CSV
clean_store_data.to_csv('clean_store_data.csv', index=False)

# Step 5: Upload cleaned data to database
# Replace with your function to upload data to database
db_connector.upload_to_db(clean_store_data, 'dim_store_details')

# Extract product data from S3
s3_address = "s3://data-handling-public/products.csv"
raw_product_data_df = DataExtractor.extract_from_s3(s3_address)
raw_product_data_df.to_csv('raw_product_data.csv')


# Clean the product data
clean_product_data_df = DataCleaning.clean_products_data(raw_product_data_df)

# Upload the cleaned product data to the sales_data database
db_connector.upload_to_db(clean_product_data_df, 'dim_products')
