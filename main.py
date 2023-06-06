from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Instantiate DatabaseConnector and DataExtractor with necessary parameters
db_connector = DatabaseConnector()
header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
data_extractor = DataExtractor(header)
data_cleaning = DataCleaning()

# Initialize database engine
engine = db_connector.init_db_engine()

# Extract and clean user data, then upload to database
user_data_table = 'legacy_users'
if user_data_table in db_connector.list_db_tables(engine):
    raw_user_data_df = DataExtractor.read_rds_table(db_connector, user_data_table)
    cleaned_user_data_df = DataCleaning.clean_user_data(raw_user_data_df)
    db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')
else:
    print(f"User data table '{user_data_table}' not found")

# Extract, clean and upload card data from PDF
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
raw_card_data_df = data_extractor.retrieve_pdf_data(link)
clean_card_data_df = data_cleaning.clean_card_data(raw_card_data_df)
db_connector.upload_to_db(clean_card_data_df, 'dim_card_details')

# Extract, clean and upload store data from API
number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_data_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
num_stores = data_extractor.list_number_of_stores(number_stores_endpoint)
store_data = data_extractor.retrieve_stores_data(store_data_endpoint, num_stores)
clean_store_data = data_cleaning.clean_store_data(store_data)
db_connector.upload_to_db(clean_store_data, 'dim_store_details')

# Extract, clean and upload product data from S3
s3_address = "s3://data-handling-public/products.csv"
raw_product_data_df = DataExtractor.extract_from_s3(s3_address)
clean_product_data_df = DataCleaning.clean_products_data(raw_product_data_df)
db_connector.upload_to_db(clean_product_data_df, 'dim_products')

