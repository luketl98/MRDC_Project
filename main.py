from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from sqlalchemy import text


# Function to reset the database schema
def reset_database_schema(engine):

    """
    Resets the database schema by dropping existing tables.
    This ensures that the database is in a clean state before new operations.
    Parameters:
        engine: SQLAlchemy engine object
            The database engine to execute the commands.
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS orders_table CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_users CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_card_details CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_store_details CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_products CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_date_times CASCADE;"))


# Instantiate DatabaseConnector with necessary parameters
db_connector = DatabaseConnector('db_creds.yaml')
local_db_connector = DatabaseConnector('local_creds.yaml')

# Initialise SQLAlchemy engine for schema reset
engine = local_db_connector.engine

# New user prompt for schema reset
reset_choice = input("Do you want to reset the database schema? (yes/no): ")
if reset_choice.lower() == 'yes':
    reset_database_schema(engine)

# Initialise data extraction and cleaning components
header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
data_extractor = DataExtractor(header)
data_cleaning = DataCleaning()

# dim_users : Extract and clean user data, then upload to database
user_data_table = 'legacy_users'

if user_data_table in db_connector.list_db_tables():
    raw_user_data_df = DataExtractor.read_rds_table(
        db_connector, user_data_table)
    cleaned_user_data_df = DataCleaning.clean_user_data(raw_user_data_df)
    local_db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')
else:
    print(f"User data table '{user_data_table}' not found")

# dim_card_details : Extract, clean and upload card data from PDF
pdf_link = ('https://data-handling-public.s3.eu-west-1.amazonaws.com/'
            'card_details.pdf')
raw_card_data_df = data_extractor.retrieve_pdf_data(pdf_link)
clean_card_data_df = DataCleaning.clean_card_data(raw_card_data_df)
local_db_connector.upload_to_db(clean_card_data_df, 'dim_card_details')

# dim_store_details : Extract, clean and upload store data from API
number_stores_endpoint = (
    "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
)
store_data_endpoint = (
    "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
)
num_stores = data_extractor.list_number_of_stores(number_stores_endpoint)
store_data = data_extractor.retrieve_stores_data(
    store_data_endpoint, num_stores
)
clean_store_data = data_cleaning.clean_store_data(store_data)
local_db_connector.upload_to_db(clean_store_data, 'dim_store_details')

# dim_products : Extract, clean and upload product data from S3
s3_address = "s3://data-handling-public/products.csv"
raw_product_data_df = DataExtractor.extract_from_s3(s3_address)
clean_product_data_df = DataCleaning.clean_products_data(raw_product_data_df)
local_db_connector.upload_to_db(clean_product_data_df, 'dim_products')

# orders_table : Extract and clean the orders data
orders_data_table = 'orders_table'
raw_orders_data_df = DataExtractor.read_rds_table(
    db_connector, orders_data_table)
clean_orders_data_df = DataCleaning.clean_orders_data(raw_orders_data_df)
local_db_connector.upload_to_db(clean_orders_data_df, 'orders_table')

# dim_date_times : Extract and clean the date times data
json_url = (
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
)
raw_date_times_data_df = data_extractor.extract_from_json_url(json_url)
clean_date_times_data_df = data_cleaning.clean_date_times_data(
    raw_date_times_data_df
)
local_db_connector.upload_to_db(clean_date_times_data_df, 'dim_date_times')

# ---------- SQL file logic ----------

# Read the SQL file
with open('cast_data_types.sql', 'r') as file:
    sql_file_content = file.read()

# Split the SQL file content by ';' and remove empty commands
raw_sql_commands = [
    command.strip() for command in sql_file_content.split(';')
    if command.strip()
]

# Filter out commands that are entirely comments
sql_commands = [
    command for command in raw_sql_commands
    if not all(line.strip().startswith("--") for line in command.split("\n"))
]

# Execute each SQL command individually
with local_db_connector.engine.connect() as connection:
    for sql_command in sql_commands:
        try:
            connection.execute(text(sql_command))
            print(f"Executed SQL command:\n{sql_command}\n" + '-'*50)
        except Exception as e:  # Catch any general exception
            print(f"Error executing SQL command:\n{sql_command}\n" +
                  f"Error message: {e}\n" + '-'*50)
    connection.commit()
