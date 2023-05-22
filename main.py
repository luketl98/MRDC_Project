from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector()

# Create the database engine
engine = db_connector.init_db_engine()

# Pass the engine to the list_db_tables method
table_names = db_connector.list_db_tables(engine)

# Print the list of table names and manually identify the user data table
print("List of tables in the database:")
for table_name in table_names:
    print(table_name)

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

# TASK 4
data_extractor = DataExtractor()
link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
raw_card_data_df = data_extractor.retrieve_pdf_data(link)

data_cleaning = DataCleaning()
clean_card_data_df = data_cleaning.clean_card_data(raw_card_data_df)

db_connector.upload_to_db(clean_card_data_df, 'dim_card_details')
