from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

db_connector = DatabaseConnector()
table_names = db_connector.list_db_tables()

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
