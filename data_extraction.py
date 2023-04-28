# Add your necessary imports at the top of the file
from sqlalchemy import Table, Column, Integer, String, Date, MetaData, create_engine
import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from sqlalchemy import text
from sqlalchemy import create_engine, inspect



def create_dim_users_table_if_not_exists(db_conn, schema):
    metadata = MetaData(schema=schema)
    
    dim_users_table = Table(
        'dim_users', metadata,
        Column('id', Integer, primary_key=True),
        Column('first_name', String),
        Column('last_name', String),
        Column('date_of_birth', Date),
        Column('company', String),
        Column('email_address', String),
        Column('address', String),
        Column('country', String),
        Column('country_code', String),
        Column('phone_number', String),
        Column('join_date', Date),
        Column('user_uuid', String)
    )
    
    if not db_conn.engine.dialect.has_table(db_conn.engine, 'dim_users', schema=schema):
        dim_users_table.create(db_conn.engine)
        print(f"Created table 'dim_users' in schema '{schema}'.")
    else:
        print(f"Table 'dim_users' already exists in schema '{schema}'.")

def check_table_exists(db_conn, schema, table_name):
    query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name = :table_name
        );
    """)

    with db_conn.engine.connect() as connection:
        result = connection.execute(query, {'schema': schema, 'table_name': table_name}).scalar()

    return result





class DataExtractor:
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
        
    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=self.db_conn.engine)
        return df
        

if __name__ == '__main__':
    db_conn = DatabaseConnector()

    # List all tables in the database
    tables = db_conn.list_db_tables()
    print(f"Available tables in the database: {tables}")

    # Extract the legacy_users table
    data_extractor = DataExtractor(db_conn)
    print(f"Connecting to: {data_extractor.db_conn.engine.url}")

# Add the following code snippet here
    table_name = "legacy_users"
    schema = "public"
    if check_table_exists(db_conn, schema, table_name):
        print(f"The '{table_name}' table exists in the '{schema}' schema.")
    else:
        print(f"The '{table_name}' table does not exist in the '{schema}' schema.")
        exit(1)

    table_name = "legacy_users"
    user_df = data_extractor.read_rds_table(table_name)
    print("Extracted legacy_users table")  # Add this line

    # Clean the user data
    cleaned_user_df = DataCleaning.clean_user_data(user_df)
    print("Cleaned user data")  # Add this line

    # Upload the cleaned data to a new table named dim_users in the sales_data database
    new_table_name = "dim_users"
    db_conn.upload_to_db(cleaned_user_df, new_table_name)

    # Read the dim_users table to check if the cleaned data has been uploaded successfully
    dim_users_df = data_extractor.read_rds_table(new_table_name)
    print(dim_users_df.head())
