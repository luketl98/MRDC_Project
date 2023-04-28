import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy import exc

class DatabaseConnector:

    def __init__(self):
        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()
        
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            db_creds = yaml.safe_load(f)
        return db_creds

    def init_db_engine(self):
        engine = create_engine(f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        try:
            # Convert DataFrame to a list of tuples
            data_tuples = [tuple(row) for row in df.to_records(index=False)]

            print(data_tuples[:5])  # Print the first 5 tuples

            # Create an INSERT query
            insert_query = f"""
                INSERT INTO {table_name} (first_name, last_name, date_of_birth, company, email_address,
                                        address, country, country_code, phone_number, join_date, user_uuid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            # Execute the INSERT query for each tuple
            with self.engine.begin() as connection:
                connection.execute(insert_query, data_tuples)

            print(f"Data successfully uploaded to the '{table_name}' table.")
        except exc.SQLAlchemyError as e:
            print(f"Error uploading data to '{table_name}': {e}")
