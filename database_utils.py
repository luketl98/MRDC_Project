from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.sql import text
import yaml

class DatabaseConnector:
    @staticmethod
    def read_db_creds(file_path='db_creds.yaml'):
        with open(file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self, use_local=False):
        if use_local:
            db_user = 'postgres'
            db_password = 'Aphrodite-186'
            db_host = 'localhost'
            db_port = 5432
            db_database = 'sales_data'
        else:
            creds = self.read_db_creds()
            db_user = creds['RDS_USER']
            db_password = creds['RDS_PASSWORD']
            db_host = creds['RDS_HOST']
            db_port = creds['RDS_PORT']
            db_database = creds['RDS_DATABASE']

        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self, engine):
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        with engine.connect() as connection:
            result = connection.execute(text(query))
            table_names = [row[0] for row in result.fetchall()]
        return table_names
    
    def upload_to_db(self, df, table_name):

        # Define the connection string
        connection_string = f"postgresql://{'postgres'}:{'Aphrodite-186'}@{'localhost'}:{5432}/{'sales_data'}"

        # Connect to the database
        engine = create_engine(connection_string)

        # Set the transaction read-only setting to off
        with engine.begin() as conn:
            conn.execute(text("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE"))

            # Upload the DataFrame to the database
            df.to_sql(table_name, conn, if_exists='replace', index=False)

    def get_table_schema(self, engine, table_name):
        inspector = inspect(engine)
        return inspector.get_columns(table_name)
