import yaml
from sqlalchemy import create_engine, inspect, text


class DatabaseConnector:
    """
    Class to handle all database related operations.
    """
    def __init__(self, local_creds=None):
        """
        Initialize DatabaseConnector with local database credentials.
        If local_creds is not provided, database credentials will be read from a file.
        """
        if local_creds:
            self.local_creds = local_creds
        else:
            self.local_creds = {
                'user': 'postgres',
                'password': 'Aphrodite-186',
                'host': 'localhost',
                'port': 5432,
                'database': 'sales_data'
            }

    @staticmethod
    def read_db_creds(file_path='db_creds.yaml'):
        """
        Static method to read database credentials from a yaml file.
        """
        with open(file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds

    def init_db_engine(self, use_local=False):
        """
        Initialize the database engine using either local or remote credentials.
        """
        if use_local:
            db_creds = self.local_creds
        else:
            db_creds = self.read_db_creds()

        db_url = f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        return create_engine(db_url)


    @staticmethod
    def list_db_tables(engine):
        """
        Static method to list all public tables in the database.
        """
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        with engine.connect() as connection:
            result = connection.execute(text(query))
            table_names = [row[0] for row in result.fetchall()]
        return table_names

    def upload_to_db(self, df, table_name):
        """
        Upload a DataFrame to the specified table in the database.
        """
        connection_string = f"postgresql://{self.local_creds['user']}:{self.local_creds['password']}@{self.local_creds['host']}:{self.local_creds['port']}/{self.local_creds['database']}"
        engine = create_engine(connection_string)

        with engine.begin() as conn:
            conn.execute(text("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE"))
            df.to_sql(table_name, conn, if_exists='replace', index=False)


    @staticmethod
    def get_table_schema(engine, table_name):
        """
        Static method to get the schema of a specified table in the database.
        """
        inspector = inspect(engine)
        return inspector.get_columns(table_name)

