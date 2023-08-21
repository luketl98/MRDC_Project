import yaml
from sqlalchemy import create_engine, inspect, text


class DatabaseConnector:
    """
    Class to handle all database related operations.
    """
    def __init__(self, credentials):
        """
        Initialize DatabaseConnector with local database credentials.
        If local_creds is not provided, database credentials will be read from a file.
        """
        read_db_creds = self.read_db_creds(credentials)
        
        self.engine = self.init_db_engine(read_db_creds)

    @staticmethod
    def read_db_creds(file_path='db_creds.yaml'):
        """
        Static method to read database credentials from a yaml file.
        """
        with open(file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds

    def init_db_engine(self, credentials):
        """
        Initialize the database engine using either local or remote credentials.
        """
        print(credentials['DB_HOST'])
        db_url = f"postgresql://\
            {credentials['DB_USER']}:\
            {credentials['DB_PASSWORD']}@\
            {credentials['DB_HOST']}:\
            {credentials['DB_PORT']}/\
            {credentials['DB_DATABASE']}"

        return create_engine(db_url)


    def list_db_tables(self):
        """
        Static method to list all public tables in the database.
        """
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            table_names = [row[0] for row in result.fetchall()]
        return table_names

    def upload_to_db(self, df, table_name):
        """
        Upload a DataFrame to the specified table in the database.
        """

        with self.engine.connect() as conn:
            # conn.execute(text("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE"))
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.commit()


    @staticmethod
    def get_table_schema(self, table_name):
        """
        Static method to get the schema of a specified table in the database.
        """
        inspector = inspect(self.engine)
        return inspector.get_columns(table_name)

