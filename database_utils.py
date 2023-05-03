from sqlalchemy import create_engine
from sqlalchemy import inspect
import yaml

class DatabaseConnector:
    @staticmethod
    def read_db_creds(file_path='db_creds.yaml'):
        with open(file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    @staticmethod
    def init_db_engine():
        db_creds = DatabaseConnector.read_db_creds()
        db_url = f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    @staticmethod
    def list_db_tables():
        engine = DatabaseConnector.init_db_engine()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)