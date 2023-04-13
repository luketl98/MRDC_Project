import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine
    
class DatabaseConnector:

    def read_db_creds():
        """read the YAML file and return 
        the database credentials as a dictionary"""

        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
        
        return db_creds

    def init_db_engine():

        db_creds = db_con.read_db_creds()
        db_url = f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = create_engine(db_url)

        return engine

    # def list_db_tables():
        
"""                     Now create a method init_db_engine which will read the 
                        credentials from the return of read_db_creds and 
                        initialise and return an sqlalchemy database engine."""


db_con = DatabaseConnector
db_con.init_db_engine()

