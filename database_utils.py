import yaml
from yaml.loader import SafeLoader
    
class DatabaseConnector:
    def read_db_creds():
        with open('db_creds.yaml', 'r') as file:
            data = yaml.load(file, Loader=yaml.SafeLoader)
            print(data)
            print(type(data))


dbcon = DatabaseConnector
dbcon.read_db_creds()