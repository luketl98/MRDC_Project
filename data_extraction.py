import pandas as pd

class DataExtractor:
    @staticmethod
    def read_rds_table(database_connector, table_name):
        engine = database_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df
