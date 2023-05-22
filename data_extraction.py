import pandas as pd
import tabula

class DataExtractor:
    @staticmethod
    def read_rds_table(database_connector, table_name):
        engine = database_connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df

    def retrieve_pdf_data(self, link):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        df_list = tabula.read_pdf(link, pages='all')
        df = pd.concat(df_list, ignore_index=True)
        return df