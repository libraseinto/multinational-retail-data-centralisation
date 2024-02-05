from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from sqlalchemy import create_engine, inspect
import psycopg2
import yaml


class DatabaseConnector:

    def __init__(self):
        pass

    def read_db_creds(self):
        '''
        This method takes a yaml file with the credentials to access the AWS server.

        The purpose of this method is to create a dictionary from the yaml file
        containing the credentials for the AWS database so then they can be used to
        access the AiCore AWS server.
        '''
        with open('db_creds.yaml', 'r') as stream:
            d = yaml.safe_load(stream)
        return d
    
    def read_progres_cred(self):
        '''
        This method takes a yaml file with the credentials to access my local postgreSQL server.

        The purpose of this method is to create a dictionary from the yaml file
        containing the credentials for my local postgreSQL server so then they can be used to
        access it and upload the databases to it.
        '''
        with open('postgres_creds.yaml', 'r') as stream:
            d = yaml.safe_load(stream)
        return d

    def init_db_engine(self):
        '''
        This method creates the engine needed to retrieve data from the AiCore AWS server.

        The purpose of this method is to read the credentials from the yaml file to initialise the
        SQLAlchemy Engine that will allow us to connect to our remote PostgreSQL instance on Amazon Web Services.
        '''
        credentials = self.read_db_creds()
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, init_db_engine):
        '''
        This method uses the engine to connect to the database and retrieve a list of the names of the tables.

        The purpose of this method is to connect to the database using the engine from the method init_db_engine
        to retrieve a list of the names of the tables in the database so the user can pick later which table
        want to retrieve.
        '''
        inspector=inspect(init_db_engine())
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name):
        '''
        This method is used to upload the selected table to postgreSQL in my local computer.

        The purpose of this method is to connect to the postgreSQL in my local computer and upload
        the selected table to it.
        '''
        credentials = self.read_progres_cred()
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOST']}:{credentials['PORT']}/{credentials['DATABASE']}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)


data_connection = DatabaseConnector()
user_data_extractor = DataExtractor()
data_cleaner = DataCleaning()

user_data_df = user_data_extractor.read_rds_table(data_connection.init_db_engine(), data_connection.list_db_tables(data_connection.init_db_engine)[1])
df_to_upload = data_cleaner.clean_user_data(user_data_df)
data_connection.upload_to_db(df_to_upload, 'dim_users')

card_info_df = user_data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
card_info_df_clean = data_cleaner.clean_card_data(card_info_df)
data_connection.upload_to_db(card_info_df_clean, 'dim_card_details')


url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
headers = {
    "Content-Type": "application/json",
    "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }
stores_df = user_data_extractor.retrieve_stores_data(url, headers)
stores_df_clean = data_cleaner.clean_store_data(stores_df)
data_connection.upload_to_db(stores_df_clean, 'dim_store_details')

products_df = user_data_extractor.extract_from_s3()
products_df_clean = data_cleaner.clean_products_data(products_df)
data_connection.upload_to_db(products_df_clean, 'dim_products')

orders_df = user_data_extractor.read_rds_table(data_connection.init_db_engine(), data_connection.list_db_tables(data_connection.init_db_engine)[2])
orders_df_clean = data_cleaner.clean_orders_data(orders_df)
data_connection.upload_to_db(orders_df_clean, 'orders_table')

events_df = user_data_extractor.extract_from_s3_json()
events_df_clean = data_cleaner.clean_events_data(events_df)
data_connection.upload_to_db(events_df_clean, 'dim_date_times')









        



