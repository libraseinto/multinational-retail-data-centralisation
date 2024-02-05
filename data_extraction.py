import boto3
import pandas as pd
import requests
import tabula


class DataExtractor:

    def __init__(self):
        pass

    def read_rds_table(self, engine, table_name):
        '''
        This method will extract a database using the name of the table into a pandas data frame.

        It uses the engine as an instance of the DatabaseConnector class to get the database from AWS server
        and the table name you want to extract as an instance of the DatabaseConnector class indicating the
        position of the table we want to extract in the list of tables.
        '''
        query = f"SELECT * FROM {table_name}"
        user_df = pd.read_sql(query, con=engine)
        return user_df
    
    def retrieve_pdf_data(self, link):
        '''
        This method extract a tabular data from a PDF file, combine it a put it into a pandas data frame.

        This method access a PDF file in a specific link, export all pages in the file and then concat them all
        into one pandas data frame and returns the data frame.
        '''
        df = tabula.read_pdf(link, pages='all')
        df_combined = pd.concat(df)
        return df_combined
    
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }
    
    def list_number_of_stores(self, number_stores_url, headers):
        '''
        This method takes an url address containing the data for all the stores and the headers and returns the number of stores in
        a json file.

        This method finds out the number of stores in the database as the info for each store is stored in a separate url.
        By finding out the number of stores you can then access the data for each store making sure you are not missing any.
        This method returns a json file with the number of each store.
        '''
        response = requests.get(number_stores_url, headers=headers)
        number_of_stores = response.json()
        return number_of_stores
    
    def retrieve_stores_data(self, retrieve_store_url, headers):
        '''
        This method obtains the data for each store and adds then to a data frame that then is returned at the end of the loop.
        
        This method uses the base url address containing the data for all the stores and uses a for loop to
        access the URL for each stores from Store 0 to Store 450. Then stores that data into a json file that then
        is appended to a dataframe which is finally concatenated and the function returns the final dataframe with
        the data for all the stores.
        '''
        dataframes = []
        for store in range(0, 451):
            url = f'{retrieve_store_url}{store}'
            response = requests.get(url, headers=headers)
            data = response.json()
            df = pd.DataFrame(data, index=[store])
            dataframes.append(df)
        final_df = pd.concat(dataframes, ignore_index=True)
        return final_df
    
    def extract_from_s3(self):
        '''
        This method extracts from an Amazon S3 bucket a CSV file and stores it in my local computer
        in the specified address
        '''
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'products.csv', 'C:/Users/alexa/VSCode Lessons and practice/retail_project/multinational-retail-data-centralisation/products.csv')
        products_df = pd.read_csv('products.csv')
        return products_df
    
    def extract_from_s3_json(self):
        '''
        This method extracts a JSON file with the data from a S3 bucket and converts it into a pandas dataframe.
        '''
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'date_details.json', 'C:/Users/alexa/VSCode Lessons and practice/retail_project/multinational-retail-data-centralisation/date_details.json')
        df = pd.read_json('date_details.json')
        return df

