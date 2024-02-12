import pandas as pd
import numpy as np
import re

class DataCleaning:
    pass

    def __init__(self):
        pass

    # def check_phone(self, phone):
    #     '''
    #     This method takes a phone number and standardize it.

    #     The purpose of this method is to take a phone number as input in the table
    #     and check that it is formatted in the correct way and does not contain special characters.
    #     It modifies them to make them the same format and length. First it checks if the number has '00' in front of it, 
    #     if not, it adds it. Also, it checks for the letter 'x', and finally checks for special characters like spaces, dashes, 
    #     parenthesis, or signs and eliminates them.
    #     '''
    #     if phone[0] == '0' and phone[1] != '0':
    #         phone = '0' + phone
    #     elif phone[0] != '0' and phone[1] != '0':
    #         phone = '00' + phone
    #     if 'x' in phone:
    #         phone = ''
    #     new_phone = phone.replace(" ","").replace("-","").replace("(0)","").replace(".","").replace("+","").replace("(","").replace(")","")
    #     return new_phone
    
    def country_code(self, string):
        '''
        This method cleans the 'country_code' column and converts into np.nan any record that is not a country code.
        '''
        if string == 'GGB':
            return 'GB'
        elif len(str(string)) > 2:
            return np.nan
        else:
            return string
    
    
    def clean_user_data(self, df):
        '''
        This method takes a data frame and applies the function "check_phone" to the entire series of numbers, it also
        checks the length, convert to NA values the corrupted and deletes them from the data frame.

        The purpose of this method is to take a phone number as input in the table
        and check that it is formatted in the correct way and does not contain special characters.
        It modifies them to make them the same format and length. First it checks if the number has '00' in front of it, 
        if not, it adds it. Also, it checks for the letter 'x', and finally checks for special characters like spaces, dashes, 
        parenthesis, or signs and eliminates them.
        '''  
        df['country_code'] = df['country_code'].apply(self.country_code)
        df['date_of_birth'] = df['date_of_birth'].apply(pd.to_datetime,infer_datetime_format=True, errors='coerce')
        df['join_date'] = df['join_date'].apply(pd.to_datetime,infer_datetime_format=True, errors='coerce')
        df_non_null = df.dropna()
        return df_non_null
    
    def clean_card_data(self, df):
        '''
        This method takes a data frame and convert to date and standardize the date values in the df to
        then create the table dim_card_details.

        The purpose of this method is to take dates and convert them into proper pandas datetime format.
        The expiry date of the card will be of the format: month/year.
        The payment date will be of the format: year-month-day.
        All non numerical characters in the card_details column will be deleted, leaving only numbers in the column.
        Records that does not comply with the formats above will be converted into NAs using the coerce keyword.
        Finally it will drop the NAs from the data frame.
        '''   
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce')
        df['card_number'] = df['card_number'].replace(regex=r'[^0-9]', value='')
        clean_df = df.dropna()
        return clean_df
    
    
    def clean_continent(self, continent):
        '''
        This method is used to standardize the continent series so they can be categorized later on.

        This method eliminates the 'ee' in front of the continent data so the data can be divided into two categories:
        'America' and 'Europe'.
        '''
        if continent == 'eeAmerica':
            continent = 'America'
            return continent
        elif continent == 'eeEurope':
            continent = 'Europe'
            return continent
        return continent


    def clean_web_shop(self, string):
        '''
        This function handles the Web Shop that has no address nor coordinates by converting the string 'N/A' into  the string 0.
        '''
        if string == 'N/A':
            string = 0
        return string


    def clean_store_data(self, df):
        '''
        This method is used to clean the store's data.

        This method uses previous defined functions and applies it to the whole relevant column to standardize the data
        and convert it in the relevant data type. 
        It performs the following actions:
         1. Column 'opening_date': converts the column into a datetime data type
         2. Column 'staff_numbers': eliminates the alphabetic characters, applies the clean_staff function and converts the
           'staff_numbers'column into integer data type.
         3. Column 'longitude' and 'latitude': converts them into floating data type
         4. Column 'store_type': converts the column into category data type
         5. Column 'country_code': converts the column into category data type
         6. Column 'continent': applies the 'clean_continent' and converts the column into category data type
        '''

        df = df.drop(63)
        df = df.drop(447)
        df['address'] = df['address'].apply(self.clean_web_shop)
        df['longitude'] = df['longitude'].apply(self.clean_web_shop)
        df['latitude'] = df['latitude'].fillna(0)
        df['locality'] = df['locality'].apply(self.clean_web_shop)
        df['opening_date'] = df['opening_date'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce')
        df = df.drop('lat', axis=1)
        df['staff_numbers'] = df['staff_numbers'].str.replace(r'[A-Za-z]', '', regex=True)
        df = df.dropna()
        df['staff_numbers'] = df['staff_numbers'].astype(int)
        df['longitude'] = df['longitude'].astype(float)
        df['latitude'] = df['latitude'].astype(float)
        df['store_type'] = df['store_type'].astype('category')
        df['country_code'] = df['country_code'].astype('category')
        df['continent'] = df['continent'].apply(self.clean_continent)
        df['continent'] = df['continent'].astype('category')
        return df

    def convert_product_weights(self, input_string):
        '''
        Checks if the input contains 'kg', 'g', or 'ml' and converts it to kg.

        This method checks if the input contains the string 'kg', 'g' or 'ml', if it does it
        removes it and convert the input into a float in kg. any other exception is either converted into NaN
        or passed.
        It also handles an exception for 1 record (12 x 100g) by converting it to 12.
        '''
        if input_string == '12 x 100g':
            return 12
        try:
            if 'kg' in input_string:
                input_string = input_string.replace('kg', '')
                weight_in_kg = float(input_string)
                return weight_in_kg
            elif 'g' in input_string:
                input_string = input_string.replace('g', '')
                weight = float(input_string)
                weight_in_kg = round(weight / 1000, 2)
                return weight_in_kg
            elif 'ml' in input_string:
                input_string = input_string.replace('ml', '')
                weight = float(input_string)
                weight_in_kg = round(weight / 1000, 2)
                return weight_in_kg
            else:
                return input_string
        except:
            return input_string


    def clean_price(self, input_price):
        '''
        Checks if the input contains '£'

        This method checks if the record in the price column contains the
        '£' symbol and removes it and the converts the record into a float. If it can't
        do it then it pass to the next record.
        '''

        try:
            if '£' in input_price:
                input_price = input_price.replace('£', '')
                input_price = float(input_price)
                return input_price
            else:
                return np.nan
        except:
            pass

    def clean_removed(self, string):
        '''
        Checks if the input is equal to 'Removed' or 'Still_avaliable' and if it is not converts the input into NaN.

        This method checks if the input is equal to 'Removed' or 'Still_avaliable', if not, it converts it to a NaN and if it
        is not a string or there is any other exception it passes.
        '''
        try:
            if string == 'Removed' or string == 'Still_avaliable':
                return string
            else:
                string = np.nan
                return string
        except:
            pass

    def clean_category(self, string):
        '''
        Checks for strings that are not product categories and converts them into a NaN.
          
         This method Checks for strings that are not product categories such as 'diy', 'food-and-drink',
        'toys-and-games', 'health-and-beauty', 'homeware', 'pets' or, 'sports-and-leisure', and if they are not it
        converts the input into a NaN. Any other exception is passed.
        '''
        try:
            if string == 'diy' or string == 'food-and-drink' or string == 'toys-and-games' or string == 'health-and-beauty' or string == 'homeware' or string == 'pets' or string == 'sports-and-leisure':
                return string
            else:
                string = np.nan
                return string
        except:
            pass

    def clear_weight_multiplier(self, input_string):
        '''
        Function to handle weight data in the form of '2 x 200g'.

        This function gets rid of the 'g' at the end of the string by using the
        replace function with an empty space, and then splits the string using the 'x'
        as the separator. Then it multiplies the first number by the second number
        and divides the result by 100 to get the weight in kg.
        '''
        try:
            clean_string = input_string.replace('g','').split('x')
            result = int(clean_string[0])*int(clean_string[1])/100
            return result
        except:
            return(input_string)

    def remove_non_numerals(self, input_string):
        '''
        This function is used to remove non numerical characters from record in the weight_kg column.
        '''
        try:
            return re.sub(r'\D', '', input_string)
        except:
            return input_string

    def clean_products_data(self, df):
        '''
        This method cleans up the data by removing all rows with missing values from the dataframe, changes
        the name of some columns to improve readability, and converts some columns into the more efficient data types.
        
        This method perform the final clean of the data frame to be ready to be uploaded to the SQL server.
        It performs the following actions:
            1. Gets rid of the first column as it was a duplication of the index column
            2. Applies the 'convert_product_weights' method to the weight column
            3. Applies the 'clean_price' method to the product_price column
            4. Converts to datetime format the data in the 'date_added' column
            5. Renames both columns 'product_price' and 'weight' to 'product_price_£' and 'weight_kg', respectively to
               add the unit of measure of the data in each column
            6. Applies the 'clean_removed' method to the 'removed' column
            7. Applies the 'clean_category' method to the 'category' column
            8. Converts both columns 'category' and 'removed; to category data types
            9. Eliminates all the NaN records in the dataframe
        '''

        df = df.drop('Unnamed: 0', axis=1)
        df['weight'] = df['weight'].apply(self.clear_weight_multiplier)
        df['weight'] = df['weight'].apply(self.convert_product_weights)
        df['weight'] = df['weight'].apply(self.remove_non_numerals)
        df['product_price'] = df['product_price'].apply(self.clean_price)
        df['date_added'] = df['date_added'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce')
        df.rename(columns={'product_price': 'product_price_£'}, inplace=True)
        df.rename(columns={'weight': 'weight_kg'}, inplace=True)
        df['removed'] = df['removed'].apply(self.clean_removed)
        df['category'] = df['category'].apply(self.clean_category)
        df['category'] = df['category'].astype('category')
        df['removed'] = df['removed'].astype('category')
        df = df.dropna()
        return df

    def clean_orders_data(self, df):
        '''
        This method cleans the orders dataframe by removing the columns 'first_name', 'last_name' and '1'
        '''
        df = df.drop(columns=['first_name','last_name','1','level_0','index']).reindex()

        # df = df.drop('first_name', axis=1)
        # df = df.drop('last_name', axis=1)
        # df = df.drop('1', axis=1)
        return df
    
    def clean_dates(self, number):
        '''
        This method cleans the dates column of the data frame by converting them into integers. If the value can't be converted into
        an integer it will be converted as NaN
        '''
        try:
            number = int(number)
            return number
        except:
            number = np.nan
            pass
    

    def clean_time_period(self, string):
        '''
        This method converts into NaN any value that is not a valid string ('Evening', 'Late_Hours', 'Midday', 'Morning')
        in the 'time_period' column and converts it into a category data type
        '''
        try:
            if string == 'Evening' or string == 'Late_Hours' or string == 'Midday' or string == 'Morning':
                return string
            else:
                string = np.nan
                return string
        except:
            pass


    def clean_events_data(self, df):
        '''
        This method cleans the Events data frame by converting into integer the numeric data, datetime the timestamps and getting rid 
        of inaccurate data.

        This method cleans the events data frame by performing the following operations:
        1. Converts into integer the values for day, month and year and converts into NaN any values that can't be converted to integers
        2. Converts into datetime format the 'timestamp' column and any value that is not in the format '%H:%M:%S' is converted into NaN
        3. Converts into NaN any value that is not a valid string ('Evening', 'Late_Hours', 'Midday', 'Morning') in the 'time_period' column
        and converts it into a category data type
        4. Eliminates all the NaN values from the data frame
        '''

        df['month'] = df['month'].apply(self.clean_dates)
        df['day'] = df['day'].apply(self.clean_dates)
        df['year'] = df['year'].apply(self.clean_dates)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce').dt.time
        df['time_period'] = df['time_period'].apply(self.clean_time_period)
        df['time_period'] = df['time_period'].astype('category')
        df = df.dropna()
        return df








        

