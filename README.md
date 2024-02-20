# Multinational Retail Data Centralisation

## Table of Contents
- [Description](#description)
- [Usage](#usage)
- [Configuration](#configuration)
- [File Structure](#file-structure)
- [Contact](#contact)
- [License](#license)

## Description
This project aims to provide an easy way for multinational retail companies to centralise their data from various sources into one database system that can be accessed by all the members of the company regardless.
The data is extracted from different sources like Amazon S3 bucket, PDF file, .csv file and json files.
After extracted, the data is cleaned to eliminate duplicates and null data and exported to SQL so people can access it and query it.

## Usage
To use this program you just need to run in your console the following:
`python database_utils.py `
This will extract the data sets, clean them, and upload the tables into SQL.

## File Structure
The project has 3 main files with specific functions. They are the following:
1. `data_extraction.py`: 
- Automates the process of data extraction from various sources, including databases, PDF files, APIs, and S3 buckets, and convert the extracted data into a format suitable for analysis or further processing.
- By encapsulating the data extraction logic into separate methods, the code promotes modularity and reusability, making it easier to maintain and extend in the future.
2. `data_cleaning.py`:
- Provides a set of tools for data cleaning and preprocessing, essential for preparing data for analysis.
- By encapsulating the data cleaning logic into separate methods, the code promotes modularity and reusability, making it easier to maintain and extend in the future.
3. `database_utils.py`:
- Extract various types of data (user data, card details, store details, product details, order details, event details) from different sources (AWS PostgreSQL server, PDF, API endpoint, S3 bucket) using the DataExtractor class.
- Clean the extracted data using the DataCleaning class.
- Upload the cleaned data to a local PostgreSQL database using the DatabaseConnector class.

## Contact
The GitHub profile for the person in charge on maintaining this code is `https://github.com/libraseinto`

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

This software is provided "as is", without warranty of any kind.







