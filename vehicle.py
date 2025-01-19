#%% step 1: fetch data from API
# import the requests library for handling HTTP requests

import requests

# define the API endpoint URL for the vehicle database scraping
url = "https://vehicle-database.p.rapidapi.com/vehicle-warranties/check-models"

# querystring contains the parameters required for the API request
querystring = {"year":"2020","make":"Toyota","data":"warranty"}

# Headers include the API key and host for authentication and identification
headers = {
	"x-rapidapi-key": "531193f132msh71720d21c9c014ap118541jsn0984870f4075",
	"x-rapidapi-host": "vehicle-database.p.rapidapi.com"
}

# Make a POST request to the API with the headers and querystring
response = requests.get(url, headers=headers, params=querystring)

# Print the raw JSON response to inspect the data structure
print(response.json())

###################################################################################
#%%
print(json_data)

###################################################################################

#%% step 2: transform the data into a pandas dataframe
# import the pandas library for data manipulation

import pandas as pd

# Parse the JSON data from the API response
json_data = response.json()

try:
    # Check if the 'data' key exists in the JSON and contains 'models'
    if 'data' in json_data and 'models' in json_data['data']:
        # Extract 'make', 'models', and 'year' from the JSON
        make = json_data['data']['make']
        models = json_data['data']['models']
        year = json_data['data']['year']

        # Create a DataFrame from the models list
        df = pd.DataFrame(models, columns=['ModelName'])
        
        # Add 'make' and 'year' columns
        df['Make'] = make
        df['Year'] = year

        # Display the transformed DataFrame
        print("Transformed DataFrame:\n", df)

        # Save the DataFrame to a CSV file (optional)
        df.to_csv('vehicle_models.csv', index=False)
        print('Data saved to "vehicle_models.csv".')

    else:
        print("Unexpected JSON structure: 'data' or 'models' not found.")
except Exception as e:
    print(f"Error transforming data: {e}")


#%% Step 3: Load Data into a Database

# Import necessary libraries for database operations
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Define the database connection string for connecting to SQL Server
DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ARMSTRONG;"
    "Database=vehicleDB;"
    "Trusted_Connection=yes;"
)

# Set up logging to track script activity
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Add console logging for real-time feedback
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Log the start of the script
logging.info("Script started.")

def upload_data(table, dataframe, upload_type):
    """
    Upload data to a specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        # Create an SQLAlchemy engine for database connection
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        # Upload the DataFrame to the database table
        logging.info(f"Uploading data to table: {table}")
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        logging.info(f"Data uploaded successfully to {table}.")
    except Exception as e:
        # Log any errors that occur during the upload process
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")


# Specify the table name and upload type
table_name = "vehicle_table"
upload_type = "append"  # Options: 'replace', 'append'

# Upload the transformed data to the database
try:
    upload_data(table_name, df, upload_type)
    logging.info("Data uploaded successfully.")
    print("Data uploaded successfully.")
except Exception as e:
    logging.error(f"Failed to upload data: {e}")
    print(f"Failed to upload data: {e}")

# Log the end of the script
logging.info("Script ended.")


# %%
