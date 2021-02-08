import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import time
import schedule
import os

"""
Initiate all the helper/processing functions 
-----------------------------------------------------------------------------------------
"""
def extract_wind_gust_speed(input_string: str):
    """
    Determines the wind speed in kmh when given the weird BOM wind speed string
    which has both kmh and knots as one long number. This has some very basic assumptions:
    1. format is always kmh then knots. 2. Knots will never exceed 99.
    """
    if len(input_string) > 3:
        wind_speed_kmh = input_string[:-2]
        
    if len(input_string) == 3:
        wind_speed_kmh = input_string[:-1]
    
    if len(input_string) == 2:
        wind_speed_kmh = input_string[0]
    
    return wind_speed_kmh

def did_it_rain_today(rainfall):
    """
    Returns a 1 or 0 based on the rainfall amount. Returns integer instead of 
    boolean to save a pipeline step for ML model
    """
    if float(rainfall) > 0:
        print(f'Rain amount: {rainfall}')
        rain = 1
    else:
        rain = 0
    return rain

def convert_wind_dir(wind_dir_string: str):
    """
    Returns wind direction in degrees (converts from string direction)
    """
    wind_conv_dict = {'E':0, 'ENE':67.5, 'ESE':112.5, 
                                      'N':90, 'NE':45, 'NNE':22.5, 
                                      'NNW':337.5, 'NW':315, 
                                      'S':180, 'SE':135, 'SSE':157.5, 
                                      'SSW':202.5, 'SW':225, 
                                      'W':270, 'WNW':292.5, 'WSW':247.5}
    return wind_conv_dict[wind_dir_string]

def get_latest_id():
    """
    Returns the highest value id in the table
    """
    connection = psycopg2.connect(os.environ['BOM_DATA_POSTGRES_URI'])
    cursor = connection.cursor()
    cursor.execute("""SELECT id from weatherData ORDER BY id DESC LIMIT 1 """)
    max_id = cursor.fetchone()
    if max_id:
        max_id = max_id[0]
    else:
        max_id = 0

    connection.close()  

    return max_id

def submit_db_data(data):
    """
    Utility to insert the data into the database
    """
    connection = psycopg2.connect(os.environ['BOM_DATA_POSTGRES_URI'])
    cursor = connection.cursor()
    for x in data:
        print(x)
        print(type(x))

    insert_query = """ INSERT INTO weatherData VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
    cursor.execute(insert_query, data)
    connection.commit()
    connection.close()

"""
The main chunk of operations 
------------------------------------------------------------------------------------
"""

def scrape_data(location:str):
    """
    Acts as the main data scraper routine for the script. Will pull down the observations 
    page off BOM for the input location then parse the HTML table for the obs that feed 
    into the ML model for predicting rain
    """

    # This silly dictionary is needed to make sure the right URL is called to access the right BOM page
    location_state = {'hobart': 'tas', 'melbourne': 'vic', 'sydney': 'nsw', 'brisbane': 'qld', 'adelaide': 'sa', 'perth': 'wa'}
    location_url_suffix = {'hobart': 'hobart', 'melbourne': 'melbourne-(olympic-park)', 'sydney': 'sydney---observatory-hill',
                            'brisbane': 'brisbane', 'adelaide': 'adelaide-(west-terrace----ngayirdapira)', 'perth': 'perth'}

    location_url = f'http://www.bom.gov.au/places/{location_state[location]}/{location}/observations/{location_url_suffix[location]}/'
    print(location_url)
    
    # Request URL
    r = requests.get(location_url)

    # Feed into Pandas to get dataframe from HTML tables
    table_df_list = pd.read_html(r.text, attrs = {'class': 'obs'})
    print(table_df_list)
    # There is multiple tables on the page (for the past days)
    for table in table_df_list: # We will only look at the first asthat is todays obs
        print(table)
        # Get the row that we are looking for (aka 3pm observations)
        three_pm_row = table.loc[table['Time (AEDT)'] == '3:00 pm']
   
        if len(three_pm_row) > 0:
            wind_gust_speed = extract_wind_gust_speed(str(three_pm_row['Wind Speed (km/h) (knots)'].iloc[0]))
            humidity_threepm = three_pm_row['Humidity(%)'].iloc[0]
            pressure_threepm = three_pm_row['Pressure (hPa)'].iloc[0]
            temp_threepm = three_pm_row['Temp (°C)'].iloc[0]
            rain_today = did_it_rain_today(three_pm_row['Rainfall since 9 am (mm)'].iloc[0])
            number_wind_dir_threepm = convert_wind_dir(three_pm_row['Wind Direction'].iloc[0])
            
            latest_id = get_latest_id() + 1
            today = time.strftime("%Y-%m-%d", time.localtime())

            combined_tuple = (latest_id, location, today, float(wind_gust_speed), float(humidity_threepm), float(pressure_threepm), float(temp_threepm), 
                            rain_today, number_wind_dir_threepm)
            
            submit_db_data(combined_tuple)
        break

"""
This is the main routine loop
"""
def scraper_routine():
    print(f"Scraping routine started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    locations = ['hobart', 'melbourne', 'sydney']

    # Loop through and scrape the data for each...
    for location in locations:
        scrape_data(location)

# And we use schedule to make this happen at 3:15pm everyday
schedule.every().day.at("15:25").do(scraper_routine)

# keep this alive so that shcedule can do its magic
while True:
    # run_pending
    print("I'm waiting for the right time 😊")
    schedule.run_pending()
    time.sleep(10)
    