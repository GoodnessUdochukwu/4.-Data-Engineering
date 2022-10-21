import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
import pytz
import json
import requests
from get_cities_weather import get_cities_weather
from get_airport_info import get_airport_info
from airport_weather_info import airport_weather_info
from airport_arrival import airport_arrival

  
def lambda_handler(event, context):
    schema="gans_data_analytics"
    host="my-project3-database.cn6lacpttysm.us-east-1.rds.amazonaws.com"
    user="admin"
    password="Utubeguti101"
    port=3306
    con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


    current_date = datetime.now(pytz.timezone('Europe/Berlin')).date()
    future_date = current_date + timedelta(days=1)


    try:
        used_city = list(pd.read_sql_table(table_name='used_cities', columns=['city'], schema='gans_data_analytics', con=con).city)
    except:
        citysql = pd.read_sql('SELECT city, pop, ctry_code FROM full_city_list WHERE pop >= 500000 ORDER BY pop DESC', con).sample(5)
        citysql.to_sql('used_cities', con=con, index=False)
        used_city = list(citysql.city)

    try:
        city_weather = pd.read_sql_table(table_name='city_weather', schema='gans_data_analytics', con=con)
        date_time_comparator1 = int(list(city_weather_data1[city_weather_data1['City'] == used_city[0]].DateTime[-1:].str[0:4])[0])   # year
        date_time_comparator2 = int(list(city_weather_data1[city_weather_data1['City'] == used_city[0]].DateTime[-1:].str[5:7])[0])   # month
        date_time_comparator3 = int(list(city_weather_data1[city_weather_data1['City'] == used_city[0]].DateTime[-1:].str[8:10])[0])  # day
    
        if (date_time_comparator1 > future_date.year) & (date_time_comparator2 > future_date.month) & (date_time_comparator3 > future_date.day):
            pass
        else:
            get_cities_weather(used_city)
    except:
        get_cities_weather(used_city)

    try:
        city_airport_info = pd.read_sql_table(table_name='city_airport_info', schema='gans_data_analytics', con=con) 
    except:
        city_airport_info = get_airport_info(used_city)    


    airport_weather_info(city_airport_info)
    
    airport_arrival(city_airport_info, future_date)
    