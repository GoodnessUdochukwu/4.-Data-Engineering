import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
import pytz
import json
import requests

schema="gans_data_analytics"
host="my-project3-database.cn6lacpttysm.us-east-1.rds.amazonaws.com"
user="admin"
password="Utubeguti101"
port=3306
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'


def get_airport_info(cities):
    
    # Search city weather and coordinates
    city_coord_lon = []
    city_coord_lat = []
    
    for i in cities:
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={i}&appid=e7d1480b3d55f1e4ebd431370b378444&units=metric"
        my_request = requests.get(url)
        
        city_coord_lon.append(my_request.json()["city"]["coord"]["lon"])
        city_coord_lat.append(my_request.json()["city"]["coord"]["lat"])
        
        
    # Search cities for all airports and their relevant info e.g. icao codes and their coordinates
    list_airport_details = []
        
    for b in range(len(city_coord_lon)):
        # if (b % 15) == 0:
        #     time.sleep(10)
        url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{city_coord_lat[b]}/{city_coord_lon[b]}/km/20/6"

        querystring = {"withFlightInfoOnly":"true"}

        headers = {
            "X-RapidAPI-Key": "fdf8741e07msh11a8578ab15a6eap107760jsn989518799de6",
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
                  }

        response1 = requests.request("GET", url, headers=headers, params=querystring)
    
        list_airport_details.append(pd.json_normalize(response1.json()['items']))    
        
    City_Airport_list = pd.concat(list_airport_details, ignore_index=True)
    City_Airport_list['airport_id'] = City_Airport_list['location.lat'].astype(str) + City_Airport_list['location.lon'].astype(str)
    
    City_Airport_list.to_sql('city_airport_info', con=con, index=False)
    
    return City_Airport_list