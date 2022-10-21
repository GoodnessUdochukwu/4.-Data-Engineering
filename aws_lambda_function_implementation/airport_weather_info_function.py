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


def airport_weather_info(cities_airports):
# Obtain the coordinates of the airport to find the weather for each airport
    Airport_lon = list(cities_airports['location.lon'])
    Airport_lat = list(cities_airports['location.lat'])
    
    Airport_weather_dict = {
                            'airport_id': [],
                            'City.loc' : [],
                            'DateTime' : [],
                            'Temperature' : [], 
                            'Description' : [],
                            'Wind_Speed(Gust)': [],
                            'Prec_Prob(%)': []
                            }
    
    for idx in range(len(Airport_lon)):
        url = f"http://api.openweathermap.org/data/2.5/forecast?lat={Airport_lat[idx]}&lon={Airport_lon[idx]}&appid=e7d1480b3d55f1e4ebd431370b378444&units=metric"
        my_request = requests.get(url)
        
        wind_speed = []
        wind_gust = []
        airport_coord_lat = []
        airport_coord_lon = []
        
        for j in my_request.json()['list']:
            Airport_weather_dict['City.loc'].append(my_request.json()['city']['name'])
            Airport_weather_dict['DateTime'].append(j['dt_txt'])
            Airport_weather_dict['Temperature'].append(j['main']['temp'])
            Airport_weather_dict['Description'].append(j['weather'][0]['description'])
            Airport_weather_dict['Prec_Prob(%)'].append(j["pop"] * 100)
            wind_speed.append(j['wind']['speed'])
            wind_gust.append(j['wind']['gust'])
            airport_coord_lat.append(Airport_lat[idx])
            airport_coord_lon.append(Airport_lon[idx])
        
        for a in range(len(wind_speed)):
            Airport_weather_dict['Wind_Speed(Gust)'].append(f'{str(wind_speed[a])}({str(wind_gust[a])})')
            Airport_weather_dict['airport_id'].append(f'{str(airport_coord_lat[a])}{str(airport_coord_lon[a])}')

    pd.DataFrame(Airport_weather_dict).to_sql('airport_weather_data', if_exists='append', con=con, index=False)
