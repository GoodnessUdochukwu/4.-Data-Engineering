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

def get_cities_weather(cities):
    weather_dict = {
        'Country': [],
        'City' : [],
        'DateTime' :  [],
        'Main' : [],         
        'Temperature' : [], 
        'Description' : [],
        'Longitude': [],
        'Latitude': [],
        'Wind_Speed(Gust)': [],
        'Prec_Prob(%)': []
    }
    for i in cities:
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={i}&appid=e7d1480b3d55f1e4ebd431370b378444&units=metric"
        my_request = requests.get(url)
        
        # wind speed and gust
        wind_speed = []
        wind_gust = []
        
        for j in my_request.json()['list']:
            weather_dict['Country'].append(my_request.json()['city']['country'])
            weather_dict['City'].append(i)
            weather_dict['DateTime'].append(j['dt_txt'])
            weather_dict['Temperature'].append(j['main']['temp'])
            weather_dict['Main'].append(j['weather'][0]['main'])
            weather_dict['Description'].append(j['weather'][0]['description'])
            weather_dict['Longitude'].append(my_request.json()["city"]["coord"]["lon"])
            weather_dict['Latitude'].append(my_request.json()["city"]["coord"]["lat"])
            weather_dict['Prec_Prob(%)'].append(j["pop"] * 100)
            wind_speed.append(j['wind']['speed'])
            wind_gust.append(j['wind']['gust'])
        
        for a in range(len(wind_speed)):
            weather_dict['Wind_Speed(Gust)'].append(f'{str(wind_speed[a])}({str(wind_gust[a])})')
        # output  
    pd.DataFrame(weather_dict).to_sql('city_weather', if_exists='replace', con=con, index=False)
