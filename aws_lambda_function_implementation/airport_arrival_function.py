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



def airport_arrival(cities_airports, dates):

    if len(list(cities_airports['icao'])) > 1:
        icao_list = list(cities_airports['icao'])
    if len(list(cities_airports['icao'])) == 1:
        icao_list = cities_airports['icao']
    

    Airport_info = []
    
    
    # Obtain flight information based on icao codes
    for i in range(len(icao_list)):
        # if (i > 0) & ((i % 15) == 0):
        #     time.sleep(10)
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao_list[i]}/{dates}T09:00/{dates}T21:00"

        querystring = {"withLeg":"true","direction":"Arrival","withCancelled":"false","withCodeshared":"true","withCargo":"false","withPrivate":"false","withLocation":"false"}
    
        headers = {
    	    "X-RapidAPI-Key": "fdf8741e07msh11a8578ab15a6eap107760jsn989518799de6",
    	    "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
                  }
    
        response2 = requests.request("GET", url, headers=headers, params=querystring)
        

        if response2.status_code != 200:
            print('It is not 200')
            continue
        if (response2.json()['arrivals']) == []:
            print('it is not empty')
            continue
        
        individ_icao_info = pd.json_normalize(response2.json()['arrivals'])
        individ_icao_info['icao'] = icao_list[i]
        Airport_info.append(individ_icao_info)
        
    
    #print(Airport_info) 
    Airport_arrival_info = pd.concat(Airport_info, ignore_index=True).loc[:,['icao', 'status', 'departure.airport.name', 'arrival.scheduledTimeLocal', 'aircraft.model', 'airline.name']]
    # Airport_arrival_info = Airport_arrival_info[Airport_arrival_info['status'] != 'Unknown']
    
    Airport_arrival_info.to_sql('airport_arrival_data', if_exists='append', con=con, index=False)