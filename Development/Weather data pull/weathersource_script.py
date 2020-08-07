import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import ast #abstract syntax tree
import warnings
import pytz #date-time conversion
import requests #to get info from server
import time
import datetime as dt
import json
import calendar
from pandas.io.json import json_normalize
from pytz import timezone
from datetime import date, timedelta
warnings.filterwarnings('ignore')

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.options.display.float_format = '{:.2f}'.format

# proxy settings for api calls
headers = {'User-Agent': 'Chrome/78and.0.3865.90'}
http_proxy  = "http://10.245.5.249:8080"
https_proxy = "https://10.245.5.249:8080"
ftp_proxy   = "ftp://10.245.5.249:8080"

proxyDict = { "http"  : http_proxy, "https" : https_proxy, "ftp"   : ftp_proxy }


def ws_historical_data(start, end, lat, long, period='day', fields='all'):
  '''
  If duration is more than 1 year separate calls should be used
  Timestamp should be converted to ISO 8601 format
  Docstring with examples and function return values:
  
  Input :
  start - (%Y-%m-%d) format
  end - (%Y-%m-%d) format
  lat - latitude 
  long - longitude
  period - hour, day (default=day)
  
  Output : return a collection of weather historical data for a latitude/longitude point
  
  '''
  headers = {'User-Agent': 'Chrome/78and.0.3865.90'}
  http_proxy  = "http://10.245.5.249:8080"
  https_proxy = "https://10.245.5.249:8080"
  ftp_proxy   = "ftp://10.245.5.249:8080"
  
  proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy, 
              "ftp"   : ftp_proxy
               }
  
  key = 'e721181f854ac2268ee8'
  start = pd.to_datetime(start,format='%Y-%m-%d')
  end = pd.to_datetime(end, format='%Y-%m-%d')
  
  start = start.strftime('%Y-%m-%dT%H:%M:%S')
  end = end.strftime('%Y-%m-%dT%H:%M:%S')
  
  weather_ = pd.DataFrame()
  link = 'https://api.weathersource.com/v1/'+key+'/points/'+lat+','+long+'/history.json?period='+period+'&timestamp_between='+start+','+end+'&fields='+fields
  print(link)
  response = requests.get(link, headers=headers,proxies=proxyDict)
  json_obj = json.loads(response.content.decode('utf-8'))
  weather_ = json_normalize(json_obj)
  
  return weather_


def ws_forecast_data(start, end, lat, long, period='day', fields='all'):
  '''
  Timestamp should be converted to ISO 8601 format
  Docstring with examples and function return values:
  
  Input :
  start - (%Y-%m-%d) format
  end - (%Y-%m-%d) format
  lat - latitude 
  long - longitude
  period - hour, day (default=day)
  
  Output : returns forecast data upto 15 days ahead of forecast data and 240 hours of hourly weather data for a latitude/longitude point
  
  '''
  headers = {'User-Agent': 'Chrome/78and.0.3865.90'}
  http_proxy  = "http://10.245.5.249:8080"
  https_proxy = "https://10.245.5.249:8080"
  ftp_proxy   = "ftp://10.245.5.249:8080"
  
  proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy, 
              "ftp"   : ftp_proxy
               }
  
  key = 'e721181f854ac2268ee8'
  start = pd.to_datetime(start,format='%Y-%m-%d')
  end = pd.to_datetime(end, format='%Y-%m-%d')
  
  start = start.strftime('%Y-%m-%dT%H:%M:%S')
  end = end.strftime('%Y-%m-%dT%H:%M:%S')
  
  weather_ = pd.DataFrame()
  link = 'https://api.weathersource.com/v1/'+key+'/points/'+lat+','+long+'/forecast.json?period='+period+'&timestamp_between='+start+','+end+'&fields='+fields
  print(link)
  response = requests.get(link, headers=headers,proxies=proxyDict)
  json_obj = json.loads(response.content.decode('utf-8'))
  weather_ = json_normalize(json_obj)
  
  return weather_


sites_latitude = {
    'Marker 1' : '39.9613','Marker 2' : '39.8971','Marker 3' : '39.9060','Marker 4' : '39.9024','Marker 5' : '39.8960','Marker 6' : '39.8339',
    'Marker 7' : '39.8412','Marker 8' : '39.8381','Marker 9' : '39.8386','Marker 10' : '39.7579','Marker 11' : '39.7621','Marker 12' : '39.7621',
    'Marker 13' : '39.7695','Marker 14' : '39.6617','Marker 15' : '39.6639','Marker 16' : '39.6702','Marker 17' : '39.6744','Marker 18' : '39.5909',
    'Marker 19' : '39.5295','Marker 20' : '39.5475'
    }
    
# longitude of the location markers
sites_longitude = {
    'Marker 1' : '-86.4034','Marker 2' : '-86.3045','Marker 3' : '-86.2001','Marker 4' : '-86.0738','Marker 5' : '-85.9783','Marker 6' : '-86.3155',
    'Marker 7' : '-86.2056','Marker 8' : '-86.0985','Marker 9' : '-85.9811','Marker 10' : '-86.3155','Marker 11' : '-86.2042','Marker 12' : '-86.0923',
    'Marker 13' : '-85.9708','Marker 14' : '-86.2935','Marker 15' : '-86.1823','Marker 16' : '-86.0669','Marker 17' : '-85.9557','Marker 18' : '-86.4212',
    'Marker 19' : '-86.5874','Marker 20' : '-86.2743'
    }


location_marker = ['Marker 1', 'Marker 2', 'Marker 3', 'Marker 4', 'Marker 5', 'Marker 6', 'Marker 7', 'Marker 8', 'Marker 9', 'Marker 10',
                   'Marker 11', 'Marker 12', 'Marker 13', 'Marker 14', 'Marker 15', 'Marker 16', 'Marker 17', 'Marker 18', 'Marker 19', 'Marker 20']



print("Libraries and functions loaded")



today_date = datetime.now(timezone('US/Eastern'))
forecast_next_date = (today_date + timedelta(days=1)).strftime('%Y-%m-%d')
forecast_end_date = (today_date + timedelta(days=2)).strftime('%Y-%m-%d')
past_start_date = (today_date - timedelta(days=2)).strftime('%Y-%m-%d')
past_end_date = (today_date - timedelta(days=1)).strftime('%Y-%m-%d')
today_date = today_date.strftime('%Y-%m-%d')

print("Running for today's date", today_date)
print("Extracting from API")
waethersourcefiles_forecast = []
waethersourcefiles_historical = []
weathersource = []
value1 = 0.0
value2 = 0.0
for i in location_marker:
    value1 = sites_latitude.get(i)
    value2 = sites_longitude.get(i)
    waethersource_data_forecast = ws_forecast_data(start=today_date, end=forecast_end_date, lat=value1, long=value2, period='day')
    waethersource_data_historical = ws_historical_data(start=past_start_date, end=past_end_date, lat=value1, long=value2, period='day')    
    waethersource_data_historical['Location'] = i
    waethersource_data_forecast['Location'] = i
    waethersourcefiles_forecast.append(waethersource_data_forecast)
    waethersourcefiles_historical.append(waethersource_data_historical)

waethersource_df_his = pd.concat(waethersourcefiles_historical)
waethersource_df_for = pd.concat(waethersourcefiles_forecast)

waethersource_df_his.reset_index(drop=True, inplace=True)
waethersource_df_for.reset_index(drop=True, inplace=True)
print("successful extraction")



waethersource_df_his['timestamp'] = pd.to_datetime(waethersource_df_his['timestamp']).dt.date
waethersource_df_for['timestamp'] = pd.to_datetime(waethersource_df_for['timestamp']).dt.date

date_list = [past_start_date, past_end_date, today_date, forecast_next_date, forecast_end_date]



for i in range(0,2):
  temp_df = waethersource_df_his[waethersource_df_his['timestamp'].astype(str) == date_list[i]]
  loc = "gs://aes-datahub-0002-raw/Weather/weather_source/USA/Indianapolis/"
  loc = loc + datetime.strptime(date_list[i], '%Y-%m-%d').strftime('%Y%m%d')[:4] + "-" + datetime.strptime(date_list[i], '%Y-%m-%d').strftime('%Y%m%d')[4:6]
  loc = loc + "/actual_data/weathersource_daily_"
  loc = loc + datetime.strptime(date_list[i], '%Y-%m-%d').strftime('%Y%m%d') +'.csv'
  temp_df.to_csv(loc)


for i in range(2,5):
  temp_df = waethersource_df_for[waethersource_df_for['timestamp'].astype(str) == date_list[i]]
  loc = "gs://aes-datahub-0002-raw/Weather/weather_source/USA/Indianapolis/"
  loc = loc + datetime.strptime(date_list[i], '%Y-%m-%d').strftime('%Y%m%d')[:4] + "-" + datetime.strptime(date_list[i], '%Y-%m-%d').strftime('%Y%m%d')[4:6]
  loc = loc + "/forecast_data/" + today_date + "/weathersource_daily_"
  loc = loc + datetime.strptime(date_list[i], '%Y-%m-%d').strftime('%Y%m%d') + '.csv'
  temp_df.to_csv(loc)


print("Saved location at aes-datahub-0002-raw/Weather/weather_source/usa/Indianapolis/")