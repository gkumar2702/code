""""
Script for getting the weather source data for Indianapolis Power & Light
"""

from datetime import datetime, timedelta
import warnings
import json
import requests #to get info from server
import pandas as pd
from pandas.io.json import json_normalize
from pytz import timezone #date-time conversion
warnings.filterwarnings('ignore')

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.options.display.float_format = '{:.2f}'.format

# proxy settings for api calls
HEADERS = {'User-Agent': 'Chrome/78and.0.3865.90'}
HTTP_PROXY = "http://proxy.ouraes.com:8080"
HTTPS_PROXY = "https://proxy.ouraes.com:8080"
FTP_PROXY = "ftp://proxy.ouraes.com:8080"

PROXY_DICT = {"http"  : HTTP_PROXY, "https" : HTTPS_PROXY, "ftp"   : FTP_PROXY}


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

    key = 'e721181f854ac2268ee8'
    start = pd.to_datetime(start, format='%Y-%m-%d')
    end = pd.to_datetime(end, format='%Y-%m-%d')

    start = start.strftime('%Y-%m-%dT%H:%M:%S')
    end = end.strftime('%Y-%m-%dT%H:%M:%S')

    weather_ = pd.DataFrame()
    link = 'https://api.weathersource.com/v1/'+key+'/points/'+lat+','+long\
	       +'/history.json?period='+period+'&timestamp_between='+start+','+end+'&fields='+fields
    print(link)
    response = requests.get(link, headers=HEADERS, proxies=PROXY_DICT)
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

  Output : returns forecast data upto 15 days ahead of forecast data and 240 hours of hourly
  weather data for a latitude/longitude point
  '''

    key = 'e721181f854ac2268ee8'
    start = pd.to_datetime(start, format='%Y-%m-%d')
    end = pd.to_datetime(end, format='%Y-%m-%d')

    start = start.strftime('%Y-%m-%dT%H:%M:%S')
    end = end.strftime('%Y-%m-%dT%H:%M:%S')

    weather_ = pd.DataFrame()
    link = 'https://api.weathersource.com/v1/'+key+'/points/'+lat+','+long\
           +'/forecast.json?period='+period+'&timestamp_between='+start+','+end+'&fields='+fields
    print(link)
    response = requests.get(link, headers=HEADERS, proxies=PROXY_DICT)
    json_obj = json.loads(response.content.decode('utf-8'))
    weather_ = json_normalize(json_obj)

    return weather_


SITES_LATITUDE = {
    'Marker 1' : '39.9613', 'Marker 2' : '39.8971', 'Marker 3' : '39.9060',
    'Marker 4' : '39.9024', 'Marker 5' : '39.8960', 'Marker 6' : '39.8339',
    'Marker 7' : '39.8412', 'Marker 8' : '39.8381', 'Marker 9' : '39.8386',
    'Marker 10' : '39.7579', 'Marker 11' : '39.7621', 'Marker 12' : '39.7621',
    'Marker 13' : '39.7695', 'Marker 14' : '39.6617', 'Marker 15' : '39.6639',
    'Marker 16' : '39.6702', 'Marker 17' : '39.6744', 'Marker 18' : '39.5909',
    'Marker 19' : '39.5295', 'Marker 20' : '39.5475'
    }

# longitude of the location markers
SITES_LONGITUDE = {
    'Marker 1' : '-86.4034', 'Marker 2' : '-86.3045', 'Marker 3' : '-86.2001',
    'Marker 4' : '-86.0738', 'Marker 5' : '-85.9783', 'Marker 6' : '-86.3155',
    'Marker 7' : '-86.2056', 'Marker 8' : '-86.0985', 'Marker 9' : '-85.9811',
    'Marker 10' : '-86.3155', 'Marker 11' : '-86.2042', 'Marker 12' : '-86.0923',
    'Marker 13' : '-85.9708', 'Marker 14' : '-86.2935', 'Marker 15' : '-86.1823',
    'Marker 16' : '-86.0669', 'Marker 17' : '-85.9557', 'Marker 18' : '-86.4212',
    'Marker 19' : '-86.5874', 'Marker 20' : '-86.2743'
    }


LOCATION_MARKER = ['Marker 1', 'Marker 2', 'Marker 3', 'Marker 4', 'Marker 5', 'Marker 6',
                   'Marker 7', 'Marker 8', 'Marker 9', 'Marker 10', 'Marker 11', 'Marker 12',
                   'Marker 13', 'Marker 14', 'Marker 15', 'Marker 16', 'Marker 17', 'Marker 18',
                   'Marker 19', 'Marker 20']



print("Libraries and functions loaded")



TODAY_DATE = datetime.now()
FORECAST_NEXT_DATE = (TODAY_DATE + timedelta(days=1)).strftime('%Y-%m-%d')
FORECAST_END_DATE = (TODAY_DATE + timedelta(days=2)).strftime('%Y-%m-%d')
PAST_START_DATE = (TODAY_DATE - timedelta(days=2)).strftime('%Y-%m-%d')
PAST_END_DATE = (TODAY_DATE - timedelta(days=1)).strftime('%Y-%m-%d')
TODAY_DATE = TODAY_DATE.strftime('%Y-%m-%d')

print("Running for today's date", TODAY_DATE)
print("Extracting from API")
WAETHERSOURCEFILES_FORECAST = []
WAETHERSOURCEFILES_HISTORICAL = []
WEATHERSOURCE = []
VALUE1 = 0.0
VALUE2 = 0.0
for i in LOCATION_MARKER:
    VALUE1 = SITES_LATITUDE.get(i)
    VALUE2 = SITES_LONGITUDE.get(i)
    waethersource_data_forecast = ws_forecast_data(start=TODAY_DATE, end=FORECAST_END_DATE,
                                                   lat=VALUE1, long=VALUE2, period='day')
    waethersource_data_historical = ws_historical_data(start=PAST_START_DATE, end=PAST_END_DATE,
                                                       lat=VALUE1, long=VALUE2, period='day')
    waethersource_data_historical['Location'] = i
    waethersource_data_forecast['Location'] = i
    WAETHERSOURCEFILES_FORECAST.append(waethersource_data_forecast)
    WAETHERSOURCEFILES_HISTORICAL.append(waethersource_data_historical)

WAETHERSOURCE_DF_HIS = pd.concat(WAETHERSOURCEFILES_HISTORICAL)
WAETHERSOURCE_DF_FOR = pd.concat(WAETHERSOURCEFILES_FORECAST)

WAETHERSOURCE_DF_HIS.reset_index(drop=True, inplace=True)
WAETHERSOURCE_DF_FOR.reset_index(drop=True, inplace=True)
print("successful extraction")

print(WAETHERSOURCE_DF_HIS.timestamp.unique())
print(WAETHERSOURCE_DF_FOR['timestamp'].unique())

WAETHERSOURCE_DF_HIS['timestamp'] = pd.to_datetime(WAETHERSOURCE_DF_HIS['timestamp'])
WAETHERSOURCE_DF_FOR['timestamp'] = pd.to_datetime(WAETHERSOURCE_DF_FOR['timestamp'])

WAETHERSOURCE_DF_HIS['timestamp'] = (WAETHERSOURCE_DF_HIS['timestamp']).apply(
    lambda row: row.strftime("%Y-%m-%d %H:%M:%S"))

WAETHERSOURCE_DF_FOR['timestamp'] = (WAETHERSOURCE_DF_FOR['timestamp']).apply(
    lambda row: row.strftime("%Y-%m-%d %H:%M:%S"))

WAETHERSOURCE_DF_HIS['timestamp'] = pd.to_datetime(WAETHERSOURCE_DF_HIS['timestamp']).dt.date
WAETHERSOURCE_DF_FOR['timestamp'] = pd.to_datetime(WAETHERSOURCE_DF_FOR['timestamp']).dt.date

DATE_LIST = [PAST_START_DATE, PAST_END_DATE, TODAY_DATE, FORECAST_NEXT_DATE, FORECAST_END_DATE]




for i in range(0, 2):
    temp_df = WAETHERSOURCE_DF_HIS[WAETHERSOURCE_DF_HIS['timestamp'].astype(str) == DATE_LIST[i]]
    loc = "gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/"
    loc = loc + datetime.strptime(DATE_LIST[i], '%Y-%m-%d').strftime('%Y%m%d')[:4] + "-" +\
    datetime.strptime(DATE_LIST[i], '%Y-%m-%d').strftime('%Y%m%d')[4:6]
    loc = loc + "/actual_data/weathersource_daily_"
    loc = loc + datetime.strptime(DATE_LIST[i], '%Y-%m-%d').strftime('%Y%m%d') +'.csv'
    temp_df.to_csv(loc)


for i in range(2, 5):
    temp_df = WAETHERSOURCE_DF_FOR[WAETHERSOURCE_DF_FOR['timestamp'].astype(str) == DATE_LIST[i]]
    loc = "gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/"
    loc = loc + datetime.strptime(DATE_LIST[i], '%Y-%m-%d').strftime('%Y%m%d')[:4] + "-"\
    + datetime.strptime(DATE_LIST[i], '%Y-%m-%d').strftime('%Y%m%d')[4:6]
    loc = loc + "/forecast_data/" + TODAY_DATE + "/weathersource_daily_"
    loc = loc + datetime.strptime(DATE_LIST[i], '%Y-%m-%d').strftime('%Y%m%d') + '.csv'
    temp_df.to_csv(loc)


print("Saved location at aes-datahub-0001-raw/Weather/weather_source/usa/Indianapolis/")
