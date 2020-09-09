#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import required packages
from google.cloud import storage
import os
import subprocess
import pandas as pd
import numpy as np
import re #regukar expression
from datetime import datetime
import ast #abstract syntax tree
import warnings
import pytz #date-time conversion
import requests #to get info from server
import time
import datetime as dt
import json
from pandas.io.json import json_normalize
from datetime import date, timedelta
warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment = None  #ignores the warning message and code runs faster # default='warn'

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)
pd.options.display.float_format = '{:.2f}'.format


# In[2]:


def getdarkskydatahourly(start,end,loc_name):
    """
    
    Input - start_date, end_date, API KEY from https://darksky.net/dev/register (now closed APPLE has acqired darksky)
    Output - hourly level weather data from start date to end date for specified location
    
    """
#     api_key = '4cd89f92b5e5dc2c74a930cdfec2524a'
    api_key = '12ea26d5539f1add37b448b7db20f6b8'
    headers = {'User-Agent': 'Chrome/78and.0.3865.90'}
    http_proxy  = "http://10.245.5.249:8080"
    https_proxy = "https://10.245.5.249:8080"
    ftp_proxy   = "ftp://10.245.5.249:8080"

    proxyDict = { 
                "http"  : http_proxy, 
                "https" : https_proxy, 
                "ftp"   : ftp_proxy
                 }
    
    weather_data_hourly = pd.DataFrame()
    date_list = []
    
    # get month,year and day from start date
    start_date = dt.datetime.strptime(start, "%d-%m-%Y")
    start_month = start_date.month
    start_year = start_date.year
    start_day = start_date.day
    
    # get month,year and day from end date
    end_date = dt.datetime.strptime(end, "%d-%m-%Y")
    end_month = end_date.month
    end_year = end_date.year
    end_day = end_date.day
    
    # store all date from start date to end date in date list 
    start_date = date(start_year, start_month, start_day)
    end_date = date(end_year, end_month, end_day)
    delta = timedelta(days=1)
    
    while start_date <= end_date:
        date_list.append(start_date.strftime("%Y-%m-%d"))
        start_date += delta
    
    # latitude of the location markers
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
    
    value1 = sites_latitude.get(loc_name)
    value2 = sites_longitude.get(loc_name)
    if(value1 == None and value2 == None):
        print('Unknown Location Name Used: %s',loc_name)
        print('\n')
        print('Use some other Consumer Location or Generation Location Name.. .. ..')
        print('\n')
    else :
        for i in range(len(date_list)):
            #requesting web page
            date_format = '%Y-%m-%dT%H:%M:%S'
            dates = dt.datetime.strptime(date_list[i], '%Y-%m-%d').strftime(date_format)
            links = 'https://api.darksky.net/forecast/'+api_key+'/'+sites_latitude.get(loc_name)+','+sites_longitude.get(loc_name)+','+str(dates)+'?exclude=currently,daily,alerts?units=us'
            print(links)
            response = requests.get(links,headers=headers,proxies=proxyDict)

            # converting into json
            weather = json.loads(response.content.decode('utf-8'))
            # getting it into data frame
            weather_data = json_normalize(weather['hourly']['data'])
            weather_data_hourly = weather_data_hourly.append(weather_data,sort=True)
    
    weather_data_hourly.reset_index(drop=True, inplace=True)
    
    return weather_data_hourly


# In[3]:


def getdarkskydatadaily(start,end,loc_name):
    """
    
    Input - start_date, end_date, API KEY from https://darksky.net/dev/register (now closed APPLE has acqired darksky)
    Output -daily level weather data from start date to end date for specified location
    
    """
#   api_key = '4cd89f92b5e5dc2c74a930cdfec2524a'
    api_key = '12ea26d5539f1add37b448b7db20f6b8'
    #api_key = '03f7d7221972645470c7e8c6f35edd85'
    headers = {'User-Agent': 'Chrome/78and.0.3865.90'}
    http_proxy  = "http://10.245.5.249:8080"
    https_proxy = "https://10.245.5.249:8080"
    ftp_proxy   = "ftp://10.245.5.249:8080"

    proxyDict = { 
                "http"  : http_proxy, 
                "https" : https_proxy, 
                "ftp"   : ftp_proxy
                 }
    
    weather_data_daily = pd.DataFrame()
    date_list = []
    
    # get month,year and day from start date
    start_date = dt.datetime.strptime(start, "%d-%m-%Y")
    start_month = start_date.month
    start_year = start_date.year
    start_day = start_date.day
    
    # get month,year and day from end date
    end_date = dt.datetime.strptime(end, "%d-%m-%Y")
    end_month = end_date.month
    end_year = end_date.year
    end_day = end_date.day
    
    # store all date from start date to end date in date list 
    start_date = date(start_year, start_month, start_day)
    end_date = date(end_year, end_month, end_day)
    delta = timedelta(days=1)
    
    while start_date <= end_date:
        date_list.append(start_date.strftime("%Y-%m-%d"))
        start_date += delta
    
    # latitude of the location markers
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
    
    value1 = sites_latitude.get(loc_name)
    value2 = sites_longitude.get(loc_name)
    if(value1 == None and value2 == None):
        print('Unknown Location Name Used: %s',loc_name)
        print('\n')
        print('Use some other Consumer Location or Generation Location Name.. .. ..')
        print('\n')
    else :
        for i in range(len(date_list)):
            #requesting web page
            date_format = '%Y-%m-%dT%H:%M:%S'
            dates = dt.datetime.strptime(date_list[i], '%Y-%m-%d').strftime(date_format)
            links = 'https://api.darksky.net/forecast/'+api_key+'/'+sites_latitude.get(loc_name)+','+sites_longitude.get(loc_name)+','+str(dates)+'?exclude=currently,hourly,alerts?units=us'
            print(links)
            response = requests.get(links,headers=headers,proxies=proxyDict)

            # converting into json
            weather = json.loads(response.content.decode('utf-8'))
            # getting it into data frame
            weather_data = json_normalize(weather['daily']['data'])
            weather_data_daily = weather_data_daily.append(weather_data,sort=True)
    
    weather_data_daily.reset_index(drop=True, inplace=True)
    
    return weather_data_daily


# In[4]:


def spearateforecastedactualsfromdarksky(location,cadence):
  
  if (cadence == 1):
    #extract current date - 2
    current_date = datetime.today().strftime('%Y-%m-%d')
    current_date_minus_2 = (datetime.today() - timedelta(days = 2)).strftime('%Y-%m-%d')
    current_date_minus_1 = (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
    
    # convert date format to required format to pass it in getdarkskydata(start_date,end_date,api_key,loc_name) function
    # Start date and End date should be provided in dd-mm-YYYY format
    current_date_minus_2 = pd.to_datetime(current_date_minus_2,format='%Y-%m-%d')
    start_date = current_date_minus_2.strftime('%d-%m-%Y')
    #current_date_minus_1 = pd.to_datetime(current_date_minus_1,format='%Y-%m-%d')
    current_date = pd.to_datetime(current_date,format='%Y-%m-%d')
    end_date = current_date.strftime('%d-%m-%Y')

    # start date and end date to hit dark sky api 
    start_date = str(start_date)
    end_date = str(end_date)
    data = getdarkskydatadaily(start_date,end_date,location)
  else:
    # extract current date and current date plus 3 
    current_date = datetime.today().strftime('%Y-%m-%d')
    current_date_plus_1 = (datetime.today() + timedelta(days = 1)).strftime('%Y-%m-%d')
    current_date_plus_3 = (datetime.today() + timedelta(days = 3)).strftime('%Y-%m-%d')

    current_date_plus_1 = pd.to_datetime(current_date_plus_1,format='%Y-%m-%d')
    start_date = current_date_plus_1.strftime('%d-%m-%Y')
    current_date_plus_3 = pd.to_datetime(current_date_plus_3,format='%Y-%m-%d')
    end_date = current_date_plus_3.strftime('%d-%m-%Y')
    
    start_date = str(start_date)
    end_date = str(end_date)
    data = getdarkskydatahourly(start_date,end_date,location)
  
  # data treatment block
  data['date_time'] = pd.to_datetime(data['time'],unit='s')
  data['date_time']=data['date_time'].astype(str)
  
  #shift data from gmt to pst time 
  def date_convert(date):
  #   date = "2020-02-05 00:00:00"
    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    my_timestamp = date # some timestamp
    old_timezone = pytz.timezone("GMT")
    new_timezone = pytz.timezone("US/Eastern")
 
    # returns datetime in the new timezone
    my_timestamp_in_new_timezone = old_timezone.localize(my_timestamp).astimezone(new_timezone)
    return pd.to_datetime(my_timestamp_in_new_timezone)
  
  data['date_time'] = data.apply(lambda x: date_convert(x['date_time']),axis=1)
  data['Date'] = data['date_time'].dt.date
  data.drop(['date_time'], axis=1, inplace=True)
  

  if (cadence == 1):
    data = data[['Date', 'apparentTemperatureMax', 'apparentTemperatureMin', 'cloudCover', 'dewPoint', 'humidity', 'icon','precipIntensity','precipIntensityMax','precipProbability','precipType',
                             'pressure', 'temperatureMax', 'temperatureMin', 'visibility', 'windBearing', 'windGust', 'windSpeed']]
  else :
    data = data[['Date', 'apparentTemperature','cloudCover', 'dewPoint', 'humidity', 'icon', 'precipIntensity', 'precipProbability', 'precipType',
                             'pressure','temperature', 'visibility', 'windBearing', 'windGust', 'windSpeed']]

    

  return data


# In[5]:


location = ['Marker 1', 'Marker 2', 'Marker 3', 'Marker 4', 'Marker 5', 'Marker 6', 'Marker 7', 'Marker 8',
            'Marker 9', 'Marker 10', 'Marker 11', 'Marker 12', 'Marker 13', 'Marker 14', 'Marker 15', 'Marker 16', 
            'Marker 17', 'Marker 18', 'Marker 19', 'Marker 20']
dictionary = {'daily': 1 , 'hourly' : 2}


# In[16]:


daily_dictionary = {}
for num in range(0,20):
    daily_dictionary["data_daily%s" %num] = spearateforecastedactualsfromdarksky(location[num],dictionary['daily'])


# In[12]:


hourly_dictionary = {}
for x in range(0,20):
    hourly_dictionary["data_hourly%s" %x] = spearateforecastedactualsfromdarksky(location[x],dictionary['hourly'])


# In[ ]:


hourly_0 = pd.DataFrame(hourly_dictionary.get('data_hourly0'))
hourly_0['Location']=1
hourly_1 = pd.DataFrame(hourly_dictionary.get('data_hourly1'))
hourly_1['Location']=2
hourly_2 = pd.DataFrame(hourly_dictionary.get('data_hourly2'))
hourly_2['Location']=3
hourly_3 = pd.DataFrame(hourly_dictionary.get('data_hourly3'))
hourly_3 ['Location']=4
hourly_4 = pd.DataFrame(hourly_dictionary.get('data_hourly4'))
hourly_4['Location']=5
hourly_5 = pd.DataFrame(hourly_dictionary.get('data_hourly5'))
hourly_5['Location']=6
hourly_6 = pd.DataFrame(hourly_dictionary.get('data_hourly6'))
hourly_6['Location']=7
hourly_7 = pd.DataFrame(hourly_dictionary.get('data_hourly7'))
hourly_7['Location']=8
hourly_8 = pd.DataFrame(hourly_dictionary.get('data_hourly8'))
hourly_8['Location']=9
hourly_9 = pd.DataFrame(hourly_dictionary.get('data_hourly9'))
hourly_9['Location']=10
hourly_10 = pd.DataFrame(hourly_dictionary.get('data_hourly10'))
hourly_10['Location']=11
hourly_11 = pd.DataFrame(hourly_dictionary.get('data_hourly11'))
hourly_11['Location']=12
hourly_12 = pd.DataFrame(hourly_dictionary.get('data_hourly12'))
hourly_12['Location']=13
hourly_13 = pd.DataFrame(hourly_dictionary.get('data_hourly13'))
hourly_13['Location']=14
hourly_14 = pd.DataFrame(hourly_dictionary.get('data_hourly14'))
hourly_14['Location']=15
hourly_15 = pd.DataFrame(hourly_dictionary.get('data_hourly15'))
hourly_15['Location']=16
hourly_16 = pd.DataFrame(hourly_dictionary.get('data_hourly16'))
hourly_16['Location']=17
hourly_17 = pd.DataFrame(hourly_dictionary.get('data_hourly17'))
hourly_17['Location']=18
hourly_18= pd.DataFrame(hourly_dictionary.get('data_hourly18'))
hourly_18['Location']=19
hourly_19 = pd.DataFrame(hourly_dictionary.get('data_hourly19'))
hourly_19['Location']=20




daily_0 = pd.DataFrame(daily_dictionary.get('data_daily0'))
daily_0['Location']=1
daily_1 = pd.DataFrame(daily_dictionary.get('data_daily1'))
daily_1['Location']=2
daily_2 = pd.DataFrame(daily_dictionary.get('data_daily2'))
daily_2['Location']=3
daily_3 = pd.DataFrame(daily_dictionary.get('data_daily3'))
daily_3 ['Location']=4
daily_4 = pd.DataFrame(daily_dictionary.get('data_daily4'))
daily_4['Location']=5
daily_5 = pd.DataFrame(daily_dictionary.get('data_daily5'))
daily_5['Location']=6
daily_6 = pd.DataFrame(daily_dictionary.get('data_daily6'))
daily_6['Location']=7
daily_7 = pd.DataFrame(daily_dictionary.get('data_daily7'))
daily_7['Location']=8
daily_8 = pd.DataFrame(daily_dictionary.get('data_daily8'))
daily_8['Location']=9
daily_9 = pd.DataFrame(daily_dictionary.get('data_daily9'))
daily_9['Location']=10
daily_10 = pd.DataFrame(daily_dictionary.get('data_daily10'))
daily_10['Location']=11
daily_11 = pd.DataFrame(daily_dictionary.get('data_daily11'))
daily_11['Location']=12
daily_12 = pd.DataFrame(daily_dictionary.get('data_daily12'))
daily_12['Location']=13
daily_13 = pd.DataFrame(daily_dictionary.get('data_daily13'))
daily_13['Location']=14
daily_14 = pd.DataFrame(daily_dictionary.get('data_daily14'))
daily_14['Location']=15
daily_15 = pd.DataFrame(daily_dictionary.get('data_daily15'))
daily_15['Location']=16
daily_16 = pd.DataFrame(daily_dictionary.get('data_daily16'))
daily_16['Location']=17
daily_17 = pd.DataFrame(daily_dictionary.get('data_daily17'))
daily_17['Location']=18
daily_18= pd.DataFrame(daily_dictionary.get('data_daily18'))
daily_18['Location']=19
daily_19 = pd.DataFrame(daily_dictionary.get('data_daily19'))
daily_19['Location']=20


# In[ ]:



marker_daily_final=pd.concat([daily_0,daily_1,daily_2,daily_3,daily_4,daily_5,daily_6,daily_7,daily_8,
                               daily_9,daily_10,daily_11,daily_12,daily_13,daily_14,daily_15,
                               daily_16,daily_17,daily_18,daily_19],ignore_index=True) 

marker_hourly_final=pd.concat([hourly_0,hourly_1,hourly_2,hourly_3,hourly_4,hourly_5,hourly_6,hourly_7,hourly_8,
                               hourly_9,hourly_10,hourly_11,hourly_12,hourly_13,hourly_14,hourly_15,
                               hourly_16,hourly_17,hourly_18,hourly_19],ignore_index=True)


# In[ ]:


marker_daily_final['Date'] = pd.to_datetime(marker_daily_final['Date'],format='%Y-%m-%d')
marker_hourly_final['Date'] = pd.to_datetime(marker_hourly_final['Date'],format='%Y-%m-%d')


# In[ ]:


current_date = datetime.today().strftime('%Y-%m-%d')
current_date = pd.to_datetime(current_date)
current_date_minus1 = current_date +  timedelta(days=-1)
current_date_minus2 = current_date +  timedelta(days=-2)
current_date_plus1 = current_date + timedelta(days=1)
current_date_plus2 = current_date + timedelta(days=2)
current_date_plus3 = current_date + timedelta(days=3)


# In[ ]:


marker_daily_final_current_date_minus1 = marker_daily_final[(marker_daily_final['Date'] == current_date_minus1)]
marker_daily_final_current_date_minus1.reset_index(drop=True,inplace=True)

marker_daily_final_current_date_minus2 = marker_daily_final[(marker_daily_final['Date'] == current_date_minus2)]
marker_daily_final_current_date_minus2.reset_index(drop=True,inplace=True)

marker_daily_final_current_date = marker_daily_final[(marker_daily_final['Date'] == current_date)]
marker_daily_final_current_date.reset_index(drop=True,inplace=True)

marker_hourly_final_current_date_plus1 = marker_hourly_final[(marker_hourly_final['Date'] == current_date_plus1)]
marker_hourly_final_current_date_plus1.reset_index(drop=True,inplace=True)

marker_hourly_final_current_date_plus2 = marker_hourly_final[(marker_hourly_final['Date'] == current_date_plus2)]
marker_hourly_final_current_date_plus2.reset_index(drop=True,inplace=True)

marker_hourly_final_current_date_plus3 = marker_hourly_final[(marker_hourly_final['Date'] == current_date_plus3)]
marker_hourly_final_current_date_plus3.reset_index(drop=True,inplace=True)


# In[ ]:


marker_daily_final_current_date_minus1.to_csv("/root/darkskyweatherdaily_"+current_date_minus1.strftime("%Y%m%d")+".csv",index=False)
marker_daily_final_current_date_minus2.to_csv("/root/darkskyweatherdaily_"+current_date_minus2.strftime("%Y%m%d")+".csv",index=False)
marker_daily_final_current_date.to_csv("/root/darkskyweatherdaily_"+current_date.strftime("%Y%m%d")+".csv",index=False)
marker_hourly_final_current_date_plus1.to_csv("/root/darkskyweatherhourly_"+current_date_plus1.strftime("%Y%m%d")+".csv",index=False)
marker_hourly_final_current_date_plus2.to_csv("/root/darkskyweatherhourly_"+current_date_plus2.strftime("%Y%m%d")+".csv",index=False)
marker_hourly_final_current_date_plus3.to_csv("/root/darkskyweatherhourly_"+current_date_plus3.strftime("%Y%m%d")+".csv",index=False)


# In[ ]:


""" daily data is stored in actual data folder and hourly data in forecasted folder"""

os.system("gsutil cp /root/darkskyweatherdaily_"+current_date_minus1.strftime("%Y%m%d")+".csv gs://aes-datahub-0002-raw/Weather/Dark_Sky/USA/Indianapolis/"+current_date_minus1.strftime("%Y-%m")+"/actual_Data/darkskyweatherdaily_"+current_date_minus1.strftime("%Y%m%d")+".csv")

os.system("gsutil cp /root/darkskyweatherdaily_"+current_date_minus2.strftime("%Y%m%d")+".csv gs://aes-datahub-0002-raw/Weather/Dark_Sky/USA/Indianapolis/"+current_date_minus2.strftime("%Y-%m")+"/actual_Data/darkskyweatherdaily_"+current_date_minus2.strftime("%Y%m%d")+".csv")

os.system("gsutil cp /root/darkskyweatherdaily_"+current_date.strftime("%Y%m%d")+".csv gs://aes-datahub-0002-raw/Weather/Dark_Sky/USA/Indianapolis/"+current_date.strftime("%Y-%m")+"/actual_Data/darkskyweatherdaily_"+current_date.strftime("%Y%m%d")+".csv")

os.system("gsutil cp /root/darkskyweatherhourly_"+current_date_plus1.strftime("%Y%m%d")+".csv gs://aes-datahub-0002-raw/Weather/Dark_Sky/USA/Indianapolis/"+current_date_plus1.strftime("%Y-%m")+"/forecasted_Data/darkskyweatherhourly_"+current_date_plus1.strftime("%Y%m%d")+".csv")

os.system("gsutil cp /root/darkskyweatherhourly_"+current_date_plus2.strftime("%Y%m%d")+".csv gs://aes-datahub-0002-raw/Weather/Dark_Sky/USA/Indianapolis/"+current_date_plus2.strftime("%Y-%m")+"/forecasted_Data/darkskyweatherhourly_"+current_date_plus2.strftime("%Y%m%d")+".csv")

os.system("gsutil cp /root/darkskyweatherhourly_"+current_date_plus3.strftime("%Y%m%d")+".csv gs://aes-datahub-0002-raw/Weather/Dark_Sky/USA/Indianapolis/"+current_date_plus3.strftime("%Y-%m")+"/forecasted_Data/darkskyweatherhourly_"+current_date_plus3.strftime("%Y%m%d")+".csv")

