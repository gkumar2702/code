'''
Authored: Mu Sigma
Updated: 15 Dec 2020
Version: 2
Description: Python script to create email structure and send
email everyday with forecasts from IPL Storm Preparedness model
(# Outages and # Customers) and daily weather forecast from
weather source API
'''

##################################### Loading Libraries #################################
# standard library imports
import logging
import ast
from datetime import date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
from configparser import ConfigParser, ExtendedInterpolation
import pandas as pd

# Setup logs
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


######################################### Setting Config ########################################
CONFIGPARSER = ConfigParser(interpolation=ExtendedInterpolation())
CONFIGPARSER.read('config_storm.ini')
logging.info('Config File Loaded')
logging.info('Config File Sections %s', CONFIGPARSER.sections())

POWER_UTILITY = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['POWER_UTILITY']
PROJECT_ID = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['PROJECT_ID']
DATASET_PREPAREDNESS = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['DATASET_PREPAREDNESS']
TABLE_PREPAREDNESS = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['TABLE_PREPAREDNESS']
FORECAST_TYPE = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['FORECAST_TYPE']
FORECAST_LEVEL_COLUMN = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['FORECAST_LEVEL_COLUMN']
DATASET_WEATHER = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['DATASET_WEATHER']
TABLE_WEATHER_DAILY = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['TABLE_WEATHER_DAILY']
LANDMARK_MAPPING = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['LANDMARK_MAPPING']
EMAIL_RECIPIENTS = list(ast.literal_eval(CONFIGPARSER['FORECASTS_EMAIL_ALERT']['EMAIL_RECIPIENTS']))
EMAIL_FROM = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['EMAIL_FROM']
SMTP_SERVER = CONFIGPARSER['FORECASTS_EMAIL_ALERT']['SMTP_SERVER']


# Fetching neccessary date for query and email structure
TODAY = date.today()
NEXT_DAY, DATE_1 = TODAY + timedelta(days=1), TODAY.strftime('%d %B %Y')
DATE_2, DAY = TODAY.strftime('%m-%d-%Y'), TODAY.strftime("%A")
logging.info('Today Date: %s \n', TODAY)
logging.info('Tomorrow Date: %s \n', NEXT_DAY)


################################ Loading Required Datasets ######################################

# Reading the storm preparedness data
try:
    PREPAREDNESS_PREDICTIONS = """select * from `{project_id}.{dataset}.{table}`
                            where Date = '{date}' and {forecast_level} = '{forecast_type}'
                            """.format(date=TODAY,
                                       project_id=PROJECT_ID,
                                       dataset=DATASET_PREPAREDNESS,
                                       table=TABLE_PREPAREDNESS,
                                       forecast_level=FORECAST_LEVEL_COLUMN,
                                       forecast_type=FORECAST_TYPE)
    PREPAREDNESS_PREDICTIONS = pd.read_gbq(PREPAREDNESS_PREDICTIONS, project_id=PROJECT_ID)
except:
    raise Exception('Storm Preparedness data not found from Storm Preparedness Big query table')
logging.info('Read IPL Preparedness Model Predictions from Bigquery %s\n',
             PREPAREDNESS_PREDICTIONS.shape)

# Reading the forecast weather data for today from weathersource
try:
    WEATHER_DATA = """select * from `{project_id}.{dataset}.{table}`
                   where  timestamp >= '{date}' and timestamp < '{next_day}' and
                   Location like '%{utl}%' and Job_Update_Time = (select MAX(Job_Update_Time)
                   from `{project_id}.{dataset}.{table}` where timestamp >= '{date}' and
                   timestamp < '{next_day}' and Location like '%{utl}%')
                   """.format(project_id=PROJECT_ID,
                              dataset=DATASET_WEATHER,
                              table=TABLE_WEATHER_DAILY,
                              date=TODAY,
                              next_day=NEXT_DAY,
                              utl=POWER_UTILITY)
    WEATHER_DATA = pd.read_gbq(WEATHER_DATA, project_id=PROJECT_ID)
except:
    raise Exception('Weather Data not found from Weather Source Big query table')
logging.info('Read today forecast for weather data from bigquery table %s\n',
             WEATHER_DATA.shape)

# Reading the csv for mapping marker with landmark location
AREA_MAPPING = pd.read_csv(LANDMARK_MAPPING)
# AREA_MAPPING['Location'] = POWER_UTILITY + '_' + AREA_MAPPING['Location'].str.replace(' ', '')
LOCATION_MERGED = pd.merge(WEATHER_DATA, AREA_MAPPING[['Location', 'Landmark']]
                                                      , on=['Location'], how='left')
logging.info('Added landmark mapping to weather data\n')


############################ Generating content for email structure #############################

# Calculating Wind Direction
def create_wind_direction(x_wind_direction):
    '''
    Input - Wind direction columns
    Output - Wind direction classes
    '''
    if(x_wind_direction >= 1) & (x_wind_direction < 45):
        direction = 'N-E-N'
    elif(x_wind_direction >= 45) & (x_wind_direction < 90):
        direction = 'N-E-E'
    elif(x_wind_direction >= 90) & (x_wind_direction < 180):
        direction = 'S-E-E'
    elif(x_wind_direction >= 135) & (x_wind_direction < 180):
        direction = 'S-E-S'
    elif(x_wind_direction >= 180) & (x_wind_direction < 225):
        direction = 'S-W-S'
    elif(x_wind_direction >= 225) & (x_wind_direction < 270):
        direction = 'S-W-W'
    elif(x_wind_direction >= 270) & (x_wind_direction < 315):
        direction = 'N-W-W'
    elif(x_wind_direction >= 315) & (x_wind_direction < 360):
        direction = 'N-W-N'
    else:
        direction = None
    return direction

WIND_DIRECTION = create_wind_direction(LOCATION_MERGED['windDirAvg'].median())
logging.info('Calculated median wind direction as %s\n', WIND_DIRECTION)

# Calculating content for General Preparedness Model
OUTAGES_LL_95 = PREPAREDNESS_PREDICTIONS['Outages_LL_95'][0]
OUTAGES_UL_95 = PREPAREDNESS_PREDICTIONS['Outages_UL_95'][0]
CUSTOMERS_LL_95 = PREPAREDNESS_PREDICTIONS['Customers_LL_95'][0]
CUSTOMERS_UL_95 = PREPAREDNESS_PREDICTIONS['Customers_UL_95'][0]

# convert string values to float
OUTAGES_LL_95 = float(OUTAGES_LL_95)
OUTAGES_UL_95 = float(OUTAGES_UL_95)
CUSTOMERS_LL_95 = float(CUSTOMERS_LL_95)
CUSTOMERS_UL_95 = float(CUSTOMERS_UL_95)

# round off float values to nearest integer
OUTAGES_LL_95 = round(OUTAGES_LL_95)
OUTAGES_UL_95 = round(OUTAGES_UL_95)
CUSTOMERS_LL_95 = round(CUSTOMERS_LL_95)
CUSTOMERS_UL_95 = round(CUSTOMERS_UL_95)
CUSTOMERS_UL_95 = '{:,}'.format(CUSTOMERS_UL_95)
logging.info('Calculated preparedness model predictions')
logging.info('Outages lower bound: %s', OUTAGES_LL_95)
logging.info('Outages upper bound: %s', OUTAGES_UL_95)
logging.info('Customers lower bound: %s', CUSTOMERS_LL_95)
logging.info('Customers upper bound: %s \n', CUSTOMERS_UL_95)

# Calculating content for weather attributes table #####
# Storing the required temperature values
TEMP_MIN = str(round(LOCATION_MERGED['tempMin'].min(), 1)) + ' F'
TEMP_MAX = str(round(LOCATION_MERGED['tempMax'].max(), 1)) + ' F'
TEMP_MEDIAN = str(round(LOCATION_MERGED['tempAvg'].median(), 1)) + ' F'
logging.info('Calculated temperature forecasts')
logging.info('Minimum Temperature: %s', TEMP_MIN)
logging.info('Maximum Temperature: %s', TEMP_MAX)
logging.info('Median Temperature: %s\n', TEMP_MEDIAN)

# Storing the required windspeed values
WIND_MIN = str(round(LOCATION_MERGED['windSpdMin'].min(), 1)) + ' mi/hr'
WIND_MAX = str(round(LOCATION_MERGED['windSpdMax'].max(), 1)) + ' mi/hr'
WIND_MEDIAN = str(round(LOCATION_MERGED['windSpdAvg'].median(), 1)) + ' mi/hr'
logging.info('Calculated Windspeed forecasts')
logging.info('Minimum Windspeed: %s', WIND_MIN)
logging.info('Maximum Windspeed: %s', WIND_MAX)
logging.info('Median Windspeed: %s \n', WIND_MEDIAN)

# Storing the required windspeed values
PRECIPITATION = str(round(LOCATION_MERGED['precipProb'].mean(), 1))
SNOWFALL = str(round(LOCATION_MERGED['snowfallProb'].mean(), 1))
logging.info('Calculated Precipitation and Snowfall forecasts')
logging.info('Mean Precipitation Probability: %s', PRECIPITATION)
logging.info('Maximum Snowfall Probability: %s\n', SNOWFALL)

# Calculating Content for Highlights Sections #####
# Getting the Location having max and min temperature
MIN_TEMP_LOC = LOCATION_MERGED[LOCATION_MERGED['tempMin'] ==
                               LOCATION_MERGED['tempMin'].min()].reset_index()['Landmark'][0]
logging.info('Min. Temperature Location: %s\n', MIN_TEMP_LOC)

# Getting the Location having max and min wind speed
MAX_WIND_LOC = LOCATION_MERGED[LOCATION_MERGED['windSpdMax'] ==
                               LOCATION_MERGED['windSpdMax'].max()].reset_index()['Landmark'][0]
logging.info('Max. Windspeed Location: %s\n', MAX_WIND_LOC)

# Getting the Location having max precipitation probability
MAX_PRECIP_LOC = LOCATION_MERGED[LOCATION_MERGED['precipProb'] ==
                                 LOCATION_MERGED['precipProb'].max()].reset_index()['Landmark'][0]
MAX_PRECIP = round(LOCATION_MERGED['precipProb'].max(), 1)
logging.info('Max. Precipitation Location: %s', MAX_PRECIP_LOC)
logging.info('Max. Precipitation: %s\n', MAX_PRECIP)

# Creating and populating weather table
WEATHER_TABLE = pd.DataFrame(columns=['Median', 'Minimum', 'Maximum', 'Probability%'],
                             index=['Temperature', 'Windspeed', 'Precipitation', 'Snowfall'])
WEATHER_TABLE.loc['Temperature'] = pd.Series({'Median': TEMP_MEDIAN, 'Minimum': TEMP_MIN,
                                              'Maximum': TEMP_MAX, 'Probability%': '-'})
WEATHER_TABLE.loc['Windspeed'] = pd.Series({'Median': WIND_MEDIAN, 'Minimum': WIND_MIN,
                                            'Maximum': WIND_MAX, 'Probability%': '-'})
WEATHER_TABLE.loc['Precipitation'] = pd.Series({'Median': '-', 'Minimum': '-', 'Maximum': '-',
                                                'Probability%': PRECIPITATION})
WEATHER_TABLE.loc['Snowfall'] = pd.Series({'Median': '-', 'Minimum': '-',
                                           'Maximum': '-', 'Probability%': SNOWFALL})
WEATHER_TABLE.loc['Wind Direction'] = pd.Series({'Median': WIND_DIRECTION, 'Minimum': '-',
                                                 'Maximum': '-', 'Probability%': '-'})

WEATHER_TABLE = WEATHER_TABLE.round(decimals=1).to_html()
logging.info('Populated weather table with above calculated values\n')


############################ Structuring and Sending out the email ##############################

# Getting recipients list
EMAIL_LIST = [elem.strip().split(',') for elem in EMAIL_RECIPIENTS]

# Defining MIME structure
MSG = MIMEMultipart()
MSG['Subject'] = "Outage Restoration | " + POWER_UTILITY + " | Daily Forecast for " + DATE_1
MSG['From'] = EMAIL_FROM

# Defining final HTML Structure
HTML = """\
       <html>
       <style>
       table, th, td {{font-size:12pt; border:2px solid black; border-collapse:collapse; text-align:center;}}
       th, td {{padding: 8px;}}
       tr:nth-child(even) {{background: #dddddd;}}
       tr:hover {{background: silver; cursor: pointer;}}
       </style>

       <head>Hi,</head>
       <br></br>
       <body>
       Please find the daily forecasts from <b>Storm Preparedness System</b> and <b>Weather Source</b> for {day}, {D1} below- 
       <br></br>
       Forecasts from <b>Storm Preparedness System</b> for {D2}-
       </br>
       <br>
       <pre><p><span style="font-size:16pt;font-family:geneva;">Estimated # Outages:</span> <span style="font-size:18pt;font-family:geneva;">{Outage_LL} - {Outage_UL}</span>            <span style="font-size:16pt;font-family:geneva;">Estimated # Customers:</span> <span style="font-size:18pt;font-family:geneva;">{Cust_LL} - {Cust_UL}</span></p></pre>
       </br>
       Weather Forecast from <b>Weather Source API</b> for {D3}- 
       <br></br>
       {table}
       <br></br>
       <b>Highlights for the day-</b>
       </br>
       <ul>
       <li><b>Windspeeds</b> can reach upper mark of <b>{wind_max}</b> across <b>{max_wind_loc}</b></li>
       <li><b>{max_precip}</b> has the highest <b>precipitation probability</b> of <b>{precip_max}%</b></li>
       <li><b>Temperatures</b> across <b>{min_temp_loc}</b> can go as low as <b>{temp_min}</b></li>
       </ul>
       <br>
       Thanks,<br>
       <b>AES Digital Team</b>
       </body>
       </html>
       """.format(D1=DATE_1, D2=DATE_2, D3=DATE_2, Outage_LL=OUTAGES_LL_95,
                  Outage_UL=OUTAGES_UL_95, day=DAY, Cust_LL=CUSTOMERS_LL_95,
                  Cust_UL=CUSTOMERS_UL_95, wind_max=WIND_MAX, max_wind_loc=MAX_WIND_LOC,
                  max_precip=MAX_PRECIP_LOC, precip_max=MAX_PRECIP, temp_min=TEMP_MIN,
                  min_temp_loc=MIN_TEMP_LOC, table=WEATHER_TABLE)
logging.info('Completed email structure\n')

# Structuring as HTML
CONTENT = MIMEText(HTML, 'html')
MSG.attach(CONTENT)

# Setting up SMTP Server and sending out the email
SERVER = SMTP(SMTP_SERVER)
SERVER.sendmail(MSG['From'], EMAIL_LIST, MSG.as_string())
logging.info('Mail Sent to %s \n', str(EMAIL_LIST).replace(']', '').replace('[', ''))
