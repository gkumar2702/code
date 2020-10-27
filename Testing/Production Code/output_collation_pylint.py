""""
Script for collating the output
"""

#!/usr/bin/env python
# coding: utf-8

#Importing the required libraries
from datetime import date, datetime
import warnings
import pandas as pd
warnings.filterwarnings("ignore")

#Reading the input files

PATH = "='gs://us-east4-composer-0001-40ca8a74-bucket/data/Outage_restoration/IPL/"

DF1 = pd.read_csv(PATH + "STORM_DURATION/Duration_Prediction.csv")
DF2 = pd.read_csv(PATH + "NUMBER_OF_OUTAGES/Outages_Prediction.csv")
DF3 = pd.read_csv(PATH + "CUSTOMER_QUANTITY/Predicted_Cust_Qty.csv")
DF4 = pd.read_csv(PATH + "RECOVERY_DURATION/Predicted_Recovery_Duration.csv")

#Merging the input files

FINAL_DF = DF1.merge(DF2, how='left', left_on='Date', right_on='Date')
FINAL_DF = FINAL_DF.merge(DF3, how='left', left_on='Date', right_on='Date')
FINAL_DF = FINAL_DF.merge(DF4, how='left', left_on='Date', right_on='Date')

# Renaming the columns

FINAL_DF.rename(columns={'predicted_DURATION':'Storm_duration_in_hours',
                         'Predicted_Cust_Qty':'Customer_quantity',
                         'Predicted_Recovery_Duration_in_hours':'Recovery_duration_in_hours',
                         'predicted_Number_of_OUTAGES':'Number_of_outages'}, inplace=True)

#Creating the Number of days ahead column

CURRENT_DATE = date.today()
FINAL_DF['Today'] = CURRENT_DATE
FINAL_DF['Number_of_days_ahead'] = 0

for i in FINAL_DF.index:
    FINAL_DF['Date'][i] = datetime.strptime(FINAL_DF['Date'][i], '%Y-%m-%d').date()
    FINAL_DF['Number_of_days_ahead'][i] = (FINAL_DF['Date'][i] - FINAL_DF['Today'][i]).days

#Replacing 1 & 2 with 'One day ahead' and 'Two day ahead'
FINAL_DF.loc[(FINAL_DF.Number_of_days_ahead == 1), 'Number_of_days_ahead'] = 'One day ahead'
FINAL_DF.loc[(FINAL_DF.Number_of_days_ahead == 2), 'Number_of_days_ahead'] = 'Two days ahead'

# Keeping only required columns and making changes to the order of the columns

FINAL_DF = FINAL_DF.drop(['Today'], axis=1)
FINAL_DF = FINAL_DF[['Date', 'Number_of_days_ahead', 'Storm_duration_in_hours',
                     'Number_of_outages', 'Customer_quantity', 'Recovery_duration_in_hours']]

#Writing the output to the big query table

FINAL_DF.to_gbq('mds_outage_restoration.IPL_Storm_Preparations', project_id='aes-analytics-0002',
                chunksize=None, reauth=False, if_exists='append', auth_local_webserver=False)
