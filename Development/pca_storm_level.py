"""
Pre-process dataset and perform PCA on it
"""

import logging
#import datetime as dt
from datetime import date, timedelta
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
logging.basicConfig(level=logging.INFO)
# logging.disable(logging.CRITICAL)

SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)

# In[2]:


## Dummifciation of new data
### Importing dataset

CURRENT_DATE = date.today()
logging.info(CURRENT_DATE)
YEAR_MONTH = pd.to_datetime(CURRENT_DATE).strftime('%Y-%m')
logging.info(YEAR_MONTH)
TODAY = pd.to_datetime(CURRENT_DATE).strftime('%Y-%m-%d')
logging.info(TODAY)
TOMORROW1 = CURRENT_DATE + timedelta(1)
logging.info(TOMORROW1)
TOMORROW = TOMORROW1.strftime('%Y%m%d')
logging.info(TOMORROW)
DAYAFTER1 = CURRENT_DATE + timedelta(2)
logging.info(DAYAFTER1)
DAYAFTER = DAYAFTER1.strftime('%Y%m%d')
logging.info(DAYAFTER)

YEAR_MONTH_1 = pd.to_datetime(TOMORROW).strftime('%Y-%m')
logging.info(YEAR_MONTH_1)
YEAR_MONTH_2 = pd.to_datetime(DAYAFTER).strftime('%Y-%m')
logging.info(YEAR_MONTH_2)

PATH1 = "gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/"\
        + YEAR_MONTH_1 + "/forecast_data/" + TODAY + "/weathersource_daily_" + TOMORROW + ".csv"
logging.info(PATH1)
PATH2 = "gs://aes-datahub-0001-raw/Weather/weather_source/USA/Indianapolis/"\
        + YEAR_MONTH_2 + "/forecast_data/" + TODAY + "/weathersource_daily_" + DAYAFTER + ".csv"
logging.info(PATH2)

#Reading the forecast files
NEW_DATA = SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option(
    "delimiter", ",").load(PATH1, index_col=0).toPandas()
NEW_DATA.reset_index(drop=True, inplace=True)
logging.info(NEW_DATA.shape)
NEW_DATA2 = SPARK.read.format('CSV').option("header", "true").option("inferSchema", "true").option(
    "delimiter", ",").load(PATH2, index_col=0).toPandas()
NEW_DATA2.reset_index(drop=True, inplace=True)
logging.info(NEW_DATA2.shape)

# Reading the storm data
STORM_DATA = SPARK.read.format('CSV').option("header", "true").option(
    "inferSchema", "true").option("delimiter", ",").load(
        'gs://aes-analytics-0001-curated/Outage_Restoration/Live_Data_Curation/Outage_Prediction/'\
        'Storm_ID_level_data.csv', index_col=0).toPandas()
logging.info(STORM_DATA.shape)


# In[3]:


def preprocess_data(new_data):
    """

    Preprocessing the data

    """
    new_data = new_data.drop(['latitude', 'longitude', 'timestampInit'], axis=1)
    new_data['Location'] = new_data['Location'].str.replace(' ', '')

    # Renaming Columns
    new_data.rename(columns={"timestamp": "Date"}, inplace=True)

    # Selecting required variables
    new_data = new_data[['cldCvrAvg', 'cldCvrMax', 'cldCvrMin', 'dewPtAvg', 'dewPtMax', 'dewPtMin',
                         'feelsLikeAvg', 'feelsLikeMax',
                         'feelsLikeMin', 'heatIndexAvg', 'heatIndexMax', 'heatIndexMin',
                         'mslPresAvg', 'mslPresMax', 'mslPresMin', 'precip', 'radSolarAvg',
                         'radSolarMax', 'radSolarTot', 'relHumAvg', 'relHumMax', 'relHumMin',
                         'sfcPresAvg', 'sfcPresMax', 'sfcPresMin',
                         'snowfall', 'spcHumAvg', 'spcHumMax', 'spcHumMin', 'tempAvg',
                         'tempMax', 'tempMin', 'Date', 'wetBulbAvg', 'wetBulbMax', 'wetBulbMin',
                         'windChillAvg', 'windChillMax', 'windChillMin', 'windDir100mAvg',
                         'windDir80mAvg', 'windDirAvg', 'windSpd100mAvg', 'windSpd100mMax',
                         'windSpd100mMin', 'windSpd80mAvg', 'windSpd80mMax', 'windSpd80mMin',
                         'windSpdAvg', 'windSpdMax', 'windSpdMin', 'Location']]


    # Converting to day level date
    new_data['Date'] = pd.to_datetime(new_data['Date']).dt.strftime('%Y-%m-%d')

    return new_data

def separate_different_markers(new_data):
    """

    Separate different markers

    """

    req_cols = ['Date', 'cldCvrAvg', 'cldCvrMax', 'cldCvrMin', 'dewPtAvg', 'dewPtMax',
                'dewPtMin', 'feelsLikeAvg', 'feelsLikeMax',
                'feelsLikeMin', 'heatIndexAvg', 'heatIndexMax', 'heatIndexMin',
                'mslPresAvg', 'mslPresMax', 'mslPresMin', 'precip', 'radSolarAvg',
                'radSolarMax', 'radSolarTot', 'relHumAvg', 'relHumMax', 'relHumMin', 'sfcPresAvg',
                'sfcPresMax', 'sfcPresMin', 'snowfall', 'spcHumAvg', 'spcHumMax', 'spcHumMin',
                'tempAvg', 'tempMax', 'tempMin', 'wetBulbAvg', 'wetBulbMax', 'wetBulbMin',
                'windChillAvg', 'windChillMax', 'windChillMin', 'windDir100mAvg',
                'windDir80mAvg', 'windDirAvg', 'windSpd100mAvg', 'windSpd100mMax',
                'windSpd100mMin', 'windSpd80mAvg', 'windSpd80mMax', 'windSpd80mMin',
                'windSpdAvg', 'windSpdMax', 'windSpdMin']

    marker1 = new_data[new_data.Location == 'Marker1'][req_cols]
    marker2 = new_data[new_data.Location == 'Marker2'][req_cols]
    marker3 = new_data[new_data.Location == 'Marker3'][req_cols]
    marker4 = new_data[new_data.Location == 'Marker4'][req_cols]
    marker5 = new_data[new_data.Location == 'Marker5'][req_cols]
    marker6 = new_data[new_data.Location == 'Marker6'][req_cols]
    marker7 = new_data[new_data.Location == 'Marker7'][req_cols]
    marker8 = new_data[new_data.Location == 'Marker8'][req_cols]
    marker9 = new_data[new_data.Location == 'Marker9'][req_cols]
    marker10 = new_data[new_data.Location == 'Marker10'][req_cols]
    marker11 = new_data[new_data.Location == 'Marker11'][req_cols]
    marker12 = new_data[new_data.Location == 'Marker12'][req_cols]
    marker13 = new_data[new_data.Location == 'Marker13'][req_cols]
    marker14 = new_data[new_data.Location == 'Marker14'][req_cols]
    marker15 = new_data[new_data.Location == 'Marker15'][req_cols]
    marker16 = new_data[new_data.Location == 'Marker16'][req_cols]
    marker17 = new_data[new_data.Location == 'Marker17'][req_cols]
    marker18 = new_data[new_data.Location == 'Marker18'][req_cols]
    marker19 = new_data[new_data.Location == 'Marker19'][req_cols]
    marker20 = new_data[new_data.Location == 'Marker20'][req_cols]

    return (marker1, marker2, marker3, marker4, marker5, marker6, marker7, marker8, marker9,
            marker10, marker11, marker12, marker13, marker14, marker15, marker16, marker17,
            marker18, marker19, marker20)

def rename_markers(marker1, marker2, marker3, marker4, marker5, marker6, marker7, marker8, marker9,
                   marker10, marker11, marker12, marker13, marker14, marker15, marker16, marker17,
                   marker18, marker19, marker20):
    """

    Renaming the markers

    """
    # Data
    location = ['MARKER1', 'MARKER2', 'MARKER3', 'MARKER4', 'MARKER5', 'MARKER6', 'MARKER7',
                'MARKER8', 'MARKER9', 'MARKER10',
                'MARKER11', 'MARKER12', 'MARKER13', 'MARKER14', 'MARKER15', 'MARKER16',
                'MARKER17', 'MARKER18', 'MARKER19', 'MARKER20']

    marker_name = [marker1, marker2, marker3, marker4, marker5, marker6, marker7, marker8, marker9,
                   marker10, marker11, marker12, marker13, marker14, marker15, marker16, marker17,
                   marker18, marker19, marker20]


    for i, j in enumerate(marker_name):
        j.rename(columns={"cldCvrAvg": location[i]+"_cldCvrAvg",
                          "cldCvrMax": location[i]+"_cldCvrMax",
                          "cldCvrMin": location[i]+"_cldCvrMin",

                          "dewPtAvg": location[i]+"_dewPtAvg",
                          "dewPtMax": location[i]+"_dewPtMax",
                          "dewPtMin": location[i]+"_dewPtMin",

                          "feelsLikeAvg": location[i]+"_feelsLikeAvg",
                          "feelsLikeMax": location[i]+"_feelsLikeMax",
                          "feelsLikeMin": location[i]+"_feelsLikeMin",

                          "heatIndexAvg": location[i]+"_heatIndexAvg",
                          "heatIndexMax": location[i]+"_heatIndexMax",
                          "heatIndexMin": location[i]+"_heatIndexMin",

                          "mslPresAvg": location[i]+"_mslPresAvg",
                          "mslPresMax": location[i]+"_mslPresMax",
                          "mslPresMin": location[i]+"_mslPresMin",

                          "precip": location[i]+"_precip",

                          "radSolarAvg": location[i]+"_radSolarAvg",
                          "radSolarMax": location[i]+"_radSolarMax",

                          "radSolarTot": location[i]+"_radSolarTot",

                          "relHumAvg": location[i]+"_relHumAvg",
                          "relHumMax": location[i]+"_relHumMax",
                          "relHumMin": location[i]+"_relHumMin",

                          "sfcPresAvg": location[i]+"_sfcPresAvg",
                          "sfcPresMax": location[i]+"_sfcPresMax",
                          "sfcPresMin": location[i]+"_sfcPresMin",

                          "snowfall": location[i]+"_snowfall",

                          "spcHumAvg": location[i]+"_spcHumAvg",
                          "spcHumMax": location[i]+"_spcHumMax",
                          "spcHumMin": location[i]+"_spcHumMin",

                          "tempAvg": location[i]+"_tempAvg",
                          "tempMin": location[i]+"_tempMin",
                          "tempMax": location[i]+"_tempMax",

                          "wetBulbAvg": location[i]+"_wetBulbAvg",
                          "wetBulbMax": location[i]+"_wetBulbMax",
                          "wetBulbMin": location[i]+"_wetBulbMin",

                          "windChillAvg": location[i]+"_windChillAvg",
                          "windChillMax": location[i]+"_windChillMax",
                          "windChillMin": location[i]+"_windChillMin",

                          "windDir100mAvg": location[i]+"_windDir100mAvg",
                          "windDir80mAvg": location[i]+"_windDir80mAvg",
                          "windDirAvg": location[i]+"_windDirAvg",

                          "windSpd100mAvg": location[i]+"_windSpd100mAvg",
                          "windSpd100mMax": location[i]+"_windSpd100mMax",
                          "windSpd100mMin": location[i]+"_windSpd100mMin",

                          "windSpd80mAvg": location[i]+"_windSpd80mAvg",
                          "windSpd80mMax": location[i]+"_windSpd80mMax",
                          "windSpd80mMin": location[i]+"_windSpd80mMin",

                          "windSpdAvg": location[i]+"_windSpdAvg",
                          "windSpdMax": location[i]+"_windSpdMax",
                          "windSpdMin": location[i]+"_windSpdMin",
                          }, inplace=True)

    return (marker1, marker2, marker3, marker4, marker5, marker6, marker7, marker8, marker9,
            marker10, marker11, marker12, marker13, marker14, marker15, marker16, marker17,
            marker18, marker19, marker20)

def merge_markers_dataframe(marker1, marker2, marker3, marker4, marker5, marker6, marker7, marker8,
                            marker9, marker10,
                            marker11, marker12, marker13, marker14, marker15, marker16, marker17,
                            marker18, marker19, marker20):
    """

    Merges the marker dataframe

    """
    final_ads_1 = pd.merge(marker1, marker2, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker3, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker4, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker5, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker6, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker7, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker8, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker9, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker10, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker11, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker12, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker13, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker14, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker15, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker16, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker17, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker18, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker19, how='left', on=['Date'])
    final_ads_1 = pd.merge(final_ads_1, marker20, how='left', on=['Date'])

    return final_ads_1


# In[4]:


# ### Dummified data
DF1 = preprocess_data(NEW_DATA)
MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20 = separate_different_markers(DF1)
MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20 = rename_markers(
    MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20)
FINAL_DF1 = merge_markers_dataframe(MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20)

DF2 = preprocess_data(NEW_DATA2)
MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20 = separate_different_markers(DF2)
MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20 = rename_markers(
    MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20)
FINAL_DF2 = merge_markers_dataframe(MR1, MR2, MR3, MR4, MR5, MR6, MR7, MR8, MR9, MR10, MR11, MR12, MR13, MR14, MR15, MR16, MR17, MR18, MR19, MR20)


# PCA on Storm Data
STORM_DATA = STORM_DATA.loc[:, ~STORM_DATA.columns.str.contains('^Unnamed')]
STORM_DATA = STORM_DATA.loc[:, ~STORM_DATA.columns.str.contains('^_c0')]
STORM_DATA_DIMS = STORM_DATA.copy(deep=True)
STORM_DATA_DIMS.drop(['MAJ_OTG_ID', 'Date', 'OUTAGES', 'CUST_QTY', 'STORM_DURATION',
                      'OUTAGED_RECOVERY', 'MARKER1_radSolarMin', 'MARKER2_radSolarMin',
                      'MARKER3_radSolarMin', 'MARKER4_radSolarMin', 'MARKER5_radSolarMin',
                      'MARKER6_radSolarMin', 'MARKER7_radSolarMin',
                      'MARKER8_radSolarMin', 'MARKER9_radSolarMin', 'MARKER10_radSolarMin',
                      'MARKER11_radSolarMin', 'MARKER12_radSolarMin',
                      'MARKER13_radSolarMin', 'MARKER14_radSolarMin', 'MARKER15_radSolarMin',
                      'MARKER16_radSolarMin', 'MARKER17_radSolarMin',
                      'MARKER18_radSolarMin', 'MARKER19_radSolarMin',
                      'MARKER20_radSolarMin'], axis=1, inplace=True)

# Calculating mean and standard deviation
MEANDATA = STORM_DATA_DIMS.mean(axis=0, skipna=True)
STDDATA = STORM_DATA_DIMS.std(axis=0, skipna=True)

# Storing required features
FEATURES = list(STORM_DATA_DIMS.columns)

# Setting standar scaler
SCALER = StandardScaler()

# Fitting on dataset
logging.info(STORM_DATA_DIMS.shape)
SCALER.fit(STORM_DATA_DIMS)

# Transforming dataframe
SCALED_DATA = SCALER.transform(STORM_DATA_DIMS)

# Data after scaling
STORM_DATA_DIMS_SCALED = pd.DataFrame(SCALED_DATA, columns=FEATURES)
round(STORM_DATA_DIMS_SCALED.describe(), 2)

# PCA
COVAR_MATRIX = PCA()

# Fitting Scaled data into covariance matrix
COVAR_MATRIX.fit(SCALED_DATA)

# Crosscheck values whether variance is 88.9 or not
VARIANCE = COVAR_MATRIX.explained_variance_ratio_
VAR = np.cumsum(np.round(COVAR_MATRIX.explained_variance_ratio_, decimals=3)*100)

def pcafunc(dff):
    """

    Doing the pca for the data

    """
    ## Scaling the new data
    for x in range(1, 1000):
        colname = dff.columns[x]
        val_a = MEANDATA.loc[colname]
        val_b = STDDATA.loc[colname]
        #STORM_DATA_DIMS.iloc[]
        val_d = dff.at[0, colname]
        val_e = (val_d-val_a)/val_b
        dff[colname] = dff[colname].replace([val_d], val_e)

    ## storing date and dropping date column
    timestamp = dff.at[0, 'Date']
    #print(timestamp)
    dff = dff.drop(['Date'], axis=1)
    logging.info(dff.shape)

    ## PCA on new data
    newdata_transformed = COVAR_MATRIX.transform(dff)
    # Transforming to a dataframe
    newdata_transformed = pd.DataFrame(newdata_transformed)
    newdata_transformed = newdata_transformed.iloc[:, 0:7]
    newdata_transformed.columns = ['PC1', 'PC2', 'PC3', 'PC4', 'PC5', 'PC6', 'PC7']
    newdata_transformed.head()

    ## Adding date column in data
    newdata_transform = newdata_transformed
    newdata_transform['Date'] = timestamp
    return newdata_transform

logging.info("PCA FUNCTION IS CREATED")

PCA1 = pcafunc(FINAL_DF1)
PCA2 = pcafunc(FINAL_DF2)

OP_PATH1 = 'gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Outage_Duration/'\
           + YEAR_MONTH + '/' + TODAY + '/' + 'PCA1.csv'
OP_PATH2 = 'gs://aes-analytics-0001-curated/Outage_Restoration/OMS/Deliverables/Outage_Duration/'\
           + YEAR_MONTH + '/' + TODAY + '/' + 'PCA2.csv'

PCA1.to_csv(OP_PATH1, index=False)
PCA2.to_csv(OP_PATH2, index=False)
logging.info("Task Completed")
