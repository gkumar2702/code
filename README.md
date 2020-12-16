### Problem Definition
The project is a two-pronged effort, led by **the Indianapolis, Indiana and Dayton, Ohio Trade & Distribution teams**.  This section consists of analysis involved in optimizing scheduling of repairs, preparation for storm events, and the estimation of **times to restoration.** 

### Model Building
Across all model levels, training was done on actual data (actual weather variables, actual consumer demand, actual number of outages etc.). The following model were used
1.	Linear Regression<br> 
2.	Random Forest<br>
3.	eXtreme Gradient Boosting (XGBoost) <br>
4.	Robust Regression <br>
5.	Multivariate Adaptive Regression model (MARS) <br>
6.	Generalized Additive Model Regression (GAM)<br>
7.  Light GBM
8.  Catboost model

### Milestones Delivered
-	Dashboard for Weather Event Monitoring <br>
-	Storm Level Models to Predict No of customers and No of Outages during a weather events automation <br>
-   One day ahead and two ahead predictions of Weather profiles across  <br>
-   Weather Profiles model for day ahead and two day ahead predictions automation for various landmarks in Indianaspolis <br>
-   Estimated Restoration Time prediction model giving predictions for Outages coming across Outage Management System (OMS) <br>
-   ETR and Storm Model Diagnostic with dahsboard using business firendly metrics on a daily basis

### Architecture Diagram
![Architecture diagram Storm](https://gitlab.com/aes-digital/musigma/outage-restoration/-/raw/master/Project%20Details/Architecture_storm_level.jpg?raw=true)
![Architecture diagram ETR](https://gitlab.com/aes-digital/musigma/outage-restoration/-/raw/master/Project%20Details/Architecture_ETR.jpg?raw=true)
### Configuration file
- We have 2 config files 
        - config_ETR.ini
        - config_storm.ini
### config_ETR.ini       
config_ETR.ini is the file used for saving the configurations of all the scripts that run in the ETR pipeline.
Each section in the configuration file point to a python script as below:
##### Settings:
project_id – Name of the Project<br>
raw_bucket_name – Folder of raw data files<br>
raw_bucket_location – Location of raw data files<br>
curated_bucket_name – Folder of curated ADS<br>
curated_bucket_location – Path of curated ADS<br>
bq_curated_dataset – BigQuery table for saving curated ADS<br>
bq_ipl_live_predictions- BigQuery table for saving live predictions<br>
bq_ipl_predictions - BigQuery table containing historic predictions<br>
###### Live OMS:
oms_last_file_read_name - Name of last file read<br>
occurn_remov – removing redundant files<br>
cat_list – List of categorical columns<br>
marker_location_csv – Location of Marker Location CSV<br>
clue_mapping_csv – Location of Clue Mapping CSV<br>
occurn_mapping_csv – Location of Occurrence Mapping CSV<br>
causemapping_csv – Location of Cause of Outage CSV<br>
live_oms_staging_path – Path for writing preprocessed dataset<br>
##### Data Collation
data_collation_staging_path - Path to read the preprocessed dataset<br>
ws_query – Query to fetch weather forecast data from weathersource bigquery table using date and Marker location<br>
oms_live_path – Path for writing the file after adding weather features<br>
##### Curated Data 
curated_data_staging_path – Path to read the dataset with added weather features<br>
ipl_predictions_query – Query to fetch outages from IPL Predictions BigQuery table<br>
clue_count_csv – Location of clue count CSV<br>
flag_list - List of features containing Flag<br>
pred_outages_query – Query to fetch historical predictions from IPL Predictions BigQuery table<br>
live_outages_backup_csv – Path to save a backup of the dataset as CSV<br>
live_outages_path – Path to write the curated dataset<br>
##### Load and Predict
staging_bucket – Path to read the curated dataset<br>
storm_profile_bucket – Path to read the storm profiles<br>
model_location – Path of the model <br>
model_features – Path to read CSV containing features to be used in the model<br>
prediction_live - Path to write the live predictions as CSV<br>
prediction_backup – Path to write a backup of live predictions<br>
#### Settings Diagnostic View
bucket_name_raw = Folder of raw data files<br>
bucket_name = Name of folder of raw data files<br>
prefix = Prefix for Diagnostic file path<br>
project_id = Project ID <br>
op_bq_schema = Output Bigquery Schema<br>
op_bq_tab = Output Bigquery table<br>
#### Diagnostic View 
numerical_cols = Numerical columns in the dataset<br>
numerical_cols_etr = Numerical columns including <br>
df_pred = Bigquery table to read the predictions<br>
df_diag = Bigquery table to read diagnostic data <br>
occurn_remov = Occurances to be removed from the dataset<br>
#### Storm level comparison
op_bq_schema = Output bigquery schema<br>
op_bq_tab = Output bigquery table<br>
df_pred = Query to read predictions from Bigquery table<br>
#### File Check settings
bucket_name_raw = Raw bucket name<br>
prefix = Prefix to read the files from<br>
bucket_name = Location of raw bucket<br>
#### File check paths
last_file_read_path = Location of last file read<br>
current_file_read_path = Location of current path<br>

#### config_storm.ini
config_storm.ini is the file used for saving the configurations of all the scripts that run in the Storm level pipeline.<br>
Each section in the configuration file point to a python script as below:<br>
#### Storm settings
project_id = Project ID to access BQ tables <br>
raw_bucket_name = Name of the raw bucket<br>
raw_bucket_location = Location of the raw bucket<br>
curated_bucket_name = Name of curated dataset<br>
curated_bucket_location = Location of curated dataset<br>
bq_curated_dataset = Bigquery curated dataset table <br>
bq_ipl_live_predictions = Live BQ table for IPL Predictions<br>
bq_ipl_predictions = BQ table for historical IPL Predictions<br>
#### PCA Calculation
weather_query = Query to read weather data from Bigquery table<br>
weather_query_2 = Query to read weather data from Bigquery table<br>
storm_id_level_data = Location for reading storm ID level<br>
pca_output_path = Location for saving PCA output<br>
PROJECT_ID = Project ID for accessing BQ tables<br>
#### Output Collation
OP_PATH = Location to read the output files from<br>
OP_BQ_SCHEMA = BQ schema name for saving output<br>
OP_BQ_TAB = BQ table to save output<br>
PROJECT_ID = Project ID for accessing BQ tables<br>
#### Settings for Weather daily
PROJECT_ID = Project ID for accessing BQ tables<br>
HIST_OP = Location for saving historical weather forecasts<br>
FORC_OP = Location to save Forecasts daily<br>
WS_LOCATION = Query to read weather data from BQ tables<br>
#### Forecast email alert
power_utility = Division of Power utility<br>
project_id = Project ID for accessing BQ tables<br>
dataset_preparedness = BQ Schema <br>
table_preparedness = Bigquery table<br>
forecast_type = One day ahead or two day ahead<br>
forecast_level_column = Number_of_days_ahead<br>
dataset_weather = Weather source schema <br>
table_weather_daily = Table name for reading weather data <br>
landmark_mapping = Location to read Lanmark Mapping CSV<br>
email_recipients = Email IDs which will receive the email alert<br>
email_from = Email ID from which the mail will be sent<br>
smtp_server = Server address for sending the mail<br>



