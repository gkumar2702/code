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
![Architecture diagram](https://gitlab.com/aes-digital/musigma/outage-restoration/-/raw/master/Project%20Details/Architecture_end_end.jpg?raw=true)

### Configuration file
config0002new.ini is the file used for saving the configurations of all the scripts that run in the ETR pipeline.
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
	




