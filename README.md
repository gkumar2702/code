### Problem Definition
The project is a two-pronged effort, led by **the Indianapolis, Indiana and Dayton, Ohio Trade & Distribution teams**.  This section consists of analysis involved in optimizing scheduling of repairs, preparation for storm events, and the estimation of **times to restoration.** 

### Approach
#### Live data Preprocessing
Preprocessing is the point at which the team has decided what assumptions will be used in the creation of the curated dataset and filters are created to reflect these assumptions in the analytical dataset. Live data is fetched from various folders and collated into one dataset.
#### Data Collation
Here we add weather features to the dataset using Location and Date. It is fetched from an external weather source on daily forecast level.
#### Curated Dataset creation
More of the required features are added to the dataset. Here the features are engineered in order to fit the model requirement. Feature selection is the process where you statistically or manually select those features which contribute the most to your prediction variable, only the features required for the model are processed for next steps.
#### Load and Predict
In this step, we take the curated data set consisting of live outage data and make predictions on them and save it in a bigquery table.
### Model Building
Across all model levels, training was done on actual data (actual weather variables, actual consumer demand, actual number of outages etc.). The following model were used
1.	Linear Regression<br> 
2.	Random Forest<br>
3.	eXtreme Gradient Boosting (XGBoost) <br>
4.	Robust Regression <br>
5.	Multivariate Adaptive Regression model (MARS) <br>
6.	Generalized Additive Model Regression (GAM)<br>

### Milestones Delivered
-	Dashboard for weather profiles created<br>
-	Live pipeline for day ahead and two day ahead predictions automation

### Architecture Diagram
![Architecture diagram](https://gitlab.com/aes-digital/musigma/outage-restoration/-/raw/master/Project%20Details/Architecture_end_end.jpg?raw=true)

### Configuration file
config0002.ini is the file used for saving the configurations of all the scripts that run in the pipeline.
Each section in the configuration file point to a python script as below:
##### Settings:
project_id – Name of the Project<br>
raw_bucket_name – Folder of raw data files<br>
raw_bucket_location – Location of raw data files<br>
curated_bucket_name – Folder of curated ADS<br>
curated_bucket_location – Path of curated ADS<br>
bq_curated_dataset – BigQuery table for saving curated ADS<br>
bq_ipl_live_predictions- BigQuery table for saving live predictions<br>
bq_ipl_predictions = BigQuery table containing historic predictions<br>
###### Live OMS:
oms_last_file_read_name = Name of last file read<br>
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
flag_list = List of features containing Flag<br>
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
	




