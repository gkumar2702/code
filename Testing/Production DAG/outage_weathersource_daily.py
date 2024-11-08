
"""
Authored by Sudheer
Updated: 23 Aug 2020
Tasks: Live data processing, weather data addition,
post processing and profiles addition, Regression run
Schedule: Everyday at 400 UTC
Description: Scheduled DAG to pull forecasted and actual weather data for locations
Environment: Composer-0002
Run-time environments: Python and SparkR
"""

import datetime
from airflow.models import Variable, DAG
from airflow.contrib.operators.dataproc_operator import (
    DataProcPySparkOperator)
from airflow.operators.bash_operator  import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# ===================Variables=================================
ENV = Variable.get("env")
print(ENV)
JOB_NAME = 'outage-weathersource-data-pull-0002'
PROJECT = 'aes-datahub-'+ENV
COMPOSER_NAME = 'composer-'+ENV
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
BUCKET = 'aes-analytics-0002-curated'
BQ_PROJECT = "aes-analytics-0002"
BQ_DATASET = "mds_outage_restoration"
BQ_TABLE_REPO = "IPL_Forecasted_Storm_Profiles"
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': True,
    'email': ['musigma.bkumar.c@aes.com', 'eric.nussbaumer@aes.com', 'ms.gkumar.c@aes.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
    # Consistent network configs for all tasks
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-c',
    'labels': {'resource-owner': 'datascience',
               'financial-identifier': 'digital'}}

# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval='0 4 * * *'
) as dag:

    WEATHERSOURCE_IPL = DataProcPySparkOperator(
        task_id='WeatherSource_datapull_IPL',
        main='/home/airflow/gcs/data/Outage_restoration/IPL/Python_scripts/weathersource_script_pylint.py',
        arguments=None,
        archives=None,
        pyfiles=None,
        files=None,
        cluster_name='dp-outage-python-0002',
        dataproc_pyspark_properties=None,
        dataproc_pyspark_jars=None,
        gcp_conn_id='google_cloud_default',
        delegate_to=None,
        region='us-east4',
        job_error_states=['ERROR'],
        dag=dag)

    CLUSTER_CLASSIFICATION = BashOperator(
        task_id='Storm_Profiles_Classification',
        bash_command="gcloud beta dataproc jobs submit spark-r    "\
            "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/cluster_classification_R.R  "\
            "--cluster=dp-outage-r-0002 --region=us-east4",
        dag=dag)

    CLUSTER_CLASSIFICATION_FORECAST = BashOperator(
        task_id='Storm_Profiles_Classificatiion_Forecast',
        bash_command="gcloud beta dataproc jobs submit spark-r    "\
            "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/classification_script.R  "\
        "--cluster=dp-outage-r-0002 --region=us-east4",
        dag=dag)

    WRITE_CSV_TO_BQ = GoogleCloudStorageToBigQueryOperator(
        task_id='Append_forecasted_profiles',
        bucket=BUCKET,
        schema_object="Outage_Restoration/Schema/ipl_personalities.json",
        skip_leading_rows=1,
        source_objects=["Outage_Restoration/Live_Data_Curation/Forecast_Storm_Profiles/"\
		                 "forecast_storm_profiles.csv"],
        create_disposition='CREATE_IF_NEEDED',
        destination_project_dataset_table=BQ_PROJECT+"."+BQ_DATASET+"."+BQ_TABLE_REPO,
        write_disposition='WRITE_APPEND')

#Pipeline
WEATHERSOURCE_IPL >> CLUSTER_CLASSIFICATION >> CLUSTER_CLASSIFICATION_FORECAST >> WRITE_CSV_TO_BQ
