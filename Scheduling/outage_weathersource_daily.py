'''
Authored by Musigma
Updated: 15 Dec 2020
Tasks: Live data processing, weather data addition,
post processing and profiles addition, Regression run
- Changed the weather source
Schedule: Everyday at 400 UTC
Description: Scheduled DAG to pull forecasted and actual weather data for locations
Environment: Composer-0001
Run-time environments: Python and SparkR
'''

# standard library import
import datetime
from airflow.models import Variable, DAG
from airflow.contrib.operators.dataproc_operator import (
    DataProcPySparkOperator)
from airflow.operators.bash_operator  import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# ===================Variables=================================
ENV = Variable.get("env")
# name of the job performed in airflow
JOB_NAME = 'outage-weathersource-data-pull-0001'
# name of the cluster
CLUSTER_NAME = 'dp-outage-python-0001'
# name of the composer to be used
COMPOSER_NAME = 'composer-'+ENV
# name of bucket
BUCKET = 'aes-analytics-0001-curated'

# name of bigQuery project
BQ_PROJECT = "aes-analytics-0001"
# name of bigQuery dataset
BQ_DATASET = "mds_outage_restoration"
# name of bigQuery table used
BQ_TABLE_REPO = "IPL_Forecasted_Storm_Profiles"

# Location of python scripts
SCRIPT_LOC_PY= '/home/airflow/gcs/data/Outage_restoration/IPL/Python_scripts/'
# Location of R scripts
SCRIPT_LOC_BASH_R = "gcloud beta dataproc jobs submit spark-r    "\
                    "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/"

# Location of R cluster
SCRIPT_LOC_BASH_CLUSTER = " --cluster=dp-outage-r-0001 --region=us-east4"
# specify location of the config file
CONFIG_FILE = 'gs://us-east4-composer-0001-40ca8a74-bucket/data/'\
              'Outage_restoration/IPL/Config_Files/config_storm.ini'
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': True,
    'email': ['musigma.bkumar.c@aes.com', 'eric.nussbaumer@aes.com', 'ms.gkumar.c@aes.com',
              'musigma.dchauhan.c@aes.com', 'musigma.skumar.c@aes.com',
              'musigma.aaggarwal.c@aes.com'],
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
        main=SCRIPT_LOC_PY + 'weathersource_script_pylint.py',
        cluster_name=CLUSTER_NAME,
        gcp_conn_id='google_cloud_default',
        region='us-east4',
        job_error_states=['ERROR'],
        dag=dag,
        files=CONFIG_FILE,
        arguments=None,
        archives=None,
        pyfiles=None,
        dataproc_pyspark_properties=None,
        dataproc_pyspark_jars=None,
        delegate_to=None)

    CLUSTER_CLASSIFICATION = BashOperator(
        task_id='Storm_Profiles_Classification',
        bash_command=SCRIPT_LOC_BASH_R + "cluster_classification_R.R  "+
                     SCRIPT_LOC_BASH_CLUSTER,
                     dag=dag)

    CLUSTER_CLASSIFICATION_FORECAST = BashOperator(
        task_id='Storm_Profiles_Classificatiion_Forecast',
        bash_command=SCRIPT_LOC_BASH_R + "classification_script.R  "+
                     SCRIPT_LOC_BASH_CLUSTER,
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

# Pipeline
WEATHERSOURCE_IPL >> CLUSTER_CLASSIFICATION >> CLUSTER_CLASSIFICATION_FORECAST >> WRITE_CSV_TO_BQ
