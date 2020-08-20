"""
Authored by Sudheer

Test creation of ADS

"""
import os
import datetime
from airflow.models import Variable
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator)
from airflow.operators.email_operator import EmailOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow import models
from tempfile import NamedTemporaryFile
from typing import Optional, Union
from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dagrun_operator  import TriggerDagRunOperator
from airflow.contrib.operators import bigquery_to_gcs

# ===================Variables=================================
env = Variable.get("env")
print(env)
JOB_NAME = 'outage_END_END_ws'
PROJECT = 'aes-datahub-'+env
COMPOSER_NAME = 'composer-'+env
BUCKET='aes-datahub-0002-curated'
COMPOSER_BUCKET = 'us-east4-composer-0002-8d07c42c-bucket'
DATAPROC_BUCKET = 'aes-datahub-0002-temp'
#LANDING_BUCKET = '{}-landing'.format(PROJECT)
RAW_BUCKET = 'aes-datahub-'+env+'-raw'
cluster_name = 'outage-python-cluster-0002'
#subnetwork_uri= 'composer-'+env,
BQ_PROJECT="aes-analytics-0002"
BQ_DATASET="mds_outage_restoration"
BQ_TABLE_chvg="IPL_Live_Input_Master_Dataset_ws"
BQ_TABLE_final="IPL_LIVE_PREDICTIONS_ws"
BQ_TABLE_repo="IPL_PREDICTIONS_ws"
cluster_name_r = 'outage-r-cluster-0002'
email=['musigma.bkumar.c@aes.com']
bq_dataset_name ="mds_outage_restoration"
# intermediate_table = BQ_PROJECT+'.'+bq_dataset_name +'.IPL_Live_Master_Dataset'
# output_file='gs://aes-analytics-0002-curated/Outage_Restoration/Staging/IPL_Live_Master_Dataset.csv'

output_date=datetime.datetime.now().strftime("%Y%m%d")


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
start_time=datetime.datetime(2020,8,20,13,30,00)

# =================== DAG Arguments =================================
default_args = {
    'start_date': start_time,
    'email_on_failure': True,
    'email': ['musigma.bkumar.c@aes.com','ms.asingh.c@aes.com','ms.gkumar.c@aes.com','musigma.pshekar.c@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    # Consistent network configs for all tasks
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-c'
	}

# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=default_args,
        schedule_interval='*/30 * * * *'
) as dag:
  oms_live_dataset_collation= DataProcPySparkOperator(task_id='OMS_LIVE_DATA_COLLATION',
                                  main='gs://us-east4-composer-0002-8d07c42c-bucket/data/Outage_restoration/IPL/Python_scripts/omslive_dataset_collation_ws.py', 
                                  arguments=None, 
                                  archives=None, 
                                  pyfiles=None, 
                                  files=None, 
                                  #job_name='{{task.task_id}}_{{ds_nodash}}', 
                                  cluster_name='outage-python-cluster-0002', 
                                  dataproc_pyspark_properties=None, 
                                  dataproc_pyspark_jars=None, 
                                  gcp_conn_id='google_cloud_default', 
                                  delegate_to=None, 
                                  region='us-east4', 
                                  job_error_states=['ERROR'], 
                                  dag=dag 
                                  
                                  )
								  
  darksky_weather_data_collation= DataProcPySparkOperator(task_id='WEATHER_DATA_COLLATION',
                                  main='gs://us-east4-composer-0002-8d07c42c-bucket/data/Outage_restoration/IPL/Python_scripts/weathersource_data_collation.py', 
                                  arguments=None, 
                                  archives=None, 
                                  pyfiles=None, 
                                  files=None, 
                                  #job_name='{{task.task_id}}_{{ds_nodash}}', 
                                  cluster_name='outage-python-cluster-0002', 
                                  dataproc_pyspark_properties=None, 
                                  dataproc_pyspark_jars=None, 
                                  gcp_conn_id='google_cloud_default', 
                                  delegate_to=None, 
                                  region='us-east4', 
                                  job_error_states=['ERROR'], 
                                  dag=dag 
                                  
                                  )

  curated_dataset_creation= DataProcPySparkOperator(task_id='CURATED_DATASET_CREATION',
                                  main='gs://us-east4-composer-0002-8d07c42c-bucket/data/Outage_restoration/IPL/Python_scripts/curated_dataset_creation_ws.py', 
                                  arguments=None, 
                                  archives=None, 
                                  pyfiles=None, 
                                  files=None, 
                                  #job_name='{{task.task_id}}_{{ds_nodash}}', 
                                  cluster_name='outage-python-cluster-0002', 
                                  dataproc_pyspark_properties=None, 
                                  dataproc_pyspark_jars=None, 
                                  gcp_conn_id='google_cloud_default', 
                                  delegate_to=None, 
                                  region='us-east4', 
                                  job_error_states=['ERROR'], 
                                  dag=dag 
                                  
                                  )
  write_csv_to_bq  = GoogleCloudStorageToBigQueryOperator(
                                                                                                task_id='write_csv_to_bq',
                                                                                                bucket=BUCKET,
                                                                                                skip_leading_rows=1,
                                                                                                autodetect=True,
                                                                                                schema_fields=None,
                                                                                                #schema_object="Outage_Restoration/Schema/classification_schema.json",
                                                                                                source_objects=["Outage_Restoration/Staging/IPL_Live_Master_Dataset_ws.csv"],    
                                                                                                create_disposition='CREATE_IF_NEEDED',
                                                                                                destination_project_dataset_table=BQ_PROJECT+"."+BQ_DATASET+"."+BQ_TABLE_chvg,
                                                                                                write_disposition='WRITE_TRUNCATE'
                                                                                )
                                                                                
  Regression_code_run = BashOperator(
  task_id='Regression_model_run',
  bash_command = "gcloud beta dataproc jobs submit spark-r    /home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/Regression_Run_ws.R  --cluster=outage-r-cluster-0002 --region=us-east4",
  dag=dag
  )
  
  write_csv_to_bq2  = GoogleCloudStorageToBigQueryOperator(
                                                                                                task_id='write_csv_to_bq2',
                                                                                                bucket=BUCKET,
                                                                                                autodetect=True,
                                                                                                schema_fields=None,
                                                                                                #schema_object="Outage_Restoration/Schema/predictions_schema.json",
                                                                                                skip_leading_rows=1,
                                                                                                source_objects=["Outage_Restoration/Live_Data_Curation/TTR_Predictions/TTR_predictions_ws_"+output_date+".csv"],    
                                                                                                create_disposition='CREATE_IF_NEEDED',
                                                                                                destination_project_dataset_table=BQ_PROJECT+"."+BQ_DATASET+"."+BQ_TABLE_repo,
                                                                                                write_disposition='WRITE_APPEND'
                                                                                )

  write_csv_to_bq_live  = GoogleCloudStorageToBigQueryOperator(
                                                                                                task_id='write_csv_to_bq_live',
                                                                                                bucket=BUCKET,
                                                                                                autodetect = True,
                                                                                                skip_leading_rows=1,
                                                                                                source_objects=["Outage_Restoration/Live_Data_Curation/TTR_Predictions/TTR_predictions_ws_"+output_date+".csv"],    
                                                                                                create_disposition='CREATE_IF_NEEDED',
                                                                                                schema_fields=None,
                                                                                                destination_project_dataset_table=BQ_PROJECT+"."+BQ_DATASET+"."+BQ_TABLE_final,
                                                                                                write_disposition='WRITE_TRUNCATE'
                                                                                )
    # Create pipeline
oms_live_dataset_collation >> darksky_weather_data_collation >> curated_dataset_creation >> Regression_code_run >> [write_csv_to_bq, write_csv_to_bq2, write_csv_to_bq_live]