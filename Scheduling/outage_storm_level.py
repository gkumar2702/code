'''
Author - Mu Sigma
Updated: 15 Dec 2020
Version: 2
Tasks: PCA calculation, Number of Outages, Outage Duration, 
Customer Quantity, Recovery Duration, Output Collation
Schedule: At the end of every day
Description: Dag to predict the number of outages, number of customers,
storm recovery and storm duration
Environment: Composer-0001
Run-time environments: Pyspark,SparkR and python 3.7 callable
'''

# standard library imports
import datetime

# third party imports
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import (
    DataProcPySparkOperator)
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow import AirflowException
from airflow.operators.bash_operator  import BashOperator

# ===================Variables=================================
ENV = Variable.get("env")
# name of the job performed in airflow
JOB_NAME = 'outage_storm_level'
# name of the composer to be used
COMPOSER_NAME = 'composer-'+ENV
# name of the python cluster being used
CLUSTER_NAME_PYTHON = 'dp-outage-python-0001'

# location of the python scripts
SCRIPT_LOC_PY = 'gs://us-east4-composer-0001-40ca8a74-bucket/data'\
                '/Outage_restoration/IPL/Python_scripts/'
# location of the R scripts
SCRIPT_LOC_BASH_R = "gcloud beta dataproc jobs submit spark-r    "\
                    "/home/airflow/gcs/data/Outage_restoration/IPL/R_scripts/"
# name of R cluster
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
    'email': ['ms.gkumar.c@aes.com', 'musigma.bkumar.c@aes.com',
              'musigma.dchauhan.c@aes.com', 'musigma.skumar.c@aes.com',
              'musigma.aaggarwal.c@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
    'gcp_conn_id': 'google_cloud_default',
    'subnetwork_uri': COMPOSER_NAME,
    'internal_ip_only': True,
    'region': 'us-east4',
    'zone': 'us-east4-c',
    'labels': {'resource-owner': 'datascience', 'financial-identifier': 'digital'}}

# =================== DAG Definition =================================
with DAG(
        dag_id=JOB_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval='30 4 * * *'
        ) as dag_def:

    PCA = DataProcPySparkOperator(
        task_id='PCA_calculation',
        main=SCRIPT_LOC_PY+'pca_storm_level_pylint.py',
        cluster_name=CLUSTER_NAME_PYTHON,
        region='us-east4',
        job_error_states=['ERROR'],
        gcp_conn_id='google_cloud_default',
        files=CONFIG_FILE,
        dag=dag_def,
        dataproc_pyspark_properties=None,
        dataproc_pyspark_jars=None,
        delegate_to=None,
        arguments=None,
        archives=None,
        pyfiles=None)

    NUMBER_OF_OUTAGES = BashOperator(
        task_id='NUMBER_OF_OUTAGES',
        bash_command=SCRIPT_LOC_BASH_R+
                     "Number_of_Outages_script.R  "+
                     SCRIPT_LOC_BASH_CLUSTER, dag=dag_def)
    CUST_QTY = BashOperator(
        task_id='CUST_QTY',
        bash_command=SCRIPT_LOC_BASH_R+"Cust_Qty.R  "+
                     SCRIPT_LOC_BASH_CLUSTER, dag=dag_def)

    OUTPUT_COLLATION = DataProcPySparkOperator(
        task_id='OUTPUT_COLLATION',
        main=SCRIPT_LOC_PY+'output_collation_pylint.py',
        cluster_name=CLUSTER_NAME_PYTHON,
        gcp_conn_id='google_cloud_default',
        region='us-east4',
        job_error_states=['ERROR'],
        dag=dag_def,
        files=CONFIG_FILE,
        dataproc_pyspark_properties=None,
        dataproc_pyspark_jars=None,
        delegate_to=None,
        arguments=None,
        archives=None,
        pyfiles=None)

# Create pipeline
[PCA, CUST_QTY]
PCA >> NUMBER_OF_OUTAGES
NUMBER_OF_OUTAGES >> OUTPUT_COLLATION
