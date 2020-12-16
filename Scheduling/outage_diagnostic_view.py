'''
Author - Mu Sigma
Updated: 15 Dec 2020
Version: 2
Tasks: Merge actuals with predicted values of the outages and
Compute actuals no of outages and customers affected
and aggregate to day level to compare with predictions
Schedule: Everyday at 7 AM
Environment: Composer-0001
Run-time environments: Pyspark,SparkR and python 3.7 callable
'''

# standard library imports 
import datetime

# third party imports
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.models import DAG

# ===================Variables=================================
ENV = Variable.get("env")

# name of the job performed in airflow
JOB_NAME = 'outage-diagnostic-backend'
# name of the composer to be used
COMPOSER_NAME = 'composer-'+ENV
# name of the python clsuter being used
CLUSTER_NAME = 'dp-outage-python-0001'
# location of the scripts
SCRIPT_LOC = '/home/airflow/gcs/data/Outage_restoration/'\
             'IPL/Python_scripts/'
# specify location of the config file
CONFIG_FILE = 'gs://us-east4-composer-0001-40ca8a74-bucket/data/'\
              'Outage_restoration/IPL/Config_Files/config_ETR.ini'
# setting up start time 
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# =================== DAG Arguments =================================
DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': True,
    'email': ['musigma.skumar.c@aes.com'],
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
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
        schedule_interval='0 7 * * *'
) as dag:
    DIAGNOSTIC = DataProcPySparkOperator(task_id='Diagnostic_Daily_Append',
                                         main=SCRIPT_LOC +
                                         'diagnostic_view_pylint.py',
                                         gcp_conn_id='google_cloud_default',
                                         cluster_name=CLUSTER_NAME,
                                         region='us-east4',
                                         job_error_states=['ERROR'],
                                         files=CONFIG_FILE,
                                         dag=dag,
                                         arguments=None,
                                         archives=None,
                                         pyfiles=None,
                                         dataproc_pyspark_properties=None,
                                         dataproc_pyspark_jars=None,
                                         delegate_to=None)

    STORM_LEVEL = DataProcPySparkOperator(task_id='Storm_Diagnostic_Daily_Append',
                                          main=SCRIPT_LOC +
                                          'storm_level_comparison_pylint.py',
                                          gcp_conn_id='google_cloud_default',
                                          cluster_name=CLUSTER_NAME,
                                          region='us-east4',
                                          job_error_states=['ERROR'],
                                          files=CONFIG_FILE,
                                          dag=dag,
                                          arguments=None,
                                          archives=None,
                                          pyfiles=None,
                                          dataproc_pyspark_properties=None,
                                          dataproc_pyspark_jars=None,
                                          delegate_to=None)

DIAGNOSTIC >> STORM_LEVEL
