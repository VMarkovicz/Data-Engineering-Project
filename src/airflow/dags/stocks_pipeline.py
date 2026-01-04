from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
from docker.types import Mount


default_args = {
    'owner': 'financial-data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'stocks_daily_pipeline',
    default_args=default_args,
    description='Daily ingestion of stock data from Cryptostock API',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['financial-data', 'cryptostock'],
)


# Common mounts used by all tasks
COMMON_MOUNTS = [
    Mount(source='C:/Users/JOHNV/Documents/CS/ED/data-engineering-project/src', target='/opt/src', type='bind'),
    Mount(source='C:/Users/JOHNV/Documents/CS/ED/data-engineering-project/datalake', target='/datalake', type='bind'),
    Mount(source='C:/Users/JOHNV/Documents/CS/ED/data-engineering-project/src/raw_datasets', target='/opt/raw_datasets', type='bind'),
]


# Task 1: Fetch data from stocks API
fetch_data = DockerOperator(
    task_id='fetch_stocks_data',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command='python /opt/src/api_ingestion/stocks_api_ingestion.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='data-engineering-project_default',
    mounts=COMMON_MOUNTS,
    environment={
        'ALPACA_API_KEY': Variable.get("ALPACA_API_KEY"),
        'ALPACA_SECRET_KEY': Variable.get("ALPACA_SECRET_KEY")
    },
    dag=dag,
)

# Task 2: Process data with Spark
process_spark = DockerOperator(
    task_id='process_with_spark',
    image='data-engineering-project-spark-master',
    api_version='auto',
    auto_remove=True,
    mount_tmp_dir=False,
    command=[
        'spark-submit',
        '--master', 'spark://spark-master:7077',
        '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
        '--driver-memory', '2g',
        '--executor-memory', '2g',
        '/opt/src/ingestion/stocks_spark_ingestion.py'
    ],
    docker_url='unix://var/run/docker.sock',
    network_mode='data-engineering-project_default',
    mounts=COMMON_MOUNTS,
    environment={
        'SPARK_MASTER_URL': 'spark://spark-master:7077'
    },
    dag=dag,
)



# Task dependencies
fetch_data >> process_spark