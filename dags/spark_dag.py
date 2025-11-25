
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


# Ensure driver (Airflow worker) also has S3A + JDBC deps via --packages
# Keep versions aligned with Spark image (see Dockerfile.spark)
DEFAULT_PACKAGES = 'org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.25.66'

S3A_CONF = {
    # MinIO S3A settings (local dev)
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.access.key': 'minioadmin',
    'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
    # Normalize any time-based configs expecting numeric values
    'spark.hadoop.fs.s3a.threads.keepalivetime': '60',
    'spark.hadoop.fs.s3a.connection.timeout': '60000',            # ms
    'spark.hadoop.fs.s3a.socket.timeout': '60000',                # ms
    'spark.hadoop.fs.s3a.connection.establish.timeout': '5000',   # ms
    'spark.hadoop.fs.s3a.connection.request.timeout': '30000',    # ms
    # Some Hadoop 3.3.x builds expect numeric (ms) for durations that default to strings like '24h'
    'spark.hadoop.fs.s3a.multipart.purge.age': '86400000',        # 24h in ms
    # Explicit credentials provider
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    # Ensure JARs are distributed to executors
    'spark.jars.ivy': '/tmp/.ivy2',
    # Memory optimization settings
    'spark.driver.memory': '2g',
    'spark.executor.memory': '2g',
    'spark.driver.maxResultSize': '1g',
    'spark.sql.shuffle.partitions': '50',
    'spark.default.parallelism': '4',
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.memory.fraction': '0.6',
    'spark.memory.storageFraction': '0.2',
    'spark.sql.autoBroadcastJoinThreshold': '10485760',
}


# Define the DAG at module level so Airflow can find it
dag = DAG(
    'sus_minio_ingest_dag',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
)

# Create one ingestion task per dataset
for dataset in ['sih', 'sinasc', 'sim']:
    SparkSubmitOperator(
        task_id=f'ingest_{dataset}',
        application=f'/opt/airflow/dags/spark_script_{dataset}.py',
        conn_id='spark_conn',
        name=f'sus-ingest-{dataset}',
        # Use jars instead of packages to avoid Maven downloads
        jars='/opt/spark/jars/postgresql-42.7.3.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
        conf=S3A_CONF,
        deploy_mode='client',
        dag=dag,
        application_args=[
            '--dataset', dataset,
            '--date', '{{ ds }}',
            '--bucket', 'landing',
            '--prefix', 'source_sus',
            '--pg-url', 'jdbc:postgresql://postgres-olap:5432/olap_db',
            '--pg-user', 'olap',
            '--pg-password', 'olap',
            '--table-prefix', 'stg_',
        ],
    )
