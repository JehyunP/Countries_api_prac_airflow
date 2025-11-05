from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


dag = DAG(
    dag_id = 'mysql_to_redshift',
    start_date = datetime(2025,11,4),
    schedule='0 9 * * *',
    max_active_runs= 1,
    catchup=False,
    default_args={
        'retries' : 1,
        'retry_delay' : timedelta(minutes=3)
    }
)


schema = Variable.get('schema')
table = 'nps'
s3_bucket = Variable.get('s3_path') 
s3_key = schema + '_' + table

sql = "select * from prod.nps where date(created_at) >= '2023-01-01'"
logging.info(sql)

mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = sql,
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    sql_conn_id = 'mysql_conn_id',
    aws_conn_id = "aws_conn_id",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},    
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",    
    method = "UPSERT",
    upsert_keys = ["id"],
    dag = dag
)

mysql_to_s3_nps >> s3_to_redshift_nps