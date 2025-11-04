from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging, requests
from airflow.decorators import task
from airflow.models import Variable


def get_redshift_connection(autocommit=False):
    '''
        Connect to redshift with postgreshook -> default autocommit = False
    '''
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def requestAPI(lat, lon, api_key):
    '''
        Request API of weather at lat/lon -> one call API 3.0
        param : 
            lat - Latitude, decimal (-90; 90)
            lon - Longitude, decimal (-180; 180)
            api_key - access secrete key
    '''
    logging.info('API Request initiates ... ')
    url = f'https://api.openweathermap.org/data/3.0/onecall'
    params = {
        'lat' : lat,
        'lon' : lon,
        'units' : 'metric',
        'exclude' : 'current,minutely,hourly,alerts',
        'appid' : api_key
    }
    response = requests.get(url, params=params)

    # to check response failed
    if response.status_code != 200:
        logging.error(f'API request failed: {response.status_code}')
        raise Exception(f'API Error : {response.status_code}')

    logging.info(f'Retrieved Weather Info successfully . ')
    return response.json()    


@task
def transform(data):
    '''
        Get data and transform into db format
    '''
    logging.info('Transform initiated ... ')
    ret = []

    # Parsing json data into ret
    for d in data['daily']:
        day = datetime.fromtimestamp(d['dt']).strftime('%Y-%m-%d')
        ret.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
    return ret


@task
def load(schema, table, data):
    '''
        Load data into schema.table
    '''
    # Connect to redshift
    cur = get_redshift_connection()
    logging.info('Connected to redshift')

    # Create table in schema if not exist

    create_querry = f'''
    create table if not exists {schema}.{table} (
        date date primary key,
        temp float,
        min_temp float,
        max_temp float,
        date_created timestamp default GETDATE()
    );'''

    # Create a temporary table to determine record priority based on date_created.
    # Each API response provides data for today plus the next 7 days (total of 8 days).
    # Since future dates will overlap with subsequent API calls, update each record using 
    # the most recent date_created value.

    create_temp_query = f'create temp table t (like {schema}.{table} including defaults);'
    logging.info('Created temporary table to replicate')

    try:
        cur.execute(create_querry)
        cur.execute(create_temp_query)
        cur.execute('commit;')
    except Exception as e:
        cur.execute('rollback;')
        logging.info(f'execution not working : {e}')
        raise

    insert_sql = f'insert into t (date, temp, min_temp, max_temp) values ' + ','.join(data)
    logging.info(insert_sql)

    try:
        cur.execute(insert_sql)
        cur.execute('commit;')
    except Exception as e:
        cur.execute('rollback;')
        logging.info(f'execution not working : {e}')
        raise

    replace_querry = f'''
        delete from {schema}.{table};
        insert into {schema}.{table}
        select date, temp, min_temp, max_temp, date_created from (
            select *, row_number() over (partition by date order by date_created desc) seq
            from t
        )
        where seq = 1;'''
    
    logging.info(replace_querry)

    try:
        cur.execute(replace_querry)
        cur.execute('commit;')
    except Exception as e:
        cur.execute('rollback;')
        logging.info(f'execution not working : {e}')
        raise


with DAG(
    dag_id='weather_api',
    # UTC : every day 6:00 am
    start_date=datetime(2025,11,3),
    max_active_runs = 1,
    catchup=False,
    tags=['API'],
    schedule= '0 6 * * *',
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    
    # Lat and Log of Seoul
    lat = 37.5642135
    lon = 127

    # API key and schema from airflow.vairable
    api_key = Variable.get('weather_api')
    schema = Variable.get('schema')
    
    # Trigger Tasks : RequestAPI -> Transform -> Load (ETL)
    data = requestAPI(lat, lon, api_key)
    trns = transform(data)
    load(schema, 'weather_api', trns)