from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import logging
import requests
from collections import defaultdict
import pandas as pd



def get_redshift_connection(autocommit=True):
    '''
        Connect to redshift with postgreshook -> default autocommit = True
    '''
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def request_api():
    '''
        Get API  from restcountries -> name, population, area
    '''
    logging.info('Requesting data from Countries API ... ')

    # Request api
    url = 'https://restcountries.com/v3.1/all'
    params = {"fields": "name,population,area"}
    result = requests.get(url, params=params)

    # To check whether the API request failed
    if result.status_code != 200:
        logging.error(f'API request failed: {result.status_code}')
        raise Exception(f'API Error : {result.status_code}')
    
    # parsing as json
    data = result.json()
    logging.info(f'Retrieved {len(data)} countries successfully')

    return data


@task 
def transform(data):
    '''
        Transform the json data into dictionary format
    '''
    logging.info(' Data transformation on going ... ')

    dic = defaultdict(list)

    # put transformed data into dictionary
    for country in data:
        dic['Country'].append(country.get('name', {}).get('common', 'Unknown'))
        dic['Population'].append(country.get('population', 0))
        dic['Area'].append(country.get('area', 0))

    logging.info(f"Transformed {len(dic['Country'])} records successfully.")

    return dict(dic)


@task
def load(schema, table, data):
    '''
        Create and insert data into table
    '''
    logging.info(f"Load Started to {table}")

    # connect to redshift 
    cur = get_redshift_connection()

    # set dictionary to dataframe
    df = pd.DataFrame(data)

    try:
        # drop table if exists then create a new table
        cur.execute('BEGIN;')
        cur.execute(f'DROP table if exists {schema}.{table};')
        cur.execute(f'''
            create table {schema}.{table} (
                Country VARCHAR(256),
                Population BIGINT,
                Area FLOAT
            );
        ''')
        logging.info('Table created successfully')

        # insert query format used at executemany
        insert_query = f''' 
            insert into  {schema}.{table} (Country, Population, Area)
            Values (%s, %s, %s);
        '''

        # insert data into table
        rows = [tuple(x) for x in df.to_numpy()]
        cur.executemany(insert_query, rows)
        cur.execute('COMMIT;')
        logging.info(f'{len(rows)} rows inserted into {schema}.{table}')

    # Fail catcher
    except Exception as e:
        logging.info(f'Load Fail : {e}')
        cur.execute('ROLLBACK;')
        raise e
    
    # close connection to redshift
    finally:
        cur.close()
    


# Initialize DAG : Airflow
with DAG(
    dag_id = 'Countries_API',
    # UTC : every saturday 6:30 am
    start_date=datetime(2025,11,3),
    catchup=False,
    tags=['API'],
    schedule= '30 6 * * 6'
) as dag:
    
    # ETL : request -> trnasform -> load
    result = request_api()
    trsfm = transform(result)
    load(Variable.get('schema'), Variable.get('country_table'), trsfm)

