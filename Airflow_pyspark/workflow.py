
# -*- coding: utf-8 -*-
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests
import os
import json
from pathlib import Path

args = {
            'owner': 'airflow'
            }

def download_xkcd():
    lenReq = requests.get('https://xkcd.com/info.0.json')
    lenObj = lenReq.json()

    for i in range(1, int(lenObj["num"])+1):
        if i != 404 and i != 1037 and i != 1331:
            jfile = requests.get(f'https://xkcd.com/{i}/info.0.json')
            jsonObj = jfile.json()

            if (os.path.exists(f'/home/airflow/xkcd/raw/{jsonObj["year"]}')):
                Path(f'/home/airflow/xkcd/raw/{jsonObj["year"]}/{jsonObj["num"]}.json').write_text(json.dumps(jsonObj))
            else:
                os.makedirs(f'/home/airflow/xkcd/raw/{jsonObj["year"]}')
                Path(f'/home/airflow/xkcd/raw/{jsonObj["year"]}/{jsonObj["num"]}.json').write_text(json.dumps(jsonObj))
dag = DAG('xkcd1', default_args=args, description='xkcd comics',
        schedule_interval='56 18 * * *',
        start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
        task_id='create_import_dir',
        path='/home/airflow',
        directory='xkcd',
        dag=dag,
)

create_local_import_dir1 = CreateDirectoryOperator(
        task_id='create_import_dir1',
        path='/home/airflow/xkcd',
        directory='raw',
        dag=dag,
)

clear_xkcddata = BashOperator(
        task_id='clear_xkcddata',
        bash_command='rm -r /home/airflow/xkcd',
)

download_xkcd = PythonOperator(
        task_id='download_xkcd',
        python_callable=download_xkcd,
        dag=dag,
)

create_path_xkcddata_hdfs = BashOperator(
        task_id='create_path_xkcddata_hdfs' ,
        bash_command='/home/airflow/hadoop/bin/hadoop fs -mkdir /user/hadoop/xkcd')
create_path_xkcddata_hdfs1 = BashOperator(
        task_id='create_path_xkcddata_hdfs1',
        bash_command='/home/airflow/hadoop/bin/hadoop fs -mkdir /user/hadoop/xkcd/raw')

clear_xkcddata_hdfs = BashOperator(
        task_id='clear_xkcddata_hdfs',
        bash_command='/home/airflow/hadoop/bin/hadoop dfs -rm -r /user/hadoop/xkcd',
 )
push_xkcddata_hdfs = BashOperator(
        task_id='push_xkcddata_hdfs',
        bash_command='/home/airflow/hadoop/bin/hadoop fs -put /home/airflow/xkcd /user/hadoop',
)

pyspark_data_to_database = SparkSubmitOperator(
        task_id='pyspark_data_to_database',
        conn_id='spark',
        application='/home/airflow/airflow/python/data_to_database.py',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='2',
        name='data_to_database',
        verbose=True,
)
dummy_op = DummyOperator(
        task_id='dummy',
        dag=dag)


dummy_op >> clear_xkcddata >> clear_xkcddata_hdfs >> create_path_xkcddata_hdfs >> create_path_xkcddata_hdfs1 >> create_local_import_dir >> create_local_import_dir1 >> download_xkcd >> push_xkcddata_hdfs >> pyspark_data_to_database
