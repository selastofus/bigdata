#!/usr/bin/env python
# coding: utf-8

# Import Spark Libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc
import os
import json
from pathlib import Path
import mysql.connector
from pyspark.sql import Row
# Initialize Spaek session
sc = pyspark.SparkContext()
spark = SparkSession(sc)

# Rad json from HDFS
comics_df = spark.read.format('json')\
.options(header='true', nullValue='null', inferschema='true')\
.load('/user/hadoop/xkcd/raw/*' + '/*.json')

comics_df.show(10)
comics_df.printSchema()
#clean df
cleaned_df = comics_df.drop("alt","extra_parts","news", "safe_title","transcript")
cleaned_df.show(20)
cleaned_df.printSchema()
#copy cleaned dataframe into final directory (csv)
if (os.path.exists(f'/home/airflow/xkcd/final')):
    os.rmdir('/home/airflow/xkcd/final')
    os.makedirs(f'/home/airflow/xkcd/final')
    cleaned_df.write.format('csv').mode('overwrite').save('/user/hadoop/xkcd/final')

else:
    os.makedirs(f'/home/airflow/xkcd/final')
    cleaned_df.write.format('csv').mode('overwrite').save('/user/hadoop/xkcd/final')

#load df into mysql database
cleaned_df.write.mode("overwrite").format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                    user="root",
                                    password="root",
                                    url="jdbc:mysql://172.19.0.4:3306/bigdata",
                                    dbtable="comics"
                                    ).save()