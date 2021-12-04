import re

from django.shortcuts import render
import plotly.graph_objs as go
from plotly.offline import plot
import plotly.express as px
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import glob
# Create your views here.
from pyspark.sql.types import StringType
from datetime import datetime
import pandas as pd

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
raw_data_files = glob.glob('/Users/jackpan/JackPanDocuments/temporary/tet/edp.2021-12-02.out')
base_df = spark.read.text(raw_data_files)
normal_log_df = base_df.filter(base_df['value'].rlike(r'URI:.*最大内存:.*已分配内存:.*最大可用内存:.*'))


def count_seconds(col_name):
    time_arr = col_name.split(':')
    return int(time_arr[0] * 3600) + int(time_arr[1] * 60) + int(float(time_arr[2]))


ts_pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})'

spend_time_pattern = r'(耗时：\d+:\d+:\d+\.\d+)'

request_uri_pattern = r'((\/\w+)+)'

# 最大内存
max_memory_pattern = r'(最大内存: \d+m)'

# 已分配内存
already_allow_memory_pattern = r'(已分配内存: \d+m)'

# 已分配内存中的剩余空间
already_allow_memory_free_pattern = r'(已分配内存中的剩余空间: \d+m)'

# 最大可用内存
max_useful_memory_free_pattern = r'(最大可用内存: \d+m)'
count_seconds_udf = udf(lambda z: count_seconds(z), StringType())

performance_log_df = normal_log_df.select(
    regexp_extract('value', ts_pattern, 1).alias('time'),
    regexp_extract('value', spend_time_pattern, 1).alias('spend_time'),
    regexp_extract('value', request_uri_pattern, 1).alias('request_uri'),
    regexp_extract('value', max_memory_pattern, 1).alias('max_memory'),
    regexp_extract('value', already_allow_memory_pattern, 1).alias('total_memory'),
    regexp_extract('value', already_allow_memory_free_pattern, 1).alias('free_memory'),
    regexp_extract('value', max_useful_memory_free_pattern, 1).alias('max_can_use_memory'),
).withColumn("spend_time", regexp_replace('spend_time', '耗时：', '')) \
    .withColumn("spend_time", count_seconds_udf('spend_time')) \
    .withColumn("max_memory", regexp_replace('max_memory', '(最大内存: |m)', '')) \
    .withColumn("total_memory", regexp_replace('total_memory', '(已分配内存: |m)', '')) \
    .withColumn("free_memory", regexp_replace('free_memory', '(已分配内存中的剩余空间: |m)', '')) \
    .withColumn("max_can_use_memory", regexp_replace('max_can_use_memory', '(最大可用内存: |m)', ''))


def index(request):
    plot_div = get_hour_pd_df(performance_log_df)
    time_div = get_time_pd_df(performance_log_df)

    spend_time_df = performance_log_df.select(col('request_uri'), col('spend_time')).orderBy(desc('spend_time')).limit(10)
    spend_time_pd_df = (spend_time_df.toPandas())
    x_data = []
    y_data = []
    for index, row in spend_time_pd_df.iterrows():
        x_data.append(row['request_uri'])
        y_data.append(row['spend_time'])

    print(x_data)
    print(y_data)
    spend_time_div = plot([go.Bar(x=x_data, y=y_data, name='test')],
                    output_type='div')
    return render(request, "loganalyzes/index.html", context={'plot_div': plot_div, 'time_div': time_div, 'spend_time_div': spend_time_div,})


def get_hour_pd_df(data_df):
    hour_df = data_df.select(hour(col('time')).alias('hour')) \
        .groupBy('hour').count().orderBy('hour')

    hour_pd_df = (hour_df.toPandas())
    data_result = {}

    for x in range(24):
        data_result[x] = 0

    for index, row in hour_pd_df.iterrows():
        data_result[row['hour']] = row['count']
    x_data = []
    y_data = []
    for key in data_result:
        x_data.append(str(key) + '点')
        y_data.append(data_result[key])
    plot_div = plot([go.Scatter(x=x_data, y=y_data,
                             mode='lines', name='test',
                             opacity=0.8, marker_color='green')],
                    output_type='div')

    return plot_div


def get_time_pd_df(data_df):
    time_df = performance_log_df.select(col('time')).groupBy(window(col('time'), '1 minutes')).count().orderBy(
        'window')
    time_pd_df = (time_df.toPandas())
    x_data = []
    y_data = []
    for index, row in time_pd_df.iterrows():
        date_start = row['window']['start']
        x_data.append(date_start)
        y_data.append(row['count'])

    print(x_data)
    print(y_data)
    time_div = plot([go.Scatter(x=x_data, y=y_data,
                             mode='lines', name='test2',
                             opacity=0.8, marker_color='green')],
                    output_type='div')

    return time_div
