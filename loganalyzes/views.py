import re
from django.shortcuts import render, redirect
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
from .forms import UploadFileForm
from django.http import HttpResponseRedirect

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
    .withColumn("spend_time", count_seconds_udf('spend_time').cast("long")) \
    .withColumn("max_memory", regexp_replace('max_memory', '(最大内存: |m)', '')) \
    .withColumn("total_memory", regexp_replace('total_memory', '(已分配内存: |m)', '')) \
    .withColumn("free_memory", regexp_replace('free_memory', '(已分配内存中的剩余空间: |m)', '')) \
    .withColumn("max_can_use_memory", regexp_replace('max_can_use_memory', '(最大可用内存: |m)', ''))


def index(request, file_url):
    print(file_url)
    plot_div = get_hour_pd_df(performance_log_df)
    time_div = get_time_pd_df(performance_log_df)
    spend_time_div = get_spend_time_div(performance_log_df)
    memory_div = get_memory_div(performance_log_df)

    return render(request, "loganalyzes/index.html", context={'plot_div': plot_div, 'time_div': time_div,
                                                              'spend_time_div': spend_time_div,
                                                              'memory_div': memory_div})


def upload_file(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        print(form.is_valid())
        if form.is_valid():
            file_url = handle_uploaded_file(request.FILES['file'])
            return redirect("index", file_url)
    else:
        form = UploadFileForm()
    return render(request, 'loganalyzes/upload.html', {'form': form})




def handle_uploaded_file(f):
    file_url = '/Users/jackpan/JackPanDocuments/temporary/tet/edp_temp.out'
    with open(file_url, 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)
    return file_url


def get_memory_div(data_df):
    # 已分配内存 和 剩余内存变化率
    already_used_memory_df = performance_log_df.select(col('time'), col('total_memory'), col('free_memory')) \
        .orderBy(asc('time'))
    already_used_memory_pd_df = (already_used_memory_df.toPandas())
    x_data = []
    y_data = []
    y1_data = []
    for index, row in already_used_memory_pd_df.iterrows():
        x_data.append(row['time'])
        y_data.append(int(row['total_memory']) - int(row['free_memory']))
        y1_data.append(int(row['total_memory']))
    memory_div = plot([go.Scatter(x=x_data, y=y_data,
                                  mode='lines', name='已使用内存MB',
                                  opacity=0.8, marker_color='green'), go.Scatter(x=x_data, y=y1_data,
                                                                                 mode='lines', name='总内存MB',
                                                                                 opacity=0.8, marker_color='red')],
                      output_type='div')
    return memory_div


def get_spend_time_div(data_df):
    spend_time_df = data_df.select(col('request_uri'), col('spend_time')).sort(desc('spend_time')).limit(20)
    spend_time_pd_df = (spend_time_df.toPandas())
    x_data = []
    y_data = []
    for index, row in spend_time_pd_df.iterrows():
        x_data.append(row['request_uri'])
        y_data.append(row['spend_time'])

    spend_time_div = plot([go.Bar(x=x_data, y=y_data, name='test')],
                          output_type='div')
    return spend_time_div


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

    time_div = plot([go.Scatter(x=x_data, y=y_data,
                                mode='lines', name='test2',
                                opacity=0.8, marker_color='green')],
                    output_type='div')

    return time_div
