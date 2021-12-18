import os
import re
import django
django.setup()
from .models import SocketLog
from django.shortcuts import render, redirect, get_object_or_404
import plotly.graph_objs as go
from paramiko.client import SSHClient
from plotly.offline import plot
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import glob
# Create your views here.
from pyspark.sql.types import StringType
import pandas as pd
from .forms import UploadFileForm

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

def count_seconds(col_name):
    time_arr = col_name.split(':')
    return int(time_arr[0] * 3600) + int(time_arr[1] * 60) + int(float(time_arr[2]))


ts_pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})'
spend_time_pattern = r'(耗时：\d+:\d+:\d+\.\d+)'
request_uri_pattern = r'((\/\w+\.?\d?)+)'

# 最大内存
max_memory_pattern = r'(最大内存: \d+m)'
# 已分配内存
already_allow_memory_pattern = r'(已分配内存: \d+m)'
# 已分配内存中的剩余空间
already_allow_memory_free_pattern = r'(已分配内存中的剩余空间: \d+m)'
# 最大可用内存
max_useful_memory_free_pattern = r'(最大可用内存: \d+m)'
count_seconds_udf = udf(lambda z: count_seconds(z), StringType())


def index(request):
    # plot_div = get_hour_pd_df(performance_log_df)
    # time_div = get_time_pd_df(performance_log_df)
    # spend_time_div = get_spend_time_div(performance_log_df)
    # memory_div = get_memory_div(performance_log_df)

    # return render(request, "loganalyzes/index.html", context={'plot_div': plot_div, 'time_div': time_div,
    #                                                           'spend_time_div': spend_time_div,
    #                                                           'memory_div': memory_div})

    # ssc = StreamingContext(sc, 1)
    #
    # lines = ssc.socketTextStream("localhost", int(9999))
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a + b)
    # counts.saveAsTextFiles("/Users/jackpan/JackPanDocuments/temporary/edp_log", "out")
    #
    # ssc.start()
    # ssc.awaitTermination()
    return render(request, "loganalyzes/index.html")


def start_monitor(request, host_id):
    log_info = get_object_or_404(SocketLog, pk=host_id)
    result_message = "Start monitor success"
    if len(log_info.log_save_position) == 0:

        client = SSHClient()
        client.load_system_host_keys()
        client.connect(log_info.host_ip, 22, log_info.username, log_info.password)
        client_folder = "/Users/jackpan/JackPanDocuments/temporary/test-sftp/"
        cutting = log_info.log_position.rindex("/")
        log_folder = log_info.log_position[0:cutting]
        sftp = client.open_sftp()
        sftp.put(client_folder + "tcpserver.c", log_folder + "/tcpserver.c")
        print(log_info.log_position)
        sftp.close()
        stdin, stdout, stderr = client.exec_command(
            'cd ' + log_folder + ' && nohup gcc tcpserver.c -o tcpserver > tcpserver_compiler_log.out 2>&1 &')
        client.close()
        client.connect(log_info.host_ip, 22, log_info.username, log_info.password)
        print(log_folder)
        server_command = 'cd ' + log_folder + ' && nohup ./tcpserver ' + log_info.log_position + ' ' + str(
            log_info.host_port) + ' > tcpserver.out 2>&1 &'
        print(server_command)
        client.exec_command(server_command)
        client.close()
        # Run client to receive data from server
        out_log = "nohup" + str(host_id) + ".out"
        # create save position
        log_save_position = client_folder + "analyze" + str(host_id) + "/"
        log_save_position_command = "mkdir " + log_save_position
        os.system(log_save_position_command)

        cmd = 'cd ' + client_folder + ' && nohup ./tcpclient ' + log_save_position + 'edp.out ' \
              + str(log_info.host_port) + ' > ' + out_log + ' 2>&1 &'
        print(cmd)
        os.system(cmd)
        log_info.log_save_position = log_save_position + 'edp.out'
        log_info.save()
    else:
        result_message = "Monitor is running!"
        print(result_message)
    return render(request, "loganalyzes/success.html", {
        'result_message': result_message
    })


def view_monitor_data(request, host_id):
    result_message = "Start monitor success"

    log_info = get_object_or_404(SocketLog, pk=host_id)
    file_url = log_info.log_save_position
    print(file_url)
    performance_log_df = analyze_edp_log_offline(file_url)
    plot_div = get_hour_pd_df(performance_log_df)
    time_div = get_time_pd_df(performance_log_df)
    spend_time_div = get_spend_time_div(performance_log_df)
    memory_div = get_memory_div(performance_log_df)
    request_memory_div = get_request_uri_memory(performance_log_df)
    return render(request, "loganalyzes/index.html", context={'plot_div': plot_div, 'time_div': time_div,
                                                              'spend_time_div': spend_time_div,
                                                              'request_memory_div': request_memory_div,
                                                              'memory_div': memory_div})
    return render(request, "loganalyzes/success.html", {
        'result_message': result_message
    })

def upload_file(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        print(form.is_valid())
        if form.is_valid():
            file_url = '/Users/jackpan/JackPanDocuments/temporary/tet/edp_temp.out'
            handle_uploaded_file(request.FILES['file'], file_url)
            performance_log_df = analyze_edp_log_offline(file_url)
            plot_div = get_hour_pd_df(performance_log_df)
            time_div = get_time_pd_df(performance_log_df)
            spend_time_div = get_spend_time_div(performance_log_df)
            memory_div = get_memory_div(performance_log_df)
            request_memory_div = get_request_uri_memory(performance_log_df)
            return render(request, "loganalyzes/index.html", context={'plot_div': plot_div, 'time_div': time_div,
                                                                      'spend_time_div': spend_time_div,
                                                                      'request_memory_div': request_memory_div,
                                                                      'memory_div': memory_div})
    else:
        form = UploadFileForm()
        return render(request, 'loganalyzes/upload.html', {'form': form})


def analyze_edp_log_offline(file_url):
    raw_data_files = glob.glob(file_url)
    base_df = spark.read.text(raw_data_files)
    base_df.show(10)
    normal_log_df = base_df.filter(base_df['value'].rlike(r'URI:.*最大内存:.*已分配内存:.*最大可用内存:.*'))
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
        .withColumn("max_memory", regexp_replace('max_memory', '(最大内存: |m)', '').cast('int')) \
        .withColumn("total_memory", regexp_replace('total_memory', '(已分配内存: |m)', '').cast('int')) \
        .withColumn("free_memory", regexp_replace('free_memory', '(已分配内存中的剩余空间: |m)', '').cast('int')) \
        .withColumn("max_can_use_memory", regexp_replace('max_can_use_memory', '(最大可用内存: |m)', '').cast('int')) \
        .withColumn("used_memory", col('total_memory') - col('free_memory'))

    performance_log_df.show(10)
    return performance_log_df


def handle_uploaded_file(f, file_url):
    with open(file_url, 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)


def get_request_uri_memory(data_df):
    # 已分配内存 和 剩余内存变化率

    request_uri_used_memory_df = data_df.select(col('request_uri'), col('used_memory')).filter(
        ~col('request_uri').startswith('/api/system/')) \
        .groupBy('request_uri').agg(F.sum('used_memory').alias('used_memory_sum')).sort(desc('used_memory_sum'))
    request_uri_used_memory_pd_df = (request_uri_used_memory_df.toPandas())
    x_data = []
    y_data = []
    for index, row in request_uri_used_memory_pd_df.iterrows():
        x_data.append(row['request_uri'])
        y_data.append(row['used_memory_sum'])
    request_memory_div = plot([go.Bar(x=x_data, y=y_data, name='test')],
                              output_type='div')

    return request_memory_div


def get_memory_div(data_df):
    # 已分配内存 和 剩余内存变化率
    already_used_memory_df = data_df.select(col('time'), col('total_memory'), col('used_memory')) \
        .orderBy(asc('time'))
    already_used_memory_pd_df = (already_used_memory_df.toPandas())
    x_data = []
    y_data = []
    y1_data = []
    for index, row in already_used_memory_pd_df.iterrows():
        x_data.append(row['time'])
        y_data.append(row['used_memory'])
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
    time_df = data_df.select(col('time')).groupBy(window(col('time'), '1 minutes')).count().orderBy(
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
