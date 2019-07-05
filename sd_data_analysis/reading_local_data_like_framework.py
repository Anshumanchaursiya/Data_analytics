#Created by Anshuman Chaursiya during summer internship between may 2019 to july 2019
#Copyright (c) 2019 Anshuman Chaursiya. All rights reserved

import os
import sys
os.environ['SPARK_HOME'] = "/Users/datami/Downloads/spark-2.4.3-bin-hadoop2.7"

from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from datetime import date
import datetime


date = {}
conf = SparkConf()
conf.set("es.nodes", "192.168.1.189:9200")
sc = SparkContext.getOrCreate(conf)

spark = SparkSession(sc)
sqlContext = SQLContext(sc)


PATH = [""]
file_list = []
filter_list = []
ip_list = []
date_range_list = []

#default deployment is rgnc
#print((date(2012, 3, 1) - date(2012, 2, 1)).days)
#print(type(date))
def list_all_file(deployment,starting_date,ending_date):
    starting_date_list = starting_date.split(",")
    ending_date_list = ending_date.split(",")

    #datetime.days takes integer argument like YY,MM,DD
    total_days = (datetime.date(int(ending_date_list[0]),int(ending_date_list[1]),int(ending_date_list[2])) - datetime.date(int(starting_date_list[0]),int(starting_date_list[1]),int(starting_date_list[2]))).days
    global ip_list
    ip_list = os.listdir("/Users/datami/anshuman/sd-sync-process/s3-data/"+deployment+"/accmi/")

    for k in range(len(ip_list)):
        global file_list
        #file_list += os.listdir("/Users/datami/anshuman/sd-sync-process/s3-data/rgnc/accmi/"+ip_list[k])

        file_list = os.listdir("/Users/datami/anshuman/sd-sync-process/s3-data/"+deployment+"/accmi/"+ip_list[k])
        file_list = prepend(file_list,ip_list[k]+"/")
        #print(file_list)


        d1 = datetime.date(int(starting_date_list[0]),int(starting_date_list[1]),int(starting_date_list[2]))
        d2 = datetime.date(int(ending_date_list[0]),int(ending_date_list[1]),int(ending_date_list[2]))
        delta = d2 - d1

        for j in range(delta.days + 1):
            a = d1 + datetime.timedelta(days=j)
            string_format_date = str(a)
            global date_range_list
            date_range_list.append(string_format_date)

        #print(total_days)
        for i in range(total_days):
            if any(date_range_list[i] in s for s in file_list):
                global filter_list
                filter_list += [s for s in file_list if date_range_list[i] in s]



    #print(filter_list)
    return filter_list


def prepend(list, str):
    # Using format()
    str += '{0}'
    list = [str.format(i) for i in list]
    return list

start_date = '2017,1,4'
end_date = '2017,1,5'
deployment = 'cbr'
filter_list = list_all_file(deployment,start_date,end_date)
filter_list = prepend(filter_list, "/Users/datami/anshuman/sd-sync-process/s3-data/"+deployment+"/accmi/")




schema = StructType([
    StructField("crId", StringType(), True),
    StructField("op",StringType(),True),
    StructField("trace",StringType(),True),
    StructField("res",StringType(),True),
    StructField("ts",StringType(),True),
    StructField("uri",StringType(),True),
    StructField("version", StringType(), True),
    StructField("req.aStore", StringType(), True)

    # StructField("req.aStore", StringType(), True),
    # StructField("req.apikey", StringType(), True),
    # StructField("req.appId", StringType(), True),
    # StructField("req.appVer", StringType(), True),
    # StructField("req.callState", StringType(), True),
    # StructField("req.clientIpAddress", StringType(), True),
    # StructField("req.correlationId", StringType(), True),
    # StructField("req.datamiHost", StringType(), True),
    # StructField("req.destHost", StringType(), True),
    # StructField("req.deviceId.make", StringType(), True),
    # StructField("req.deviceId.model", StringType(), True),
    # StructField("req.deviceId.osType", StringType(), True),
    # StructField("req.deviceId.osVersion", StringType(), True),
    # StructField("req.deviceId.sdkType", StringType(), True),
    # StructField("req.deviceId.uid", StringType(), True),
    # StructField("req.deviceId.version", StringType(), True),
    # StructField("req.dmiSdk", StringType(), True),
    # StructField("req.host", StringType(), True),
    # StructField("req.ipList.clientIp", StringType(), True),
    # StructField("region", StringType(), True),
    # StructField("flags", StringType(), True)

])

#check whehter the file is present or not withing the date range
#if not present then exit
if(len(filter_list)==0):
    print("File is not present between the date range Please input different date range")
    exit()

df = spark.read.json(filter_list[0],schema)
list_len = len(filter_list)
for i in range(1,list_len):
    df1 = spark.read.json(filter_list[i],schema)
    df = df.unionAll(df1)


df.show()


start_date = start_date.split(",")

#pusding data into elastic search
df.write.format("org.elasticsearch.spark.sql").mode('append').option("es.resource","accmi-audit-cbr-2017-01-04/logs").save()

