import os
import sys
os.environ['SPARK_HOME'] = "/Users/datami/Downloads/spark-2.4.3-bin-hadoop2.7"

from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

conf = SparkConf()
conf.set("es.nodes", "localhost:9200")
sc = SparkContext.getOrCreate(conf)

spark = SparkSession(sc)
sqlContext = SQLContext(sc)


PATH = ["/Users/datami/PycharmProjects/sd_data_analytics/resource/backup-cbr-aacmi-audit-1970-01-01-00-ip-10-10-52-60-1878.gz","/Users/datami/PycharmProjects/sd_data_analytics/resource/backup-cbr-aacmi-audit-2016-12-19-21-ip-10-10-62-61-1.gz"]
file_list = []
filter_list = []
def list_all_file(ip,starting_date,ending_date):
    ip_list = os.listdir("/Users/datami/anshuman/sd-sync-process/s3-data/rgnc/accmi/")
    '''
    for k in range(len(ip_list)):
        global file_list
        file_list += os.listdir("/Users/datami/anshuman/sd-sync-process/s3-data/rgnc/accmi/"+ip_list[k])
    '''
    file_list = os.listdir("/Users/datami/anshuman/sd-sync-process/s3-data/rgnc/accmi/"+ip)
    print(file_list,"*****")

    date = starting_date[:8]
    day1 = starting_date[8:]
    day2 = ending_date[8:]
    print(day1,day2)
    print(date,"***")
    total_days = int(day2)-int(day1)+1
    print("total_days",total_days)
    for i in range(total_days):
        day = int(day1)+i
        if(len(str(day))==1):
            print("hello")
        if any(date+str(day) in s for s in file_list):
            print("present")
            global filter_list
            filter_list += [s for s in file_list if date+str(day) in s]



    return filter_list


def prepend(list, str):
    # Using format()
    str += '{0}'
    list = [str.format(i) for i in list]
    return list

start_date1 = '2017-08-29'

filter_list = list_all_file('ip-10-6-52-32','2017-08-29','2017-08-29')
filter_list = prepend(filter_list, "/Users/datami/anshuman/sd-sync-process/s3-data/rgnc/accmi/ip-10-6-52-32/")
file_list = os.listdir("/Users/datami/anshuman/sd-sync-process/s3-data/rgnc/accmi/ip-10-6-52-32")

#PATH = ['/Users/datami/anshuman/sd-sync-process/s3-data/rgnc/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-01-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-02-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-03-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-04-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-05-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-06-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-07-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-08-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-09-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-10-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-10-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-11-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-11-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-12-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-12-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-13-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-13-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-14-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-14-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-15-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-15-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-16-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-16-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-17-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-17-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-18-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-18-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-19-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-19-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-20-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-20-ip-10-10-52-60-1.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-21-ip-10-10-52-60-0.gz', '/Users/datami/anshuman/sd-sync-process/s3-data/accmi/ip-10-10-52-60/backup-cbr-aacmi-audit-2017-01-14-21-ip-10-10-52-60-1.gz']
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



df = spark.read.json(filter_list[0],schema)
list_len = len(filter_list)
for i in range(1,list_len):
    df1 = spark.read.json(filter_list[i],schema)
    df = df.unionAll(df1)


df.show()



#pusding data into elastic search
df.write.format("org.elasticsearch.spark.sql").mode('append').option("es.resource","accmi-audit-"+start_date1+"/logs").save()

