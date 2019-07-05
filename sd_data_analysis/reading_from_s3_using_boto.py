import os
import sys
import boto
import boto.s3.connection
from boto.s3.connection import S3Connection
import boto3
from boto3.session import Session


# os.environ['SPARK_HOME'] = "/Users/datami/Downloads/spark-2.4.3-bin-hadoop2.7"
# sys.path.append("/usr/downloads/spark/spark-2.4.3-bin-hadoop2.7/python/")
# sys.path.append("/usr/downloads/spark/spark-2.4.3-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip")
#
# from pyspark import SparkContext, SparkConf
# from pyspark.sql.session import SparkSession
# from pyspark.sql import SQLContext
#
# conf = SparkConf()
# conf.set("es.nodes", "localhost:9200")
#
# sc = SparkContext.getOrCreate(conf)
# ss = SparkSession(sc)
# sqlContext = SQLContext(sc)
#
# #args = input("")
# region_deploynet="cbr"
# JSON_PATH = "s3a://logs-backup.cloudmi.datami.com/deployments/"+region_deploynet+"/aacmi/audit/ip-10-10-52-60/backup-cbr-aacmi-audit-1970-01-01-00-ip-10-10-52-60-0.gz"
#df = sqlContext.read.json(JSON_PATH)
#df = sqlContext.read.load("s3a://logs-backup.cloudmi.datami.com/deployments/"+region_deploynet+"/aacmi/audit/ip-10-10-52-60/*.gz")
#df.show()
#df.write.format("org.elasticsearch.spark.sql").mode('append').option("es.resource","accmi-logs-2019-06-10/logs").save()

key_id = ''
secret_key = ''
bucket = ''
prefix =  ''


s3 = boto3.client('s3')

def get_all_s3_keys(s3_path):
    keys = []

    if not s3_path.startswith('s3://'):
        s3_path = 's3://' + s3_path

    bucket = s3_path.split('//')[1].split('/')[0]
    prefix = '/'.join(s3_path.split('//')[1].split('/')[1:])

    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append("s3a://logs-backup.cloudmi.datami.com/"+obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys

#get_all_s3_keys('logs-backup.cloudmi.datami.com/deployments/cbr/aacmi/audit/ip-10-10-62-61/')


