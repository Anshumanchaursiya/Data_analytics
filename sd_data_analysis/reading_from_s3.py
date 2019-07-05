import os
import sys
from reading_from_s3_using_boto import get_all_s3_keys
from pyspark.sql.types import StructType, StructField, StringType
os.environ['SPARK_HOME'] = "/Users/datami/Downloads/spark-2.4.3-bin-hadoop2.7"
sys.path.append("/usr/downloads/spark/spark-2.4.3-bin-hadoop2.7/python/")
sys.path.append("/usr/downloads/spark/spark-2.4.3-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip")

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

conf = SparkConf()
conf.set("es.nodes", "localhost:9200")

sc = SparkContext.getOrCreate(conf)
ss = SparkSession(sc)
sqlContext = SQLContext(sc)


region_deploynet="cbr"
#JSON_PATH = ["s3a://logs-backup.cloudmi.datami.com/deployments/cbr/aacmi/audit/ip-10-10-62-61/backup-cbr-aacmi-audit-2017-01-06-14-ip-10-10-62-61-0.gz","s3a://logs-backup.cloudmi.datami.com/deployments/cbr/aacmi/audit/ip-10-10-62-61/backup-cbr-aacmi-audit-2017-01-06-15-ip-10-10-62-61-0.gz"]
JSON_PATH = 's3a://logs-backup.cloudmi.datami.com/deployments/cbr/aacmi/audit/ip-10-10-52-60/backup-cbr-aacmi-audit-1970-01-01-00-ip-10-10-52-60-0.gz'
schema = StructType([
    StructField("trace", StringType(), True)
])

#JSON_PATH = get_all_s3_keys('logs-backup.cloudmi.datami.com/deployments/cbr/aacmi/audit/ip-10-10-62-61/')


df = sqlContext.read.json(JSON_PATH)
#df.printSchema();

#df = sqlContext.read.load("s3a://logs-backup.cloudmi.datami.com/deployments/"+region_deploynet+"/aacmi/audit/ip-10-10-52-60/*.gz")
df.show()
#df.write.format("org.elasticsearch.spark.sql").mode('append').option("es.resource","accmi-logs-2019-06-10/logs").save()

df.write.format("org.elasticsearch.spark.sql").mode('append').option("es.resource","test/logs").save()
