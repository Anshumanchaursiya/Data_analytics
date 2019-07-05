import os
import sys
os.environ['SPARK_HOME'] = "/Users/datami/Downloads/spark-2.4.3-bin-hadoop2.7"
sys.path.append("/usr/downloads/spark/spark-2.4.3-bin-hadoop2.7/python/")
sys.path.append("/usr/downloads/spark/spark-2.4.3-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip")
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#import to run sql query
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
