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
sqlContext = SQLContext(sc)
import input

from pyspark.sql.functions import lit

import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

#import org.apache.spark.sql.SparkSession

df = spark.read.csv("demo.csv",header="true")
first_col = df.select("sr")
newdf = df.withColumn("data",lit("20.20.2"))
#df1.show()
#newdf.show()


df1 = spark.read.csv("/Users/datami/PycharmProjects/project1/input/rep26.csv",header="true")
df2 = spark.read.csv("/Users/datami/PycharmProjects/project1/input/rep27.csv",header="true")
list_x = df1.select(' CampaignId')



list_y = df1.select(" CurrentTotalDataUsage(MB)_5-2-2019")
#df1.select(" CampaignId").collect().map(_(0)).toList

print(list_x)
#plotdf=df1.select(" CurrentTotalDataUsage(MB)_5-2-2019");
#plotdf.plot( style=[])
#plt.show(

print(list_y)



#first_col.show()
#df1.show()

