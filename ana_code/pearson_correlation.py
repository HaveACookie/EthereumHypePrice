# the following code runs on pyspark

from pyspark.sql.functions import *
from pyspark.ml.stat import Correlation

df = spark.read.option("inferSchema", "true").option("header", "true").csv("merged_4.csv")

df.corr("Price","Sales")
'''
result: 0.734
'''

df.corr("Price","tweets")
'''
result: 0.908
'''