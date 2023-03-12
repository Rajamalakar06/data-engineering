# Databricks notebook source
from pyspark.sql import SparkSession

# Create SparkSession 
#Spark session internally creates a sparkContext variable of SparkContext. You can create multiple SparkSession objects but only one #SparkContext per JVM. In case if you want to create another new SparkContext you should stop existing Sparkcontext (using stop()) before #creating a new one.

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 

# COMMAND ----------

# Create RDD from parallelize    
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd=spark.sparkContext.parallelize(dataList)
rdd.collect()

# COMMAND ----------

print(rdd.getNumPartitions())
print(rdd.glom().collect())

# COMMAND ----------

rdd1 = spark.sparkContext.textFile("/FileStore/tables/Rdd/Rdd_test.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd2.collect()

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

df.write.format("csv").save("dbfs:/FileStore/table1")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/table1", True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

import pandas as pd
from io import StringIO
pdf = df.toPandas()
print(pdf)
pdf.to_csv("/tmp/pandas.csv", encoding="UTF-8")
