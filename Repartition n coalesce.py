# Databricks notebook source
rdd=sc.parallelize(range(6))
rddCollect = rdd.collect()
print("Number of Partitions: "+str(rdd.getNumPartitions()))
print("Action: First element: "+str(rdd.first()))
print(rddCollect)
print(rdd.glom().collect())

# COMMAND ----------

rdd1=rdd.repartition(3)
print(rdd1.glom().collect())

# COMMAND ----------

rdd2= rdd.coalesce(3)
print(rdd2.glom().collect())

# COMMAND ----------


