# Databricks notebook source
df=spark.read.format("csv").option("header","true").option("inferschema","true").load("/*.csv")
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

df.write.format("csv").save("dbfs:/FileStore/table1")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/table1

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

# COMMAND ----------

# MAGIC %fs mkdirs /tmp/output

# COMMAND ----------


