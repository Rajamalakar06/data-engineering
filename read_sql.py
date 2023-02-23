# Databricks notebook source
jdbcHostname = "rstestsv.database.windows.net"
jdbcDatabase = "testdb"
jdbcPort = 1433
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
  "user" : "sraja3@rstestsv",
  "password" : "Jan@2012#",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}  

# COMMAND ----------

df = spark.read.jdbc(url=jdbcUrl,table="saleslt.customer",properties=connectionProperties)
df1 = spark.read.jdbc(url=jdbcUrl,table="saleslt.SalesOrderHeader",properties=connectionProperties)
df2 = spark.read.jdbc(url=jdbcUrl,table="saleslt.SalesOrderDetail",properties=connectionProperties)

# COMMAND ----------

df1.groupby().sum("SubTotal").collect()

# COMMAND ----------

df.createOrReplaceTempView("customer")
display(df)

# COMMAND ----------

sc=df.join(df1,df['CustomerID']==df1['CustomerID'],"inner").select(df['CustomerID'],'SalesOrderID','OrderDate','SalesOrderNumber','status','SubTotal','TaxAmt','Freight','TotalDue')
display(sc)

# COMMAND ----------

display(df2)

# COMMAND ----------

import pyspark.sql.functions as F

LO_SOL = df2.groupby('SalesOrderID').agg(F.sum('LineTotal').alias("LOT")).sort('SalesOrderID')

# COMMAND ----------

CU_Sales=sc.join(LO_SOL,sc['SalesOrderID']==LO_SOL['SalesOrderID'],"inner").select('CustomerID',sc['SalesOrderID'],sc['OrderDate'],'status','SubTotal','TaxAmt','Freight','TotalDue','LOT')

# COMMAND ----------

display(CU_Sales.sort('CustomerID',ascending = True))

# COMMAND ----------

CU_Sales.write.format('delta').mode("overwrite").save('/output/sales')

# COMMAND ----------

# MAGIC %fs ls dbfs:/output/sales

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from DELTA.`/output/sales/`

# COMMAND ----------

df.sal = spark.read.format("delta").load("/output/sales/")
display(df.sal)

# COMMAND ----------

from pyspark.sql.functions import col

CU_Sales = CU_Sales.withColumn("Tot_Sal",col('SubTotal')+col('TaxAmt'))

# COMMAND ----------

from pyspark.sql.functions import col

CU_Sales = CU_Sales.withColumn("Diff",(col('Tot_Sal')/col('TotalDue')*100)).withColumn("Tot_Frit",(col('SubTotal')+col('TaxAmt')+col('Freight')))




# COMMAND ----------

CU_Sales.write.format("delta").mode("overwrite").option("mergeSchema",  True).save("/output/sales/")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from DELTA.`/output/sales/`
