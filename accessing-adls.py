# Databricks notebook source
tenant =dbutils.secrets.get(scope="aztst-scope",key="tenant")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.aztst.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.aztst.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.aztst.dfs.core.windows.net", dbutils.secrets.get(scope="aztst-scope",key="Client"))
spark.conf.set("fs.azure.account.oauth2.client.secret.aztst.dfs.core.windows.net", dbutils.secrets.get(scope="aztst-scope",key="app-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.aztst.dfs.core.windows.net",f"https://login.microsoftonline.com/{tenant}/oauth2/token")

# COMMAND ----------

df=spark.read.option("header",True).csv("abfs://input@aztst.dfs.core.windows.net/*.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

CREATE TABLE <database-name>.<table-name>;

COPY INTO <database-name>.<table-name>
FROM 'abfss://container@storageAccount.dfs.core.windows.net/path/to/folder'
FILEFORMAT = CSV
COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------


