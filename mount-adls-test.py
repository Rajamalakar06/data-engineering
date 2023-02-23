# Databricks notebook source
tenant=dbutils.secrets.get(scope="aztst-scope",key="tenant")

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="aztst-scope",key="Client"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="aztst-scope",key="app-secret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://input@aztst.dfs.core.windows.net/",
  mount_point = "/mnt/input",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt')

# COMMAND ----------


