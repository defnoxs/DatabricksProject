# Databricks notebook source
container_name = 'project'
account_name = 'projektasdb'
mount_point = '/mnt/project'

# COMMAND ----------

application_id = dbutils.secrets.get(scope='db-project-kv', key='application-id')
tenant_id = dbutils.secrets.get(scope='db-project-kv', key='tenant-id')
secret = dbutils.secrets.get(scope='db-project-kv', key='secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/'
