# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlsaprimework001.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsaprimework001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsaprimework001.dfs.core.windows.net", "318c6d5f-121a-4590-be50-37d0a849bc3c")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsaprimework001.dfs.core.windows.net", "HWO8Q~~e0L9btA809lyeGmCwZ1ucjy9VxYGxedeE")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsaprimework001.dfs.core.windows.net", "https://login.microsoftonline.com/c17897af-4a4b-4687-81dc-4233c72580ad/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@adlsaprimework001.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df_silver = spark.read.format("csv")\
                      .option("header", "true")\
                      .option("inferSchema", "true")\
                      .load("abfss://bronze@adlsaprimework001.dfs.core.windows.net/amazon_prime_titles.csv")
df_silver.display()


# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data** **Cleaning**

# COMMAND ----------

df_silver = df_silver.fillna({'rating': 'Unrated', 'country': 'Unknown'})
df_silver.display()

# COMMAND ----------

df_silver = df_silver.dropDuplicates()
df_silver.display()

# COMMAND ----------

null_counts = df_silver.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_silver.columns])
display(null_counts)

# COMMAND ----------

df_silver = df_silver.na.fill({'rating': 'Unrated', 'country': 'Unknown','date_added': '01/01/2018','duration': 'Unknown','description': 'Unknown','director': 'Unknown','cast': 'Unknown','release_year': '2020','title': 'Unknown','type': 'Unknown','show_id': 'Unknown'})
df_silver.display()

# COMMAND ----------

df_silver = df_silver.dropna('any')
df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumnRenamed('title','show_title')
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Tranformation**

# COMMAND ----------

df_silver = df_silver.withColumn('is_country',when(col('country') == 'Unknown',0).otherwise(1))
df_silver.display()


# COMMAND ----------

df_silver.write.format("parquet")\
               .mode("append")\
               .option("path","abfss://silver@adlsaprimework001.dfs.core.windows.net/amazon_prime_titles_silver")\
               .save()

# COMMAND ----------



# COMMAND ----------

