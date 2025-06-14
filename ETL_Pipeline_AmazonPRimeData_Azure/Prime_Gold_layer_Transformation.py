# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.adlsaprimework001.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsaprimework001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsaprimework001.dfs.core.windows.net", "318c6d5f-121a-4590-be50-37d0a849bc3c")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsaprimework001.dfs.core.windows.net", "HWO8Q~~e0L9btA809lyeGmCwZ1ucjy9VxYGxedeE")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsaprimework001.dfs.core.windows.net", "https://login.microsoftonline.com/c17897af-4a4b-4687-81dc-4233c72580ad/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://silver@adlsaprimework001.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_gold = spark.read.format("parquet")\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load("abfss://silver@adlsaprimework001.dfs.core.windows.net/amazon_prime_titles_silver")
df_gold.display()

# COMMAND ----------

df_gold = df_gold.withColumn("date_added",to_date(df_gold["date_added"],"MM/dd/yyyy"))
df_gold = df_gold.withColumn("Year_added",year(df_gold["date_added"]))
df_gold.display()

# COMMAND ----------

df_gold = df_gold.withColumn("category_1", split(df_gold["listed_in"], ",")[0])
df_gold = df_gold.withColumn("category_2", split(df_gold["listed_in"], ",")[1])
df_gold.display()

# COMMAND ----------

df_gold = df_gold.withColumn("category_2", when(df_gold["category_2"].isNull(),lit("Unknown")).otherwise(df_gold["category_2"]))
df_gold.display()

# COMMAND ----------

df_gold.write.format("delta")\
             .mode("append")\
             .save("abfss://gold@adlsaprimework001.dfs.core.windows.net/amazon_prime_titles_gold.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database gold_layer;

# COMMAND ----------

df_gold.write.format("delta")\
             .mode("append")\
             .option("path","abfss://gold@adlsaprimework001.dfs.core.windows.net/Prime_gold")\
             .saveAsTable('gold_layer.Prime_gold')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_layer.prime_gold

# COMMAND ----------

