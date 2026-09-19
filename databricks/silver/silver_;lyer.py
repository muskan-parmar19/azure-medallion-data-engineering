# Databricks notebook source
# MAGIC %md
# MAGIC ### silver layer script
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Data acccess using app

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.awstoragedatalakemusk.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awstoragedatalakemusk.dfs.core.windows.net", "<OAUTH_PROVIDER>")
spark.conf.set("fs.azure.account.oauth2.client.id.awstoragedatalakemusk.dfs.core.windows.net", "<CLIENT_ID>")
spark.conf.set("fs.azure.account.oauth2.client.secret.awstoragedatalakemusk.dfs.core.windows.net", "<CLIENT_SECRET>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awstoragedatalakemusk.dfs.core.windows.net", "<OAUTH_ENDPOINT>")

# MAGIC %md
# MAGIC #### data loading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Calendar data 
# MAGIC

# COMMAND ----------

df_Cal = spark.read.format("csv")\
    .option('header',True)\
              .option('InferSchema',True)\
                 .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Calendar')
      

# COMMAND ----------

df_Cal.show()

# COMMAND ----------

df_Cust = spark.read.format("csv")\
     .option('header',True)\
         .option('Inferschema',True)\
             .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_Cust.display()

# COMMAND ----------

df_procat = spark.read.format("csv")\
     .option('header',True)\
         .option('Inferschema',True)\
             .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_prosub = spark.read.format("csv")\
     .option('header',True)\
         .option('Inferschema',True)\
             .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Product_Subcategories')

# COMMAND ----------

df_pro = spark.read.format("csv")\
     .option('header',True)\
         .option('Inferschema',True)\
             .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_return = spark.read.format("csv")\
     .option('header',True)\
         .option('Inferschema',True)\
             .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sales = spark.read.format("csv")\
     .option('header',True)\
         .option('Inferschema',True)\
             .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_terri = spark.read.format("csv")\
     .option('header',True)\
         .option('inferSchema',True)\
         .load('abfss://bronze@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Tranformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar
# MAGIC

# COMMAND ----------

df_Cal.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

 df_Cal = df_Cal.withColumn('Month',month('date'))\
     .withColumn('Year',year('date'))


# COMMAND ----------

df_Cal.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Calendar')\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ### customer
# MAGIC

# COMMAND ----------

df_Cust.display()

# COMMAND ----------

df_Cust = df_Cust.withColumn('Full_name',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
df_Cust.display()

# COMMAND ----------

df_Cust.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Customers')\
            .save()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Subcategories

# COMMAND ----------

df_prosub.display()

# COMMAND ----------

df_prosub.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Product_Subcategories')\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Products
# MAGIC

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
               .withColumn('ProductName',split(col('ProductName'),',')[0])

df_pro.display()


# COMMAND ----------

df_pro.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Products')\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ### returns
# MAGIC

# COMMAND ----------

df_return.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Returns')\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ### territories
# MAGIC

# COMMAND ----------

df_terri.display()

# COMMAND ----------

df_terri.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Territories')\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales

# COMMAND ----------

 df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_order')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_terri.display()

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awstoragedatalakemusk.dfs.core.windows.net/AdventureWorks_Sales')\
            .save()