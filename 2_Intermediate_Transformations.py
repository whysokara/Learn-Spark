# Databricks notebook source
dbutils.fs.ls('/FileStore/tables')
csv = '/FileStore/tables/BigMart_Sales.csv'
json = '/FileStore/tables/drivers.json'
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read \
        .format('csv') \
        .option('inferSchema', True) \
        .option('header', True) \
        .load(csv)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION and UNION BY Name

# COMMAND ----------


data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas'),
        ('2','sid')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

new_df = df1.union(df2)
# new_df = df1.unionByName(df2) when column order is different

# COMMAND ----------

new_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Functions

# COMMAND ----------

df.select(initcap('Item_Type')).display()
# df.select(lower('Item_Type')).display()
# df.select(upper('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Functions

# COMMAND ----------

df = df.withColumn('Current Date',current_date())
df.display()

# COMMAND ----------

df = df.withColumn('A week later', date_add(col('Current date'),7))
# df = df.withColumn('A week later', date_format(date_add(col('Current date'),7),'dd-MM-yyyy'))

# COMMAND ----------


df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DateDiff

# COMMAND ----------

## Current minus 7 days
df = df.withColumn('Date Difference',datediff('A week later','Current Date'))



# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('A week laterr',date_format('A week later','dd-MM-yyyy'))

df.display()
     

# COMMAND ----------


