# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

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

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Reading JSON

# COMMAND ----------

json_df = spark.read \
            .format('json')\
            .option('inferSchema', True)\
            .option('header', True)\
            .option('multiLine', False)\
            .load(json)

# COMMAND ----------

display(json_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Defination

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC DDL Schema

# COMMAND ----------


my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight DOUBLE, 
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

df = spark.read \
    .format('csv')\
    .schema(my_ddl_schema)\
    .option('header', True)\
    .load(csv)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT

# COMMAND ----------



## df_sel = df.select('Item_Identifier', 'Item_Weight', 'Item_Fat_Content')
df_sel = df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content'))
df_sel.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER/WHERE

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

df.filter((col('Outlet_Location_Type').isin('Tier 1','Tier 2')) & (col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### withColumnRenamed

# COMMAND ----------

df = df.withColumnRenamed('Item_Weight', 'Item_Wt')

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

df = df.withColumn('flag', lit("new"))

# COMMAND ----------

df = df.withColumn('Multiply', col("Item_Wt")*col("Item_MRP"))

# COMMAND ----------

    df.display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF') )\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg') ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Wt',col('Item_Wt').cast(StringType()))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Order By

# COMMAND ----------

df = df.dropna()

# COMMAND ----------

df = df.sort(col('Item_Wt').desc())

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.sort(col('Item_Wt').asc())

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

df = df.withColumnRenamed('Item_Wt','Item_Weight')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(9).display()

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop Duplicates

# COMMAND ----------

df.count()

# COMMAND ----------

df.drop_duplicates().display()

# COMMAND ----------


