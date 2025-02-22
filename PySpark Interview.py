# Databricks notebook source
# MAGIC %md
# MAGIC # **PYSPARK INTERVIEW QUESTIONS - ANSH LAMBA**

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1 While ingesting customer data from an external source, you notice duplicate entries. How would you remove duplicates and retain only the latest entry based on a timestamp column?**

# COMMAND ----------

data = [("101", "2023-12-01", 100), ("101", "2023-12-02", 150), 
        ("102", "2023-12-01", 200), ("102", "2023-12-02", 250)]
columns = ["product_id", "date", "sales"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution**

# COMMAND ----------

df = df.withColumn('date',col('date').cast(DateType())) ## converting string to date




# COMMAND ----------

# drop duplicates
df = df.orderBy('product_id','date',ascending = [1,0]).dropDuplicates(subset=['product_id']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2. While processing data from multiple files with inconsistent schemas, you need to merge them into a single DataFrame. How would you handle this inconsistency in PySpark?**

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **4. You are working with a real-time data pipeline, and you notice missing values in your streaming data Column - Category. How would you handle null or missing values in such a scenario?**
# MAGIC
# MAGIC **df_stream = spark.readStream.schema("id INT, value STRING").csv("path/to/stream")**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **5. You need to calculate the total number of actions performed by users in a system. How would you calculate the top 5 most active users based on this information?**

# COMMAND ----------

data = [("user1", 5), ("user2", 8), ("user3", 2), ("user4", 10), ("user2", 3)]
columns = ["user_id", "actions"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **6. While processing sales transaction data, you need to identify the most recent transaction for each customer. How would you approach this task?**

# COMMAND ----------

data = [("cust1", "2023-12-01", 100), ("cust2", "2023-12-02", 150),
        ("cust1", "2023-12-03", 200), ("cust2", "2023-12-04", 250)]
columns = ["customer_id", "transaction_date", "sales"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **7. You need to identify customers who haven’t made any purchases in the last 30 days. How would you filter such customers?**

# COMMAND ----------

data = [("cust1", "2025-12-01"), ("cust2", "2024-11-20"), ("cust3", "2024-11-25")]
columns = ["customer_id", "last_purchase_date"]

df = spark.createDataFrame(data, columns)

df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **8. While analyzing customer reviews, you need to identify the most frequently used words in the feedback. How would you implement this?**

# COMMAND ----------

data = [("customer1", "The product is great"), ("customer2", "Great product, fast delivery"), ("customer3", "Not bad, could be better")]
columns = ["customer_id", "feedback"]

df = spark.createDataFrame(data, columns)

df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **9. You need to calculate the cumulative sum of sales over time for each product. How would you approach this?**

# COMMAND ----------

data = [("product1", "2023-12-01", 100), ("product2", "2023-12-02", 200),
        ("product1", "2023-12-03", 150), ("product2", "2023-12-04", 250)]
columns = ["product_id", "date", "sales"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **10. While preparing a data pipeline, you notice some duplicate rows in a dataset. How would you remove the duplicates without affecting the original order?**

# COMMAND ----------

data = [("John", 25), ("Jane", 30), ("John", 25), ("Alice", 22)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **11. You are working with user activity data and need to calculate the average session duration per user. How would you implement this?**

# COMMAND ----------

data = [("user1", "2023-12-01", 50), ("user1", "2023-12-02", 60), 
        ("user2", "2023-12-01", 45), ("user2", "2023-12-03", 75)]
columns = ["user_id", "session_date", "duration"]
df = spark.createDataFrame(data, columns)

df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **12. While analyzing sales data, you need to find the product with the highest sales for each month. How would you accomplish this?**

# COMMAND ----------

data = [("product1", "2023-12-01", 100), ("product2", "2023-12-01", 150), 
        ("product1", "2023-12-02", 200), ("product2", "2023-12-02", 250)]
columns = ["product_id", "date", "sales"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **13. You are working with a large Delta table that is frequently updated by multiple users. The data is stored in partitions, and sometimes updates can cause inconsistent reads due to concurrent transactions. How would you ensure ACID compliance and avoid data corruption in PySpark?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **14. You need to process a large dataset stored in PARQUET format and ensure that all columns have the right schema (Almost). How would you do this?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **15. You are reading a CSV file and need to handle corrupt records gracefully by skipping them. How would you configure this in PySpark?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **22. You have a dataset containing the names of employees and their departments. You need to find the department with the most employees.**

# COMMAND ----------

data = [("Alice", "HR"), ("Bob", "Finance"), ("Charlie", "HR"), ("David", "Engineering"), ("Eve", "Finance")]
columns = ["employee_name", "department"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **23. While processing sales data, you need to classify each transaction as either 'High' or 'Low' based on its amount. How would you achieve this using a when condition**

# COMMAND ----------

data = [("product1", 100), ("product2", 300), ("product3", 50)]
columns = ["product_id", "sales"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **24. While analyzing a large dataset, you need to create a new column that holds a timestamp of when the record was processed. How would you implement this and what can be the best USE CASE?**

# COMMAND ----------

data = [("product1", 100), ("product2", 200), ("product3", 300)]
columns = ["product_id", "sales"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **25. You need to register this PySpark DataFrame as a temporary SQL object and run a query on it. How would you achieve this?**

# COMMAND ----------

data = [("product1", 100), ("product2", 200), ("product3", 300)]
columns = ["product_id", "sales"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **26. You need to register this PySpark DataFrame as a temporary SQL object and run a query on it (FROM DIFFERENT NOTEBOOKS AS WELL)?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **27. You need to query data from a PySpark DataFrame using SQL, but the data includes a nested structure. How would you flatten the data for easier querying?**

# COMMAND ----------

data = [("product1", {"price": 100, "quantity": 2}), 
        ("product2", {"price": 200, "quantity": 3})]
columns = ["product_id", "product_info"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **28. You are ingesting data from an external API in JSON format where the schema is inconsistent. How would you handle this situation to ensure a robust pipeline?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **29. While reading data from Parquet, you need to optimize performance by partitioning the data based on a column. How would you implement this?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **30. You are working with a large dataset in Parquet format and need to ensure that the data is written in an optimized manner with proper compression. How would you accomplish this?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **31. Your company uses a large-scale data pipeline that reads from Delta tables and processes data using complex aggregations. However, performance is becoming an issue due to the growing dataset size. How would you optimize the performance of the pipeline?**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **43. You are processing sales data. Group by product categories and create a list of all product names in each category.**

# COMMAND ----------

data = [("Electronics", "Laptop"), ("Electronics", "Smartphone"), ("Furniture", "Chair"), ("Furniture", "Table")]
columns = ["category", "product"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **44. You are analyzing orders. Group by customer IDs and list all unique product IDs each customer purchased.**

# COMMAND ----------

data = [(101, "P001"), (101, "P002"), (102, "P001"), (101, "P001")]
columns = ["customer_id", "product_id"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **45. For customer records, combine first and last names only if the email address exists.**

# COMMAND ----------

data = [("John", "Doe", "john.doe@example.com"), ("Jane", "Smith", None)]
columns = ["first_name", "last_name", "email"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **46. You have a DataFrame containing customer IDs and a list of their purchased product IDs. Calculate the number of products each customer has purchased.**

# COMMAND ----------

data = [
    (1, ["prod1", "prod2", "prod3"]),
    (2, ["prod4"]),
    (3, ["prod5", "prod6"]),
]
myschema = "customer_id INT ,product_ids array<STRING>"

df = spark.createDataFrame(data, myschema)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **47. You have employee IDs of varying lengths. Ensure all IDs are 6 characters long by padding with leading zeroes.**

# COMMAND ----------

data = [
    ("1",),
    ("123",),
    ("4567",),
]
schema = ["employee_id"]

df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **48. You need to validate phone numbers by checking if they start with "91"**

# COMMAND ----------

data = [
    ("911234567890",),
    ("811234567890",),
    ("912345678901",),
]
schema = ["phone_number"]

df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **49. You have a dataset with courses taken by students. Calculate the average number of courses per student.**

# COMMAND ----------

data = [
    (1, ["Math", "Science"]),
    (2, ["History"]),
    (3, ["Art", "PE", "Biology"]),
]
schema = ["student_id", "courses"]

df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **50. You have a dataset with primary and secondary contact numbers. Use the primary number if available; otherwise, use the secondary number.**

# COMMAND ----------

data = [
    (None, "1234567890"),
    ("9876543210", None),
    ("7894561230", "4567891230"),
]
schema = ["primary_contact", "secondary_contact"]

df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **51. You are categorizing product codes based on their lengths. If the length is 5, label it as "Standard"; otherwise, label it as "Custom".**

# COMMAND ----------

data = [
    ("prod1",),
    ("prd234",),
    ("pr9876",),
]
schema = ["product_code"]

df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------


