#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# New notebook

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RetailX_Phase1_RDD") \
    .getOrCreate()

sc = spark.sparkContext


# In[2]:


sales_rdd = sc.textFile("Files/sales_data_cap.csv")


# In[3]:


header = sales_rdd.first()

data_rdd = sales_rdd.filter(lambda row: row != header)


# In[4]:


mapped_rdd = data_rdd.map(lambda row: row.split(",")) \
    .map(lambda x: (x[2], float(x[4]) * float(x[5])))


# In[5]:


filtered_rdd = mapped_rdd.filter(lambda x: x[1] > 0)


# In[6]:


product_sales = filtered_rdd.reduceByKey(lambda a, b: a + b)


# In[7]:


result = product_sales.take(10)

for row in result:
    print(row)


# In[8]:


product_sales.saveAsTextFile("Files/output/product_sales")


# In[9]:


sales_df = spark.read.csv("Files/sales_data_cap.csv", header=True, inferSchema=True)
customer_df = spark.read.csv("Files/customer_data.csv", header=True, inferSchema=True)
product_df = spark.read.csv("Files/product_data.csv", header=True, inferSchema=True)


# In[10]:


from pyspark.sql import Row

rdd_df = mapped_rdd.map(lambda x: Row(product_id=x[0], revenue=x[1])).toDF()
rdd_df.show(5)


# In[14]:


from pyspark.sql.functions import col

sales_df = sales_df.withColumn("revenue", col("quantity") * col("price"))

high_value_df = sales_df.filter(col("revenue") > 3000)

high_value_df.show(5)


# In[15]:


sales_customer_df = sales_df.join(customer_df, "customer_id")

city_revenue_df = sales_customer_df.groupBy("city") \
    .sum("revenue") \
    .withColumnRenamed("sum(revenue)", "total_revenue")

city_revenue_df.show()


# In[17]:


full_df = sales_df \
    .join(customer_df, "customer_id") \
    .join(product_df, "product_id")

full_df.show(5)


# In[18]:


full_df.createOrReplaceTempView("retail")


# In[19]:


top_products = spark.sql("""
SELECT product_id, SUM(revenue) AS total_revenue
FROM retail
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 5
""")

top_products.show()


# In[20]:


monthly_trend = spark.sql("""
SELECT 
    month(timestamp) AS month,
    SUM(revenue) AS total_revenue
FROM retail
GROUP BY month
ORDER BY month
""")

monthly_trend.show()


# In[4]:


from pyspark.sql.functions import col

# Load Bronze data
sales_df = spark.read.csv(
    "Files/bronze/sales_data_cap.csv",
    header=True,
    inferSchema=True
)

sales_df.show(5)


# In[5]:


# Remove nulls and invalid records
sales_clean = sales_df \
    .dropna() \
    .filter(col("quantity") > 0)

# Add revenue column
sales_clean = sales_clean.withColumn(
    "revenue",
    col("quantity") * col("price")
)

sales_clean.show(5)


# In[6]:


sales_clean.write.mode("overwrite") \
    .parquet("Files/silver/sales_clean")


# In[7]:


from pyspark.sql.functions import sum

gold_df = sales_clean.groupBy("product_id") \
    .agg(sum("revenue").alias("total_revenue"))

gold_df.show()


# In[8]:


gold_df.write.mode("overwrite") \
    .parquet("Files/gold/product_sales")


# In[9]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RetailX").getOrCreate()
sc = spark.sparkContext

# Load data
sales_rdd = sc.textFile("Files/bronze/sales_data_cap.csv")

# Remove header
header = sales_rdd.first()
data = sales_rdd.filter(lambda x: x != header)

# Map → (product_id, revenue)
mapped = data.map(lambda x: x.split(",")) \
             .map(lambda x: (x[2], float(x[4]) * float(x[5])))

# Filter
filtered = mapped.filter(lambda x: x[1] > 0)

# Reduce
product_sales = filtered.reduceByKey(lambda a, b: a + b)

product_sales.take(10)


# In[12]:


from pyspark.sql.functions import col

sales_df = spark.read.csv("Files/bronze/sales_data_cap.csv", header=True, inferSchema=True)
customer_df = spark.read.csv("Files/bronze/customer_data.csv", header=True)
product_df = spark.read.csv("Files/bronze/product_data.csv", header=True)

# Add revenue
sales_df = sales_df.withColumn("revenue", col("quantity") * col("price"))

# High value transactions
high_value = sales_df.filter(col("revenue") > 5000)

# Join
full_df = sales_df.join(customer_df, "customer_id") \
                  .join(product_df, "product_id")

# Aggregation
city_revenue = full_df.groupBy("city").sum("revenue")

# SQL
full_df.createOrReplaceTempView("retail")

top_products = spark.sql("""
SELECT product_id, SUM(revenue) as total_revenue
FROM retail
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 5
""")

monthly_trend = spark.sql("""
SELECT month(timestamp) as month, SUM(revenue) as total_revenue
FROM retail
GROUP BY month
ORDER BY month
""")


# In[13]:


sales_clean = sales_df.dropna().filter(col("quantity") > 0)

sales_clean = sales_clean.withColumn("revenue", col("quantity") * col("price"))

sales_clean.write.mode("overwrite").parquet("Files/silver/sales_clean")


# In[ ]:


from pyspark.sql.functions import sum

gold_df = sales_clean.groupBy("product_id") \
    .agg(sum("revenue").alias("total_revenue"))

gold_df.write.mode("overwrite").parquet("Files/gold/product_sales")


# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load CSV files
customer_df = spark.read.csv("Files/customer_data.csv", header=True, inferSchema=True)
product_df = spark.read.csv("Files/product_data.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("Files/sales_data_cap.csv", header=True, inferSchema=True)

# Save as Delta Tables
customer_df.write.mode("overwrite").saveAsTable("customer")
product_df.write.mode("overwrite").saveAsTable("product")
sales_df.write.mode("overwrite").saveAsTable("sales")


# In[2]:


from pyspark.sql.types import *

schema = "product_id INT, customer_id INT, quantity INT, price DOUBLE"

df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .load("Files/stream_data")

query = df.writeStream \
    .format("console") \
    .start()


# In[3]:


from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = "product_id INT, customer_id INT, quantity INT, price DOUBLE"

df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load("Files/stream_data")


# In[4]:


df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()


# In[5]:


df_agg = df.groupBy("product_id") \
    .agg(sum("quantity").alias("total_quantity"))


# In[7]:


df_agg.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()


# In[8]:


df_spike = df_agg.filter(col("total_quantity") > 10)

