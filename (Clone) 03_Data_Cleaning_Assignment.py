# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   SUM(CASE WHEN annual_income IS NULL THEN 1 ELSE 0 END) as null_income,
# MAGIC   SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) as null_city,
# MAGIC   COUNT(*) as total_records
# MAGIC FROM customer_data_partitioned;
# MAGIC

# COMMAND ----------

# Cell 2 - Clean data with PySpark
from pyspark.sql.functions import col, when, avg as spark_avg

# Calculate mean for imputation
mean_income = spark.sql("SELECT AVG(annual_income) FROM customer_data_partitioned WHERE annual_income IS NOT NULL").collect()[0][0]

# Create cleaned dataset
df_cleaned = spark.table("customer_data_partitioned") \
    .withColumn("annual_income_cleaned", 
                when(col("annual_income").isNull(), mean_income)
                .otherwise(col("annual_income"))) \
    .withColumn("city_cleaned",
                when(col("city").isNull(), "Unknown")
                .otherwise(col("city")))

# Save cleaned data
df_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_data_final_cleaned")

display(df_cleaned.limit(10))
