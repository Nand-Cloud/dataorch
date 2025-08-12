# Databricks notebook source
# Cell 1 - Load and inspect data
df = spark.table("customer_data_partitioned")
display(df.limit(10))


# COMMAND ----------

# Cell 2 - Check schema
df.printSchema()


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DESCRIBE FORMATTED customer_data_partitioned;
# MAGIC
