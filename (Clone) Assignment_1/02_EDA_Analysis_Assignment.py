# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_records,
# MAGIC   ROUND(AVG(age), 2) as avg_age,
# MAGIC   ROUND(AVG(annual_income), 2) as avg_income,
# MAGIC   MIN(age) as min_age,
# MAGIC   MAX(age) as max_age
# MAGIC FROM customer_data_partitioned;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   customer_segment, 
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM customer_data_partitioned 
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY count DESC;
# MAGIC
