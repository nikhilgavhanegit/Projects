# Databricks notebook source
dbutils.fs.mount(
    source="wasbs://{0}@{1}.blob.core.windows.net".format('source', 'swamiadls'),
    mount_point="/mnt/<Mount name>",
    extra_configs={
        "fs.azure.account.key.{0}.blob.core.windows.net".format('swamiadls'): 'Vx7oRm87IZ0766dF5HGYRE0fy80h9x/PiRQhAZTq4IjKhbP/7+FRzr29JRK8/KrxH0fw65IsgSJB+AStyyds1A=='
    }
)

# COMMAND ----------

dbutils.fs.ls("/mnt/<Mount name>")

# COMMAND ----------

df = spark.read.format('csv').options(header='True', inferSchema='True').load('dbfs:/mnt/<Mount name>/pizza_sales.csv')
df.show()

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC count(distinct order_id) order_id,
# MAGIC sum(quantity) quantity,
# MAGIC date_format(order_date,'MMM') month_name,
# MAGIC date_format(order_date,'EEEE') day_name,
# MAGIC hour(order_time) order_time,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) total_sales,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_analysis
# MAGIC group by 3,4,5,8,9,10

# COMMAND ----------

