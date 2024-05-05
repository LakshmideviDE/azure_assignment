
%run "/Workspace/Users/21691f0074@mits.ac.in/assignment/bronze_to_silver/util"

raw_sales_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/sales/20240107_sales_data.csv', header=True, inferSchema=True)

# COMMAND ----------

renamed_sales_df = toSnakeCase(raw_sales_df)


# COMMAND ----------

writeTo = f'dbfs:/mnt/Silver/sales_view/customer_sales'
write_delta_upsert(renamed_sales_df, writeTo)