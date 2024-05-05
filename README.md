Project Setup
Create Container in ADLS:

Create a container in ADLS named sales_view_devtst.
Create folders for customer, product, store, and sales and upload files in sequence for real-time functionality.
ADF Pipeline Setup:

Create an ADF Pipeline to retrieve the latest modified files from the folders in ADLS.
Parameterize the Pipeline for dynamic functionality.
Parameters should facilitate processing of files for any day.
Bronze Layer Setup:

Create Bronze/sales_view/ container.
Subfolders: customer, product, store, sales.
Copy raw data from ADF pipeline to respective subfolders.
Silver Layer Transformation
Customer File Transformation:

Convert all column headers to snake case in lower case.
Split "Name" column into "first_name" and "last_name".
Extract domain from email column.
Assign gender based on "M" or "F".
Split Joining date into date and time.
Format date column to "yyyy-MM-dd".
Assign "expenditure-status" based on spent column.
Write data to silver layer [table_name: customer].
Product File Transformation:

Convert column headers to snake case in lower case.
Create "sub_category" column based on category_id.
Write data to silver layer [table_name: product].
Store File Transformation:

Convert column headers to snake case in lower case.
Create "store category" column from email.
Format created_at and updated_at to "yyyy-MM-dd".
Write data to silver layer [table_name: store].
Sales File Transformation:

Convert column headers to snake case in lower case.
Write data to silver layer [table_name: customer_sales].
Gold Layer Analysis
Gold Layer Analysis:
Retrieve data using product and store tables.
Get specific data fields from customer_sales.
Write data to gold layer [table_name: StoreProductSalesAnalysis].
Note: All dates should be maintained in "yyyy-MM-dd" format.

File Paths:
Silver Layer: silver/sales_view/tablename/{delta pearquet}
Gold Layer: gold/sales_view/tablename/{delta pearquet}
