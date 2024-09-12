# Sales Order Data Processing with PySpark
This script reads sales order data from a CSV file, processes it to extract yearly data, and saves the processed data in Delta format for each year. The goal is to extract the year from the ModifiedDate column, adjust the date by subtracting a year, and store the transformed data in separate tables for each year.

# Requirements
Apache Spark with PySpark and Delta Lake installed.
A CSV file containing sales order details, stored at /FileStore/tables/Sales_SalesOrderDetail.csv.

# Schema Definition
The data schema is defined explicitly using PySpark's StructType and StructField to specify data types for each column. The schema includes fields like SalesOrderID, ProductID, UnitPrice, and ModifiedDate (used to extract the year).

# Code Overview
**Schema Definition:**
A custom schema is defined for reading the CSV data, ensuring that each column is correctly typed.

schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    ...
    StructField("ModifiedDate", DateType(), True)
])

**Data Loading:** The sales order data is loaded from the CSV file using the defined schema.

df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

**Year Extraction:** A new column, OrderYear, is created by extracting the year from the ModifiedDate field.

df = df.withColumn("OrderYear", year(col("ModifiedDate")))

**Create Yearly DataFrames:** The script identifies distinct years in the data and processes the records for each year. It subtracts one year from the ModifiedDate for comparison purposes using date_sub().

df_previous_year = df_year.withColumn("PreviousYearDate", date_sub(col("ModifiedDate"), 365))

**Data Storage:** For each year, the resulting DataFrame is written to Delta format in separate tables.

yearly_data[y].write.format("delta").mode("overwrite").saveAsTable(f"default.sales_data_{y}")

**Example Output**
The processed data for each year is saved as a Delta table, e.g., sales_data_2012.

# Usage Instructions
Ensure PySpark and Delta Lake are configured correctly in your Spark environment.
Update the CSV file path if necessary.
Run the script to load and process the data.
The processed data is stored in separate Delta tables for each year under the default Spark catalog.
