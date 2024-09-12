# Sales Data Yearly Processing with SQL
This script performs a hybrid of SQL and PySpark operations to process sales data by year. It extracts unique years from the ModifiedDate column, processes each year’s data, and stores the results in Delta tables.

# Requirements
Apache Spark with PySpark and Delta Lake installed.
The SalesData table, which contains sales order details with a ModifiedDate field.

# Overview
**Extract Unique Years:** The script extracts distinct years from the ModifiedDate column in the SalesData table using a SQL query.
SELECT DISTINCT YEAR(ModifiedDate) AS OrderYear
FROM SalesData
**Process Data for Each Year:** For each year, the script performs the following:It queries the sales data for that specific year.It creates a new column, PreviousYearDate, by subtracting 365 days from the ModifiedDate.The result is saved as a Delta table with the naming convention sales_data2_<year>.
for y in years_list:
    query = f"""
    SELECT *, 
           YEAR(ModifiedDate) AS OrderYear, 
           DATE_SUB(ModifiedDate, 365) AS PreviousYearDate
    FROM SalesData
    WHERE YEAR(ModifiedDate) = {y}
    """
    
    # Execute the query and save the results
    df_year = spark.sql(query)
    df_year.write.mode("overwrite").format("delta").saveAsTable(f"default.sales_data2_{y}", header=True)

# Output
Separate Delta tables are created for each year, named sales_data2_<year>.

**Each table includes:**
All original columns from SalesData.
The extracted year in OrderYear.
A PreviousYearDate column with the date shifted back by 365 days.

# Usage Instructions
Ensure the SalesData table is available in your Spark SQL environment.
Execute the script to process data for each distinct year in the ModifiedDate column.
The processed data for each year is stored in separate Delta tables, accessible through Spark.
# Example Output
Tables: sales_data2_2019, sales_data2_2020, etc.
Columns:
OrderYear: Year of the order extracted from ModifiedDate.
PreviousYearDate: Date subtracted by 365 days from ModifiedDate.
