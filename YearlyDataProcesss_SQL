from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql import functions as F
from pyspark.sql import Window

# Define the schema
schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("SpecialOfferID", IntegerType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("UnitPriceDiscount", DecimalType(10, 2), True),
    StructField("LineTotal", DecimalType(20, 2), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", DateType(), True)
])

# Load the data with the schema
df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

# Show the first few rows to verify the data
df.show(5)

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("SalesData")

%sql
SELECT DISTINCT YEAR(ModifiedDate) AS OrderYear
FROM SalesData

years = spark.sql("SELECT DISTINCT YEAR(ModifiedDate) AS OrderYear FROM SalesData").collect()
years_list = [row["OrderYear"] for row in years]

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
