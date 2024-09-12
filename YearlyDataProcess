from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import year, date_sub, col

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
# df.show(5)

# Extract year from the date column (assuming there is a date column named 'OrderDate')
df = df.withColumn("OrderYear", year(col("ModifiedDate")))

# Get the distinct years in the data
years = df.select("OrderYear").distinct().collect()
years_list = [row["OrderYear"] for row in years]

# Create a dictionary to hold DataFrames for each year
yearly_data = {}

for y in years_list:
    # Filter the data for each year and subtract one year
    df_year = df.filter(col("OrderYear") == y)
    df_previous_year = df_year.withColumn("PreviousYearDate", date_sub(col("ModifiedDate"), 365))
    
    # Store the DataFrame in the dictionary
    yearly_data[y] = df_previous_year

# Show the data for a specific year (e.g., 2012)
if 2012 in yearly_data:
    yearly_data[2012].show()

# Create Separate Datasets for Each Year
for y in years_list:
    yearly_data[y].write.format("delta").mode("overwrite").saveAsTable(f"default.sales_data_{y}", header=True)
