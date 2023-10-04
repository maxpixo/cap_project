# %%
import findspark
findspark.init("/Users/max/Drive/spark")

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, concat_ws, lower, initcap
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import yaml


# %%
# Create a Spark Session
spark = SparkSession.builder.appName("CreditCardSystem").getOrCreate()

print("Spark Session created.")

# %%


# Read the JSON File from the data folder
customer_data = spark.read.json('/Users/max/Drive/VS_Projects/cap_project/data/cdw_sapp_custmer.json')

print("Data read successfully!")

# %%
# Apply Transformations Based on Mapping Document
transformed_data = customer_data.select(
    col("SSN").alias("SSN"),
    initcap(col("FIRST_NAME")).alias("FIRST_NAME"),
    lower(col("MIDDLE_NAME")).alias("MIDDLE_NAME"),
    initcap(col("LAST_NAME")).alias("LAST_NAME"),
    col("CREDIT_CARD_NO").alias("Credit_card_no"),
    concat_ws(", ", col("STREET_NAME"), col("APT_NO")).alias("FULL_STREET_ADDRESS"),
    col("CUST_CITY").alias("CUST_CITY"),
    col("CUST_STATE").alias("CUST_STATE"),
    col("CUST_COUNTRY").alias("CUST_COUNTRY"),
    col("CUST_ZIP").alias("CUST_ZIP"),
    regexp_replace(col("CUST_PHONE").cast("string"), r"(\d{3})(\d{4})", r"($1)$2").alias("CUST_PHONE"),
    col("CUST_EMAIL").alias("CUST_EMAIL"),
    col("LAST_UPDATED").alias("LAST_UPDATED")
)

print("Data transformation completed successfully!")

# %%

transformed_data = transformed_data.withColumn("SSN", col("SSN").cast(IntegerType()))
transformed_data = transformed_data.withColumn("FIRST_NAME", col("FIRST_NAME").cast(StringType()))
transformed_data = transformed_data.withColumn("MIDDLE_NAME", col("MIDDLE_NAME").cast(StringType()))
transformed_data = transformed_data.withColumn("LAST_NAME", col("LAST_NAME").cast(StringType()))
transformed_data = transformed_data.withColumn("Credit_card_no", col("Credit_card_no").cast(StringType()))
transformed_data = transformed_data.withColumn("FULL_STREET_ADDRESS", col("FULL_STREET_ADDRESS").cast(StringType()))
transformed_data = transformed_data.withColumn("CUST_CITY", col("CUST_CITY").cast(StringType()))
transformed_data = transformed_data.withColumn("CUST_STATE", col("CUST_STATE").cast(StringType()))
transformed_data = transformed_data.withColumn("CUST_COUNTRY", col("CUST_COUNTRY").cast(StringType()))
transformed_data = transformed_data.withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType()))
transformed_data = transformed_data.withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType()))
transformed_data = transformed_data.withColumn("CUST_EMAIL", col("CUST_EMAIL").cast(StringType()))
transformed_data = transformed_data.withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))

print("All columns successfully casted!")



# %%
with open("/Users/max/Drive/VS_Projects/cap_project/config/config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)


db_url = config["database"]["url"]
db_user = config["database"]["user"]
db_password = config["database"]["password"]
table_name = "TEST_DB.Employees_01"


transformed_data.write.format("jdbc") \
 .mode("overwrite") \
 .option("url", db_url) \
 .option("dbtable", table_name) \
 .option("user", db_user) \
 .option("password", db_password) \
 .save()

print("Data stored into the Table")




