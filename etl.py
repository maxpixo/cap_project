import findspark
findspark.init("/Users/max/Drive/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col, regexp_replace, concat_ws, lower, initcap, when, concat, year, month, dayofmonth, lpad, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from app_config import db_config,urls,files_name
import requests
import json
from tqdm import tqdm
from time import sleep

# Create spark session
spark = SparkSession.builder.appName("Cap_app").getOrCreate()


spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")



# Progress Bar
def progress(r, msg):
    for item in tqdm(range(r), desc=msg, colour="green"):
        sleep(0.03)

# Load data into Database
def save_df_mysql(df, table_name):
    df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", db_config["url"]) \
    .option("dbtable", table_name) \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .save()
    


# Load data from API
def get_loan_data_as_dataframe(sp):
    

    # Send an HTTP GET request to the API
    response = requests.get(urls["loan_api"])

    # Check the status code of the response
    if response.status_code == 200:
        # Convert the JSON response to a Python dictionary
        data_dict = json.loads(response.text)


        # Create a Spark DataFrame from the JSON data
        loan_df = sp.createDataFrame(data_dict)

        return loan_df
    else:
        # Handle Status Code errors
        print(f"Error: Status Code {response.status_code}")
        return None






################################  TRANSFORM  ################################


###   Transform branch   ###
def transform_data_branch(raw_data):
    transformed_branch = raw_data.select(
        col("BRANCH_CODE").cast("integer").alias("BRANCH_CODE"),
        col("BRANCH_NAME").cast("string").alias("BRANCH_NAME"),
        col("BRANCH_STREET").cast("string").alias("BRANCH_STREET"),
        col("BRANCH_CITY").cast("string").alias("BRANCH_CITY"),
        col("BRANCH_STATE").cast("string").alias("BRANCH_STATE"),
        when(col("BRANCH_ZIP").isNull(), 99999).otherwise(col("BRANCH_ZIP").cast(IntegerType())).alias("BRANCH_ZIP"),
        regexp_replace(col("BRANCH_PHONE"), r"(\d{3})(\d{3})(\d{4})", r"($1)$2-$3").alias("BRANCH_PHONE"),
        col("LAST_UPDATED").cast("timestamp").alias("LAST_UPDATED")
    )


    return transformed_branch

###   Transform customer   ###
def transform_data_customer(raw_data):
    transformed_customer = raw_data.select(
        col("SSN").cast(IntegerType()).alias("SSN"),
        initcap(col("FIRST_NAME")).cast(StringType()).alias("FIRST_NAME"),
        lower(col("MIDDLE_NAME")).cast(StringType()).alias("MIDDLE_NAME"),
        initcap(col("LAST_NAME")).cast(StringType()).alias("LAST_NAME"),
        col("CREDIT_CARD_NO").cast(StringType()).alias("Credit_card_no"),
        concat_ws(", ", col("STREET_NAME"), col("APT_NO")).cast(StringType()).alias("FULL_STREET_ADDRESS"),
        col("CUST_CITY").cast(StringType()).alias("CUST_CITY"),
        col("CUST_STATE").cast(StringType()).alias("CUST_STATE"),
        col("CUST_COUNTRY").cast(StringType()).alias("CUST_COUNTRY"),
        col("CUST_ZIP").cast(IntegerType()).alias("CUST_ZIP"),
        regexp_replace(col("CUST_PHONE").cast(StringType()), r"(\d{3})(\d{4})", r"($1)$2").alias("CUST_PHONE"),
        col("CUST_EMAIL").cast(StringType()).alias("CUST_EMAIL"),
        col("LAST_UPDATED").cast(TimestampType()).alias("LAST_UPDATED")
    )

    return transformed_customer



###   Transform credit   ###
def transform_data_credit(raw_data):

    transformed_data = raw_data.select(
        col("CREDIT_CARD_NO").cast(StringType()).alias("CUST_CC_NO"),
        date_format(
            to_date(
                concat_ws("-", col("YEAR"), col("MONTH"), col("DAY")),
                "yyyy-MM-dd"
            ),
            "yyyyMMdd"
        ).cast("string").alias("TIMEID"),
        col("CUST_SSN").cast(IntegerType()).alias("CUST_SSN"),
        col("BRANCH_CODE").cast(IntegerType()).alias("BRANCH_CODE"),
        col("TRANSACTION_TYPE").cast(StringType()).alias("TRANSACTION_TYPE"),
        col("TRANSACTION_VALUE").cast(DoubleType()).alias("TRANSACTION_VALUE"),
        col("TRANSACTION_ID").cast(IntegerType()).alias("TRANSACTION_ID")
    )

    return transformed_data




###   Transform loan api   ###
def transform_data_api(raw_data):

    transformed_data = raw_data.select(
        col("Application_ID").cast(StringType()).alias("Application_ID"),
        col("Gender").cast(StringType()).alias("Gender"),
        col("Married").cast(StringType()).alias("Married"),
        col("Dependents").cast(StringType()).alias("Dependents"),
        col("Education").cast(StringType()).alias("Education"),
        col("Self_Employed").cast(StringType()).alias("Self_Employed"),
        col("Credit_History").cast(IntegerType()).alias("Credit_History"),
        col("Property_Area").cast(StringType()).alias("Property_Area"),
        col("Income").cast(StringType()).alias("Income"),
        col("Application_Status").cast(StringType()).alias("Application_Status")
    )

    return transformed_data






def etl_etl():

    #EXTRACT  ###
    df_branch = spark.read.json(files_name["branch_file"])
    df_customer = spark.read.json(files_name["customer_file"])
    df_credit = spark.read.json(files_name["credit_file"])
    df_loan_api = get_loan_data_as_dataframe(spark)
    
    progress(50, "DATA EXTRACTING...")

    #TRANSFORM  ###
    transform_customer = transform_data_customer(df_customer)
    transform_branch = transform_data_branch(df_branch)
    transform_credit = transform_data_credit(df_credit)
    transform_loan_api = transform_data_api(df_loan_api)
    
    progress(50, "DATA TRANSFORMING...")

    #LOAD  ###
    save_df_mysql(transform_branch, "CDW_SAPP_BRANCH")
    save_df_mysql(transform_customer, "CDW_SAPP_CUSTOMER")
    save_df_mysql(transform_credit, "CDW_SAPP_CREDIT_CARD")
    save_df_mysql(transform_loan_api, "CDW_SAPP_loan_application")
    
    progress(50, "DATA LOADING IN DATABASE...")
    


    spark.stop()



