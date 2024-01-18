# Databricks notebook source
# SWITCHING TO SPARK FOR BASE LANGUAGE

spark

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE SPARK CONNECTION WITH LIVE DATA TO MAKE REQUEST WITH SERVER

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

api_url = "https://twms.testgrid.co/api/devwebapi/arealist.php"

response = requests.get(api_url)

    # DEFINING CALLBACK FOR RESPONSE API 

if response.status_code == 200:
    spark = SparkSession.builder.appName("Metro Cash And Carry").getOrCreate()

    # DEFINING SCHEMA OF TABLE

    schema = StructType([
        StructField("code", StringType(), True),
        StructField("area", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("city", StringType(), True),
    ])

    api_data = response.json()

    #MODEL FOR FETCHING RESPONSE FROM API

    rows = [(item["code"], item["area"],  item["latitude"], item["longitude"], item["city"]) for item in api_data]

    # INITIALIZING DATAFRAME FOR PERFORMING OBSERVATION OF DATA

    df = spark.createDataFrame(rows, schema=schema)
    display(df)

else:
    print(f"Failed to retrieve data from the API. Status code: {response.status_code}")


# COMMAND ----------

import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Metro Cash And Carry").getOrCreate()

api_url = 'https://twms.testgrid.co/api/devwebapi/arealist.php'
response = requests.get(api_url)

if response.status_code == 200:
    api_data = response.json()

    df = spark.createDataFrame(api_data, ['code', 'area', 'latitude', 'longitude', 'city'])

    df.write.format("delta").mode("append").save("/FileStore/tables/delta/createtable")
else:
    print(f"Failed to retrieve data from the API. Status code: {response.status_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC PRINT SCHEMA OF TABLE 

# COMMAND ----------

#here i have print my api formatted schema

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE SPARK SESSION

# COMMAND ----------

# CREATE MY SPARK SESSION FOR STREAMING RESPONSE

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Protob Conversion to Parquet") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md 
# MAGIC SPARK STRUCTURED STREAMING JOB

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %md
# MAGIC CREATING DELTA LAKE TABLE 

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
    .tableName("metro") \
    .addColumn("code", "STRING") \
    .addColumn("area", "STRING") \
    .addColumn("latitude", "STRING") \
    .addColumn("longitude", "STRING") \
    .addColumn("city", "STRING") \
    .property("description", "table created for demo purpose") \
    .location("/FileStore/tables/delta/createtable") \
    .execute()

# COMMAND ----------

from pyspark.sql import SparkSession

# CREATING SPARK SESSION AGAIN

spark = SparkSession.builder.appName("Metro").getOrCreate()

# GETTING MY METRO TABLE DATA

df = spark.sql("SELECT * FROM metro")

# SHOW DATAFRAME

df.show()

# COMMAND ----------

# PRINT SCHEMA

df.printSchema()


# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

api_url = "https://twms.testgrid.co/api/devwebapi/arealist.php"

response = requests.get(api_url)

    # DEFINING CALLBACK FOR RESPONSE API 

if response.status_code == 200:
    spark = SparkSession.builder.appName("Metro Cash And Carry").getOrCreate()

    # DEFINING SCHEMA OF TABLE

    schema = StructType([
        StructField("code", StringType(), True),
        StructField("area", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("city", StringType(), True),
    ])

    api_data = response.json()

    #MODEL FOR FETCHING RESPONSE FROM API

    rows = [(item["code"], item["area"],  item["latitude"], item["longitude"], item["city"]) for item in api_data]

    # INITIALIZING DATAFRAME FOR PERFORMING OBSERVATION OF DATA

    df = spark.createDataFrame(rows, schema=schema)
    display(df)

else:
    print(f"Failed to retrieve data from the API. Status code: {response.status_code}")


# COMMAND ----------

# MAGIC %md
# MAGIC DATA EXPLORATION OF RECORDS

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import matplotlib.pyplot as plt

api_url = "https://twms.testgrid.co/api/devwebapi/arealist.php"

response = requests.get(api_url)

if response.status_code == 200:
    try:
        spark = SparkSession.builder.appName("Metro Cash And Carry").getOrCreate()

        schema = StructType([
            StructField("code", StringType(), True),
            StructField("area", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("city", StringType(), True),
        ])

        api_data = response.json()

        rows = [(item["code"], item["area"], item["latitude"], item["longitude"], item["city"]) for item in api_data]

        df = spark.createDataFrame(rows, schema=schema)

        display(df)

        df.describe().show()

        df = df.withColumn("latitude", df["latitude"].cast(DoubleType()))
        df = df.withColumn("longitude", df["longitude"].cast(DoubleType()))

        df.select("latitude", "longitude").toPandas().hist()

        # Explore correlations
        df.corr().show()

    except Exception as e:
        print(f"An error occurred: {e}")
else:
    print(f"Failed to retrieve data from the API. Status code: {response.status_code}")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC DATA CLEANING OF RECORDS

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

def fetch_and_process_data(api_url, numeric_column_name):
    response = requests.get(api_url)

    if response.status_code != 200:
        print(f"Failed to retrieve data from the API. Status code: {response.status_code}")
        return None

    spark = SparkSession.builder.appName("Metro Cash And Carry").getOrCreate()

    schema = StructType([
        StructField("code", StringType(), True),
        StructField("area", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("city", StringType(), True),
    ])

    api_data = response.json()

    rows = [(item["code"], item["area"], item["latitude"], item["longitude"], item["city"]) for item in api_data]

    df = spark.createDataFrame(rows, schema=schema)
    display(df)

    for col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    if numeric_column_name not in df.columns:
        print(f"Column '{numeric_column_name}' does not exist in the DataFrame.")
        return None

    df = df.filter((col(numeric_column_name) > 0) & (col(numeric_column_name).isNotNull()))

    return df

api_url = "https://twms.testgrid.co/api/devwebapi/arealist.php"
numeric_column_name = "area"
result_df = fetch_and_process_data(api_url, numeric_column_name)


