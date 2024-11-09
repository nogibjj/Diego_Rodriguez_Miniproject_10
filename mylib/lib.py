"""
library functions for using Pyspark to process WDI
"""
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import (
     StructType, 
     StructField, 
     IntegerType, 
     StringType
)

LOG_FILE = "Pyspark_Result.md"


def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")




def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def extract(
    url="""
   https://media.githubusercontent.com/media/nickeubank/MIDS_Data/master/World_Development_Indicators/wdi_small_tidy_2015.csv
    """,
    file_path="data/wdi.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
 

    return file_path

def load_data(spark, data="data/wdi.csv", name="wdi"):
    """Load data with original headers and rename columns for easier handling."""
    
    # Define schema with shorter column names for easier use in the DataFrame
    schema = StructType([
        StructField("Country Name", StringType(), True),
        StructField("Adolescent fertility rate (births per 1,000 women ages 15-19)", IntegerType(), True),
        StructField("Antiretroviral therapy coverage for PMTCT (% of pregnant women living with HIV)", IntegerType(), True),
        StructField("Battle-related deaths (number of people)", IntegerType(), True),
        StructField("CPIA building human resources rating (1=low to 6=high)", IntegerType(), True),
        StructField("CPIA business regulatory environment rating (1=low to 6=high)", IntegerType(), True),
        StructField("CPIA debt policy rating (1=low to 6=high)", IntegerType(), True)
    ])

    # Load data using the schema and header options
    df = spark.read.option("header", "true").schema(schema).csv(data)

    # Rename columns to shorter names after loading
    df = (
        df.withColumnRenamed("Country Name", "country")
          .withColumnRenamed("Adolescent fertility rate (births per 1,000 women ages 15-19)", "fertility_rate")
          .withColumnRenamed("Antiretroviral therapy coverage for PMTCT (% of pregnant women living with HIV)", "viral")
          .withColumnRenamed("Battle-related deaths (number of people)", "battle")
          .withColumnRenamed("CPIA building human resources rating (1=low to 6=high)", "cpia_1")
          .withColumnRenamed("CPIA business regulatory environment rating (1=low to 6=high)", "cpia_2")
          .withColumnRenamed("CPIA debt policy rating (1=low to 6=high)", "debt")
    )

    # Log the first 10 rows for verification
    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query, name): 
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()

def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()

def example_transform(df):
    """does an example transformation on a predefiend dataset"""
    conditions = [
        (col("country") == "Chile")
          | (col("country") == "Colombia")
          | (col("country") == "Brazil")
          | (col("country") == "Peru") 
          | (col("country") == "Uruguay")  
          | (col("country") == "Venezuela"),
    ]

    categories = ["South America"]

    df = df.withColumn("Occupation_Category", when(
        conditions[0], categories[0]
        ).otherwise("Other"))

    log_output("transform data", df.limit(10).toPandas().to_markdown())

    return df.show()