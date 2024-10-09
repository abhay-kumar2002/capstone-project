# Databricks notebook source
import dlt
from pyspark.sql.functions import col, count

@dlt.table(
    name="bronze_crimes",
    comment="Ingests the crime data in streaming mode"
)
def bronze_crimes():
    
    df = (spark.readStream
          .format("cloudFiles")  
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("dbfs:/FileStore/tables/"))

   
    for column in df.columns:
        df = df.withColumnRenamed(column, column.replace(" ", "_"))

    return df

# COMMAND ----------


import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="silver_crimes_cleaned",
    comment="Cleaned and filtered crimes data"
)
@dlt.expect("valid_location", "location IS NOT NULL")  
def silver_crimes_cleaned():
   
    df = dlt.read("bronze_crimes")
    
  
    cleaned_df = (df.filter(col("location").isNotNull())
                    .filter(col("crime_type") == "THEFT")
                    .filter(col("date") > "2022-01-01"))
    
    return cleaned_df

# COMMAND ----------


import dlt
from pyspark.sql.functions import count

@dlt.table(
    name="gold_crimes_by_location",
    comment="Aggregated crime counts by location"
)
def gold_crimes_by_location():
   
    return (dlt.read("silver_crimes_cleaned")
            .groupBy("location")
            .agg(count("*").alias("crime_count")))
