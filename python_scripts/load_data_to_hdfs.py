#!/usr/bin/env python
# coding: utf-8

# In[20]:


from pyspark.sql.session import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import *

from config import *

spark = SparkSession                 .builder                 .appName("load_data_to_hdfs")                 .getOrCreate()

def readData(fileName):
    source_schema = StructType([
                            StructField("VendorID", IntegerType(), nullable=True),
                            StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
                            StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
                            StructField("passenger_count", IntegerType(), nullable=True),
                            StructField("trip_distance", DoubleType(), nullable=True),
                            StructField("RatecodeID", IntegerType(), nullable=True),
                            StructField("store_and_fwd_flag", StringType(), nullable=True),
                            StructField("PULocationID", IntegerType(), nullable=True),
                            StructField("DOLocationID", IntegerType(), nullable=True),
                            StructField("payment_type", IntegerType(), nullable=True),
                            StructField("fare_amount", DoubleType(), nullable=True),
                            StructField("extra", DoubleType(), nullable=True),
                            StructField("mta_tax", DoubleType(), nullable=True),
                            StructField("tip_amount", DoubleType(), nullable=True),
                            StructField("tolls_amount", DoubleType(), nullable=True),
                            StructField("improvement_surcharge", DoubleType(), nullable=True),
                            StructField("total_amount", DoubleType(), nullable=True),
                            StructField("congestion_surcharge", DoubleType(), nullable=True)
                       ])
    
    data_df = spark.read.format("csv")                    .option("header", True)                     .option("schema", source_schema)                     .load(source+"dataset")
    
    return data_df
  
    
def writeDataToHdfs(data_df):
    data_df.write.orc(hdfs_source+"dataset", "overwrite")

    
def main():
    data_df = readData("dataset")
    
    writeDataToHdfs(data_df)
    

if __name__ == "__main__":
    main()

