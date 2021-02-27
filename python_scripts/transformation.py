#!/usr/bin/env python
# coding: utf-8

# In[19]:


from pyspark.sql.session import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql import Window

from config import *

spark = SparkSession                 .builder                 .appName("Transformation")                 .getOrCreate()

def readData(fileName):
    data_df = spark.read.orc(hdfs_source+fileName)
    
    return data_df

# Removing nulls & extracting columns
def data_preprocess(data_df):
    data_df2 = data_df.where("vendorID is not null")

    data_df3 = data_df2.withColumn("hour", fn.hour("tpep_pickup_datetime"))                        .withColumn("week", fn.weekofyear("tpep_pickup_datetime"))                        .withColumn("month", fn.month("tpep_pickup_datetime"))                        .withColumn("year", fn.year("tpep_pickup_datetime"))
    
    return data_df3

# Week level aggregation of sales of each vendor
def weekly_sales_aggr(data_df):
    week_df = data_df.groupBy("vendorID", "week", "year")                       .agg(fn.round(fn.sum("total_amount"), 2).alias("weekly_sales"))

    week_df.orderBy("vendorID", "year", "week")            .write.orc(hdfs_output+"weekly_sales_aggr", mode="overwrite")

# Month level aggregation of sales of each vendor
def monthly_sales_aggr(data_df):
    month_df = data_df.groupBy("vendorID", "month", "year")                        .agg(fn.round(fn.sum("total_amount"), 2).alias("monthly_sales"))

    month_df.orderBy("vendorID", "year", "month")             .write.orc(hdfs_output+"monthly_sales_aggr", mode="overwrite")

# Average amount of congestion surcharge each vendor charged in each month 
def avg_monthly_surcharge(data_df):
    avg_surcharge_df = data_df.groupBy("vendorID", "month", "year")                                .agg(fn.round(fn.avg("congestion_surcharge"), 2).alias("avg_monthly_surcharge"))

    avg_surcharge_df.orderBy("vendorID", "year", "month")                     .write.orc(hdfs_output+"avg_monthly_surcharge", mode="overwrite")

# Distribution of percentage of trips at each hour of day 
def trips_hourly(data_df):
    total_count = data_df.count()

    hour_df = data_df.groupBy("hour")                       .agg(fn.round(100*(fn.count("*")/total_count), 2).alias("percentage_trips_hourly"))

    hour_df.orderBy("hour")            .write.orc(hdfs_output+"trips_hourly", mode="overwrite")

# Top 3 payment types users used in each month. Get payment method name from id from payments otc table in mysql
def top_payments_monthly(data_df):
    payment_otc = spark.read.jdbc(mysql_url,"source.payment_otc",
                              properties={"user":mysql_un,"password":mysql_password})

    payment_df = data_df.groupBy("payment_type", "month", "year")                              .count()

    window = Window.partitionBy("month", "year")                    .orderBy(fn.desc("count"))

    payment_df2 = payment_df.withColumn("rank", fn.rank().over(window))

    payment_df3 = payment_df2.where("rank <= 3")                              .select("month", "year", "payment_type", "count")

    payment_df4 = payment_df3.join(payment_otc, ["payment_type"])                             .select("month", "year", "payment_name", "count")

    payment_df4.orderBy("year", "month")                .write.orc(hdfs_output+"top_payments_monthly", mode="overwrite")

# Distribution of payment type in each month
def payment_dist_monthly(data_df):
    total_count = data_df.count()

    payment_dist_df = data_df.groupBy("payment_type", "month", "year")                               .count()

    payment_dist_df2 = payment_dist_df.withColumn("payment_type_prcnt_dist",
                                                    fn.round(100*(fn.col("count")/total_count), 2))\
                                      .select("month", "year", "payment_type", "payment_type_prcnt_dist")

    payment_dist_df2.orderBy("year", "month")                     .write.orc(hdfs_output+"payment_dist_monthly", mode="overwrite")

# Total passengers each Vendor served in each month
def total_passengers_monthly(data_df):
    passengers_df = data_df.groupBy("vendorID", "month", "year")                             .agg(fn.sum("passenger_count").cast("Integer").alias("total_passengers_per_month")) 
    passengers_df.orderBy("vendorID", "month", "year")                  .write.orc(hdfs_output+"total_passengers_monthly", mode="overwrite")

# Main Function
def main():
    data_df = readData("dataset")
    
    data_df_new = data_preprocess(data_df)
    
    # Transformation functions
    weekly_sales_aggr(data_df_new)
    monthly_sales_aggr(data_df_new)
    avg_monthly_surcharge(data_df_new)
    trips_hourly(data_df_new)
    top_payments_monthly(data_df_new)
    payment_dist_monthly(data_df_new)
    total_passengers_monthly(data_df_new)
    
if __name__ == "__main__":
    main()

