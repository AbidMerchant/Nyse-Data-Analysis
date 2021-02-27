#!/usr/bin/env python
# coding: utf-8

# In[7]:


from pyspark.sql.session import SparkSession
from pyspark.sql import functions as fn

from config import *

spark = SparkSession                 .builder                 .appName("load_to_sql")                 .getOrCreate()

def load_to_sql_from_hdfs(fileName, tableName):
    data_df = spark.read.orc(hdfs_output+fileName)
    
    data_df.write.jdbc(mysql_url,
                       tableName,
                       properties={"user":mysql_un,"password":mysql_password})
    
def main():
    load_to_sql_from_hdfs("weekly_sales_aggr", "output.weekly_sales_aggr")
    load_to_sql_from_hdfs("monthly_sales_aggr", "output.monthly_sales_aggr")
    load_to_sql_from_hdfs("avg_monthly_surcharge", "output.avg_monthly_surcharge")
    load_to_sql_from_hdfs("trips_hourly", "output.trips_hourly")
    load_to_sql_from_hdfs("top_payments_monthly", "output.top_payments_monthly")
    load_to_sql_from_hdfs("payment_dist_monthly", "output.payment_dist_monthly")
    load_to_sql_from_hdfs("total_passengers_monthly", "output.total_passengers_monthly")
    
if __name__ == "__main__":
    main()

