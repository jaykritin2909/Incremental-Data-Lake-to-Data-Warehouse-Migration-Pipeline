import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, trim
from datetime import datetime

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'last_run_date'])
last_run_date = args['last_run_date']  # Passed from Airflow

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read data from S3
orders_df = spark.read.csv(
    "s3://company-data-lake/raw/historical_orders.csv", 
    header=True, 
    inferSchema=True
)

# Step 2: Data cleaning
orders_df = orders_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
                     .withColumn("customer_name", trim(col("customer_name"))) \
                     .filter(col("order_amount") > 0)

# Step 3: Incremental load - only load data after last_run_date
orders_df = orders_df.filter(col("order_date") > last_run_date)

# Step 4: Remove duplicates
orders_df = orders_df.dropDuplicates(["order_id"])

# Step 5: Write cleaned data to Redshift
orders_df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://redshift-cluster:5439/dev") \
    .option("dbtable", "public.historical_orders_cleaned") \
    .option("user", "admin") \
    .option("password", "Password123") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .mode("append") \
    .save()

job.commit()
