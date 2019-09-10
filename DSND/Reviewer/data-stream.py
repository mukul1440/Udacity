import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date
from pyspark.sql.window import Window


# TODO Create a schema for incoming resources
# schema

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("host_id", IntegerType()),
    StructField("host_name", StringType()),
    StructField("neighbourhood_group", StringType()),
    StructField("neighbourhood", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("room_type", StringType()),
    StructField("price", IntegerType()),
    StructField("minimum_nights", IntegerType()),
    StructField("number_of_reviews", IntegerType()),
    StructField("last_review", DateType()),
    StructField("reviews_per_month", FloatType()),
    StructField("calculated_host_listings_count", IntegerType()),
    StructField("availability_365", IntegerType())
])

# TODO create a spark udf to convert time to YYYYmmDDhh format
@psf.udf(StringType())
def udf_convert_time(timestamp):
    return psf.udf(lambda timestamp: parse_date(timestamp).strftime("%Y") + parse_date(timestamp).strftime("%m") + parse_date(timestamp).strftime("%d") + parse_date(timestamp).strftime("%H"))
    

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers",'localhost:9092') \
        .option("subscribe",'SFCrimeStats') \
        .option("maxOffsetsPerTrigger",200) \
        .load() \
        .cache()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    req_columns = []
    for col, type in df.dtypes:
        if type == "int" || type == "double" || type == "float":
            temp_type = [col]
            req_columns = req_columns + temp_type
    kafka_df = df.select(req_columns)
    for col in kafka_df.columns:
        kafka_df = kafka_df.withColumn(col, kafka_df[col].cast("string"))

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")

    distinct_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # TODO get different types of original_crime_type_name in 60 minutes interval
    window = Window().partitionBy().orderBy(psf.col("crime_id"))
    tempdf = distinct_table.select("*", lag('call_datetime').over(w).alias('call_datetime' + "_LAG"))
    tempdf = tempdf.withColumn("interval", (tempdf['call_datetime'] -  tempdf['call_datetime' + "_LAG"]) / 60)
    
    counts_df = tempdf.where(col("interval") >= 60)

    # TODO use udf to convert timestamp to right format on a call_date_time column
    converted_df = counts_df.withColumn("call_datetime", udf_convert_time(counts_df.call_datetime))

    # TODO apply aggregations using windows function to see how many calls occurred in 2 day span
    calls_per_2_days = converted_df.groupBy(window(psf.col("call_datetime"), "2 day")).agg(psf.count("crime_id").alias("call_count_per_day")).select("window.start", "window.end", "call_count_per_day")

    # TODO write output stream
    query = df.selectExpr("CAST(window.start AS STRING)", "CAST(window.end AS STRING)", "CAST(call_count_per_day AS STRING)") \
                         .write \
                         .format("kafka") \
                         .option("kafka.bootstrap.servers", "localhost:9092") \
                         .option("topic", "SFCrimeStats") \
                         .save()


    # TODO attach a ProgressReporter
    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark = SparkSession.builder \
           .master("local") \
           .appName("SF Crime Stats") \
           .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
