import re
import findspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from decouple import config


def write_row_in_mongo(df):
    mongo_uri = config('MONGOACCESS')

    df.write.format("mongo").mode("append").option("uri", mongo_uri).save()


if __name__ == "__main__":
    findspark.init()

    # Path to the pre-trained model
    path_to_model = r''

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.mongodb.input.uri",
                config('MONGOACCESS')) \
        .config("spark.mongodb.output.uri",
                config('MONGOACCESS')) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    # Spark Context
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # Schema for the incoming data
    schema = StructType([StructField("message", StringType())])

    # Read the data from kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(value AS STRING) as message")

    df = df \
        .withColumn("value", from_json("message", schema))

    # Pre-processing the data
    pre_process = udf(
        lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
    )
    df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

    # Load the pre-trained model
    pipeline_model = PipelineModel.load(path_to_model)
    # Make predictions
    prediction = pipeline_model.transform(df)
    # Select the columns of interest
    prediction = prediction.select(prediction.message, prediction.prediction)

    # Load prediction in Mongo
    query = prediction.writeStream.queryName("tweets") \
        .foreachBatch(write_row_in_mongo).start()
    query.awaitTermination()
