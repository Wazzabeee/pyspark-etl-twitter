import re
from datetime import datetime

import findspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from decouple import config


def write_row_in_mongo(dataframe):
    mongo_url = config('MONGOACCESS')

    dataframe.write.format("mongo").mode("append").option("uri", mongo_url).save()
    pass


if __name__ == "__main__":
    findspark.init()
    mongo_url = config('MONGOACCESS')
    # Config
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.mongodb.input.uri",
                mongo_url) \
        .config("spark.mongodb.output.uri",
                mongo_url) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    schema = StructType(
        [StructField("created_at", StringType()),
         StructField("message", StringType())]
    )

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "CAST(value AS STRING) as message")

    df = df \
        .withColumn("value", from_json("message", schema)) \
        .select('timestamp', 'value.*')

    # Changing datetime format
    date_process = udf(
        lambda x: datetime.strftime(
            datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
        )
    )
    df = df.withColumn("created_at", date_process(df.created_at))

    # Pre-processing the data
    pre_process = udf(
        lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
    )
    df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

    pipeline_model = PipelineModel.load(r'C:\Users\Clement\PycharmProjects\pythonProject1\models')
    prediction = pipeline_model.transform(df)

    prediction = prediction.select(prediction.created_at, prediction.cleaned_data, prediction.prediction)

    # Load prediction in Mongo
    query = prediction.writeStream.queryName("test_tweets") \
        .foreachBatch(write_row_in_mongo).start()
    query.awaitTermination()
