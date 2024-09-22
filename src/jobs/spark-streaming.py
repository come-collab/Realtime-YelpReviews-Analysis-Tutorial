import pyspark
from pyspark.sql import SparkSession

def start_streaming(spark):
   stream_df = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9999).load()
   query = stream_df.writeStream.outputMode("append").format("console").start()
   query.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark)