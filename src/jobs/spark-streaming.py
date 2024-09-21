from pyspark.sql import SparkSession
import sys
import os


def start_streaming(spark):
    # Create the stream and read from the socket
    stream_df = spark.readStream.format("socket") \
                                .option("host", "localhost") \
                                .option("port", 9999) \
                                .option("includeTimestamp", "true") \
                                .load()
    
    # Process and display the stream to the console
    query = stream_df.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()

if __name__ == "__main__":
    try:
        spark_conn = SparkSession.builder.appName("SocketStreamConsumer") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
        start_streaming(spark_conn)
    except Exception as e:
        print(f"Error occurred: {e}")
