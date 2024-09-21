from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Create a Spark session
spark = SparkSession.builder.appName("SparkStreamingTest").getOrCreate()

# Create a StreamingContext with a 1-second batch size
ssc = StreamingContext(spark.sparkContext, 1)

# Define a socket stream (on localhost:9999)
lines = ssc.socketTextStream("localhost", 9999)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Map each word to a pair (word, 1)
pairs = words.map(lambda word: (word, 1))

# Reduce by key (word)
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the word counts
wordCounts.pprint()

# Start the streaming computation
ssc.start()

# Wait for the streaming to finish
ssc.awaitTermination()