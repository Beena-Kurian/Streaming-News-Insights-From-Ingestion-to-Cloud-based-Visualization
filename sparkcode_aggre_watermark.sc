import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
 
val spark = SparkSession.builder
  .appName("NewsStreamProcessing")
  .getOrCreate()
 
// Define the schema for the incoming data
val newsArticleSchema = new StructType()
  .add("source", StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true)
  )), true)
  .add("title", StringType, true)
  .add("author", StringType, true)
  .add("description", StringType, true)
  .add("content", StringType, true)
  .add("publishedAt", StringType, true)
  .add("url", StringType, true)
  .add("urlToImage", StringType, true)

// Kafka Configuration
val kafkaBootstrapServers = "localhost:9092"
val kafkaTopic = "news-stream"
 
// Read data from Kafka topic "news-stream"
val newsStreamDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", kafkaTopic)
  .load()
 
// Convert the binary 'value' column from Kafka to string
val newsStreamData = newsStreamDF.selectExpr("CAST(value AS STRING) AS json_data")

// Parse the data into a structured DataFrame based on the schema
val newsStreamParsed = newsStreamData.select(
  from_json(col("json_data"), newsArticleSchema).as("data")
).select("data.*")
 
// Apply watermark on the 'publishedAt' column (timestamp column) for handling late data
val newsStreamWithWatermark = newsStreamParsed
  .withColumn("publishedAt", col("publishedAt").cast("timestamp"))
  .withWatermark("publishedAt", "2 minutes")  // Updated to 2-minute watermark

val articleCountBySource = newsStreamWithWatermark
  .groupBy(
    window(col("publishedAt"), "2 minutes"),  // Updated to 2-minute window
    col("source.name")
  )
  .agg(count("*").alias("article_count"))
 

///////////////////////////////////////////////////////////////////////////////////////
// Write the aggregated data to a file sink in kurianbeena2025 Parquet format (append mode only)
///////////////////////////////////////////////////////////////////////////////////////
val query = articleCountBySource.writeStream
  .outputMode("append")  // Append mode for continuous output (required for file sinks)
  .format("parquet")     // Write data in Parquet format
  .option("path", "file:////home//kafka-project/results/output")  
  .option("checkpointLocation", "file:////home/kurianbeena2025/kafka-project/results/checkpoint") 
  .trigger(Trigger.ProcessingTime("2 minutes"))  // Trigger every 2 minutes
  .start()
 
// Await termination of the streaming query
query.awaitTermination()
 
///////////////////////////////////////////////////////////////////////////////////////
// view data
///////////////////////////////////////////////////////////////////////////////////////
val df = spark.read.parquet("file:///home/kurianbeena2025/kafka-project/results/output")
df.orderBy(col("article_count").desc).show(false) 

 