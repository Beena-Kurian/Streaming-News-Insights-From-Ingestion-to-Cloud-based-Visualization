import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("RawNewsStream").getOrCreate()
import spark.implicits._

// Define schema for incoming JSON from Kafka
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

// Kafka configuration
val kafkaBootstrapServers = "localhost:9092"
val kafkaTopic = "news-stream"

// Read from Kafka
val newsStreamDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", kafkaTopic)
  .option("startingOffsets", "earliest")
  .option("failOnDataLoss", "false")
  .load()

// Extract Kafka Message (JSON)
val newsStreamData = newsStreamDF.selectExpr("CAST(value AS STRING) AS json_data")

// Parse JSON into structured columns
val newsStreamParsed = newsStreamData.select(
  from_json(col("json_data"), newsArticleSchema).as("data")
).select("data.*")

// Select the desired fields (and cast timestamp if needed)
val rawArticles = newsStreamParsed.select(
  col("publishedAt").cast("timestamp").alias("publishedAt"),
  $"source.name".alias("source_name"),
  $"title",
  $"author",
  $"description",
  $"content",
  $"url",
  $"urlToImage"
)
---------------------------------------------------------------------------------

// Write to GCS as Parquet
val rawQuery = rawArticles.writeStream
  .format("parquet")
  .outputMode("append")
  .option("path", "gs://big_data_news_stream/news-data/raw-articles")
  .option("checkpointLocation", "gs://big_data_news_stream/news-data/checkpoint-raw")
  .start()

rawQuery.awaitTermination()

---------------------------------------------------------------------------------
//to check read data from bucket
val rawDF = spark.read.parquet("gs://big_data_news_stream/news-data/raw-articles")
rawDF.show(5, false)
---------------------------------------------------------------------------------


