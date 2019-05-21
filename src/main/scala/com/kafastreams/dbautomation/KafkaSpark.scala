package com.kafastreams.dbautomation
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/***
  * KafkaSpark builder,
  * this object is central place for creating the spark context and maintaining.
  */
object KafkaSpark {
  val spark = SparkSession.builder()
    .master("local")
    .appName("First Spark Job")
    .getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(
    StartConsumerGroup
      .config
      .getInt("kafkaProject.spark_config.poll_interval")))
  sc.setLogLevel("ERROR")
  ssc.checkpoint("checkpoint")
}
