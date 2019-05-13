package com.kafastreams.dbautomation

import com.kafastreams.dbautomation.StartConsumerGroup.config
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
