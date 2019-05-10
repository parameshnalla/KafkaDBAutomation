package com.kafastreams.dbautomation

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSpark {
  val spark = SparkSession.builder()
    .master("local")
    .appName("First Spark Job")
    .getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))
  sc.setLogLevel("ERROR")
  ssc.checkpoint("checkpoint")
}
