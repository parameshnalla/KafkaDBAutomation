package com.kafastreams.dbautomation

/** *
  * Class to provide KafaConfiguration
  */

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

case class KafkaConsumer(
                          topics: Array[String],
                          kafkaParams: Map[String, Object],
                          consumerName: String
                        ) extends Thread {


  override def run() = {

    val stream = KafkaUtils.createDirectStream[String, String](
      KafkaSpark.ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    println(s"Consumer picked message: $consumerName")
    stream.foreachRDD(x =>
      if (!x.isEmpty()) {
        println("RDD is not empty !!!! count is " + x.count())

        val key = x.map(record => record.key().asInstanceOf[String])
        val value = x.map(record => record.value().asInstanceOf[String])

        val temp2 = value.map(x => x + s"$consumerName")
        import KafkaSpark.spark.implicits._
        val df = temp2.toDF()
        df.coalesce(1)
          .write.format("csv").mode("append").save("/Users/pnalla/myplayground/kafka/temp")
        df.show(100, false)
      }
      else println("RDD is Empty")
    )
    KafkaSpark.ssc.start()
    KafkaSpark.ssc.awaitTermination()
  }

}

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