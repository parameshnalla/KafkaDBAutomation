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

class KafkaConsumer {

  def startKafkaSparkConsumer(topics: Array[String]) = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("First Spark Job")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    sc.setLogLevel("ERROR")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, KafkaConsumer.kafkaParams)
    )
    stream.foreachRDD(x =>
      if (!x.isEmpty()) {
        println("RDD is not empty !!!! count is " + x.count())

        val key = x.map(record => record.key().asInstanceOf[String])
        val value = x.map(record => record.value().asInstanceOf[String])

        val temp = value.map(y => {
          y.replaceAll("Struct", "")
            .replace("}", "")
            .replace("{", "")
        })
        val temp2 = temp.map(
          x => (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3))
        )
        import spark.implicits._
        val df = temp2.toDF()
        df.coalesce(1)
          .write.format("csv").mode("append").save("/Users/pnalla/myplayground/kafka/temp")
        df.show(100, false)
      }
      else println("RDD is Empty")
    )
    ssc.start()
    ssc.awaitTermination()
  }

}

object KafkaConsumer extends App {
  val logger = Logger.getLogger(classOf[KafkaConsumer])

  val config: Config = ConfigFactory.load()
  val kafkaConsumerConfig = config.getConfig("kafkaProject.kafka_consumer_config")
  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConsumerConfig
      .getList("brokers")
      .unwrapped()
      .toArray()
      .mkString(","),
    ConsumerConfig.GROUP_ID_CONFIG -> kafkaConsumerConfig.getString("group_id"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> kafkaConsumerConfig.getString("auto_off_set_config"),
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (kafkaConsumerConfig.getBoolean("enable_auto_offset_commit"): java.lang.Boolean)
  )
  val topics = kafkaConsumerConfig
    .getList("topics")
    .unwrapped()
    .toArray().map(_.toString)

  logger.info(s"Starting kafkaConsumer with config: ${kafkaParams.mkString("\n")}")
  logger.info(s"Working on the topics : $topics")


}