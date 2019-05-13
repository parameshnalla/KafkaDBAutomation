package com.kafastreams.dbautomation

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


object StartConsumerGroup extends App {

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


  val jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
  val jaasCfg = String.format(jaasTemplate, "ny9wddry", "6xR25HSiFmxFLYYVdlgLE3rsM1df9Uoa")
  val additionalParams = Map[String, Object](
    "security.protocol" -> "SASL_SSL",
    "sasl.mechanism" -> "SCRAM-SHA-256",
    "sasl.jaas.config" -> jaasCfg
  )

  val topics = kafkaConsumerConfig
    .getStringList("topics")
    .toArray()
    .map(_.toString)

  val consumers = Array(
    KafkaConsumer(topics, kafkaParams++additionalParams, "consumer1"),
    KafkaConsumer(topics, kafkaParams++additionalParams, "consumer2"),
    KafkaConsumer(topics, kafkaParams++additionalParams, "consumer3")
  )


  val numberOfConsumerToSpin = kafkaConsumerConfig.getInt("number_consumers_to_start")
  val consumerList = (1 to numberOfConsumerToSpin).toList
  val pool: ExecutorService = Executors.newFixedThreadPool(numberOfConsumerToSpin)
  consumerList.foreach(consumer =>
    pool.execute(KafkaConsumer(topics, kafkaParams ++ additionalParams, s"consumer-$consumer"))
  )
  pool.shutdown()
}
