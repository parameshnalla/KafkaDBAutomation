package com.kafastreams.dbautomation
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

import com.kafastreams.dbautomation.StartConsumerGroup.pool
import kafka.server.KafkaConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.ExecutionContext


object StartConsumerGroup extends  App {


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
    .getStringList("topics")
    .toArray()
    .map(_.toString)

  val consumers = Array(
    KafkaConsumer(topics,kafkaParams,"consumer1"),
    KafkaConsumer(topics,kafkaParams,"consumer2")
  )


  val numberOfConsumerToSpin = kafkaConsumerConfig.getInt("number_consumers_to_start")
  val consumerList = (1 to numberOfConsumerToSpin).toList
  val pool: ExecutorService = Executors.newFixedThreadPool(numberOfConsumerToSpin)
  consumerList.foreach( consumer =>
    pool.execute(KafkaConsumer(topics,kafkaParams,s"consumer-$consumer"))
  )
  pool.shutdown()
}
