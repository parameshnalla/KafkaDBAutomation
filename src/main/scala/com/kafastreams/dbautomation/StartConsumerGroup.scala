package com.kafastreams.dbautomation

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


/**
  * Basic flow of this POC and the code.
  * Get the message from the Kafka topic
  * process the message then write to phoenix table (Data written into the Hbase table through phoneix)
  * commit the offset in kafka.
  */

/***
  * This object is the main program to start our consumer group.
  * Number consumers spin based on the configuration we set in the configuration file.
  * (number_consumers_to_start).
  * By changing the this property we can increase the number consumers.
  * We are using the Executor service to spin multiple consumers effectively.
  * We will create these consumers and add them into the executor pool to start the consumers.
  */
object StartConsumerGroup extends App {

  val config: Config = ConfigFactory.load()
  val kafkaConsumerConfig = config.getConfig("kafkaProject.kafka_consumer_config")
  val zooKeeperURL = kafkaConsumerConfig.getString("zookeeper_server")

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
    KafkaConsumer(topics, kafkaParams, "consumer1"),
    KafkaConsumer(topics, kafkaParams, "consumer2"),
    KafkaConsumer(topics, kafkaParams, "consumer3")
  )


  val numberOfConsumerToSpin = kafkaConsumerConfig.getInt("number_consumers_to_start")
  val consumerList = (1 to numberOfConsumerToSpin).toList
  // Creating the threadpool based on the config we provided.
  val pool: ExecutorService = Executors.newFixedThreadPool(numberOfConsumerToSpin)
  // Starting the consumers based on property file.
  consumerList.foreach(consumer =>
    pool.execute(KafkaConsumer(topics, kafkaParams, s"consumer-$consumer"))
  )
  pool.shutdown()
}
