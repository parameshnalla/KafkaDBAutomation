package com.kafastreams.dbautomation

object StartConsumerGroup extends  App {

  val consumer1 = new KafkaConsumer
  val consumer2 = new KafkaConsumer
  consumer1.startKafkaSparkConsumer(KafkaConsumer.topics)
  consumer2.startKafkaSparkConsumer(KafkaConsumer.topics)

}
