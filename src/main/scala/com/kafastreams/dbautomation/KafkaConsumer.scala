package com.kafastreams.dbautomation

/** *
  * Class to provide KafkaConsumer
  */

import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j
import org.apache.log4j.Logger

/**
  * Kafka Consumer model class
  * which have the business logic to process
  * the messages we received from subscribed topics
  * This consumer model created as a runnable which helps to run
  * consumers using the executorService service.
  * @param topics Topics consumer subscribed to.
  * @param kafkaParams KafkaConsumer core params to configure the consumer.
  * @param consumerName Name of the consumer itself.
  */
case class KafkaConsumer(
                          topics: Array[String],
                          kafkaParams: Map[String, Object],
                          consumerName: String
                        ) extends Runnable {
  /**
    * Run method where we have the consumer
    * business logic for stream processing.
    */
  override def run() = {

    // processStream process the stream we received from Kafka topic.
    def processStream(): Unit = {
      // creating teh Direct stream using the spark context
      val stream = KafkaUtils.createDirectStream[String, String](
        KafkaSpark.ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      try {
        // Processing the direct stream.
        stream.foreachRDD(records =>
          if (!records.isEmpty()) {
            println("RDD is not empty !!!! count is " + records.count())
            val offsetRanges = records.asInstanceOf[HasOffsetRanges].offsetRanges
            val key = records.map(record => record.key().asInstanceOf[String])
            val value = records.map(record => record.value().asInstanceOf[String])
            
           
            //CHECK01: Printing each record from DStreams for Testing
            value.foreach(x => println("Testing Record String : " +x))
           
            // Just for testing added consumername to kafka message value.
            // This needs to be updated while running on POC data.
            val recordValue = value.map(x => x)
            import KafkaSpark.spark.implicits._
            val df = recordValue.toDF()
            
            //CHECK02: Print the count of dataframe
             print("Dataframe count: " +df.count())

            //CHECK03: Print the dataframe
             df.collect().foreach( record => println("record =>" +record))
           
            // Performing the upsert(Update/Insert) to the phoenix table.
            /*df.write.format("org.apache.phoenix.spark")
              .mode(SaveMode.Overwrite).options(
              collection.immutable.Map(
              "zkUrl" -> s"${StartConsumerGroup.zooKeeperURL}/hbase-unsecure",
              "table" -> "SCHEMA.TEST")).save()
*/
            // Enable this only for testing locally.
            df.coalesce(1)
              .write
              .format("csv")
              .mode("append")
              .save("/Users/pnalla/myplayground/kafka/temp")
            df.show(100, false)
            stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            println(s"OffsetRanges commited \n :${offsetRanges.toList.mkString("\n")}")
          }
          else {
            println("RDD is Empty")
            println("Hello I'm alive")
          }
        )
        KafkaSpark.ssc.start()
      } catch {
        case _: NullPointerException => println("Stream is empty . Retrying ....")
        case _: java.lang.IllegalStateException => println("Stream not stopped correctly. Retrying ....")
        case ex: Exception => println(s"Generic exception occured \n ${ex.getMessage} \n ${ex.getStackTrace}")
      }
    }

    processStream()
    // Honorable shutdown. That mean we are waiting spark complete its'
    // operation that is currently executing.
    KafkaSpark.ssc.awaitTerminationOrTimeout(20000)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        println(s"Shutting down streaming app... $consumerName")
        KafkaSpark.ssc.stop(true, true)
        println(s"Shutdown of streaming app complete. $consumerName")
      }
    })
  }

}

