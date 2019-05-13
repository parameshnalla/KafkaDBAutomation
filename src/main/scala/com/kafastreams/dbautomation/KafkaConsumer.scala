package com.kafastreams.dbautomation

/** *
  * Class to provide KafkaConsumer
  */

import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

case class KafkaConsumer(
                          topics: Array[String],
                          kafkaParams: Map[String, Object],
                          consumerName: String
                        ) extends Thread {

  override def run() = {
    def processStream(): Unit = {
      val stream = KafkaUtils.createDirectStream[String, String](
        KafkaSpark.ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      try {
        stream.foreachRDD(records =>
          if (!records.isEmpty()) {
            println("RDD is not empty !!!! count is " + records.count())

            val offsetRanges = records.asInstanceOf[HasOffsetRanges].offsetRanges

            val key = records.map(record => record.key().asInstanceOf[String])
            val value = records.map(record => record.value().asInstanceOf[String])

            val recordValue = value.map(x => x + s"$consumerName")
            import KafkaSpark.spark.implicits._
            val df = recordValue.toDF()
            df.coalesce(1)
              .write
              .format("csv")
              .mode("append")
              .save("/Users/pnalla/myplayground/kafka/temp")
            df.show(100, false)
            stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            println(s"OffsetRanges commited \n :${offsetRanges.toList.mkString("\n")}")
          }
          else println("RDD is Empty")
        )
        KafkaSpark.ssc.start()
      } catch {
        case _: NullPointerException => println("Stream is empty . Retrying ....")
        case _: java.lang.IllegalStateException => println("Stream not stopped correctly. Retrying ....")
        case ex: Exception => println(s"Generic exception occured \n ${ex.getMessage} \n ${ex.getStackTrace}")
      }

    }
    processStream()
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

