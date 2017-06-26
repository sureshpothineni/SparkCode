package com.spark.stream

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import java.util.Calendar
import java.text.SimpleDateFormat

object SparkKafkaLog {
   def main(args: Array[String]) {

      val sparkConf = new SparkConf().setAppName("KafkaLog")
      val sc = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, Seconds(5))
      ssc.checkpoint("checkpoint")

      val currentDate = Calendar.getInstance.getTime
      val minuteFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dataFormatted = minuteFormat.format(currentDate)

      val params =  Map("zookeeper.connect"->"lbdp002a:2181", "bootstrap.servers"-> "lbdp004a.rnd.pncint.net:9092", "zookeeper.connection.timeout.ms" -> "100000")
      val topics = Map("testTopic1"->1)
      val lines = KafkaUtils.createStream(ssc, "lbdp002a:2181", "test", topics)
      //lines.saveAsTextFiles("/user/pl24116/"+dataFormatted+"/spark/","kfk")

      lines.map(_._2).foreachRDD { x =>  
        println(x)
        x.foreach(println)
        x.saveAsTextFile("/user/pl24116/"+dataFormatted)  
      }
      
      println("Testing Kafka")
      ssc.start()
      ssc.awaitTermination()
   }
}