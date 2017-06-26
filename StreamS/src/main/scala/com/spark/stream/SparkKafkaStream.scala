package com.spark.stream

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object SparkKafkaStream {
   def main(args: Array[String]) {

      //val Array(zkQuorum, group, topics, numThreads) = args
      val sparkConf = new SparkConf().setAppName("KafkaWordCount")
      val sc = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, Seconds(2))
      ssc.checkpoint("checkpoint")

      //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      //val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      val params =  Map("zookeeper.connect"->"lbdp155a.uat.pncint.net:2181", "bootstrap.servers"-> "lbdp155a.uat.pncint.net:9092", "group.id" -> "test", "zookeeper.connection.timeout.ms" -> "100000")
      //val lines = KafkaUtils.createStream[String, String](ssc,params, Map("test1"->1), StorageLevel.MEMORY_AND_DISK)
      val topics = Map("test1"->1)
      val lines = KafkaUtils.createStream(ssc, "lbdp155a.uat.pncint.net:2181", "test", topics).map(_._2)
      lines.print()
      val words = lines.flatMap(_.split(" "))
      words.print()
      val wordCounts1 = words.map( x => (x,1)).reduceByKey((x,y) => x+y)
      wordCounts1.print()
      val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(6), Seconds(2), 2)
      wordCounts.print()
      println("lines :"+lines)
      lines.print()
      lines.count()
      //val wordCounts = lines.map(_._2)
      //println("wordCounts :"+wordCounts)
      //wordCounts.print()
      
      //val words1 : String = lines.map(_._2)
      //words1.flatMap ( x => x.split(" ") )
      //val wordCounts1 = words1.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
      //wordCounts.print()
      //val wordCounts = words.map(x => (x, 1L))
      //   .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
      //wordCounts.print()
      println("Testing Kafka")
      ssc.start()
      ssc.awaitTermination()
   }
  
}