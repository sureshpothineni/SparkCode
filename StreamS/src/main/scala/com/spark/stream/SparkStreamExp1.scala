package com.spark.drw

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext

object SparkStreamExp1 {
  def main (args : Array[String]): Unit ={
      val envCode = args(0)
      val table = args(1)
     
      //val srcTargetDir : String = "/bdp"+envCode+"/drw/01/str/pub/drwhd01"+envCode+"/"
      
      val conf = new SparkConf().setMaster("local[2]").setAppName("Testing Spark Stream")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(10))
      val lines = ssc.socketTextStream("lbdp167a", 7399)      
      val count =lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)
      count.print()
      ssc.start()
      ssc.awaitTermination()
   }
  
}