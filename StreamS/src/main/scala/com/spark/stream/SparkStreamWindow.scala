package com.spark.stream

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

object SparkStreamWindow {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CIM Offers Final")
    val sc = new SparkContext(conf)

    
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("lbdp167a.uat.pncint.net",8399)
    val lines2 = ssc.socketTextStream("lbdp167a.uat.pncint.net",8399)
    val lines3 = lines.union(lines2)
    //val lines1 = lines.countByValueAndWindow(Seconds(15), Seconds(10))
    val lines1 = lines.window(Seconds(20), Seconds(10))
    //def createStreamingContext() = {
      // Create a StreamingContext with a 1 second batch size
    //  val ssc = new StreamingContext(sc, Seconds(1))
    //  ssc.checkpoint("")
    //}

    //val ssc1 = StreamingContext.getOrCreate("", createStreamingContext _)
    //val lines1 = lines.reduceByWindow( {(x, y) => x + y}, {(x, y) => x-y}, Seconds(15), Seconds(10))
    //val count = lines1.flatMap(x => x._1.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    //lines1.count()
    val linesReduce = lines.reduceByWindow((x,y) => x+y, Seconds(15), Seconds(10))
    val count =lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    count.print()
      //count.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
  
}