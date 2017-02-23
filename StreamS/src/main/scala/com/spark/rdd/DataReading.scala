package com.spark.rdd

import org.apache.spark._
object DataReading {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Testing Reading")
    val sc = new SparkContext(conf)
    
    val readRdd = sc.textFile("hdfs://bdp-isilonuat01.uat.pncint.net:8020/user/pl24116/input.txt", 2)
    val eachLineSplit = readRdd.map { x => x.split(" ") }.filter { x => x.length>0 }
    val firstValue = eachLineSplit.map { x => (x(0) ,1) }.reduceByKey(_ + _)
    
  }
}