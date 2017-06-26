package com.spark.rdd

import org.apache.spark._
object DataReading {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Testing Reading")
    val sc = new SparkContext(conf)
    
    val readRdd = sc.textFile("/user/pl24116/input.txt", 2)
    val eachLineSplit = readRdd.map { x => x.split(" ") }.filter { x => x.length>0 }
    val firstValue = eachLineSplit.map { x => (x(0) ,1) }.reduceByKey(_ + _)
    
    val mapVal = readRdd.map { x => (x.split(" ")(0), x)}
    mapVal.filter{case (x, y) => x.size > 0 }
    
    val mapVal1 = readRdd.flatMap { x => x.split(" ") }
    
    
  }
}