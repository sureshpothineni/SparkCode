package com.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object dataExtraction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Data Extraction")
    val sc = new SparkContext(conf)
    
    val inputFileRDD = sc.textFile("/user/pl24116/criss_sample")
    val dataLines1 = inputFileRDD.filter(x => x.contains("DTL"))
    val dataLines = inputFileRDD.filter(x => x.contains("DTL")).map(x => ( x.split('|').toList(1) , (x.split('|').toList(0).split(":")(1) , x.split('|').toList drop 2)))
    val dataLinesGroup = dataLines.groupByKey()
    dataLines.take(3).foreach(println)
    //val dataLinesKey = dataLines.map(x => (x(1), (x(0).split(":")(1).stringPrefix, x(2)))).groupByKey()
    
    //val dataLinesKey = dataLines.map(x => (x(1)))
    
    //.groupByKey()

    //dataFilter2 = dataFilter.map(lambda x: (x[1],(x[0].split(":")[1].strip(),x[2:]))).groupByKey().mapValues(list) 
  }
}