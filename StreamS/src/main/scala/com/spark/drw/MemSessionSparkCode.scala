package com.spark.drw

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.sys.process._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.Column
import org.apache.spark.sql.SaveMode
import org.apache.spark.HashPartitioner
//import scala.collection.Iterator
//import java.util.ArrayList
import java.util._
import scala.collection.JavaConversions._

object MemSessionSparkCode {
  def main(args: Array[String]): Unit = {
    val envCode = args(0)
    val envVer = args(1)
    val load_date = args(2)

    println("Input load_date  :"+load_date)

    val conf = new SparkConf().setAppName("MemSession Generation for iFind")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val hiveContext: HiveContext = new HiveContext(sc)
    hiveContext.setConf("spark.sql.parquet.binaryAsString", "true")

    val config : Configuration = new Configuration()
    val fs : FileSystem = FileSystem.get(config)

    val srcTargetDir: String = "/bdp"+envCode+"/bdh/"+envVer+"/str/pub/bdhhd01"+envCode+"_etl_stage/teller_txn_dly"

    val tableHDFSDir = srcTargetDir + "/z_load_date=" + load_date
    println(tableHDFSDir)
    val tellerTxnDlyDF = hiveContext.read.parquet(tableHDFSDir)
    //val tellerTxnFilter = hiveContext.sql("select count(*) from bdhhd01q_etl_stage.teller_txn_dly where z_load_date='2017-05-31' ")

    val tellerTxnFilter = tellerTxnDlyDF.filter(tellerTxnDlyDF("log_logonid").notEqual("") && tellerTxnDlyDF("log_custidct").isNotNull).orderBy(tellerTxnDlyDF("log_logonid"), tellerTxnDlyDF("log_caldate"), tellerTxnDlyDF("log_time")).select(tellerTxnDlyDF("rec_num"), tellerTxnDlyDF("log_caldate"), tellerTxnDlyDF("log_time"), tellerTxnDlyDF("teller_key"), tellerTxnDlyDF("log_logonid"), tellerTxnDlyDF("log_trncode"), tellerTxnDlyDF("log_acctnbrc"), tellerTxnDlyDF("log_tamt"), tellerTxnDlyDF("log_cash"), tellerTxnDlyDF("log_custidct").cast(IntegerType))

    val tellerRDD1 = tellerTxnFilter.rdd.map(x => (x.getString(4), x.toString())).partitionBy(new HashPartitioner(1))

    val tellerRDDMap1 = tellerRDD1.mapPartitions(x => memGen(x))

    val removeSrcFilePath: Path = new Path("/bdp"+envCode+"/bdh/"+envVer+"/str/wrk/bdhhd01"+envCode+"_efg_ifi_wrk/memgen")
    println("removeSrcFilePath :"+removeSrcFilePath)
    if(fs.exists(removeSrcFilePath)){
      fs.delete(removeSrcFilePath, true)
    }
    val filePath="/bdp"+envCode+"/bdh/"+envVer+"/str/wrk/bdhhd01"+envCode+"_efg_ifi_wrk/memgen"
    println("filePath"+filePath)
    tellerRDDMap1.coalesce(4).saveAsTextFile(filePath)

  }

  def memGen(dataIter: Iterator[(String, String)]): Iterator[String] = {
    val tellerOutput = new ArrayList[String]()
    var memValue: Int = 0
    while (dataIter.hasNext) {
      var data: (String, String) = dataIter.next()
      var curr: Int = data._2.replace("[", "").replace("]", "").takeRight(1).toInt
      var dataVal: String = data._2.replace("[", "").replace("]", "").replace(",", "|")
      var logRecNum: String = data._2.replace("[", "").replace("]", "").replace(",", "|").split("\\|")(0)
      //var logCalDate: String = data._2.replace("[", "").replace("]", "").replace(",", "|").split("\\|")(1)
      //var logCalTime: String = data._2.replace("[", "").replace("]", "").replace(",", "|").split("\\|")(2)
      if (memValue == 0 && curr == 1) {
        memValue = 1
        tellerOutput.add(dataVal.toString() + "|" + logRecNum + memValue.toString)
      } else if (curr == 0) {
        tellerOutput.add(dataVal.toString() + "|" + logRecNum + memValue.toString)
      } else {
        memValue += 1
        tellerOutput.add(dataVal.toString() + "|" + logRecNum + memValue.toString)
      }
    }
    return tellerOutput.iterator()
  }

}
