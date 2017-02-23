package com.spark.drw


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

object MergeFiles {
  def main (args : Array[String]): Unit ={
      val envCode = args(0)
      val tableName = args(1)
      
      val srcTargetDir : String = "/bdp"+envCode+"/drw/01/str/pub/drwhd01"+envCode+"/"+tableName+"/"
      val tempTargetDir : String = "/bdp"+envCode+"/drw/01/str/raw/drwhd01"+envCode+"_raw/tmp/"
      
      val conf = new SparkConf().setAppName("DRW Merge Files")
      val sc = new SparkContext(conf)
      val sqlContext : SQLContext = new SQLContext(sc)
      val config : Configuration = new Configuration()
      val fs : FileSystem = FileSystem.get(config)

      // Read data from Parquet File
      val filesDF = sqlContext.read.parquet(srcTargetDir)
      // Merge all partitions into single partitions
      filesDF.coalesce(1).write.parquet(tempTargetDir)

      val tmpHDFSPath :Path = new Path(tempTargetDir)
      val fileStatusVal : Array[FileStatus] = fs.listStatus(tmpHDFSPath)
      
      //Placing single partition file into main table
      fileStatusVal.foreach { x => 
        val tmpFileNames : String =x.getPath.getName.toString()
        if(tmpFileNames.contains("part")){
          val tmpFilePath = new Path(tempTargetDir+"/"+tmpFileNames)
          val destFilePath = new Path(srcTargetDir+"/"+tmpFileNames)
          fs.rename(tmpFilePath,destFilePath)
        }
      }

      // Remove all individual files
      val inputFileNames : Array[String] = filesDF.inputFiles
      inputFileNames.foreach { x =>
        val fileName = x.split("/").last
        val removeSrcFilePath: Path = new Path(srcTargetDir+"/"+fileName)
        fs.deleteOnExit(removeSrcFilePath)
      }
      
      fs.deleteOnExit(tmpHDFSPath)
  }
}