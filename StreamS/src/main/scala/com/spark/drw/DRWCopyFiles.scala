package com.spark.drw

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import scala.sys.process._
import java.text.SimpleDateFormat
import java.util.Calendar

object DRWCopyFiles {
  def main (args : Array[String]): Unit ={
      val envCode = args(0)
     
      val srcTargetDir : String = "/bdp"+envCode+"/drw/01/str/pub/drwhd01"+envCode+"/"
      val tempTargetDir : String = "/bdp"+envCode+"/drw/01/str/raw/drwhd01"+envCode+"_raw/tmpcopy/"
      
      val conf = new SparkConf().setAppName("DRW Copy Files")
      val config : Configuration = new Configuration()

      val fs : FileSystem = FileSystem.get(config)

      val srcHDFSPath :Path = new Path(srcTargetDir)
      val fileStatusVal : Array[FileStatus] = fs.listStatus(srcHDFSPath)
      
      val sc = new SparkContext(conf)
      val sqlContext : SQLContext = new SQLContext(sc)
      sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

      fileStatusVal.foreach { x => 
        val tableName : String = x.getPath.getName
        val tableHDFSDir = srcTargetDir+"/"+tableName
        var isConsolidationDone = false
        val tableHDFSPath :Path = new Path(tableHDFSDir)
        val tableFileStatusVal : Array[FileStatus] = fs.listStatus(tableHDFSPath)
        val noofFiles = tableFileStatusVal.length
        if (noofFiles > 1 &&  (tableName.contains("_insert")) ) {
          println("tableHDFSDir :"+tableHDFSDir+" noofFiles :"+noofFiles)
          val tmpHDFSPath :Path = new Path(tempTargetDir)
          // Read data from Parquet File
          val filesDF = sqlContext.read.parquet(tableHDFSDir)
          // Merge all partitions into single partitions
          filesDF.coalesce(1).write.parquet(tempTargetDir)
    
          val tmpFileStatusVal : Array[FileStatus] = fs.listStatus(tmpHDFSPath)
          
          var now = Calendar.getInstance().getTime()
          var minuteFormat = new SimpleDateFormat("yy/MM/dd hh:mm:ss")
          var currentMinuteAsString = minuteFormat.format(now)

          val destTableDirName = tableName.substring(0, tableName.lastIndexOf("_insert"))
          //Placing single partition file into main table
          tmpFileStatusVal.foreach { x => 
            val tmpFileNames : String =x.getPath.getName.toString()
            if(tmpFileNames.contains("part")){
              println(currentMinuteAsString+" "+"tablName :"+tableName+":Part FileName :"+tmpFileNames+":Destination Table :"+destTableDirName)
              val tmpFilePath = new Path(tempTargetDir+"/"+tmpFileNames)
              val destFilePath = new Path(srcTargetDir+"/"+destTableDirName+"/"+tmpFileNames)
              fs.rename(tmpFilePath,destFilePath)
            }
          }

          now = Calendar.getInstance().getTime()
          minuteFormat = new SimpleDateFormat("yy/MM/dd hh:mm:ss")
          currentMinuteAsString = minuteFormat.format(now)
          
          println(currentMinuteAsString+" "+"Before removing files")
          // Remove all individual files
          val inputFileNames : Array[String] = filesDF.inputFiles
          inputFileNames.foreach { x =>
            val fileName = x.split("/").last
            val removeSrcFilePath: Path = new Path(srcTargetDir+"/"+tableName+"/"+fileName)
            if(fs.exists(removeSrcFilePath)){
              fs.delete(removeSrcFilePath, false)
            }
          }
          
          now = Calendar.getInstance().getTime()
          minuteFormat = new SimpleDateFormat("yy/MM/dd hh:mm:ss")
          currentMinuteAsString = minuteFormat.format(now)

          println(currentMinuteAsString+" "+"After removing files")

          if(fs.exists(tmpHDFSPath)){
            fs.delete(tmpHDFSPath, true)
          }
          isConsolidationDone = true

          println("Consolidation Table Name :"+destTableDirName)
          val refreshCommand = "python /data/bdp"+envCode+"/drw/01/global/code/scripts/impala_refresh.py "+" "+destTableDirName +" "+envCode
          println("refreshCommand :"+refreshCommand)
          val pb = Process(refreshCommand)
          val p = pb.run
          val exitCode = p.exitValue
      }
        
    }
    
  }
}