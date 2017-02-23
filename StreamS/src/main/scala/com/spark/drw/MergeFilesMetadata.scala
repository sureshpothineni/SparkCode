package com.spark.drw

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import scala.sys.process._

object MergeFilesMetadata {
  def main (args : Array[String]): Unit ={
      val envCode = args(0)
     
      val srcTargetDir : String = "/bdp"+envCode+"/drw/01/str/pub/drwhd01"+envCode+"/"
      val tempTargetDir : String = "/bdp"+envCode+"/drw/01/str/raw/drwhd01"+envCode+"_raw/tmp/"
      
      val conf = new SparkConf().setAppName("DRW Merge Files")
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
        val tableHDFSPath :Path = new Path(tableHDFSDir)
        val tableFileStatusVal : Array[FileStatus] = fs.listStatus(tableHDFSPath)
        val noofFiles = tableFileStatusVal.length
        var isConsolidationDone = false
        if (noofFiles > 30 && (tableName.contains("metadata") || tableName.contains("info") )) {
          println("tableHDFSDir :"+tableHDFSDir+"No.of Files :"+noofFiles)
          val tmpHDFSPath :Path = new Path(tempTargetDir)
          if(fs.exists(tmpHDFSPath)){
            fs.delete(tmpHDFSPath, true)
          }
          // Read data from Parquet File
          val filesDF = sqlContext.read.parquet(tableHDFSDir)
          // Merge all partitions into single partitions
          filesDF.coalesce(1).write.parquet(tempTargetDir)
    
          val tmpFileStatusVal : Array[FileStatus] = fs.listStatus(tmpHDFSPath)
          
          //Placing single partition file into main table
          tmpFileStatusVal.foreach { x => 
            val tmpFileNames : String =x.getPath.getName.toString()
            if(tmpFileNames.contains("part")){
              println("tablName :"+tableName+":Part FileName :"+tmpFileNames)
              val tmpFilePath = new Path(tempTargetDir+"/"+tmpFileNames)
              val destFilePath = new Path(srcTargetDir+"/"+tableName+"/"+tmpFileNames)
              fs.rename(tmpFilePath,destFilePath)
            }
          }
    
          // Remove all individual files
          val inputFileNames : Array[String] = filesDF.inputFiles
          inputFileNames.foreach { x =>
            val fileName = x.split("/").last
            val removeSrcFilePath: Path = new Path(srcTargetDir+"/"+tableName+"/"+fileName)
            if(fs.exists(removeSrcFilePath)){
              fs.delete(removeSrcFilePath, false)
            }
          }
          
          if(fs.exists(tmpHDFSPath)){
            fs.delete(tmpHDFSPath, true)
          }
          isConsolidationDone = true
      }
        
      if(isConsolidationDone){
          println("Consolidation Table Name :"+tableName)
          val refreshCommand = "python /data/bdp"+envCode+"/drw/01/global/code/scripts/impala_refresh.py "+" "+tableName +" "+envCode
          println("refreshCommand :"+refreshCommand)
          val pb = Process(refreshCommand)
          val p = pb.run
          val exitCode = p.exitValue
      }
          
    }
    
  }
}