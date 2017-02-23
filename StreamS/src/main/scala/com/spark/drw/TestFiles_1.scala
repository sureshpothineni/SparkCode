package com.spark.drw


import scala.sys.process._
import org.apache.spark.{SparkConf, SparkContext,SparkFiles}

import java.sql.{DriverManager,Connection,Statement}

object TestFiles_1 {
  def main(args : Array[String]){
      val tableName = args(0)
      val JDBCDriver = "org.apache.hive.jdbc.HiveDriver"
      //val ConnectionURL = "jdbc:hive2://bdpimp01-qa.pncint.net:21050/;auth=noSasl;principal=xabdphiv/_HOST@QA.PNCINT.NET;"
      val ConnectionURL = "jdbc:hive2://bdpimp01-qa.pncint.net:21050/;AuthMech=1;KrbHostFQDN=lbdp217a.qa.pncint.net;KrbServiceName=impala"
      
      //val JDBCDriver = "com.cloudera.impala.jdbc41.Driver"
      //val ConnectionURL = "jdbc:impala://bdpimp01-qa.pncint.net:21050/default;auth=noSasl"
      
      Class.forName(JDBCDriver).newInstance
      //val con = DriverManager.getConnection(ConnectionURL)
      //val stmt = con.createStatement()
      //val rs = stmt.executeQuery("")
      
      val scriptPath = "/home/xx68746/test.py"
      
      val envCode="q"
      
      var impalaConnectionString : String = ""
      if(envCode.equals("t")){
        impalaConnectionString=" -V -k -i bdpimp01-uat.pncint.net:21000 --ssl -s xabdpimp --quiet -B -q"
      } else if(envCode.equals("q")) {
        impalaConnectionString=" -V -k -i bdpimp01-qa.pncint.net:21000 --ssl -s xabdpimp --quiet -B -q"
      } else{
        impalaConnectionString="  -V  -k -i bdpimp01.pncint.net:21000  --ssl  -s xabdpimp  --quiet -B -q"
      }
      
      //val tableName="drw_prjct_sts"
      //val impcommand = "impala-shell -V -k -i bdpimp01-qa.pncint.net:21000 --ssl -s xabdpimp --query=refresh drwhd01q.drw_prjct_qdt_500095665"
      val impcommand = "python /data/bdpq/drw/01/global/code/scripts/impala_refresh.py "+" "+tableName +" "+envCode
      println(impcommand)
      val pb = Process(impcommand)
      val p = pb.run
      val exitCode = p.exitValue
      
      
      /**
      fileStatusVal.foreach { x => 
        val tableName = x.getPath.getName
        val tableHDFSDir = srcTargetDir+"/"+tableName
        val tableHDFSPath :Path = new Path(tableHDFSDir)
        val tableFileStatusVal : Array[FileStatus] = fs.listStatus(tableHDFSPath)
        val noofFiles = tableFileStatusVal.length
        if (noofFiles > 100 && (tableName.contains("qdt") || tableName.contains("sts") )) {
          // Read data from Parquet File
          val filesDF = sqlContext.read.parquet(tableHDFSDir)
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
          
      }*/
  }
}