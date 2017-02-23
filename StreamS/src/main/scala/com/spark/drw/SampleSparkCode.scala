package com.spark.drw

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column

import scala.sys.process._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.Column

object SampleSparkCode {
  def main (args : Array[String]): Unit ={
      val envCode = args(0)
      val table = args(1)
     
      //val srcTargetDir : String = "/bdp"+envCode+"/drw/01/str/pub/drwhd01"+envCode+"/"
      val srcTargetDir : String = "/bdpq/drw/01/str/pub/drwhd01q/"
      val conf = new SparkConf().setAppName("DRW Merge Files")
      val config : Configuration = new Configuration()

      val sc = new SparkContext(conf)
      val sqlContext : SQLContext = new SQLContext(sc)
      sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
      
      val hiveContext : HiveContext = new HiveContext(sc)
      hiveContext.setConf("spark.sql.parquet.binaryAsString", "true")
      
      hiveContext.sql("")

      //val tableName : String = table
      val tableName : String = "employee"
      val tableHDFSDir = srcTargetDir+"/"+tableName
      println(tableHDFSDir)
      val empDF = hiveContext.read.parquet(tableHDFSDir)
      empDF.filter("empname")
      println("Count :"+empDF.count())
      println("Collect :"+empDF.collect())
      case class Employee (empid: Int, empname: String, address: String, sal: Double)
      
      empDF.map { x => (x, "Suresh") }.collect()
      //empDF.map { x => (x.get(0),x) }.filter(x => x.) .collect()
      /**empDF.select("empId").filter("empid=101")
      var newEmpDF = empDF.map(r => Employee(r.getInt(0),r.getString(1),r.getString(2),r.getDecimal(3).doubleValue()))
      var newEMPDF1 = empDF.toDF()
      newEMPDF1.write.format("parquet").save("/bdpq/drw/01/str/pub/drwhd01q/employee4")
      
      val depttableName : String = "department"
      val depttableHDFSDir = srcTargetDir+"/"+depttableName
      println(depttableHDFSDir)

      val empDF11 = empDF.withColumnRenamed("empid", "empid")

      val deptDF = hiveContext.read.parquet(depttableHDFSDir)

      //empDF.join(deptDF, empDF11.col("empid"). == deptDF.col("empid"), "inner").
      val joniDF = empDF.join(deptDF, empDF("empid") === deptDF("empid"),"inner").toDF()
      val joniDF1 = empDF.join(deptDF, empDF("empid") === deptDF("empid"),"inner").select(empDF("empid").alias("employee_id"),empDF("empname").alias("Employee_Name"),deptDF("deptname").alias("Department"))
      
      joniDF1.write.mode("overwrite").format("parquet").save("/bdpq/drw/01/str/pub/drwhd01q/emp")

      var newEMPDF2 = empDF.toDF()
      newEMPDF2.registerTempTable("drwhd01q.employee7")
      newEMPDF2.write.format("parquet").saveAsTable("drwhd01q.employee5")
      
     
      var newJavaRDD = newEmpDF.toJavaRDD()
      //var empDF1 = hiveContext.createDataFrame(newJavaRDD,empSchema)
      
      var empSchema :StructType = empDF.schema
      var empDataDF = empDF.select("empid","empname","address","empsal").toJavaRDD
      var empDF10 = sqlContext.applySchema(empDataDF, empSchema)
      empDF10.registerTempTable("drwhd01q.employeetmp")
      empDF10.foreach { x =>  
        println("Emp Id :"+x.get(0))
      }
      
      //var tmpDF = hiveContext.createDataFrame(empDataDF, empSchema)
      //tmpDF.write.mode("overwrite")format("parquet").saveAsTable("employee3")

      val empObjects = ListBuffer[Employee]()
      
      empDF.collect().foreach { x => 
        val empId = x.getInt(0)
        val empName = x.getString(1)
        val empAdd = x.getString(2)
        val empSal = x.getDecimal(3)
        var empObj = Employee(empId, empName, empAdd, empSal.doubleValue())
        
        println ("Empd Id:"+empId +":"+empName+":"+empAdd+":"+empSal)
        empObjects.append(empObj)
      }
      
      empObjects.foreach { x =>  
        var empRDD = sc.parallelize(Seq(x))
        //var tmpRDD = empRDD.toJavaRDD()
        //var empDF1 = hiveContext.createDataFrame(empRDD, classOf[Employee])
        //var empDF1 = hiveContext.createDataFrame(empRDD, classOf[Employee])
        //empDF1.registerTempTable("employee3")
        //var empDF1 = sqlContext.createDataFrame(empRDD, classOf[Employee])
        //empDF1.write.format("parquet").save(srcTargetDir+"employee3")
        //empDF1.select("empid","empname").write.format("parquet").save(srcTargetDir+"employee1")
      }*/
      
      
  }
}