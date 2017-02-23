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
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.sys.process._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.Column
import org.apache.spark.sql.SaveMode


object CIMSparkCode {
  def main (args : Array[String]): Unit ={
      val envCode = args(0)
      val table = args(1)
     
      //val srcTargetDir : String = "/bdp"+envCode+"/drw/01/str/pub/drwhd01"+envCode+"/"
      val conf = new SparkConf().setAppName("CIM Offers Final")
      val sc = new SparkContext(conf)
      val sqlContext : SQLContext = new SQLContext(sc)
      sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
      val hiveContext : HiveContext = new HiveContext(sc)
      hiveContext.setConf("spark.sql.parquet.binaryAsString", "true")
      
      val cal : Calendar = Calendar.getInstance
      
        
      val dateFormat : SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      //val dateKey = dateFormat.format(cal.getTime)
      val dateKey="20160822"
      println(dateKey)
      //val srcTargetDir : String = "/bdpd/bdh/01/str/pub/bdhhd01d_bdh/"
      val srcTargetDir : String = "/bdpp/bda/01/str/pub/dlgmai01pii/bdahd01p_dlmai1_mai_ada2cim/cim_offers"
      
      val l  = List("9","10","11","17","18")
      
      //val tableName : String = table
      val custIntratableName : String = "cust_intractn_event_fact"
      //val tableHDFSDir = srcTargetDir+"/"+custIntratableName+"/date_key="+dateKey
      val tableHDFSDir = srcTargetDir+"/date_key=20161114"
      println(tableHDFSDir)
      val custEventFactDF = hiveContext.read.parquet(tableHDFSDir)
      
      //val sqlDF = hiveContext.sql("select count(distinct(successful_responses)) ,count(distinct(accept_responses))	from (select date_key,case when nbr_successful_responses=1 then cif_permanent_key else null end  as successful_responses ,case when nbr_accept_responses=1 then cif_permanent_key else null end  as accept_responses from  test_offers ) X ")
      
      hiveContext.sql("select count(distinct(cif_permanent_key)) ,count(distinct(cif_key)) from  test_offers")
      
      hiveContext.sql("use bdahd01p_dlmai1_mai_ada2cim")
      hiveContext.sql("select count(*) from test_offers")
      val permanentKeyDF = custEventFactDF.select(custEventFactDF("cif_permanent_key").alias("cif_permanent_key")).distinct()            
      val cifKeyDF = custEventFactDF.select(custEventFactDF("cif_key").alias("cif_key")).distinct()
      
      custEventFactDF.groupBy("cif_permanent_key")
      val unionKey = permanentKeyDF.unionAll(cifKeyDF) 
      .select(
          permanentKeyDF("cif_permanent_key").alias("CIF Permanent Key") , 
          cifKeyDF("cif_key").alias("CIF Key"))
      
          
      custEventFactDF.select(custEventFactDF("cif_permanent_key"), custEventFactDF("cif_key")).distinct().count   //728205  //727715 727628
      
      //select count(distinct(successful_responses))
     	//--,count(distinct(accept_responses))	
      
      
      println("Count :"+custEventFactDF.count())
      val custEventFactFilterDF = custEventFactDF.filter(custEventFactDF("channel_key").isin("9","10","11","17","18"))
      val custEventFactFilterDF1516 = custEventFactDF.filter(custEventFactDF("channel_key").isin("15","16"))

      //val wrkCIMOffersDF = custEventFactFilterDF.unionAll(distinctCustEventFactFilterDF)
      
      //println("Final Count"+wrkCIMOffersDF.count())
      
      val timeDimTableName : String = "time_dim"
      val timeDimHDFS = srcTargetDir+"/"+timeDimTableName
      val timeDimDF = hiveContext.read.parquet(timeDimHDFS)
      //println("Count :"+timeDimDF.count())
      
      val offerDimTableName : String = "offer_dim"
      val offerDimHDFS = srcTargetDir+"/"+offerDimTableName
      val offerDimDF = hiveContext.read.parquet(offerDimHDFS)
      //println("Count :"+offerDimDF.count())
      
      val interactionReasonTableName : String = "interaction_reason_dim"
      val interactionReasonHDFS = srcTargetDir+"/"+interactionReasonTableName
      val interactionReasonDimDF = hiveContext.read.parquet(interactionReasonHDFS)

      val locationDimTableName : String = "location_dim"
      val locationDimHDFS = srcTargetDir+"/"+locationDimTableName
      val locationDimDF = hiveContext.read.parquet(locationDimHDFS)

      val atmHireDimTableName : String = "atm_hier_dim"
      val atmHireDimHDFS = srcTargetDir+"/"+atmHireDimTableName
      val atmHireDimDF = hiveContext.read.parquet(atmHireDimHDFS)

      val agentDimTableName : String = "agent_hier_dim"
      val agentDimHDFS = srcTargetDir+"/"+agentDimTableName
      val agentHireDimDF = hiveContext.read.parquet(agentDimHDFS)

      val subDispostionDimTableName : String = "sub_disposition_dim"
      val subDispostionDimHDFS = srcTargetDir+"/"+subDispostionDimTableName
      val subDispositionDimDF = hiveContext.read.parquet(subDispostionDimHDFS)

      val offerYearMonth = (calendarYear : String, calendarMonth : String ) => {
        var calendarMonthPad =""
        if(calendarMonth.length() ==1) {
          calendarMonthPad = "0"+calendarMonth  
        }
        calendarYear+calendarMonthPad
      }
      val offerYearMonthFunc = udf(offerYearMonth)
      
      val offerKey = (arg: Double) => {
        if (arg == -1 ){
           println(" If Condition")
           1
        } else {
           println(" Else If Condition")
           0
        }
      }
      val offerKeyfunc = udf(offerKey)
      
      val returnZeroValue = () => {
        0
      }
      val returnZeroValueFunc = udf(returnZeroValue)

      val cifFlag = (CIF_PERM_KEY: String, CUST_PROSPECT_TYPE_KEY: Double)  => {
        if (CIF_PERM_KEY == null && CUST_PROSPECT_TYPE_KEY == 1 ){
           println(" If Condition")
           1
        } else {
           println(" Else If Condition")
           0
        }
      }
      val cifFlagFunc = udf(cifFlag)
      
      val channelF = (channel_key: java.math.BigDecimal) => {
        if(channel_key == 9)  "OLB"
        else if (channel_key == 10)  "ATM"
        else if (channel_key ==  11) "CCC"
        else if (channel_key == 17)  "ATW"
        else if (channel_key == 18)  "MBL"
        else ""          
      }
      
      val channelFunc = udf(channelF)

      val audience = (offerName : String) => {
        if (offerName.substring(1, 1).equalsIgnoreCase("B")) "BUS"
        else "CON"
      }
      val audienceFunc = udf(audience)
      
      val controlGrp = (offerType : String) => {
        if (offerType.toUpperCase().equals("CONTROL GROUP")) 1
        else 0
      }
      val controlGrpFunc = udf(controlGrp)
      
      val custEventAfterJoinDF = custEventFactFilterDF.join(timeDimDF, custEventFactFilterDF("DATE_KEY") === timeDimDF("DATE_KEY"),"left_outer" )
                                                .join(offerDimDF, custEventFactFilterDF("offer_key") === offerDimDF("offer_key"), "left_outer")
                                                .join(interactionReasonDimDF, custEventFactFilterDF("call_key") === interactionReasonDimDF("call_key"), "left_outer")
                                                .join(locationDimDF, custEventFactFilterDF("location_key") === locationDimDF("location_key"), "left_outer")
                                                .join(atmHireDimDF, custEventFactFilterDF("atm_hier_key") === atmHireDimDF("atm_hier_key"), "left_outer")
                                                .join(agentHireDimDF, custEventFactFilterDF("agent_key") === agentHireDimDF("agent_key"), "left_outer")
                                                .join(subDispositionDimDF, custEventFactFilterDF("SUB_DISPOSITION_KEY") === subDispositionDimDF("SUB_DISPOSITION_KEY"), "left_outer")
                                                .withColumn("OFFER_YEAR_MONTH", offerYearMonthFunc(timeDimDF("CALENDAR_YEAR"),timeDimDF("CALENDAR_MONTH")))
                                                .withColumn("INVALID_OFFERKEY_FLAG", offerKeyfunc(custEventFactFilterDF("offer_key")))
                                                 /*.select(
                                                  custEventFactFilterDF("CHANNEL_KEY").alias("CHANNEL_KEY")
                                                  ,custEventFactFilterDF("CUST_PROSPECT_TYPE_KEY").alias("CUST_PROSPECT_TYPE_KEY")
                                                  ,custEventFactFilterDF("DATE_KEY").alias("DATE_KEY")
                                                  ,timeDimDF("CALENDAR_DATE").alias("OFFER_DATE")
                                                  ,timeDimDF("CALENDAR_MONTH").alias("OFFER_MONTH")
                                                  ,timeDimDF("CALENDAR_YEAR").alias("OFFER_YEAR")
                                                  ,offerYearMonthFunc(timeDimDF("CALENDAR_YEAR"),timeDimDF("CALENDAR_MONTH")).alias("OFFER_YEAR_MONTH")
                                                  ,offerKeyfunc(custEventFactFilterDF("offer_key")).alias("INVALID_OFFERKEY_FLAG")
                                                  ,returnZeroValueFunc().alias("INVALID_OFFER_FLAG")
                                                  ,cifFlagFunc(custEventFactFilterDF("CIF_PERM_KEY"), custEventFactFilterDF("CUST_PROSPECT_TYPE_KEY")).alias("INVALID_CIF_FLAG")
                                                  ,custEventFactFilterDF("LOCATION_KEY").alias("LOCATION_KEY")
                                                  ,custEventFactFilterDF("ATM_HIER_KEY").alias("ATM_HIER_KEY")
                                                  ,custEventFactFilterDF("AGENT_KEY").alias("AGENT_KEY")
                                                  ,custEventFactFilterDF("NBR_RESPONSES").alias("NBR_RESPONSES")
                                                  ,custEventFactFilterDF("NBR_ACCEPT_RESPONSES").alias("NBR_ACCEPT_RESPONSES")
                                                  ,custEventFactFilterDF("NBR_DECLINE_RESPONSES").alias("NBR_DECLINE_RESPONSES")
                                                  ,custEventFactFilterDF("NBR_MAYBE_LATER_RESPONSES").alias("NBR_MAYBE_LATER_RESPONSES")
                                                  ,custEventFactFilterDF("NBR_OFFERS_PRESENTED").alias("NBR_OFFERS_PRESENTED")
                                                  ,custEventFactFilterDF("NBR_SUCCESSFUL_RESPONSES").alias("NBR_SUCCESSFUL_RESPONSES")
                                                  ,custEventFactFilterDF("NBR_QUALIFIED_OFFERS").alias("NBR_QUALIFIED_OFFERS")
                                                  ,custEventFactFilterDF("NBR_QUALIFIED_OFFERS_TOP_3").alias("NBR_QUALIFIED_OFFERS_TOP_3")
                                                  ,custEventFactFilterDF("NBR_QUALIFIED_OFFERS_NBA").alias("NBR_QUALIFIED_OFFERS_NBA")
                                                  ,custEventFactFilterDF("NBR_QUALIFIED_OFFERS_POS_2").alias("NBR_QUALIFIED_OFFERS_POS_2")
                                                  ,custEventFactFilterDF("NBR_QUALIFIED_OFFERS_POS_3").alias("NBR_QUALIFIED_OFFERS_POS_3")
                                                  ,custEventFactFilterDF("NBR_NBA_PRESENTED").alias("NBR_NBA_PRESENTED")
                                                  ,custEventFactFilterDF("CIF_PERM_KEY").alias("CIF_PERMANENT_KEY")
                                                  ,custEventFactFilterDF("CIF_KEY").alias("CIF_KEY")
                                                  ,custEventFactFilterDF("SESSION_ID").alias("SESSION_ID")
                                                  ,custEventFactFilterDF("OFFER_QUALIFICATION_RANK").alias("OFFER_QUALIFICATION_RANK")
                                                  ,custEventFactFilterDF("OFFER_DISPLAY_RANK").alias("OFFER_DISPLAY_RANK")
                                                  ,custEventFactFilterDF("NBR_CONTROL_GROUP_OFFERS").alias("NBR_CONTROL_GROUP_OFFERS")
                                                  ,custEventFactFilterDF("NBR_NO_OFFER_OFFERS").alias("NBR_NO_OFFER_OFFERS")
                                                  ,custEventFactFilterDF("NBR_EXPEDITE_OFFERS").alias("NBR_EXPEDITE_OFFERS")
                                                  ,custEventFactFilterDF("CARD_NUMBER").alias("CARD_NUMBER")
                                                  ,custEventFactFilterDF("NBR_QUALIFIED_OFFERS_NOTOFFERD").alias("NBR_QUALIFIED_OFFERS_NOTOFFERD")
                                                  ,custEventFactFilterDF("CALL_KEY").alias("CALL_KEY")
                                                  ,custEventFactFilterDF("BRANCH_ID").alias("BRANCH_ID")
                                                  ,custEventFactFilterDF("EMPLOYEE_ID").alias("EMPLOYEE_ID")
                                                  ,custEventFactFilterDF("BANK").alias("BANK")
                                                  ,channelFunc(custEventFactFilterDF("channel_key")).alias("CHENNEL")
                                                  ,offerDimDF("OFFER_ID").alias("OFFER_ID")
                                                  ,offerDimDF("OFFER_NAME").alias("OFFER_NAME")
                                                  ,offerDimDF("CREATED_DATE").alias("CREATED_DATE")
                                                  ,offerDimDF("UPDATED_DATE").alias("UPDATED_DATE")
                                                  ,offerDimDF("DISPLAY_NAME").alias("DISPLAY_NAME")
                                                  ,offerDimDF("SOURCE_SYSTEM").alias("SOURCE_SYSTEM")
                                                  ,offerDimDF("OFFER_TYPE").alias("OFFER_TYPE")
                                                  ,offerDimDF("OFFER_CHANNEL").alias("OFFER_CHANNEL")
                                                  ,audienceFunc(offerDimDF("offer_name")).alias("CONTROL_GROUP")
                                                  ,controlGrpFunc(offerDimDF("offer_type")).alias("AUDIENCE")
                                                  ,interactionReasonDimDF("CALL_CATEGORY").alias("CALL_CATEGORY")
                                                  ,interactionReasonDimDF("CALL_REASON").alias("CALL_REASON")
                                                  ,locationDimDF("LOCATION").alias("OLB_LOCATION")
                                                  ,locationDimDF("LOCATION_DESC").alias("OLB_LOCATION_DESC")
                                                  ,locationDimDF("WBB_LOCATION").alias("OLB_WBB_LOCATION")
                                                  ,atmHireDimDF("TERMINAL_ID").alias("ATM_TERMINAL_ID")
                                                  ,atmHireDimDF("PREVIOUS_TERMINAL_ID").alias("ATM_PREVIOUS_TERMINAL_ID")
                                                  ,atmHireDimDF("ONE_TO_ONE__ENABLE_FLAG").alias("ATM_ONE_TO_ONE__ENABLE_FLAG")
                                                  ,atmHireDimDF("TERMINAL_NICKNAME").alias("ATM_TERMINAL_NICKNAME")
                                                  ,atmHireDimDF("INDUSTRY_MARKET").alias("ATM_INDUSTRY_MARKET")
                                                  ,atmHireDimDF("RECAP_GROUP").alias("ATM_RECAP_GROUP")
                                                  ,atmHireDimDF("TERRITORY").alias("ATM_TERRITORY")
                                                  ,atmHireDimDF("NATURAL_MARKET").alias("ATM_NATURAL_MARKET")
                                                  ,atmHireDimDF("CITY").alias("ATM_CITY")
                                                  ,atmHireDimDF("STATE").alias("ATM_STATE")
                                                  ,agentHireDimDF("LOGON_ID").alias("CCC_LOGON_ID")
                                                  ,agentHireDimDF("FIRST_NAME").alias("CCC_FIRST_NAME")
                                                  ,agentHireDimDF("LAST_NAME").alias("CCC_LAST_NAME")
                                                  ,agentHireDimDF("DIVSISION_ID").alias("CCC_DIVISION_ID")
                                                  ,agentHireDimDF("DIVISION_NAME").alias("CCC_DIVISION_NAME")
                                                  ,agentHireDimDF("GROUP_ID").alias("CCC_GROUP_ID")
                                                  ,agentHireDimDF("GROUP_NAME").alias("CCC_GROUP_NAME")
                                                  ,agentHireDimDF("TEAM_ID").alias("CCC_TEAM_ID")
                                                  ,agentHireDimDF("TEAM_NAME").alias("CCC_TEAM_NAME")
                                                  ,agentHireDimDF("SEGMENT_ROLE").alias("CCC_SEGMENT_ROLE")
                                                  ,agentHireDimDF("EMPLOYMENT_STATUS").alias("CCC_EMP_STATUS")
                                                  ,subDispositionDimDF("SUB_DISPOSITION").alias("SUB_DISPOSITION")*/
                                        //)
      
      val custEven1516tAfterJoinDF = custEventFactFilterDF1516.join(timeDimDF, custEventFactFilterDF1516("DATE_KEY") === timeDimDF("DATE_KEY"),"left_outer" )
                                                .join(offerDimDF, custEventFactFilterDF1516("offer_key") === offerDimDF("offer_key"), "left_outer")
                                                .join(interactionReasonDimDF, custEventFactFilterDF1516("call_key") === interactionReasonDimDF("call_key"), "left_outer")
                                                .join(locationDimDF, custEventFactFilterDF1516("location_key") === locationDimDF("location_key"), "left_outer")
                                                .join(atmHireDimDF, custEventFactFilterDF1516("atm_hier_key") === atmHireDimDF("atm_hier_key"), "left_outer")
                                                .join(agentHireDimDF, custEventFactFilterDF1516("agent_key") === agentHireDimDF("agent_key"), "left_outer")
                                                .join(subDispositionDimDF, custEventFactFilterDF1516("SUB_DISPOSITION_KEY") === subDispositionDimDF("SUB_DISPOSITION_KEY"), "left_outer")
                                                 .select(
                                                  custEventFactFilterDF1516("CHANNEL_KEY").alias("CHANNEL_KEY")
                                                  ,custEventFactFilterDF1516("CUST_PROSPECT_TYPE_KEY").alias("CUST_PROSPECT_TYPE_KEY")
                                                  ,custEventFactFilterDF1516("DATE_KEY").alias("DATE_KEY")
                                                  ,timeDimDF("CALENDAR_DATE").alias("OFFER_DATE")
                                                  ,timeDimDF("CALENDAR_MONTH").alias("OFFER_MONTH")
                                                  ,timeDimDF("CALENDAR_YEAR").alias("OFFER_YEAR")
                                                  ,offerYearMonthFunc(timeDimDF("CALENDAR_YEAR"),timeDimDF("CALENDAR_MONTH")).alias("OFFER_YEAR_MONTH")
                                                  ,offerKeyfunc(custEventFactFilterDF1516("offer_key")).alias("INVALID_OFFERKEY_FLAG")
                                                  ,returnZeroValueFunc().alias("INVALID_OFFER_FLAG")
                                                  ,cifFlagFunc(custEventFactFilterDF1516("CIF_PERM_KEY"), custEventFactFilterDF1516("CUST_PROSPECT_TYPE_KEY")).alias("INVALID_CIF_FLAG")
                                                  ,custEventFactFilterDF1516("LOCATION_KEY").alias("LOCATION_KEY")
                                                  ,custEventFactFilterDF1516("ATM_HIER_KEY").alias("ATM_HIER_KEY")
                                                  ,custEventFactFilterDF1516("AGENT_KEY").alias("AGENT_KEY")
                                                  ,custEventFactFilterDF1516("NBR_RESPONSES").alias("NBR_RESPONSES")
                                                  ,custEventFactFilterDF1516("NBR_ACCEPT_RESPONSES").alias("NBR_ACCEPT_RESPONSES")
                                                  ,custEventFactFilterDF1516("NBR_DECLINE_RESPONSES").alias("NBR_DECLINE_RESPONSES")
                                                  ,custEventFactFilterDF1516("NBR_MAYBE_LATER_RESPONSES").alias("NBR_MAYBE_LATER_RESPONSES")
                                                  ,custEventFactFilterDF1516("NBR_OFFERS_PRESENTED").alias("NBR_OFFERS_PRESENTED")
                                                  ,custEventFactFilterDF1516("NBR_SUCCESSFUL_RESPONSES").alias("NBR_SUCCESSFUL_RESPONSES")
                                                  ,custEventFactFilterDF1516("NBR_QUALIFIED_OFFERS").alias("NBR_QUALIFIED_OFFERS")
                                                  ,custEventFactFilterDF1516("NBR_QUALIFIED_OFFERS_TOP_3").alias("NBR_QUALIFIED_OFFERS_TOP_3")
                                                  ,custEventFactFilterDF1516("NBR_QUALIFIED_OFFERS_NBA").alias("NBR_QUALIFIED_OFFERS_NBA")
                                                  ,custEventFactFilterDF1516("NBR_QUALIFIED_OFFERS_POS_2").alias("NBR_QUALIFIED_OFFERS_POS_2")
                                                  ,custEventFactFilterDF1516("NBR_QUALIFIED_OFFERS_POS_3").alias("NBR_QUALIFIED_OFFERS_POS_3")
                                                  ,custEventFactFilterDF1516("NBR_NBA_PRESENTED").alias("NBR_NBA_PRESENTED")
                                                  ,custEventFactFilterDF1516("CIF_PERM_KEY").alias("CIF_PERMANENT_KEY")
                                                  ,custEventFactFilterDF1516("CIF_KEY").alias("CIF_KEY")
                                                  ,custEventFactFilterDF1516("SESSION_ID").alias("SESSION_ID")
                                                  ,custEventFactFilterDF1516("OFFER_QUALIFICATION_RANK").alias("OFFER_QUALIFICATION_RANK")
                                                  ,custEventFactFilterDF1516("OFFER_DISPLAY_RANK").alias("OFFER_DISPLAY_RANK")
                                                  ,custEventFactFilterDF1516("NBR_CONTROL_GROUP_OFFERS").alias("NBR_CONTROL_GROUP_OFFERS")
                                                  ,custEventFactFilterDF1516("NBR_NO_OFFER_OFFERS").alias("NBR_NO_OFFER_OFFERS")
                                                  ,custEventFactFilterDF1516("NBR_EXPEDITE_OFFERS").alias("NBR_EXPEDITE_OFFERS")
                                                  ,custEventFactFilterDF1516("CARD_NUMBER").alias("CARD_NUMBER")
                                                  ,custEventFactFilterDF1516("NBR_QUALIFIED_OFFERS_NOTOFFERD").alias("NBR_QUALIFIED_OFFERS_NOTOFFERD")
                                                  ,custEventFactFilterDF1516("CALL_KEY").alias("CALL_KEY")
                                                  ,custEventFactFilterDF1516("BRANCH_ID").alias("BRANCH_ID")
                                                  ,custEventFactFilterDF1516("EMPLOYEE_ID").alias("EMPLOYEE_ID")
                                                  ,custEventFactFilterDF1516("BANK").alias("BANK")
                                                  ,channelFunc(custEventFactFilterDF1516("channel_key")).alias("CHENNEL")
                                                  ,offerDimDF("OFFER_ID").alias("OFFER_ID")
                                                  ,offerDimDF("OFFER_NAME").alias("OFFER_NAME")
                                                  ,offerDimDF("CREATED_DATE").alias("CREATED_DATE")
                                                  ,offerDimDF("UPDATED_DATE").alias("UPDATED_DATE")
                                                  ,offerDimDF("DISPLAY_NAME").alias("DISPLAY_NAME")
                                                  ,offerDimDF("SOURCE_SYSTEM").alias("SOURCE_SYSTEM")
                                                  ,offerDimDF("OFFER_TYPE").alias("OFFER_TYPE")
                                                  ,offerDimDF("OFFER_CHANNEL").alias("OFFER_CHANNEL")
                                                  ,audienceFunc(offerDimDF("offer_name")).alias("CONTROL_GROUP")
                                                  ,controlGrpFunc(offerDimDF("offer_type")).alias("AUDIENCE")
                                                  ,interactionReasonDimDF("CALL_CATEGORY").alias("CALL_CATEGORY")
                                                  ,interactionReasonDimDF("CALL_REASON").alias("CALL_REASON")
                                                  ,locationDimDF("LOCATION").alias("OLB_LOCATION")
                                                  ,locationDimDF("LOCATION_DESC").alias("OLB_LOCATION_DESC")
                                                  ,locationDimDF("WBB_LOCATION").alias("OLB_WBB_LOCATION")
                                                  ,atmHireDimDF("TERMINAL_ID").alias("ATM_TERMINAL_ID")
                                                  ,atmHireDimDF("PREVIOUS_TERMINAL_ID").alias("ATM_PREVIOUS_TERMINAL_ID")
                                                  ,atmHireDimDF("ONE_TO_ONE__ENABLE_FLAG").alias("ATM_ONE_TO_ONE__ENABLE_FLAG")
                                                  ,atmHireDimDF("TERMINAL_NICKNAME").alias("ATM_TERMINAL_NICKNAME")
                                                  ,atmHireDimDF("INDUSTRY_MARKET").alias("ATM_INDUSTRY_MARKET")
                                                  ,atmHireDimDF("RECAP_GROUP").alias("ATM_RECAP_GROUP")
                                                  ,atmHireDimDF("TERRITORY").alias("ATM_TERRITORY")
                                                  ,atmHireDimDF("NATURAL_MARKET").alias("ATM_NATURAL_MARKET")
                                                  ,atmHireDimDF("CITY").alias("ATM_CITY")
                                                  ,atmHireDimDF("STATE").alias("ATM_STATE")
                                                  ,agentHireDimDF("LOGON_ID").alias("CCC_LOGON_ID")
                                                  ,agentHireDimDF("FIRST_NAME").alias("CCC_FIRST_NAME")
                                                  ,agentHireDimDF("LAST_NAME").alias("CCC_LAST_NAME")
                                                  ,agentHireDimDF("DIVSISION_ID").alias("CCC_DIVISION_ID")
                                                  ,agentHireDimDF("DIVISION_NAME").alias("CCC_DIVISION_NAME")
                                                  ,agentHireDimDF("GROUP_ID").alias("CCC_GROUP_ID")
                                                  ,agentHireDimDF("GROUP_NAME").alias("CCC_GROUP_NAME")
                                                  ,agentHireDimDF("TEAM_ID").alias("CCC_TEAM_ID")
                                                  ,agentHireDimDF("TEAM_NAME").alias("CCC_TEAM_NAME")
                                                  ,agentHireDimDF("SEGMENT_ROLE").alias("CCC_SEGMENT_ROLE")
                                                  ,agentHireDimDF("EMPLOYMENT_STATUS").alias("CCC_EMP_STATUS")
                                                  ,subDispositionDimDF("SUB_DISPOSITION").alias("SUB_DISPOSITION")
                                        ).distinct()
                                        
        val wrkCIMOffersDF = custEventAfterJoinDF.unionAll(custEven1516tAfterJoinDF)
        
        //println("custEventAfterJoinDF :"+custEventAfterJoinDF.count());
        //println("custEven1516tAfterJoinDF :"+custEven1516tAfterJoinDF.count());
        //println("wrkCIMOffersDF Count:"+wrkCIMOffersDF.count())
            
        val wrkCIMOfferNotIn1516DF = wrkCIMOffersDF.filter(wrkCIMOffersDF("channel_key").isin("15","16") && wrkCIMOffersDF("invalid_offer_flag").notEqual(1)
                                                      && wrkCIMOffersDF("invalid_offerkey_flag").notEqual(1) 
                                                      && wrkCIMOffersDF("invalid_cif_flag").notEqual(1) )
        
        val wrkCIMOffersIn1516Partition = Window.partitionBy(wrkCIMOffersDF("CIF_PERMANENT_KEY"), wrkCIMOffersDF("OFFER_DATE"),  wrkCIMOffersDF("BANK")
                          ,  wrkCIMOffersDF("BRANCH_ID"),  wrkCIMOffersDF("EMPLOYEE_ID"),  wrkCIMOffersDF("OFFER_ID"),wrkCIMOffersDF("NBR_QUALIFIED_OFFERS"))
                          .orderBy(wrkCIMOffersDF("NBR_QUALIFIED_OFFERS").desc, wrkCIMOffersDF("SUB_DISPOSITION").desc,  wrkCIMOffersDF("NBR_OFFERS_PRESENTED").desc
                          ,  wrkCIMOffersDF("NBR_OFFERS_PRESENTED").desc,  wrkCIMOffersDF("NBR_DECLINE_RESPONSES").desc,  wrkCIMOffersDF("NBR_MAYBE_LATER_RESPONSES").desc)
        
        val wrkCIMOfferIn1516PartitionDF= wrkCIMOffersDF.select(
                                                  wrkCIMOffersDF("CHANNEL_KEY").alias("CHANNEL_KEY")
												                          ,wrkCIMOffersDF("CUST_PROSPECT_TYPE_KEY").alias("CUST_PROSPECT_TYPE_KEY")
                                                  ,wrkCIMOffersDF("DATE_KEY").alias("DATE_KEY")
                                                  ,wrkCIMOffersDF("OFFER_DATE").alias("OFFER_DATE")
                                                  ,wrkCIMOffersDF("OFFER_MONTH").alias("OFFER_MONTH")
                                                  ,wrkCIMOffersDF("OFFER_YEAR").alias("OFFER_YEAR")
                                                  ,wrkCIMOffersDF("OFFER_YEAR_MONTH").alias("OFFER_YEAR_MONTH")
                                                  ,wrkCIMOffersDF("INVALID_OFFERKEY_FLAG").alias("INVALID_OFFERKEY_FLAG")
                                                  ,wrkCIMOffersDF("INVALID_OFFER_FLAG").alias("INVALID_OFFER_FLAG")
                                                  ,wrkCIMOffersDF("INVALID_CIF_FLAG").alias("INVALID_CIF_FLAG")
                                                  ,wrkCIMOffersDF("LOCATION_KEY").alias("LOCATION_KEY")
                                                  ,wrkCIMOffersDF("ATM_HIER_KEY").alias("ATM_HIER_KEY")
                                                  ,wrkCIMOffersDF("AGENT_KEY").alias("AGENT_KEY")
                                                  ,wrkCIMOffersDF("NBR_RESPONSES").alias("NBR_RESPONSES")
                                                  ,wrkCIMOffersDF("NBR_ACCEPT_RESPONSES").alias("NBR_ACCEPT_RESPONSES")
                                                  ,wrkCIMOffersDF("NBR_DECLINE_RESPONSES").alias("NBR_DECLINE_RESPONSES")
                                                  ,wrkCIMOffersDF("NBR_MAYBE_LATER_RESPONSES").alias("NBR_MAYBE_LATER_RESPONSES")
                                                  ,wrkCIMOffersDF("NBR_OFFERS_PRESENTED").alias("NBR_OFFERS_PRESENTED")
                                                  ,wrkCIMOffersDF("NBR_SUCCESSFUL_RESPONSES").alias("NBR_SUCCESSFUL_RESPONSES")
                                                  ,wrkCIMOffersDF("NBR_QUALIFIED_OFFERS").alias("NBR_QUALIFIED_OFFERS")
                                                  ,wrkCIMOffersDF("NBR_QUALIFIED_OFFERS_TOP_3").alias("NBR_QUALIFIED_OFFERS_TOP_3")
                                                  ,wrkCIMOffersDF("NBR_QUALIFIED_OFFERS_NBA").alias("NBR_QUALIFIED_OFFERS_NBA")
                                                  ,wrkCIMOffersDF("NBR_QUALIFIED_OFFERS_POS_2").alias("NBR_QUALIFIED_OFFERS_POS_2")
                                                  ,wrkCIMOffersDF("NBR_QUALIFIED_OFFERS_POS_3").alias("NBR_QUALIFIED_OFFERS_POS_3")
                                                  ,wrkCIMOffersDF("NBR_NBA_PRESENTED").alias("NBR_NBA_PRESENTED")
                                                  ,wrkCIMOffersDF("CIF_PERMANENT_KEY").alias("CIF_PERMANENT_KEY")
                                                  ,wrkCIMOffersDF("CIF_KEY").alias("CIF_KEY")
                                                  ,wrkCIMOffersDF("SESSION_ID").alias("SESSION_ID")
                                                  ,wrkCIMOffersDF("OFFER_QUALIFICATION_RANK").alias("OFFER_QUALIFICATION_RANK")
                                                  ,wrkCIMOffersDF("OFFER_DISPLAY_RANK").alias("OFFER_DISPLAY_RANK")
                                                  ,wrkCIMOffersDF("NBR_CONTROL_GROUP_OFFERS").alias("NBR_CONTROL_GROUP_OFFERS")
                                                  ,wrkCIMOffersDF("NBR_NO_OFFER_OFFERS").alias("NBR_NO_OFFER_OFFERS")
                                                  ,wrkCIMOffersDF("NBR_EXPEDITE_OFFERS").alias("NBR_EXPEDITE_OFFERS")
                                                  ,wrkCIMOffersDF("CARD_NUMBER").alias("CARD_NUMBER")
                                                  ,wrkCIMOffersDF("NBR_QUALIFIED_OFFERS_NOTOFFERD").alias("NBR_QUALIFIED_OFFERS_NOTOFFERD")
                                                  ,wrkCIMOffersDF("CALL_KEY").alias("CALL_KEY")
                                                  ,wrkCIMOffersDF("BRANCH_ID").alias("BRANCH_ID")
                                                  ,wrkCIMOffersDF("EMPLOYEE_ID").alias("EMPLOYEE_ID")
                                                  ,wrkCIMOffersDF("BANK").alias("BANK")
                                                  ,wrkCIMOffersDF("channel_key").alias("CHENNEL")
                                                  ,wrkCIMOffersDF("OFFER_ID").alias("OFFER_ID")
                                                  ,wrkCIMOffersDF("OFFER_NAME").alias("OFFER_NAME")
                                                  ,wrkCIMOffersDF("CREATED_DATE").alias("CREATED_DATE")
                                                  ,wrkCIMOffersDF("UPDATED_DATE").alias("UPDATED_DATE")
                                                  ,wrkCIMOffersDF("DISPLAY_NAME").alias("DISPLAY_NAME")
                                                  ,wrkCIMOffersDF("SOURCE_SYSTEM").alias("SOURCE_SYSTEM")
                                                  ,wrkCIMOffersDF("OFFER_TYPE").alias("OFFER_TYPE")
                                                  ,wrkCIMOffersDF("OFFER_CHANNEL").alias("OFFER_CHANNEL")
                                                  ,wrkCIMOffersDF("CONTROL_GROUP").alias("CONTROL_GROUP")
                                                  ,wrkCIMOffersDF("AUDIENCE").alias("AUDIENCE")
                                                  ,wrkCIMOffersDF("CALL_CATEGORY").alias("CALL_CATEGORY")
                                                  ,wrkCIMOffersDF("CALL_REASON").alias("CALL_REASON")
                                                  ,wrkCIMOffersDF("OLB_LOCATION").alias("OLB_LOCATION")
                                                  ,wrkCIMOffersDF("OLB_LOCATION_DESC").alias("OLB_LOCATION_DESC")
                                                  ,wrkCIMOffersDF("OLB_WBB_LOCATION").alias("OLB_WBB_LOCATION")
                                                  ,wrkCIMOffersDF("ATM_TERMINAL_ID").alias("ATM_TERMINAL_ID")
                                                  ,wrkCIMOffersDF("ATM_PREVIOUS_TERMINAL_ID").alias("ATM_PREVIOUS_TERMINAL_ID")
                                                  ,wrkCIMOffersDF("ATM_ONE_TO_ONE__ENABLE_FLAG").alias("ATM_ONE_TO_ONE__ENABLE_FLAG")
                                                  ,wrkCIMOffersDF("ATM_TERMINAL_NICKNAME").alias("ATM_TERMINAL_NICKNAME")
                                                  ,wrkCIMOffersDF("ATM_INDUSTRY_MARKET").alias("ATM_INDUSTRY_MARKET")
                                                  ,wrkCIMOffersDF("ATM_RECAP_GROUP").alias("ATM_RECAP_GROUP")
                                                  ,wrkCIMOffersDF("ATM_TERRITORY").alias("ATM_TERRITORY")
                                                  ,wrkCIMOffersDF("ATM_NATURAL_MARKET").alias("ATM_NATURAL_MARKET")
                                                  ,wrkCIMOffersDF("ATM_CITY").alias("ATM_CITY")
                                                  ,wrkCIMOffersDF("ATM_STATE").alias("ATM_STATE")
                                                  ,wrkCIMOffersDF("CCC_LOGON_ID").alias("CCC_LOGON_ID")
                                                  ,wrkCIMOffersDF("CCC_FIRST_NAME").alias("CCC_FIRST_NAME")
                                                  ,wrkCIMOffersDF("CCC_LAST_NAME").alias("CCC_LAST_NAME")
                                                  ,wrkCIMOffersDF("CCC_DIVISION_ID").alias("CCC_DIVISION_ID")
                                                  ,wrkCIMOffersDF("CCC_DIVISION_NAME").alias("CCC_DIVISION_NAME")
                                                  ,wrkCIMOffersDF("CCC_GROUP_ID").alias("CCC_GROUP_ID")
                                                  ,wrkCIMOffersDF("CCC_GROUP_NAME").alias("CCC_GROUP_NAME")
                                                  ,wrkCIMOffersDF("CCC_TEAM_ID").alias("CCC_TEAM_ID")
                                                  ,wrkCIMOffersDF("CCC_TEAM_NAME").alias("CCC_TEAM_NAME")
                                                  ,wrkCIMOffersDF("CCC_SEGMENT_ROLE").alias("CCC_SEGMENT_ROLE")
                                                  ,wrkCIMOffersDF("CCC_EMP_STATUS").alias("CCC_EMP_STATUS")
                                                  ,wrkCIMOffersDF("SUB_DISPOSITION").alias("SUB_DISPOSITION")
                                                  ,rowNumber().over(wrkCIMOffersIn1516Partition).alias("RIND"))

        //val wrkCIMOfferIn1516DF1 = wrkCIMOfferIn1516PartitionDF.where(wrkCIMOfferIn1516PartitionDF("channel_key").i)
        val wrkCIMOfferIn1516DF= wrkCIMOfferIn1516PartitionDF.filter(!wrkCIMOfferIn1516PartitionDF("channel_key").isin("15","16") && wrkCIMOfferIn1516PartitionDF("invalid_offer_flag").notEqual(1)
                                                     && wrkCIMOfferIn1516PartitionDF("invalid_offerkey_flag").notEqual(1) 
                                                      && wrkCIMOfferIn1516PartitionDF("invalid_cif_flag").notEqual(1)
                                                      && wrkCIMOfferIn1516PartitionDF("RIND") <= 1)
        //println(wrkCIMOffer1516DF.count())
        wrkCIMOfferIn1516DF.write.mode(SaveMode.Overwrite).format("parquet").save("")                                      
        println("wrkCIMOfferIn1516DF "+wrkCIMOfferIn1516DF.count())
        
        //wrkCIMOfferIn1516DF.mapPartitions { x => ??? }
  }
  
  def offerFlag(offerKey: Double): Double = {
    return 0
  }
}