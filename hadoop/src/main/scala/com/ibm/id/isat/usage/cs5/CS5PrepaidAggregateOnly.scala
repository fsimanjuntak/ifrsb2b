package com.ibm.id.isat.usage.cs5

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat



 /*****************************************************************************************************
 * 
 * CS5 Transformation for Usage Revenue
 * Input: CCN CS5 Conversion
 * Output: CS5 CDR Inquiry, CS5 Transformation, CS5 Reject Reference, CS5 Reject BP
 * 
 * @author anico@id.ibm.com
 * @author IBM Indonesia for Indosat Ooredoo
 * 
 * June 2016
 * 
 *****************************************************************************************************/   
object CS5PrepaidAggregateOnly {
  def main(args: Array[String]): Unit = {
    
    /*****************************************************************************************************
     * 
     * Parameter check and setting
     * 
     *****************************************************************************************************/
    
    var prcDt = ""
    var jobID = ""
    var reprocessFlag = ""
    var configDir = ""
    var inputDir = ""
      
    try {
      prcDt = args(0)
      jobID = args(1)
      reprocessFlag = args(2)
      configDir = args(3)
      inputDir = args(4)
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    /*
     * Initiate Spark Context
     */
    //val sc = new SparkContext("local", "testing spark ", new SparkConf());
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")   //Disable metadata parquet
    
    /*
     * Log level initialization
     */
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO) //Level.ERROR
    
    /*
     * Get SYSDATE -90 for Filtering Reject Permanent
     */
    val dateFormat = new SimpleDateFormat("yyyyMMdd");
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -90);
    val minDate = dateFormat.format(cal.getTime())   
    
    /*
     * Assign child dir
     */
    val childDir = "/" + prcDt + "_" + jobID
    
    /*
     * Target directory assignments
     */
    val pathCS5TransformOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_TRANSFORM_DIR")
    val pathRejBP = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_REJ_BP_DIR") + childDir
    val pathRejRef = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_REJ_REF_DIR") + childDir
    val pathCS5CDRInqOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_CDR_INQ_DIR") + childDir
    val pathAggHourlyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_HOURLY_PREPAID.OUTPUT_SPARK_AGG_HOURLY_PREPAID")
    val pathAggDailyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_DAILY_PREPAID.OUTPUT_SPARK_AGG_DAILY_PREPAID")
    val pathAggGreenReportOutput = Common.getConfigDirectory(sc, configDir, "USAGE.GREEN_REPORT_PREPAID.OUTPUT_SPARK_GREEN_REPORT_PREPAID")
    val pathTdwSmyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_TDW_SUMMARY_DIR") + childDir
    val pathRejRefPerm =  Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_REJ_REF_PERM_DIR") + childDir
    val pathAddonIPCNOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_ADDON_IPCN_DIR")
    
    //val path_ref = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/Reference";
    val path_ref = "/user/apps/CS5/reference";
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    
     /*****************************************************************************************************
     * 
     * CS5 TRANSFORMATION READ
     * 
     *****************************************************************************************************/   
    val cs5Output = sqlContext.read.load(pathCS5TransformOutput+"/*/JOB_ID="+jobID+"/*parquet*")
     cs5Output.registerTempTable("cs5Output") 
        


     /*****************************************************************************************************
     * 
     * AGGREGATION
     * 
     *****************************************************************************************************/
     
      val cs5HourlyAggOutput = CS5Aggregate.genHourlySummary(sqlContext)
      cs5HourlyAggOutput.registerTempTable("cs5HourlyAggOutput") 
      
      val cs5DailyAggOutput = CS5Aggregate.genDailySummary(sqlContext)
      cs5DailyAggOutput.registerTempTable("cs5DailyAggOutput") 
      
      val cs5GreenReportAggOutput = CS5Aggregate.genGreenReportSummary(sqlContext)
      cs5GreenReportAggOutput.registerTempTable("cs5GreenReportAggOutput") 
      
      val cs5TdwSmyAggOutput = CS5Aggregate.genPrepaidTdwSummary(sqlContext)
     
     /*****************************************************************************************************
     * 
     * WRITE OUTPUT
     * 
     *****************************************************************************************************/
     

                
      /*
       * Output Aggregation Hourly
       */
      Common.cleanDirectoryWithPattern(sc, pathAggHourlyOutput, "/*/job_id=" + jobID)
      cs5HourlyAggOutput.write.partitionBy("transaction_date","job_id","src_tp")
        .mode("append")
        .save(pathAggHourlyOutput)
     
      /*
       * Output Aggregation Daily
       */
      Common.cleanDirectoryWithPattern(sc, pathAggDailyOutput, "/*/job_id=" + jobID)
      cs5DailyAggOutput.write.partitionBy("transaction_date","job_id","src_tp")
        .mode("append")
        .save(pathAggDailyOutput)
        
      /*
       * Output Aggregation Green Report
       */
      Common.cleanDirectory(sc, pathAggGreenReportOutput +"/process_id=" + prcDt + "_" + jobID + "_CS5")
      cs5GreenReportAggOutput.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save( pathAggGreenReportOutput +"/process_id=" + prcDt + "_" + jobID + "_CS5")
        
      /*
       * Output Aggregation TDW Summary
       */
      Common.cleanDirectory(sc, pathTdwSmyOutput)
      cs5TdwSmyAggOutput.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save( pathTdwSmyOutput )
        
        
        
        
        
    sc.stop();
    
  }
  

  
  
}