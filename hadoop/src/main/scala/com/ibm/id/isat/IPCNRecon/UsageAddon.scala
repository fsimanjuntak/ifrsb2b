package com.ibm.id.isat.IPCNRecon

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date._
import com.ibm.id.isat.utils._
import com.ibm.id.isat.usage.cs3._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.spark.sql._
import org.apache.spark.sql.Row


/** 
 *  IPCN Usage CS5 - Add on Purchase Preparation
 */
object UsageAddon {
    def main(args: Array[String]): Unit = {
      
       /*****************************************************************************************************
     * 
     * Parameter check and setting
     * 
     *****************************************************************************************************/
    
    var prcDt = ""
    var jobID = ""
   //var reprocessFlag = ""
    var configDir = ""
    var inputDir = ""
      
    try {
      prcDt = args(0)
      jobID = args(1)
     //reprocessFlag = args(2)
      configDir = args(2)
      inputDir = args(3)
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
   
      
     /*
     * Initiate Spark Context
     * val sc = new SparkContext("local", "test", new SparkConf())
     */ 
     //val sc = new SparkContext("local", "testing spark ", new SparkConf())
     val sc = new SparkContext(new SparkConf());
    
     /*
     * Assign child dir
     */
    val childDir = "/" + prcDt + "/" + prcDt + "_" + jobID
    
    /*
     * Target directory assignments
     */
    //val pathIPCNInput = Common.getConfigDirectory(sc, configDir, "IPCN.IPCN_DIR.IPCN_INPUT_DIR")
    val pathAddOnPurchaseOutput = Common.getConfigDirectory(sc, configDir, "IPCN.IPCN_DIR.ADD_ON_PURCHASE") + childDir
       
    /* 
     * Log level initialization
     */
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO) //Level.ERROR
       
    /*
     * Get source file
     */
     
    val ipcnInputRDD = sc.textFile(inputDir);
   // val ipcnInputRDD = sc.textFile("C:\\Users\\IBM_ADMIN\\Documents\\BENA\\GBS\\Indosat\\Hadoop\\Hadoop New Development\\Workspace\\hadoop\\Input_IPCN2.txt");
    
    /*
     * Splitting the CDR Info
     */
    
    val ipcnsplit1=ipcnInputRDD.map(line =>line.split("\t")(1)) 
   // ipcnsplit1.collect().foreach(println)
    
    /*
     * IPCN Filter 
    */
     
    val ipcnFilterRDD = {
      
      ipcnsplit1.filter(line => SelectIPCN.ipcnFilterValidation(line) )
      }
     ipcnFilterRDD.persist()
 
    // ipcnFilterRDD.collect().foreach(println)
   
    /*
     * IPCN Transformation
     */
     val ipcnTransformationRDD = {
     ipcnFilterRDD.map(line => SelectIPCN.addonpurchase(line, prcDt, jobID))
     
      } 
    ipcnTransformationRDD.persist()
     /*
      * Formatting the transformation RDD
      */
   
     val ipcnformattedRDD = ipcnTransformationRDD.map(x =>x.toString().replaceAll("," , "|"))
     val ipcnformattedRDD2 = ipcnformattedRDD.map(x =>x.toString().replace("[" , "")).map(x =>x.toString().replace("]" , ""))
   
   // ipcnformattedRDD.collect().foreach(println)
   // ipcnformattedRDD2.collect().foreach(println)
   
   /*
    * Writing Output to the Hdfs Location
    */
   ipcnTransformationRDD.saveAsTextFile(pathAddOnPurchaseOutput)
    
    sc.stop();
    
    }  
}