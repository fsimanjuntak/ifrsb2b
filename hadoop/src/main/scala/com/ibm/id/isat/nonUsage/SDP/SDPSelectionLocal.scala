package com.ibm.id.isat.nonUsage.SDP


import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.SparkConf;
import org.apache.spark.sql//Context.implicits.*;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import com.ibm.id.isat.utils.ReferenceSchema
import com.ibm.id.isat.utils.Common
import java.util.Calendar
import java.text.SimpleDateFormat
import com.ibm.id.isat.utils.ReferenceDFLocal
    


/*
 * This object is used to select SDP Adjustment or SDP Lifecycle 
 * data from SDP conversion data
 * @author Meliani Efelina
 */

object SDPSelectionLocal  {


    // define sql Context
    // val sc = new SparkContext(new SparkConf())  
    val sc = new SparkContext("local", "testing spark ", new SparkConf());  
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    
    
      
    def main(args: Array[String]) {
    
        println("Start Processing SDP Selection");  
        System.setProperty("hadoop.home.dir", "C:\\winutil\\") 
        
        // Do not show all the logs except ERROR
        import org.apache.log4j.{Level, Logger}
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)
        
        /*
         *  Get all reference Data
         */
        
        val PRC_DT=args(0)
        val JOB_ID=args(1)
        val CONF_DIR=args(2)
        val INPUT_DIR=args(3)
        val childDir =  Common.getChildDirectory(PRC_DT, JOB_ID);
        val PART = args(4).toInt
        

        println(INPUT_DIR);
        val existDir= Common.isDirectoryExists(sc, INPUT_DIR)
        val sdpAdjSrcRDD = sc.textFile(INPUT_DIR.concat("*"));
        println ("existdir");
        println(existDir);
        if (existDir == false)
           sys.exit(0);
        
        
        println ("sdpAdjSrcRDD");
        println(sdpAdjSrcRDD.count());
        if (sdpAdjSrcRDD.count() == 0)
           sys.exit(0);
        
       
        val OUTPUT_SPLIT_ADJ = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SPLIT_SDP_LOCAL.OUTPUT_SPLIT_ADJ_LOCAL").concat(childDir)
        val OUTPUT_SPLIT_LCY = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SPLIT_SDP_LOCAL.OUTPUT_SPLIT_LCY_LOCAL").concat(childDir)
        
        val sourceSDPRDD = sc.textFile(INPUT_DIR);
        
        val sourceSDPAdjRDD = sourceSDPRDD.filter { y => y.contains("accountAdjustment") }.map { x => x.replace("\t", "|").replace("accountAdjustment|", "").replace("\"", "")}
        
        Common.cleanDirectory(sc, OUTPUT_SPLIT_ADJ )
        sourceSDPAdjRDD.saveAsTextFile(OUTPUT_SPLIT_ADJ)
        
        val sourceSDPLcyRDD = sourceSDPRDD.filter { y => y.contains("lifeCycleChange") }.map { x => x.replace("\t", "|").replace("lifeCycleChange|", "").replace("\"", "")}
        Common.cleanDirectory(sc, OUTPUT_SPLIT_LCY )
        sourceSDPAdjRDD.saveAsTextFile(OUTPUT_SPLIT_LCY)
        
    }
  
}