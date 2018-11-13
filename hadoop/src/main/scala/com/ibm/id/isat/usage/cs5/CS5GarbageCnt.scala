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
import java.util.Date
import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.HashMap
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

/*
 * 
 */
object CS5GarbageCnt {
  def main(args: Array[String]): Unit = {
    
    /*****************************************************************************************************
     * 
     * Parameter check and setting
     * 
     *****************************************************************************************************/
    
    var outputDir = ""
    var inputDir = ""
        var prcDt = ""
    var jobID = ""
      
    try {
      prcDt = args(0)
      jobID = args(1)
      inputDir = args(2)
      outputDir = args(3)
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
    
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    
    
    /*
     * Parameter Setting
     */
    
    val path_ref = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/Reference";
    //val path_ref = "/user/apps/CS5/reference";
    
        
        
    /*
     * Target directory assignments
     */
    //val pathCS5TransformOutput = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/output/detail"
    
    
    
    /*
     * Get source file
     */
    //inputDir = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/CS5PrepaidInput.2"
    
    
    
    /*
     * Get source file
     */
    
    val cs5InputRDD = sc.textFile(inputDir);
    

    
     /*****************************************************************************************************
     * 
     * Transformation CS5 and Reject BP
     * Reject BP rules :  null subscriber id or null service class or null trigger time
     * 
     *****************************************************************************************************/
    
    /*
     * Get transformed result, either from:
     * 1) conversion result for main process, or
     * 2) rejected data for reprocess
     */
    val cs5Transform1RDD = {
          //CREATE NEW TRANSFORMATION FROM CONVERSION RESULT
          
          //1. Filtered data
          val cs5InputFilteredRDD = cs5InputRDD
            .filter(line => CS5Select.cs5ConvertedRowIsValid(line) )
            .filter(line => CS5Select.getPaymentCategory(line)=="PREPAID" )
          
          //2. Transform the rest
          cs5InputFilteredRDD
            .flatMap(line => this.genCS5TransformedRowList(  Common.getTuple(line, "\t", 1).concat("|").concat(Common.getTuple(line, "\t", 2)).concat("|").concat(Common.getTuple(line, "\t", 3))  , "PREPAID" , prcDt, jobID  ) )
          
      
    }
    

   
    /*
     * DEBUG: print
     */
    cs5Transform1RDD.collect foreach {case (a) => println (a)}
    
    val CS5GarbageSchema = StructType(Array(
    StructField("TransactionDate", StringType, true),
    StructField("Hit", IntegerType, true),
    StructField("UsageVolume", DoubleType, true)
  ));

    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    /*
     * Create new dataframe from CS5 Transform 1 result
     */    
    val cs5Transform1DF = sqlContext.createDataFrame(cs5Transform1RDD, CS5GarbageSchema)
    cs5Transform1DF.registerTempTable("CS5Transform1")
    //cs5Transform1DF.show()
    
    val cs5GbgCnt = sqlContext.sql(
        """select TransactionDate,sum(Hit) HIT,sum(UsageVolume) VOLUME
           from CS5Transform1 group by TransactionDate
        """
        )
    //cs5GbgCnt.show()
    
       /*
       * Output Transformation  
       */

      Common.cleanDirectory(sc, outputDir)
      cs5GbgCnt.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(outputDir) 

        

      
          
            
    
    sc.stop();
  }
  
  /**
   * Get List of Row of CS5 values.
   * Split values based on delimiter.
   * Transpose data based on multiple CCR records.
   * Similar with getMapOfLine but has multiple returns.
   * 
   * @param line String of conversion result CS5 for postpaid data
   * @param prcDt Process date in string
   * @param paymentType "PREPAID" or "POSTPAID"
   * @param jobId Job ID in string
   * @return List of row of CS5 postpaid values. 
   *   Using schema: CS5Schema.CS5Schema or CS5Schema.CS5PostSchema
   */
  def genCS5TransformedRowList( line:String , paymentType:String, prcDt:String, jobId:String ) : List[Row] = {
    
    //Create list for return value
    var listTransformedRow = new ListBuffer[Row]()
    
    //First level of delimiter
    val level1 = line.split("\\|")
    
    //Get multiple second level: credit control records    
    val level2_ccr_multiple = level1(13).split("\\[")
    
    //Second level: serving element
    val level2_serv_elmt = level1(12).split("#")
    
    //Second level: service session id
    val level2_serv_sess_id = level1(6).split("\\:")
    
    var i=0
    
    //Build list per CCR records
    while ( i<level2_ccr_multiple.length ) {
        
        //Get single second level: credit control record    
         val level2_ccr = level2_ccr_multiple(i).split("#")
         
        //Third level: ccr used service unit
        val level3_usu = level2_ccr(1).split("\\*")
        
        //Third level: ccr ccaccount data
        val level3_ccr_cca = level2_ccr(8).split("\\*")
        
        //Merge in one map collection
        val lineMap = Map(
        "level1" -> level1, 
        "level2_ccr" -> level2_ccr, 
        "level2_serv_sess_id" -> level2_serv_sess_id,
        "level2_serv_elmt" -> level2_serv_elmt,
        "level3_usu" -> level3_usu,
        "level3_ccr_cca" -> level3_ccr_cca)
        

      
        //Add map to list
      if ( !this.cs5RecordsIsNotGarbage(lineMap) ) {

            listTransformedRow.append(this.genCS5TransformedRow(lineMap, prcDt, jobId))
        }
         
        i=i+1
        
      }
    
    //Return List
    return listTransformedRow.toList

  }
  
  /**
   * Filter CS5 Garbage from E/// August 2016
   * where CHARGING_CONTEXT_ID like'%@3gpp.org%'
   * and CCR_CCA_USED_OFFERS is null
   * and CCR_RATED_UNITS like'%:0:%:%:%,%:0:%:%:%,%:0:%:%:%';
   * @return true if record is valid
   * 	false if record is not valid
   */
   def cs5RecordsIsNotGarbage( lineMap: Map[String, Array[String]] ) : Boolean = {
    
    try {
        val chargingContext = CS5Select.getChargingContextId(lineMap)
        val usedOffers = CS5Select.getUsedOfferId(lineMap)
        val ratedUnits = CS5Select.getRatedUnits(lineMap)
        
        //create match using regex
        val patt = ".~0~.~.~.*.~0~.~.~.*.~0~.~.~.".r
        val ratedUnitsStatus = patt.findFirstIn(ratedUnits) //if valid then empty

        return !(chargingContext.contains("@3gpp.org") && usedOffers.isEmpty() && (ratedUnits.isEmpty() || !ratedUnitsStatus.isEmpty) )
    }
    catch {
      case e: Exception => return true
    }
  }
  
    /**
   * Generate Transformed String and select columns for CS5 PREPAID
   * @param lineMap Map of String from genCS5TransformedRowList 
   * @return Transformed row
   */
   def genCS5TransformedRow( lineMap:Map[String, Array[String]], prcDt:String, jobId:String) : Row = {  
     
    //Get Map of line    
    //val lineMap = this.getMapOfLine(line, "PREPAID")   
    
     
    return Row(this.getTransactionDate(lineMap)
          ,1 //HIT
          ,this.getUsageVolume(lineMap)      
          )
  }
 
      /**
    * Get transaction date from CCR_TRIGGER_TIME
    */
  def getTransactionDate( lineMap: Map[String, Array[String]] ) : String = {
    return Common.safeGet( lineMap("level2_ccr"),3 ).take(8)
  }
  
     /**
   * Get usage volume
   */
  def getUsageVolume( lineMap: Map[String, Array[String]] ) : Double = {
    //val volume = Common.toDouble( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 1, "*", 3 ) )
    val volume = Common.toDouble( Common.safeGet( lineMap("level3_usu"),3 ) )
    return volume
  }
}