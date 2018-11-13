package com.ibm.id.isat.usage.cs5

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._

case class TransfCS5(transDate: String, transHour: String, serviceClass: String)

object CCNCS5PrepaidTLookupTest {
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext("local", "testing spark ", new SparkConf());
    
    /*
     * Parameter Setting
     */
    val prc_dt = "20160101";
    val seq_id = "1234";
    
    /*
     * Get source file
     */
    val cs5ConvertRDD = sc.textFile("C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/ConversionOutputCS5.txt");
    
    /*
     * Put in one line
     */
    val cs5Conv1LRDD = cs5ConvertRDD.map(line => line.split("\t")(1).concat("|").concat(line.split("\t")(2)).concat("|").concat(line.split("\t")(3)) );
  
    /*
     * Transformation Test
     */
    val out_TRANSACTION_DATE = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(3).substring(0, 8) );
    val out_TRANSACTION_HOUR = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(3).substring(8, 10) );
    val out_SERVICE_SCENARIO = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(4) );
    //val out_SERVICE_CLASS_ID = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(8).split("\\*")(1) );
    val out_SERVICE_CLASS_ID = cs5Conv1LRDD.map( line => Common.getTupleInTuple(Common.getTupleInTuple(line, "|", 13, "[", 0), "#", 8, "*", 1) );
    val out_CALL_CLASS_CAT = cs5Conv1LRDD.map( line => Common.getKeyVal( line.split("\\|")(13).split("\\[")(0).split("#")(10) , "DWSCallClassCategory", "\\*", "\\]" ) );
    val out_ROAMING_POSITION = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(6) );
    val out_FAF = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(8).split("\\*")(8) );
    val out_FAF_NUMBER = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(8).split("\\*")(9) );
    val out_RATING_GROUP = cs5Conv1LRDD.map( line => line.split("\\|")(6).split("\\:")(0) ); //need to create if condition for other service scenario types
    val out_CONTENT_TYPE = cs5Conv1LRDD.map( line => line.split("\\|")(6).split("\\:")(1) ); //need to create if condition for other service scenario types
    val out_CALL_REFERENCE = cs5Conv1LRDD.map( line => line.split("\\|")(11) );
    val out_SERVICE_OFFER_ID = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(8).split("\\*")(10) );
    val out_ACCOUNT_GROUP_ID = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(8).split("\\*")(2) );
    val out_REAL_FILENAME = cs5Conv1LRDD.map( line => Common.getTuple(line,"|",15) );
    val out_USAGE_VOLUME = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(1).split("\\*")(2) );
    val out_IMSI = cs5Conv1LRDD.map( line => Common.getKeyVal( line.split("\\|")(10) , "1", "#", "[" ) ); 
    
        
    val outRDD = cs5Conv1LRDD.map( line => line.split("\\|")(13).split("\\[")(0).split("#")(3).substring(0, 8)
          .concat("|").concat( line.split("\\|")(13).split("\\[")(0).split("#")(3).substring(8, 10) )
          .concat("|").concat( Common.getTupleInTuple(Common.getTupleInTuple(line, "|", 13, "[", 0), "#", 8, "*", 1) ) 
          );
    
    /*
     * Print
     */
    out_IMSI.collect foreach {case (a) => println (a)}
    
    //////////// DATAFRAME EXPERIMENT ///////////////////
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val transfCS5 = outRDD.map(_.split("|"))
    val transfCS5DF = outRDD.map(p => TransfCS5(p.split("\\|")(0), p.split("\\|")(1),p.split("\\|")(2))).toDF()
    transfCS5DF.registerTempTable("transfCS5DF")
    
    transfCS5DF.show()
    
    val refSCSChema = StructType(Array(
    StructField("SERVICE_CLASS_ID", StringType, true),
    StructField("SERVICE_CLASS_CODE", StringType, true),
    StructField("SERVICE_CLASS_NAME", StringType, true),
    StructField("PROMO_PACKAGE_ID", StringType, true),
    StructField("PROMO_PACKAGE_CODE", StringType, true),
    StructField("PROMO_PACKAGE_NAME", StringType, true),
    StructField("SERVICE_CLASS_EFF_DT", StringType, true),
    StructField("SERVICE_CLASS_END_DT", StringType, true),
    StructField("BRAND_SC_ID", StringType, true),
    StructField("BRAND_SC_NAME", StringType, true),
    StructField("BRAND_SC_EFF_DT", StringType, true),
    StructField("BRAND_SC_END_DT", StringType, true)
    ))
    
    val refSCDF = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .option("delimiter",",")
    .schema(refSCSChema)
    .load("C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/Test/REF_SC_20160524.csv")
    refSCDF.registerTempTable("refSCDF")
    
    refSCDF.show()
    
    val transfCS5withSC = sqlContext.sql("select a.*,b.PROMO_PACKAGE_CODE,b.BRAND_SC_NAME from transfCS5DF a left join refSCDF b on a.serviceClass=b.SERVICE_CLASS_CODE");
    
    transfCS5withSC.show()
  
    sc.stop();
    
  }
  
  def getAPartyNumber( content:String ) : String = {
    
    val msisdn = Common.getKeyVal( Common.getTuple(content, "|", 10) , "0", "#", "[" )
    
    return msisdn
  }
    
}