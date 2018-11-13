package com.ibm.id.isat.usage.cs3
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.functions._

object CS3Schema {
  
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext("local", "testing spark ", new SparkConf());
    val inputRDD = sc.textFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3.txt");
    
    println("Reference");
  }
  
    
  val CS3Schema = StructType(Array(
    StructField("trgrdate", StringType, true),
    StructField("trgrtime", StringType, true),
    StructField("APartyNo", StringType, true),
    StructField("BPartyNo", StringType, true),
    StructField("svcClss", StringType, true),
    StructField("originRealm", StringType, true),
    StructField("originHost", StringType, true),
    StructField("paymentCat", StringType, true),
    StructField("trafficCase", StringType, true),
    StructField("trafficCaseName", StringType, true),
    StructField("directionType", StringType, true),
    StructField("usageDuration", DoubleType, true),
    StructField("APartyNoPad", StringType, true),
    StructField("BPartyNoPad", StringType, true),
    StructField("realFileName", StringType, true),
    StructField("usgVol", DoubleType, true),
    StructField("triggerTime", StringType, true),
    StructField("acValBfr", StringType, true),
    StructField("acValAfr", StringType, true),    
    StructField("svcUsgDrct", StringType, true),
    StructField("svcUsgDest", StringType, true),    
    StructField("hit", IntegerType, true),  
    StructField("finalCharge", DoubleType, true),    
    StructField("extText", StringType, true),    
    StructField("daUsed", StringType, true),   
    StructField("subsID", StringType, true),       
    StructField("nodeID", StringType, true),       
    StructField("accValueInfo", StringType, true),      
    StructField("fileDate", StringType, true),   
    StructField("key", StringType, true),  
    StructField("daInfoOrigin", StringType, true),
    StructField("daInfo", StringType, true),        
    StructField("accountID", StringType, true),    
    StructField("revCode", StringType, true),
    StructField("usgAmount", DoubleType, true),
    StructField("SID", StringType, true),
    StructField("prcDt", StringType, true), 
    StructField("jobID", StringType, true),
    StructField("prcDtOld", StringType, true), 
    StructField("jobIDOld", StringType, true)
  ));
  
   val CS3RepcsSchema = StructType(Array(
    StructField("trgrdate", StringType, true),
    StructField("trgrtime", StringType, true),
    StructField("APartyNo", StringType, true),
    StructField("BPartyNo", StringType, true),
    StructField("svcClss", StringType, true),
    StructField("originRealm", StringType, true),
    StructField("originHost", StringType, true),
    StructField("paymentCat", StringType, true),
    StructField("trafficCase", StringType, true),
    StructField("trafficCaseName", StringType, true),
    StructField("directionType", StringType, true),
    StructField("usageDuration", IntegerType, true),
    StructField("APartyNoPad", StringType, true),
    StructField("BPartyNoPad", StringType, true),
    StructField("realFileName", StringType, true),
    StructField("usgVol", IntegerType, true),
    StructField("triggerTime", StringType, true),
    StructField("acValBfr", StringType, true),
    StructField("acValAfr", StringType, true),    
    StructField("svcUsgDrct", StringType, true),
    StructField("svcUsgDest", StringType, true),    
    StructField("hit", IntegerType, true),  
    StructField("finalCharge", StringType, true),    
    StructField("extText", StringType, true),    
    StructField("daUsed", StringType, true),   
    StructField("subsID", StringType, true),       
    StructField("nodeID", StringType, true),       
    StructField("accValueInfo", StringType, true),      
    StructField("fileDate", StringType, true),   
    StructField("key", StringType, true),  
    StructField("daInfoOrigin", StringType, true),
    StructField("daInfo", StringType, true),        
    StructField("accountID", StringType, true),    
    StructField("revCode", StringType, true),
    StructField("usgAmount", DoubleType, true),
    StructField("SID", StringType, true),
    StructField("prcDt", StringType, true), 
    StructField("jobID", StringType, true),
    StructField("prcDtOld", StringType, true), 
    StructField("jobIDOld", StringType, true)
  ));
  
}


/*
    StructField("aPrefix", StringType, true), 
    StructField("svcCtyName", StringType, true), 
    StructField("svcProviderID", StringType, true), 
    StructField("svcCityName", StringType, true), 
    StructField("bPrefix", StringType, true), 
    StructField("destCtyName", StringType, true), 
    StructField("destProviderID", StringType, true), 
    StructField("dstCityName", StringType, true), 

*/