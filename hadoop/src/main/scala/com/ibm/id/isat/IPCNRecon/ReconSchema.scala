package com.ibm.id.isat.IPCNRecon

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.functions._

object ReconSchema {
      def main(args: Array[String]): Unit = {
    
    }
      
    val IPCNLogSchema = StructType(Array(
    StructField("EventID", StringType, true),
    StructField("MSISDN", StringType, true),
    StructField("IPCNStatus", StringType, true),
    StructField("OfferInterface", StringType, true),
    StructField("OfferInterfaceResult", StringType, true),
    StructField("USSDAnswer", StringType, true),
    StructField("SSPKeyword", StringType, true),
    StructField("SSPActivationResult", StringType, true),
    StructField("SSPTransactionID", StringType, true),
    StructField("Timestamp", StringType, true)
  ));  
    
    val AddonSchema = StructType(Array(
    StructField("TriggerTime", StringType, true),
    StructField("TriggerTimeNorm", StringType, true),
    StructField("ServerMSISDN", StringType, true),
    StructField("NodeName", StringType, true),
    StructField("OriginRealm", StringType, true),
    StructField("OriginHost", StringType, true),
    StructField("SSPTransactionID", StringType, true),
    StructField("ServiceClassID", StringType, true),
    StructField("AreaName", StringType, true),
    StructField("ServiceID", StringType, true),
    StructField("ANumCharching", StringType, true),
    StructField("OfferID1", StringType, true),
    StructField("OfferID2", StringType, true),
    StructField("OfferID3", StringType, true),
    StructField("OfferID4", StringType, true),
    StructField("OfferID5", StringType, true),
    StructField("AccountValueDeducted", StringType, true),
    StructField("DAID1", StringType, true),
    StructField("DAValueChange1", StringType, true),
    StructField("DAUOM1", StringType, true),
    StructField("DAID2", StringType, true),
    StructField("DAValueChange2", StringType, true),
    StructField("DAUOM2", StringType, true),
    StructField("DAID3", StringType, true),
    StructField("DAValueChange3", StringType, true),
    StructField("DAUOM3", StringType, true),
    StructField("DAID4", StringType, true),
    StructField("DAValueChange4", StringType, true),
    StructField("DAUOM4", StringType, true),
    StructField("DAID5", StringType, true),
    StructField("DAValueChange5", StringType, true),
    StructField("DAUOM5", StringType, true),
    StructField("EVID", StringType, true),
    StructField("accValueBefore", StringType, true),
    StructField("accValueAfter", StringType, true)
  ));  
}