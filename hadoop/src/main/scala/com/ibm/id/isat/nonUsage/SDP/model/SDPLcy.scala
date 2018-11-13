package com.ibm.id.isat.nonUsage.SDP.model

import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.SparkConf;
import org.apache.spark.sql//Context.implicits.*;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.functions._


object SDPLcy{
 
   
    // Define columns for SDP Adjustment
    val SDPLcySchema = StructType(Array(
        
    StructField("oriNodeTp", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("subsNum", StringType, true),
    StructField("svcClsId", StringType, true),
    StructField("initAmt", StringType, true),
    StructField("cldAccVal", DoubleType, true),
    StructField("acFlagsBef", StringType, true),
    StructField("acFlagsAf", StringType, true),
    StructField("actDt", StringType, true),
    StructField("fileNm", StringType, true),
    StructField("oriNodeId", StringType, true),
    StructField("origTimeStmp", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("subLcy", StringType, true),
    StructField("daDtl", StringType, true),
    StructField("jobId", StringType, true),
    StructField("rcrd_id", StringType, true),
    StructField("prcDt", StringType, true),
    StructField("srcTp", StringType, true),
    StructField("balBfr", StringType, true),
    StructField("balAft", StringType, true),
    StructField("fileDt", StringType, true)
     ));
    
    
    val SDPLcySubSchema = StructType(Array(
    StructField("timeStamp", StringType, true),
    StructField("subsNum", StringType, true),
    StructField("svcClsId", StringType, true),
    StructField("origTimeStmp", StringType, true),
    StructField("fileNm", StringType, true),
    StructField("prefix", StringType, true),
    StructField("city", StringType, true),
    StructField("provider_id", StringType, true),
    StructField("country", StringType, true),
    StructField("promo_package_code", StringType, true),
    StructField("promo_package_name", StringType, true),
    StructField("brand_name", StringType, true),
    StructField("hlr_branch_nm", StringType, true),
    StructField("hlr_region_nm", StringType, true),
    StructField("offer_id", StringType, true),
    StructField("areaname", StringType, true),
    StructField("jobId", StringType, true),
    StructField("rcrd_id", StringType, true),
    StructField("prcDt", StringType, true),
    StructField("srcTp", StringType, true),
    StructField("fileDt", StringType, true),
    StructField("da_id", StringType, true),
    StructField("cldAccVal", DoubleType, true)
     ));

    
    // Define columns for SDP Lifecycle REJ REF 1
    val SDPLcySubRejSchema = StructType(Array(
    StructField("transaction_date", StringType, true),
    StructField("subsNum", StringType, true), 
    StructField("prefix", StringType, true),
    StructField("city", StringType, true),
    StructField("provider_id", StringType, true),
    StructField("country", StringType, true),
    StructField("svcClsId", StringType, true),
    StructField("promo_package_code", StringType, true),
    StructField("promo_package_name", StringType, true),
    StructField("brand_name", StringType, true),
    StructField("total_amount", DoubleType, true),
    StructField("revenue_code", StringType, true),
    StructField("real_filename", StringType, true),
    StructField("hlr_branch_nm", StringType, true),
    StructField("hlr_region_nm", StringType, true),
    StructField("offer_id", StringType, true),
    StructField("areaname", StringType, true),
    StructField("mgr_svccls_id", StringType, true),
    StructField("origTimeStmp", StringType, true),
    StructField("jobId", StringType, true),
    StructField("rcrd_id", StringType, true),
    StructField("prcDt", StringType, true),
    StructField("srcTp", StringType, true),
    StructField("fileDt", StringType, true)
     ));
   

}



object SDPLcyTertio{
  
    // Define columns for SDP Lifecycle
    val SDPLcyTertioSchema = StructType(Array(
       
    StructField("MSISDN", StringType, true),
    StructField("SIM", StringType, true),
    StructField("IMSI", StringType, true),
    StructField("MASTER_PHONE", StringType, true),
    StructField("PUK1", StringType, true),
    StructField("PUK2", StringType, true),
    StructField("SUBSCRIBERID", StringType, true),
    StructField("ACCOUNTID", StringType, true),
    StructField("ACTIVATION_DATE", StringType, true),
    StructField("DELETE_DATE", StringType, true),
    StructField("DELETE_BY", StringType, true),
    StructField("AREA_CODES_SERVICE_CLASS_ID", StringType, true),
    StructField("SERVICE_CLASS_NAME", StringType, true),
    StructField("LANGUAGE_DESC", StringType, true),
    StructField("ORGANIZATIONID", StringType, true),
    StructField("ORGANIZATIONNAME", StringType, true),
    StructField("MOCExpired", StringType, true),
    StructField("MTCExpired", StringType, true)
     ));

    val SDPLcyTertioV2Schema = StructType(Array(
    StructField("MSISDN", StringType, true),
    StructField("MASTER_PHONE", StringType, true),
    StructField("PUK1", StringType, true),
    StructField("PUK2", StringType, true),
    StructField("SUBSCRIBERID", StringType, true),
    StructField("ACCOUNTID", StringType, true),
    StructField("ACTIVATION_DATE", StringType, true),
    StructField("DELETE_DATE", StringType, true),
    StructField("DELETE_BY", StringType, true),
    StructField("AREA_CODES_SERVICE_CLASS_ID", StringType, true),
    StructField("SERVICE_CLASS_NAME", StringType, true),
    StructField("LANGUAGE_DESC", StringType, true),
    StructField("ORGANIZATIONID", StringType, true),
    StructField("ORGANIZATIONNAME", StringType, true),
    StructField("MOCExpired", StringType, true),
    StructField("MTCExpired", StringType, true)
     ));

}