package com.ibm.id.isat.nonUsage.SDP.model

import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.SparkConf;
import org.apache.spark.sql//Context.implicits.*;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import com.ibm.id.isat.utils.ReferenceSchema
import com.ibm.id.isat.utils.Common


object SDPAdj{
  
    
   // Define columns for SDP Adjustment
    val SDPAdjSchema = StructType(Array(
    StructField("adjRcrdTp", StringType, true),
    StructField("subsNum", StringType, true),
    StructField("adjTimeStamp", StringType, true),
    StructField("adjAction", StringType, true),
    StructField("balBef", StringType, true),
    StructField("balAf", StringType, true),
    StructField("adjAmt", StringType, true),
    StructField("svcClsId", StringType, true),
    StructField("originNodeType", StringType, true),
    StructField("originNodeId", StringType, true),
    StructField("origTrnscTimeStamp", StringType, true),
    StructField("trscTp", StringType, true),
    StructField("trscCd", StringType, true),
    StructField("newServiceClass", StringType, true),
    StructField("actDt", StringType, true),
    StructField("ddcAmt", StringType, true),
    StructField("fileNm", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("subAdj", StringType, true),
    StructField("daDtl", StringType, true),
    StructField("jobId", StringType, true),
    StructField("rcrd_id", StringType, true),
    StructField("prcDt", StringType, true),
    StructField("srcTp", StringType, true),
    StructField("fileDt", StringType, true)
     ));
    
    // Define columns for SDP Adjustment
    val SDPAdjSubSchema = StructType(Array(
    StructField("adjRcrdTp", StringType, true),
    StructField("subsNum", StringType, true), 
    StructField("adjTimeStamp", StringType, true),
    StructField("svcClsId", StringType, true),
    StructField("origTrnscTimeStamp", StringType, true),
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
    StructField("adjAmt", StringType, true),
    StructField("action", StringType, true),
    StructField("clearedAccountValue", StringType, true)
     ));
    
    // Define columns for SDP Adjustment REJ REF 2
    val SDPAdjSubInqSchema = StructType(Array(
    StructField("rcrd_id", StringType, true),
    StructField("daDtl", StringType, true)
     ));
    
    
    // Define columns for SDP Adjustment REJ REF 2
    val SDPAdjSubRejSchema = StructType(Array(
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
    StructField("origTrnscTimeStamp", StringType, true),
    StructField("jobId", StringType, true),
    StructField("rcrd_id", StringType, true),
    StructField("prcDt", StringType, true),
    StructField("srcTp", StringType, true),
    StructField("fileDt", StringType, true),
    StructField("rejrsn", StringType, true)
     ));
    
    
}