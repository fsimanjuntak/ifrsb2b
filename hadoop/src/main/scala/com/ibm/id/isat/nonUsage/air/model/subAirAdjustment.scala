package com.ibm.id.isat.nonUsage.air.model

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.functions._


/**
 * 
 * Created by IBM
 * Updated : 26 May 2016
 */

object subAirAdjustment {
  
   
  
   /*
   * @staging model
   * 
   */
   val stgAirAdjSchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originNodeID", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("realFilename", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("dedicatedAccount", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("accountFlagBefore", StringType, true),
    StructField("accountFlagAfter", StringType, true),
    StructField("jobID", StringType, true),// additional column 
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true)
   ));
  
   
   
   /*
   * @ha1 model
   * 
   */
  val ha1SubAirAdjSchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originNodeID", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("realFilename", StringType, true),
    //StructField("dedicatedAccount", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("accountFlagBefore", StringType, true),
    StructField("accountFlagAfter", StringType, true),
    StructField("jobID", StringType, true),// additional column 
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true),
    StructField("prefix", StringType, true),
    StructField("country", StringType, true),
    StructField("city", StringType, true),
    StructField("provider_id", StringType, true),
    StructField("svcClassName", StringType, true),
    StructField("promo_package_code", StringType, true),
    StructField("promo_package_name", StringType, true),
    StructField("brand_name", StringType, true),
    StructField("hlr_branch_nm", StringType, true),
    StructField("hlr_region_nm", StringType, true),
//    StructField("svcClassCode", StringType, true),    
//    StructField("regBranchCity", StringType, true),
    StructField("offerId", StringType, true),
    StructField("areaName", StringType, true),
    StructField("tupple1", StringType, true),    
    StructField("tupple3", StringType, true)
   ));
  
     
  /*
   * @staging reprocess model
   * This structure type is used for reprocess reject reference
   * 
   */
   val stgReprocessAirRejRefSchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originNodeID", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("realFilename", StringType, true),
    StructField("dedicatedAccount", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("accountFlagBefore", StringType, true),
    StructField("accountFlagAfter", StringType, true),
//    StructField("city", StringType, true),
//    StructField("prefix", StringType, true),
    StructField("jobID", StringType, true),
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column 
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true),// additional column
    StructField("rejRsn", StringType, true)
   ));
   
 
    /*
   * @staging reprocess model
   * This structure type is used for reprocess reject revenue
   * 
   */
   val stgReprocessAirRejRevSchema = StructType(Array(
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("prefix", StringType, true),
    StructField("city", StringType, true),
    StructField("provider_id", StringType, true),
    StructField("country", StringType, true),
    StructField("svcClassName", StringType, true),
    StructField("promo_package_code", StringType, true),
    StructField("promo_package_name", StringType, true),
    StructField("brand_name", StringType, true),
    StructField("total_amount", StringType, true),
    StructField("total_hits", StringType, true),
    StructField("revenue_code", StringType, true),
    StructField("service_type", StringType, true),
    StructField("gl_code", StringType, true),
    StructField("gl_name", StringType, true),
    StructField("real_filename", StringType, true),
    StructField("hlr_branch_nm", StringType, true),
    StructField("hlr_region_nm", StringType, true),
    StructField("revenueflag", StringType, true),
    StructField("mccmnc,", StringType, true),
    StructField("lac", StringType, true),
    StructField("ci", StringType, true),
    StructField("laci_cluster_id", StringType, true),
    StructField("laci_cluster_nm", StringType, true),
    StructField("laci_region_id", StringType, true),
    StructField("laci_region_nm", StringType, true),
    StructField("laci_area_id", StringType, true),
    StructField("laci_area_nm", StringType, true),
    StructField("laci_salesarea_id", StringType, true),
    StructField("laci_salesarea_nm", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("mgr_svccls_id", StringType, true),
    StructField("offerId", StringType, true),
    StructField("areaName", StringType, true),
    StructField("jobID", StringType, true),
    StructField("recordID", StringType, true),
    StructField("prcDT", StringType, true),
    StructField("area", StringType, true),
    StructField("fileDT", StringType, true)
           
   ));
   
   
   /*    val ha1SgReprocessRevAirAdjSchema = StructType(Array(
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("prefix", StringType, true),
    StructField("city", StringType, true),
    StructField("provider_id", StringType, true),
    StructField("country", StringType, true),
    StructField("svcClassName", StringType, true),
    StructField("promo_package_code", StringType, true),
    StructField("promo_package_name", StringType, true),
    StructField("brand_name", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("real_filename", StringType, true),
    StructField("hlr_branch_nm", StringType, true),
    StructField("hlr_region_nm", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("jobID", StringType, true),// additional column
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column 
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true)
   ));*/
  
}