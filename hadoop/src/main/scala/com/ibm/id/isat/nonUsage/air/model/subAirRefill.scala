package com.ibm.id.isat.nonUsage.air.model

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

object subAirRefill {
  
  
  /*
   * @staging model
   * 
   * 
   */
   val stgAirReffSchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originNodeID", StringType, true),      
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("refillType", StringType, true),
    StructField("refillProfileID", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("realFilename", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("accountInformationBeforeRefill", StringType, true),
    StructField("accountInformationAfterRefill", StringType, true),
    StructField("voucherBasedRefill", StringType, true),
    StructField("activationDate", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("jobID", StringType, true),// additional column 
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true)
   ));
  
  
  /*
   * @ha1 model
   * 
   * 
   */
  val ha1SubAirRefSchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originNodeID", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("refillType", StringType, true),
    StructField("refillProfileID", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("realFilename", StringType, true),
    //StructField("dedicatedAccount", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("accountInformationBeforeRefill", StringType, true),
    StructField("accountInformationAfterRefill", StringType, true),
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
    StructField("regBranchCity", StringType, true),
    StructField("activationDate", StringType, true),
    StructField("offerId", StringType, true),
    StructField("areaName", StringType, true),
    StructField("tupple1", StringType, true),    
    StructField("tupple5", StringType, true)
   ));
  
  
  /*
   * @ha1 model
   * 
   * 
   */
  val ha1SubAirBeforeRefInquirySchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originNodeID", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("refillType", StringType, true),
    StructField("refillProfileID", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("realFilename", StringType, true),
    //StructField("dedicatedAccount", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("accountInformationBeforeRefill", StringType, true),
    StructField("accountInformationAfterRefill", StringType, true),
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
    StructField("regBranchCity", StringType, true),
    StructField("activationDate", StringType, true),
    StructField("offerId", StringType, true),
    StructField("areaName", StringType, true),
    StructField("tupple1Before", StringType, true),
    StructField("tupple2Before", StringType, true),
    StructField("tupple5Before", StringType, true)
   ));
  
  val ha1SubAirAfterRefInquirySchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originNodeID", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),
    StructField("refillType", StringType, true),
    StructField("refillProfileID", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("realFilename", StringType, true),
    //StructField("dedicatedAccount", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("accountInformationBeforeRefill", StringType, true),
    StructField("accountInformationAfterRefill", StringType, true),
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
    StructField("regBranchCity", StringType, true),
    StructField("activationDate", StringType, true),
    StructField("offerId", StringType, true),
    StructField("areaName", StringType, true),
    StructField("tupple1Before", StringType, true),
    StructField("tupple2Before", StringType, true),
    StructField("tupple5Before", StringType, true),
    StructField("tupple1After", StringType, true),
    StructField("tupple2After", StringType, true),
    StructField("tupple5After", StringType, true)
   ));
  
  
   val ha1CdrInquirySchema = StructType(Array(
    StructField("KEY", StringType, true),
    StructField("MSISDN", StringType, true),
    StructField("TRANSACTIONTIMESTAMP", StringType, true),
    StructField("SUBSCRIBERTYPE", StringType, true),
    StructField("AMOUNT", DoubleType, true),
    StructField("DA_DETAIL", StringType, true),
    StructField("CURRENTSERVICECLASS", StringType, true),
    StructField("ACTIVATIONDATE", StringType, true),
    StructField("SERVICECLASSID", StringType, true),
    StructField("SERVICECLASSNAME", StringType, true),
    StructField("PROMOPACKAGENAME", StringType, true),
    StructField("BRANDNAME", StringType, true),
    StructField("NEWSERVICECLASS", StringType, true),
    StructField("NEWSERVICECLASSNAME", StringType, true),
    StructField("NEWPROMOPACKAGENAME", StringType, true),
    StructField("NEWBRANDNAME", StringType, true),// additional column 
    StructField("ORIGINNODETYPE", StringType, true),// additional column 
    StructField("ORIGINHOSTNAME", StringType, true),// additional column
    StructField("EXTERNALDATA1", StringType, true),// additional column
    StructField("PROGRAMNAME", StringType, true),
    StructField("PROGRAMOWNER", StringType, true),
    StructField("PROGRAMCATEGORY", StringType, true),
    StructField("BANKNAME", StringType, true),
    StructField("BANKDETAIL", StringType, true),
    StructField("MAINACCOUNTBALANCEBEFORE", StringType, true),
    StructField("MAINACCOUNTBALANCEAFTER", StringType, true),
    StructField("LAC", StringType, true),
    StructField("CELLID", StringType, true),
    StructField("TRANSACTIONTYPE", StringType, true),
    StructField("FILENAMEINPUT", StringType, true),
    StructField("VOUCHER_TYPE", StringType, true),
    StructField("RELOAD_TYPE", StringType, true),
    StructField("JOBID", StringType, true)
   ));
  
  
   /*
    * @staging reprocess model - Reject Rev - Loan Balance
    * This structure type is used for reprocess reject reference
    * 
    */
    //stgReprocessAirLoanRejRevSchema
    
    
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
    StructField("refillType", StringType, true),
    StructField("refillProfileID", StringType, true),
    StructField("externalData1", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("subscriberNumber", StringType, true),
    StructField("currentServiceclass", StringType, true),
    StructField("msisdnPad", StringType, true),
    StructField("realFilename", StringType, true),
    StructField("dedicatedAccount", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("accountInformationBeforeRefill", StringType, true),
    StructField("accountInformationAfterRefill", StringType, true),
    StructField("jobID", StringType, true),
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column 
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true),// additional column
    StructField("activationDate", StringType, true),
    StructField("voucherBasedRefill", StringType, true),
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
    StructField("regBranchCity", StringType, true),
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
    StructField("realFilename", StringType, true),
    StructField("hlr_branch_nm", StringType, true),
    StructField("hlr_region_nm", StringType, true),
    StructField("revenueflag", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("offerId", StringType, true),
    StructField("areaName", StringType, true),
    StructField("jobID", StringType, true),// additional column
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column 
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true)
   ));
   
   
    /*
   * @staging model subAirRefillLoan
   * 
   * 
   */        
   val stgAirReffLoanSchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originHostName", StringType, true),      
    StructField("timeStamp", StringType, true),    
    StructField("accountNumber", StringType, true),  
    StructField("refillProfileID", StringType, true),  
    //StructField("subscriberNumber", StringType, true),  
    StructField("currentServiceclass", StringType, true),
    StructField("transactionAmount", StringType, true),        
    StructField("accountInformationAfterRefill", StringType, true),      
    StructField("subscribernumber", StringType, true),      
    StructField("realFilename", StringType, true),  
    StructField("originTimeStamp", StringType, true),      
    StructField("jobID", StringType, true),// additional column 
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true)
   ));
   
  /*
   * @staging reprocess Air Loan - Reject Revenue
   * This structure type is used for reprocess reject reference
   * 
   */
   val stgReprocessAirLoanRejRevSchema = StructType(Array(
    StructField("msisdn", StringType, true),
    StructField("prefix", StringType, true),
    StructField("regBranchCity", StringType, true),
    StructField("provider_id", StringType, true),    
    StructField("country", StringType, true),       
    StructField("currentServiceclass", StringType, true),
    StructField("promo_package_code", StringType, true),
    StructField("promo_package_name", StringType, true),
    StructField("brand_name", StringType, true),
    StructField("total_amount", StringType, true),
    StructField("total_hits", StringType, true),  
    StructField("revenue_code", StringType, true),
    StructField("service_type", StringType, true),// additional column 
    StructField("gl_code", StringType, true),// additional column 
    StructField("gl_name", StringType, true),// additional column
    StructField("realFilename", StringType, true),// additional column
    StructField("hlr_branch_nm", StringType, true),
    StructField("hlr_region_nm", StringType, true),
    StructField("revenueflag", StringType, true),
    StructField("originTimeStamp", StringType, true),
    StructField("offerId", StringType, true),
    StructField("areaName", StringType, true),
    StructField("recordID", StringType, true),
    StructField("prcDT", StringType, true),
    StructField("fileDT", StringType, true),
    StructField("area", StringType, true),
    StructField("tanggal", StringType, true),
    StructField("jobID", StringType, true),
    StructField("rej_rsn", StringType, true)
   ));
   
   /*
   * @staging reprocess Air Loan - Reject Ref
   * This structure type is used for reprocess reject reference
   * 
   */
   val stgReprocessAirLoanRejRefSchema = StructType(Array(
    StructField("originNodeType", StringType, true),
    StructField("originHostName", StringType, true),
    StructField("timeStamp", StringType, true),
    StructField("accountNumber", StringType, true),    
    StructField("refillProfileID", StringType, true),       
    StructField("currentServiceclass", StringType, true),
    StructField("transactionAmount", StringType, true),
    StructField("accountInformationAfterRefill", StringType, true),
    StructField("subscribernumber", StringType, true),
    StructField("realFilename", StringType, true),
    StructField("originTimeStamp", StringType, true),  
    StructField("jobID", StringType, true),
    StructField("recordID", StringType, true),// additional column 
    StructField("prcDT", StringType, true),// additional column 
    StructField("area", StringType, true),// additional column
    StructField("fileDT", StringType, true),// additional column
    StructField("rejRsn", StringType, true)
   ));
 
}