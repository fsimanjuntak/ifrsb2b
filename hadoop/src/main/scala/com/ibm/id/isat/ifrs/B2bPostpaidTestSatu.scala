package com.ibm.id.isat.ifrs

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import com.ibm.id.isat.utils.Common
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
import com.ibm.id.isat.utils.ReferenceDF
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SQLContext

object B2bPostpaidTestSatu {
  def main(args: Array[String]): Unit = {
    val (prcDt, jobId, configDir, env) = try {
      (args(0), args(1), args(2), args(3))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process date> <job identifier> <input directory> <config directory> <LOCAL|PRODUCTION>")
      return
    }
    
    // Initialize Spark Context
    val sc = env match {
      case "PRODUCTION" => new SparkContext(new SparkConf())
      case "LOCAL" => new SparkContext("local[*]", "local spark", new SparkConf())
      case _ => return
    }
    
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false") //Disable metadata parquet
    
    // Initialize SQL Context
    val sqlContext = new SQLContext(sc)
    
    // Initialize File System (for renameFile)
    val fs = FileSystem.get(sc.hadoopConfiguration);
    
    // Initialize Logging
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    rootLogger.setLevel(Level.OFF)
    
    sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("C:/Users/FransSimanjuntak/sparksource/test.txt")
      .registerTempTable("testb2b")
    
//     ROUND(CAST(USERNAME AS DECIMAL(38,10)),9)
    sqlContext.sql("select format_number(CAST(USERNAME AS DOUBLE),9), USERNAME from testb2b").show(false)
    println("[INFO] Process done.")
    
  }
  
  val ifrsBillChargeSchema = new StructType(Array(
    StructField("ACCOUNT_NUM", StringType, true),
    StructField("INVOICE_NUM", StringType, true),
    StructField("BILL_DTM", StringType, true),
    StructField("ACTUAL_BILL_DTM", StringType, true),
    StructField("SUBS_REF", StringType, true),
    StructField("SUBS_PD_ID", StringType, true),
    StructField("SUBSCRIPTION_TYPE", StringType, true),
    StructField("SVC_ID", StringType, true),
    StructField("SVC_START_DT", StringType, true),
    StructField("SVC_END_DT", StringType, true),
    StructField("CUST_REF", StringType, true),
    StructField("PD_SEQ", StringType, true),
    StructField("PRODUCT_ID", StringType, true),
    StructField("PRODUCT_NM", StringType, true),
    StructField("PRODUCT_START", StringType, true),
    StructField("PRODUCT_END", StringType, true),
    StructField("TARIFF_ID", StringType, true),
    StructField("OTC_SEQ", StringType, true),
    StructField("OTC_ID", StringType, true),
    StructField("CHRG_TP", StringType, true),
    StructField("CHRG_START", StringType, true),
    StructField("CHRG_END", StringType, true),
    StructField("CURRENCY_CODE", StringType, true),
    StructField("CHARGE_ORI", StringType, true),
    StructField("KMK_NUMBER", StringType, true),
    StructField("CONVRT_RATE", StringType, true),
    StructField("CHARGE_IDR", StringType, true),
    StructField("AGRMT_NUM", StringType, true),
    StructField("AGRMT_START", StringType, true),
    StructField("AGRMT_END", StringType, true),
    StructField("REGION_CODE", StringType, true),
    StructField("BRANCH_CODE", StringType, true)))
  
  val cweoDailyAssetSchema = new StructType(Array(
    StructField("SA_ID", StringType, true),
    StructField("BA_ID", StringType, true),
    StructField("CA_ID", StringType, true),
    StructField("CCA_ID", StringType, true),
    StructField("AST_REFR", StringType, true),
    StructField("AST_ID", StringType, true),
    StructField("SVC_ID", StringType, true),
    StructField("AST_NM", StringType, true),
    StructField("AST_ST", StringType, true),
    StructField("AST_BLC_ST", StringType, true),
    StructField("RSN", StringType, true),
    StructField("ACTVN_DT", StringType, true),
    StructField("TMT_DT", StringType, true),
    StructField("ACT_RFS_DT", StringType, true),
    StructField("PD_ID", StringType, true),
    StructField("PD_NM", StringType, true),
    StructField("PD_TP", StringType, true),
    StructField("PRN_AST_REFR", StringType, true),
    StructField("PD_CGY", StringType, true),
    StructField("BANDWIDTH", StringType, true),
    StructField("BANDWITH_UNIT_OF_MSR", StringType, true),
    StructField("BLD_SITE_A", StringType, true),
    StructField("ADR_SITE_A", StringType, true),
    StructField("CITY_SITE_A", StringType, true),
    StructField("ZIP_CODE_SITE_A", StringType, true),
    StructField("BLD_SITE_B", StringType, true),
    StructField("ADR_SITE_B", StringType, true),
    StructField("CITY_SITE_B", StringType, true),
    StructField("ZIP_CODE_SITE_B", StringType, true),
    StructField("LCL_EXG_PORT", StringType, true),
    StructField("IP_ADR", StringType, true),
    StructField("LINK_REF_NUM", StringType, true),
    StructField("ACS_PORT", StringType, true),
    StructField("ICCID", StringType, true),
    StructField("IMSI", StringType, true),
    StructField("IS_LIMITLESS", StringType, true),
    StructField("USG_LIMIT", StringType, true),
    StructField("OTC", StringType, true),
    StructField("MRC", StringType, true),
    StructField("LAST_UDT_DT", StringType, true),
    StructField("CARD_TYPE", StringType, true),
    StructField("CONTRACT_CODE", StringType, true),
    StructField("PRODUCT_SEQUENCE", StringType, true),
    StructField("SUBSCRIPTION_REF", StringType, true),
    StructField("BILLING_PACKAGE_ID", StringType, true),
    StructField("BILLING_PRODUCT_ID", StringType, true),
    StructField("BILLING_TARIFF_ID", StringType, true),
    StructField("BILLING_RATING_TARIFF_ID", StringType, true),
    StructField("PROD_TYPE", StringType, true),
    StructField("PRODUCT_SUBSCRIPTION_TYPE", StringType, true),
    StructField("PRODUCT_SUBSCRIPTION_ID", StringType, true),
    StructField("PRODUCT_PART_NUMBER", StringType, true),
    StructField("CS_PRODUCT_ID", StringType, true),
    StructField("PRODUCT_LINE", StringType, true),
    StructField("AGRMT_NUM", StringType, true),
    StructField("AGREEMENT_NAME", StringType, true),
    StructField("AGREEMENT_START_DATE", StringType, true),
    StructField("AGREEMENT_END_DATE", StringType, true),
    StructField("CONTRACT_SLA", StringType, true)))
  
  val cweoDailyAgreementSchema = new StructType(Array(
    StructField("AGRMT_NUM", StringType, true),
    StructField("AGRMT_NM", StringType, true),
    StructField("AGRMT_TP", StringType, true),
    StructField("AGRMT_ST", StringType, true),
    StructField("AGRMT_REVISION", StringType, true),
    StructField("AGRMT_START", StringType, true),
    StructField("AGRMT_END", StringType, true),
    StructField("AUTO_RENEW_ST", StringType, true),
    StructField("ISAT_CTR_NUM", StringType, true),
    StructField("CONTRACT_PERIOD", StringType, true),
    StructField("CONTRACT_INITIAL_AMT", StringType, true)))
  
  val cweoDailyCstaccSchema = new StructType(Array(
    StructField("CA_ID", StringType, true),
    StructField("CCA_ID", StringType, true),
    StructField("CA_REFR", StringType, true),
    StructField("CA_NM", StringType, true),
    StructField("CA_ID_TP", StringType, true),
    StructField("CA_ID_REFR", StringType, true),
    StructField("CA_ID_EXP_DT", StringType, true),
    StructField("CCA_NM", StringType, true),
    StructField("CUST_AC_TEAM", StringType, true),
    StructField("CA_TP", StringType, true),
    StructField("CUST_SEG", StringType, true),
    StructField("CA_ST", StringType, true),
    StructField("CA_ST_DT", StringType, true),
    StructField("MAIN_CTC_NUM", StringType, true),
    StructField("ID_REFR", StringType, true),
    StructField("CGY", StringType, true),
    StructField("ID_EXP_DT", StringType, true),
    StructField("VIP_F", StringType, true),
    StructField("VIP_TP", StringType, true),
    StructField("VIP_CARD_NUM", StringType, true),
    StructField("VIP_SEQ", StringType, true),
    StructField("VIP_STRT_DT", StringType, true),
    StructField("VIP_EXP_DT", StringType, true),
    StructField("VIP_DSC", StringType, true),
    StructField("VIP_INTGR_ST", StringType, true),
    StructField("ADR_TP", StringType, true),
    StructField("ADR", StringType, true),
    StructField("ADR_LINE_2", StringType, true),
    StructField("CITY", StringType, true),
    StructField("PROV", StringType, true),
    StructField("BLD_NM", StringType, true),
    StructField("ZIP_CODE", StringType, true),
    StructField("CUST_STMT_F", StringType, true),
    StructField("BILL_PRD_UNITS", StringType, true),
    StructField("BILL_PRD", StringType, true),
    StructField("BILL_DT", StringType, true),
    StructField("INV_INTEG_ST", StringType, true),
    StructField("INV_INTEG_MSG", StringType, true),
    StructField("PLC_OF_BRTH", StringType, true),
    StructField("DT_OF_BRTH", StringType, true),
    StructField("GND", StringType, true),
    StructField("MTHR_MDN_NM", StringType, true),
    StructField("HOBBY", StringType, true),
    StructField("RLG", StringType, true),
    StructField("MAR_ST", StringType, true),
    StructField("EMAIL_ADR", StringType, true),
    StructField("ED", StringType, true),
    StructField("PSN_NPWP", StringType, true),
    StructField("OCP", StringType, true),
    StructField("INCM_RNG_MO", StringType, true),
    StructField("DIV", StringType, true),
    StructField("LEN_TERM_WRK_YR", StringType, true),
    StructField("LEN_TERM_WRK_MO", StringType, true),
    StructField("CO_NM", StringType, true),
    StructField("CO_NPWP", StringType, true),
    StructField("BSN_LINE", StringType, true),
    StructField("NUM_OF_EMPLYEE", StringType, true),
    StructField("CO_SCALE", StringType, true),
    StructField("CTC_TP", StringType, true),
    StructField("CTC_NM", StringType, true),
    StructField("CA_ELIG_F", StringType, true),
    StructField("LAST_UDT_DT", StringType, true),
    StructField("CUST_SALUTATION", StringType, true)))
  
  val cweoDailyOrderSchema = new StructType(Array(
    StructField("ORDER_LINE_ITEM_ID", StringType, true),
    StructField("ORDER_LINE_ITEM_STATUS", StringType, true),
    StructField("ORDER_ID", StringType, true),
    StructField("ORDER_NUM", StringType, true),
    StructField("ORDER_TYPE", StringType, true),
    StructField("ORDER_STATUS", StringType, true),
    StructField("ORDER_CREATED_DATE", StringType, true),
    StructField("ORDER_SUBMISSION_DATE", StringType, true),
    StructField("ORDER_COMPLETION_DATE", StringType, true),
    StructField("OPPORTUNITY_NUM", StringType, true),
    StructField("QUOTE_NUM", StringType, true),
    StructField("SR_NUM", StringType, true),
    StructField("DOCUMENT_REQUIRED", StringType, true),
    StructField("PRIORITY", StringType, true),
    StructField("SERVICE_ID", StringType, true),
    StructField("SERVICE_ACCOUNT_ID", StringType, true),
    StructField("BILLING_ACCOUNT_ID", StringType, true),
    StructField("CUSTOMER_ACCOUNT_ID", StringType, true),
    StructField("CORP_CUSTOMER_ID", StringType, true),
    StructField("PROMISE_TO_PAY_AMT", StringType, true),
    StructField("PROMISE_TO_PAY_DATE", StringType, true),
    StructField("NIK", StringType, true),
    StructField("CREATED_BY", StringType, true),
    StructField("SUBMITTED_BY", StringType, true),
    StructField("DEALER_ID", StringType, true),
    StructField("OUTLET", StringType, true),
    StructField("CHANNEL", StringType, true),
    StructField("PRICE_LIST", StringType, true),
    StructField("TOTAL", StringType, true),
    StructField("ASSIGNED_TO", StringType, true),
    StructField("SALES_PERSON", StringType, true),
    StructField("CONTRACT_SIGNING_OFFICER", StringType, true),
    StructField("ACTION", StringType, true),
    StructField("PRODUCT_ID", StringType, true),
    StructField("PRODUCT", StringType, true),
    StructField("PARENT_ORDER_ID", StringType, true),
    StructField("PRODUCT_TYPE", StringType, true),
    StructField("PRODUCT_CATEGORY", StringType, true),
    StructField("BLOCK_STATUS", StringType, true),
    StructField("PROMO_CODE", StringType, true),
    StructField("QTY", StringType, true),
    StructField("AGREEMENT_NAME", StringType, true),
    StructField("QUADRANT", StringType, true),
    StructField("REASON", StringType, true),
    StructField("START_PRICE", StringType, true),
    StructField("NET_PRICE", StringType, true),
    StructField("EXTENDED_NET_PRICE", StringType, true),
    StructField("OTC_SUB_TOTAL", StringType, true),
    StructField("MRC_SUB_TOTAL", StringType, true),
    StructField("OTC", StringType, true),
    StructField("MRC", StringType, true),
    StructField("ENTITLEMENT", StringType, true),
    StructField("ACTIVATION_DATE", StringType, true),
    StructField("PROJECT_NAME", StringType, true),
    StructField("ACTUAL_RFS_DATE", StringType, true),
    StructField("REQUESTED_RFS_DATE", StringType, true),
    StructField("BANDWITH", StringType, true),
    StructField("BANDWITH_UNIT_OF_MEASURE", StringType, true),
    StructField("SITE_A_ADDRESS_LINE_1", StringType, true),
    StructField("SITE_A_ADDRESS_LINE_2", StringType, true),
    StructField("SITE_A_BULDING_NAME", StringType, true),
    StructField("SITE_A_CITY", StringType, true),
    StructField("SITE_A_COUNTRY", StringType, true),
    StructField("SITE_A_ZIP_CODE", StringType, true),
    StructField("SITE_B_ADDRESS_LINE_1", StringType, true),
    StructField("SITE_B_ADDRESS_LINE_2", StringType, true),
    StructField("SITE_B_BUILDING_NAME", StringType, true),
    StructField("SITE_B_CITY", StringType, true),
    StructField("SITE_B_COUNTRY", StringType, true),
    StructField("SITE_B_ZIP_CODE", StringType, true),
    StructField("INTEGRATION_STATUS", StringType, true),
    StructField("IS_LIMITLESS", StringType, true),
    StructField("OLD_IS_LIMITLESS", StringType, true),
    StructField("USAGE_LIMIT", StringType, true),
    StructField("OLD_USAGE_LIMIT", StringType, true),
    StructField("ICCID", StringType, true),
    StructField("OLD_ICCID", StringType, true),
    StructField("ASSET_NAME", StringType, true),
    StructField("OLD_SERVICE_NUMBER", StringType, true),
    StructField("LOCAL_EXCHANGE_PORT", StringType, true),
    StructField("IP_ADDRESS", StringType, true),
    StructField("LINK_REF_NUMBER", StringType, true),
    StructField("ACCESS_PORT", StringType, true),
    StructField("LAST_UPDATE_DATE", StringType, true),
    StructField("CARD_TYPE", StringType, true),
    StructField("NUMBER_OF_E1", StringType, true),
    StructField("ORDER_COMMENT", StringType, true),
    StructField("CONTRACT_START_DATE", StringType, true),
    StructField("CONTRACT_END_DATE", StringType, true),
    StructField("APPROVED_BY", StringType, true),
    StructField("MIDI_LOCATION", StringType, true),
    StructField("PRODUCT_LINE", StringType, true),
    StructField("FAB_SIGNED_DATE", StringType, true),
    StructField("FAB_EARLIER_FLAG", StringType, true),
    StructField("CONTRACT_TYPE", StringType, true),
    StructField("QUOTE_CREATED_DATE", StringType, true),
    StructField("QUOTE_SUBMISSION_DATE", StringType, true),
    StructField("QUOTE_COMPLETION_DATE", StringType, true),
    StructField("LEAD_TIME", StringType, true),
    StructField("LEAD_TIME_IN_WEEKDAYS", StringType, true),
    StructField("LEAD_TIME_REASON", StringType, true),
    StructField("INITIAL_RFS_DATE", StringType, true),
    StructField("DEPARTMENT_SD", StringType, true),
    StructField("PROJECT_MANAGER_NAME", StringType, true),
    StructField("ASS_PROJECT_MANAGER_NAME", StringType, true),
    StructField("OUTASK", StringType, true),
    StructField("INTERFACE_TYPE", StringType, true),
    StructField("TRANSMISSION_TYPE", StringType, true),
    StructField("TOTAL_PENDING_ESTIMATION", StringType, true),
    StructField("UPDATED_RFS_DATE", StringType, true),
    StructField("CHURN_REASON", StringType, true),
    StructField("CHURN_COMMENT", StringType, true),
    StructField("TERMINATION_DT", StringType, true),
    StructField("PRODUCT_SEQ", StringType, true),
    StructField("BILLING_PRODUCT_ID", StringType, true),
    StructField("SUBSCRIPTION_REF", StringType, true),
    StructField("AGREEMENT_NUMBER", StringType, true)))
  
}