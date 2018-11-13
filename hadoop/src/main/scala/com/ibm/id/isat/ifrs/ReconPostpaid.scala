package com.ibm.id.isat.ifrs

import org.apache.spark.sql.hive.HiveContext
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
import org.apache.spark.sql.types.DateType

object ReconPostpaid {
  def main(args: Array[String]): Unit = {
    
    var prcDt = ""
    var jobId = ""
    var configDir = ""
    try {
      prcDt = args(0)
      jobId = args(1)
      configDir = args(2)
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    
    val sqlContext=new HiveContext(sc)
    
    val pathPostpaidReSummary = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.POSTPAID_RE_SUMMARY")
    val pathInputPostpaid = Common.getConfigDirectory(sc, configDir, "IFRS.INPUT.IFRS_POSTPAID_INPUT_DIR")
    val pathReconPostpaid = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.IFRS_POSTPAID_RECON_DIR")
    
    val smyRePostpaid=sqlContext.read.option("basePath",pathPostpaidReSummary).parquet(pathPostpaidReSummary+"/prc_dt="+prcDt)
    smyRePostpaid.registerTempTable("RE_POSTPAID")
    
    val billDetailIfrsDf = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|")
    .option("header", "true").schema(billDetailIfrsSchema).load(pathInputPostpaid+"/"+prcDt+"_"+jobId+"/bill_detail_ifrs")
    billDetailIfrsDf.registerTempTable("bill_detail_ifrs")
    
    val billAccountProductDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .option("delimiter", "~").schema(billAccountProductSchema).load(pathInputPostpaid+"/"+prcDt+"_"+jobId+"/bill_account_product")
    billAccountProductDf.registerTempTable("bill_account_product")
    
    val refBillAccountProductDf = broadcast(sqlContext.sql("""
      select
      account_num,
      product_seq,
      first(product_family_id) product_family_id,
      first(branch_code) branch_code,
      first(region_code) region_code,
      first(customer_type_id) customer_type_id,
      first(customer_type_name) customer_type_name,
      first(customer_name) customer_name,
      first(account_name) account_name,
      case when nvl(first(account_type),'')='' then '3' else first(account_type) end account_type
      from bill_account_product
      group by
      account_num,
      product_seq
      union all
      select
      account_num,
      0 product_seq,
      0 product_family_id,
      first(branch_code) branch_code,
      first(region_code) region_code,
      first(customer_type_id) customer_type_id,
      first(customer_type_name) customer_type_name,
      first(customer_name) customer_name,
      first(account_name) customer_name,
      case when nvl(first(account_type),'')='' then '3' else first(account_type) end account_type
      from bill_account_product
      where nvl(customer_type_id, '') <> ''
      group by
      account_num
    """))
    refBillAccountProductDf.registerTempTable("ref_bill_account_product")
    
    val postpaidInput=sqlContext.sql("""
      select '"""+prcDt+"""' prc_dt,revenue_code_name,date_format(cast(unix_timestamp(actual_bill_dtm, 'MM/dd/yyyy') as timestamp), 'yyyyMMdd') start_dt_orig ,date_format(cast(unix_timestamp(actual_bill_dtm, 'MM/dd/yyyy') as timestamp), 'yyyyMMdd') start_dt,
      customer_type_id, 
      case when account_type in (5, 11, 12) then 'Hybrid SAMU'
          when account_type in (6) then 'Matrix SAMU'
          when account_type in (7) then 'IPhone'
          when account_type in (8) then 'IPhone SAMU'
          when account_type in (9) then 'MIDI'
          when account_type in (10) then 'MIDI SAMU'
          when account_type in (3) then 'Matrix'
          when account_type in (2, 4) then 'StarOne'
      end account_type_name,'INPUT_RBM' type
      ,sum(REVENUE_MNY) revenue_mny
      from bill_detail_ifrs
      left join ref_bill_account_product on bill_detail_ifrs.account_num = ref_bill_account_product.account_num
          and bill_detail_ifrs.product_seq_override = ref_bill_account_product.product_seq
      --where account_type not in (2, 4, 5, 11, 12, 9, 10) and customer_type_id not in (4, 5, 29)
      group by revenue_code_name,date_format(cast(unix_timestamp(actual_bill_dtm, 'MM/dd/yyyy') as timestamp), 'yyyyMMdd')
      ,date_format(cast(unix_timestamp(actual_bill_dtm, 'MM/dd/yyyy') as timestamp), 'yyyyMMdd'),customer_type_id , account_type
  """)
    
    val refPostpaidEventReferenceDf = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .schema(postpaidEventReferenceSchema)
      .load("/user/apps/hadoop_spark/reference/ref_postpaid_event_reference/*.csv"))
    refPostpaidEventReferenceDf.registerTempTable("ref_postpaid_event_reference")
    refPostpaidEventReferenceDf.persist()
    refPostpaidEventReferenceDf.first()
    
    val refSidDistributionDf = sqlContext.sql("""
      select
      rev_code,
      gl_account_b2b_credit,
      gl_account_b2b_credit_desc
      from ref_postpaid_event_reference
    """)
    refSidDistributionDf.registerTempTable("ref_sid_distribution")
    
    val refPostpaidEventReferenceIphoneDf = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("/user/apps/hadoop_spark/reference/ref_postpaid_event_reference_iphone/*csv"))
    refPostpaidEventReferenceDf.registerTempTable("ref_postpaid_event_reference_iphone")
    refPostpaidEventReferenceIphoneDf.persist()
    refPostpaidEventReferenceIphoneDf.first()
    
    val refSidDistributionIphoneDf = sqlContext.sql("""
      select
      rev_code,
      gl_account_b2b_credit,
      gl_account_b2b_credit_desc
      from ref_postpaid_event_reference_iphone
    """)
    refSidDistributionIphoneDf.registerTempTable("ref_sid_distribution_iphone")

    val postpaidReWithoutGL=sqlContext.sql("""
      select prc_dt,revenue_code_name,cast(start_dt as string) as start_dt_orig,
        case when nvl(inst_month_1,'')<>'' or nvl(inst_month_2,'')<>'' or nvl(inst_month_3,'')<>'' or nvl(inst_month_4,'')<>'' or nvl(inst_month_5,'')<>'' or nvl(inst_month_6,'')<>'' or nvl(inst_month_7,'')<>'' or nvl(inst_month_8,'')<>'' or nvl(inst_month_9,'')<>'' or nvl(inst_month_10,'')<>'' or nvl(inst_month_11,'')<>'' or nvl(inst_month_12,'')<>''
        then ''
        else cast(start_dt as string) end start_dt,customer_type_id,account_type_name ,'RE_NONGL' type,
        cast(sum(revenue_mny) as double) revenue_mny 
      from RE_POSTPAID 
      left join ref_sid_distribution 
        on customer_segment <> 'B2B IPHONE'
        and RE_POSTPAID.revenue_code_name = ref_sid_distribution.rev_code
      left join ref_sid_distribution_iphone 
        on customer_segment = 'B2B IPHONE' 
        and lower(RE_POSTPAID.revenue_code_name) = lower(ref_sid_distribution_iphone.rev_code)
      where --revenue_code_id not in (55, 54, 5110) and 
        nvl(
        case customer_segment 
          when 'B2B IPHONE' then ref_sid_distribution_iphone.gl_account_b2b_credit
          else ref_sid_distribution.gl_account_b2b_credit 
        end,
        ''
        ) = ''
      group by prc_dt,revenue_code_name,customer_type_id,account_type_name,start_dt,case when nvl(inst_month_1,'')<>'' or nvl(inst_month_2,'')<>'' or nvl(inst_month_3,'')<>'' or nvl(inst_month_4,'')<>'' or nvl(inst_month_5,'')<>'' or nvl(inst_month_6,'')<>'' or nvl(inst_month_7,'')<>'' or nvl(inst_month_8,'')<>'' or nvl(inst_month_9,'')<>'' or nvl(inst_month_10,'')<>'' or nvl(inst_month_11,'')<>'' or nvl(inst_month_12,'')<>''
        then ''
        else start_dt end order by prc_dt
    """)
    
    val postpaidReWithGL=sqlContext.sql("""
      select prc_dt,revenue_code_name,cast(start_dt as string) as start_dt_orig,
      case when nvl(inst_month_1,'')<>'' or nvl(inst_month_2,'')<>'' or nvl(inst_month_3,'')<>'' or nvl(inst_month_4,'')<>'' or nvl(inst_month_5,'')<>'' or nvl(inst_month_6,'')<>'' or nvl(inst_month_7,'')<>'' or nvl(inst_month_8,'')<>'' or nvl(inst_month_9,'')<>'' or nvl(inst_month_10,'')<>'' or nvl(inst_month_11,'')<>'' or nvl(inst_month_12,'')<>''
        then ''
        else cast(start_dt as string) end start_dt,customer_type_id,account_type_name,'RE_WITHGL' type,
      cast(sum(revenue_mny) as double) revenue_mny 
      from RE_POSTPAID 
      group by prc_dt,revenue_code_name,customer_type_id,account_type_name,start_dt, 
      case when nvl(inst_month_1,'')<>'' or nvl(inst_month_2,'')<>'' or nvl(inst_month_3,'')<>'' or nvl(inst_month_4,'')<>'' or nvl(inst_month_5,'')<>'' or nvl(inst_month_6,'')<>'' or nvl(inst_month_7,'')<>'' or nvl(inst_month_8,'')<>'' or nvl(inst_month_9,'')<>'' or nvl(inst_month_10,'')<>'' or nvl(inst_month_11,'')<>'' or nvl(inst_month_12,'')<>''
        then ''
        else start_dt end
    """)

    Common.cleanDirectoryWithPattern(sc, pathReconPostpaid, "/prc_dt="+prcDt+"/type="+"*")
    postpaidInput.repartition(1).write.partitionBy("prc_dt","type")
    .mode("append").save(pathReconPostpaid)
    
    postpaidReWithoutGL.repartition(1).write.partitionBy("prc_dt","type")
    .mode("append").save(pathReconPostpaid)
    
    postpaidReWithGL.repartition(1).write.partitionBy("prc_dt","type")
    .mode("append").save(pathReconPostpaid)
  }
  import org.apache.spark.sql.types._

  val billDetailIfrsSchema: StructType = StructType(Array(
      StructField("ACCOUNT_NUM", StringType, true),
      StructField("BILL_SEQ", StringType, true),
      StructField("BILL_VERSION", StringType, true),
      StructField("REVENUE_CODE_ID", StringType, true),
      StructField("REVENUE_MNY", StringType, true),
      StructField("ACTUAL_BILL_DTM", StringType, true),
      StructField("CURRENCY_CODE", StringType, true),
      StructField("CUSTOMER_REF", StringType, true),
      StructField("PRODUCT_SEQ_OVERRIDE", StringType, true),
      StructField("START_DAT_OVERRIDE", StringType, true),
      StructField("END_DAT_OVERRIDE", StringType, true),
      StructField("ONE_OFF_NUMBER_OVERRIDE", StringType, true),
      StructField("RECURRING_NUMBER_OVERRIDE", StringType, true),
      StructField("EARLY_TERMINATION_MNY_OVERRIDE", StringType, true),
      StructField("CHARGE_PRODUCT_ID", StringType, true),
      StructField("CHARGE_PACKAGE_ID", StringType, true),
      StructField("CHARGE_REVENUE_CODE_ID", StringType, true),
      StructField("CHARGE_COST_MNY", StringType, true),
      StructField("CHARGE_START_DAT", StringType, true),
      StructField("CHARGE_END_DAT", StringType, true),
      StructField("CHARGE_NUMBER", StringType, true),
      StructField("CHARGE_CHARGE_SEQ", StringType, true),
      StructField("CHARGE_PRODUCT_SEQ", StringType, true),
      StructField("CHARGE_TARIFF_ID", StringType, true),
      StructField("CHARGE_PACKAGE_SEQ", StringType, true),
      StructField("PACKAGE_BASIC_COST_MNY", StringType, true),
      StructField("PACKAGE_DISC_REV_CODE_ID", StringType, true),
      StructField("PRE_OVERRIDE_COST_MNY", StringType, true),
      StructField("OVERRIDE_REV_CODE_ID", StringType, true),
      StructField("EARLY_TERM_MULT_COST_MNY", StringType, true),
      StructField("EARLY_TERM_MULT_REV_CODE_ID", StringType, true),
      StructField("CHARGE_CONTRACT_REF", StringType, true),
      StructField("OTC_LABEL", StringType, true),
      StructField("OTC_MNY", StringType, true),
      StructField("OTC_SEQ", StringType, true),
      StructField("OTC_ID", StringType, true),
      StructField("OTC_BILL_SEQ", StringType, true),
      StructField("OTC_DTM", StringType, true),
      StructField("OTC_CREATED_DTM", StringType, true),
      StructField("OTC_STATUS", StringType, true),
      StructField("OTC_REVENUE_CODE_ID", StringType, true),
      StructField("OTC_PRODUCT_SEQ", StringType, true),
      StructField("REVENUE_CODE_NAME", StringType, true),
      StructField("REVENUE_CODE_DESC", StringType, true),
      StructField("SERVICE_CLASS", StringType, true),
      StructField("SERVICE_ID", StringType, true),
      StructField("SMS_TYPE", StringType, true),
      StructField("CARRIER", StringType, true),
      StructField("SOURCE_CLASS_ID", StringType, true),
      StructField("PRODUCT_CHARGE_TYPE", StringType, true),
      StructField("EVENT_COUNT", StringType, true),
      StructField("VOLUME", StringType, true),
      StructField("DURATION", StringType, true)))

  val billAccountProductSchema: StructType = StructType(Array(
  StructField("CUSTOMER_REF", StringType, true),
  StructField("ACCOUNT_NUM", StringType, true),
  StructField("SUBSCRIPTION_REF", StringType, true),
  StructField("BILL_CHARGE_SEQ", StringType, true),
  StructField("BILL_EVENT_SEQ", StringType, true),
  StructField("CUSTOMER_CATEGORY", StringType, true),
  StructField("EVENT_BILLED_TO_DTM", StringType, true),
  StructField("NEXT_BILL_DTM", StringType, true),
  StructField("TERMINATION_DAT", StringType, true),
  StructField("BILLING_STATUS", StringType, true),
  StructField("TOTAL_BILLED_TOT", StringType, true),
  StructField("TOTAL_PAID_TOT", StringType, true),
  StructField("CURRENCY_CODE", StringType, true),
  StructField("TAX_INCLUSIVE_BOO", StringType, true),
  StructField("PREPAY_BOO", StringType, true),
  StructField("UNBILLED_ADJUSTMENT_MNY", StringType, true),
  StructField("DEPOSIT_MNY", StringType, true),
  StructField("CREDIT_ADJ_BILLED_TO_DTM", StringType, true),
  StructField("DEBIT_ADJ_BILLED_TO_DTM", StringType, true),
  StructField("ACCOUNT_NAME", StringType, true),
  StructField("UNBILLED_REVENUE_CHANGE", StringType, true),
  StructField("COLLECTION_BALANCE_SEQ", StringType, true),
  StructField("TOTAL_UNBILLED_OTC_TOT", StringType, true),
  StructField("UNBILLED_REALTIME_OTC_MNY", StringType, true),
  StructField("OTC_BILLED_TO_DTM", StringType, true),
  StructField("TOTAL_PAID_DISCOUNT_TOT", StringType, true),
  StructField("PENDING_PAID_TOT", StringType, true),
  StructField("ACCOUNTING_METHOD", StringType, true),
  StructField("BILL_PERIOD", StringType, true),
  StructField("BILL_PERIOD_UNITS", StringType, true),
  StructField("BILLS_PER_STATEMENT", StringType, true),
  StructField("START_DAT_ACC_DETAILS", StringType, true),
  StructField("END_DAT_ACC_DETAILS", StringType, true),
  StructField("ACCOUNT_ACCOUNT_NUM", StringType, true),
  StructField("CREDIT_LIMIT_MNY", StringType, true),
  StructField("PACKAGE_DISC_ACCOUNT_NUM", StringType, true),
  StructField("EVENT_DISC_ACCOUNT_NUM", StringType, true),
  StructField("PRODUCT_ID", StringType, true),
  StructField("PRODUCT_SEQ", StringType, true),
  StructField("PACKAGE_SEQ", StringType, true),
  StructField("CUST_SUBSCRIPTION_REF", StringType, true),
  StructField("SUBS_PRODUCT_SEQ", StringType, true),
  StructField("PARENT_PRODUCT_SEQ", StringType, true),
  StructField("EVENT_SOURCE_COUNT", StringType, true),
  StructField("HAS_ADD_ON_PRODUCTS_BOO", StringType, true),
  StructField("PRODUCT_FAMILY_ID", StringType, true),
  StructField("PRODUCT_DESC", StringType, true),
  StructField("PRODUCT_NAME", StringType, true),
  StructField("PRODUCT_TYPE", StringType, true),
  StructField("PRODUCT_FAMILY_NAME", StringType, true),
  StructField("PRODUCT_FAMILY_DESC", StringType, true),
  StructField("EVENT_DISCOUNT_ID", StringType, true),
  StructField("AWARDED_BONUS_COUNT", StringType, true),
  StructField("CARRIED_OVER_USAGE", StringType, true),
  StructField("DISC_CUSTOMER_CATEGORY", StringType, true),
  StructField("EVENT_SOURCE", StringType, true),
  StructField("PERIOD_END_DAT", StringType, true),
  StructField("PERIOD_NUM", StringType, true),
  StructField("PERIOD_START_DAT", StringType, true),
  StructField("PRODUCT_QUANTITY", StringType, true),
  StructField("TOTAL_DISCOUNTABLE_USAGE", StringType, true),
  StructField("CUST_PRODUCT_SEQ", StringType, true),
  StructField("START_DAT_PRODUCT", StringType, true),
  StructField("END_DAT_PRODUCT", StringType, true),
  StructField("PACKAGE_CODE", StringType, true),
  StructField("PACKAGE_NAME", StringType, true),
  StructField("REGION_CODE", StringType, true),
  StructField("BRANCH_CODE", StringType, true),
  StructField("ACCOUNT_TYPE", StringType, true),
  StructField("ATTRIBUTE_PRODUCT_ID", StringType, true),
  StructField("PRODUCT_ATTRIBUTE_SUBID", StringType, true),
  StructField("ATTRIBUTE_UA_NAME", StringType, true),
  StructField("ATTRIBUTE_BILL_NAME", StringType, true),
  StructField("PROD_ATTR_CUSTOMER_REF", StringType, true),
  StructField("PROD_ATTR_PRODUCT_SEQ", StringType, true),
  StructField("PROD_ATTRIBUTE_SUBID", StringType, true),
  StructField("PROD_ATTR_PRODUCT_ID", StringType, true),
  StructField("ATTRIBUTE_VALUE", StringType, true),
  StructField("TARIFF_CUSTOMER_REF", StringType, true),
  StructField("TARIFF_PRODUCT_SEQ", StringType, true),
  StructField("TARIFF_ID", StringType, true),
  StructField("TARIFF_START_DAT", StringType, true),
  StructField("TARIFF_END_DAT", StringType, true),
  StructField("CUSTOMER_TYPE_ID", StringType, true),
  StructField("ROOT_CUSTOMER_REF", StringType, true),
  StructField("CHILD_CUSTOMER_COUNT", StringType, true),
  StructField("ACCOUNT_COUNT", StringType, true),
  StructField("COMPANY_NAME", StringType, true),
  StructField("PARENT_CUSTOMER_REF", StringType, true),
  StructField("CUST_BILL_PERIOD", StringType, true),
  StructField("CUST_NEXT_BILL_DTM", StringType, true),
  StructField("CUST_BILLS_PER_STATEMENT", StringType, true),
  StructField("CUSTOMER_TYPE_NAME", StringType, true),
  StructField("CUSTOMER_TYPE_DESC", StringType, true),
  StructField("CUSTOMER_NAME", StringType, true),
  StructField("REGION_NAME", StringType, true),
  StructField("BRANCH_NAME", StringType, true)))
  
  val postpaidEventReferenceSchema = new StructType(Array(
  StructField("rev_code", StringType, true),
  StructField("contract_template_id", StringType, true),
  StructField("ctr_desc", StringType, true),
  StructField("ctr_po_1", StringType, true),
  StructField("ctr_po_2", StringType, true),
  StructField("ctr_po_3", StringType, true),
  StructField("ctr_po_4", StringType, true),
  StructField("ctr_po_5", StringType, true),
  StructField("ctr_po_6", StringType, true),
  StructField("ctr_po_7", StringType, true),
  StructField("ctr_po_8", StringType, true),
  StructField("offer_id", StringType, true),
  StructField("sfc", StringType, true),
  StructField("duration", StringType, true),
  StructField("inst_month_1", StringType, true),
  StructField("inst_month_2", StringType, true),
  StructField("inst_month_3", StringType, true),
  StructField("inst_month_4", StringType, true),
  StructField("inst_month_5", StringType, true),
  StructField("inst_month_6", StringType, true),
  StructField("inst_month_7", StringType, true),
  StructField("inst_month_8", StringType, true),
  StructField("inst_month_9", StringType, true),
  StructField("inst_month_10", StringType, true),
  StructField("inst_month_11", StringType, true),
  StructField("inst_month_12", StringType, true),
  StructField("gl_account_b2c", StringType, true),
  StructField("gl_account_b2c_desc", StringType, true),
  StructField("gl_account_b2b", StringType, true),
  StructField("gl_account_b2b_desc", StringType, true),
  StructField("gl_account_b2b_credit", StringType, true),
  StructField("gl_account_b2b_credit_desc", StringType, true),
  StructField("voice_share", StringType, true),
  StructField("sms_share", StringType, true),
  StructField("data_share", StringType, true),
  StructField("vas_share", StringType, true),
  StructField("voice_b2c", StringType, true),
  StructField("sms_b2c", StringType, true),
  StructField("data_b2c", StringType, true),
  StructField("vas_b2c", StringType, true),
  StructField("voice_b2b", StringType, true),
  StructField("sms_b2b", StringType, true),
  StructField("data_b2b", StringType, true),
  StructField("vas_b2b", StringType, true)))

}