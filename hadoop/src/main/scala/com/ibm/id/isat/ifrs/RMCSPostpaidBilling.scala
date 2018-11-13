package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.ibm.id.isat.utils.Common
import com.ibm.id.isat.utils.ReferenceDF
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType

object RMCSPostpaidBilling {
  def main(args: Array[String]): Unit = {
    val (config, prcDt, env) = try {
      (args(0), args(1), args(2))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <config path> <process date> <LOCAL|PRODUCTION>")
      return
    };
    
    // Initialize Spark Context
    val sc = env match {
      case "PRODUCTION" => new SparkContext(new SparkConf())
      case "LOCAL" => new SparkContext("local[*]", "local spark", new SparkConf())
      case _ => return
    };
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false"); //Disable metadata parquet
    
    // Initialize Spark SQL
    val sqlContext = new HiveContext(sc)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // Initialize Logging
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    
    // Paths
    val pathPostpaidReSummary = Common.getConfigDirectory(sc, config, "IFRS.OUTPUT.POSTPAID_RE_SUMMARY")
    val pathRmcsPostpaidBilling = Common.getConfigDirectory(sc, config, "IFRS.OUTPUT.RMCS_POSTPAID_BILLING")
    
    val pathRefPostpaidEventReference = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE") + "/*.*"
    val pathRefPostpaidEventReferenceIphone= Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE_IPHONE") + "/*.*"
    val pathRefPostpaidEventReferenceMerge = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.MERGE_POSTPAID_EVENT_REF") + "/*.csv"
    
    val refPostpaidEventReferenceMergeDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(postpaidEventReferenceMergeSchema)
    .load(pathRefPostpaidEventReferenceMerge))
    refPostpaidEventReferenceMergeDf.registerTempTable("ref_postpaid_event_reference")
    refPostpaidEventReferenceMergeDf.persist()
    refPostpaidEventReferenceMergeDf.first()
    
    val postpaidReSummaryDf = sqlContext.read
    .option("basePath", pathPostpaidReSummary)
    .parquet(pathPostpaidReSummary+"/prc_dt="+prcDt)
    postpaidReSummaryDf.registerTempTable("postpaid_re_summary")
    
    val billingBaseDf = sqlContext.sql("""
    select
        start_dt,
        cast(invoice_num as string) invoice_num,
        customer_segment,
        prc_dt,
        po_id,
        po_group,
        profit_center,
        currency_code,
        contract_id,
        revenue_code_name,
        sum(case when qty < 999999 then qty else 999999 end) quantity,
        sum(revenue_mny) bill_amount
    from postpaid_re_summary
    left join ref_postpaid_event_reference 
      on (case when customer_segment = 'B2B IPHONE' then 'IPHONE' else 'POSTPAID' end) = ref_postpaid_event_reference.src_tp
        and lower(postpaid_re_summary.revenue_code_name) = lower(ref_postpaid_event_reference.rev_code)
    where 
      nvl(ref_postpaid_event_reference.gl_account_b2b_credit,'') = ''
    group by 
        start_dt,
        invoice_num,
        customer_segment,
        prc_dt,
        po_id,
        po_group,
        contract_id,
        revenue_code_name,
        profit_center,
        currency_code
    order by invoice_num
    """)
    billingBaseDf.registerTempTable("billing_base")
    
    val postpaidSummaryContractPartitionedDf = billingBaseDf
        .repartition(col("invoice_num"))
        .persist()
    postpaidSummaryContractPartitionedDf.registerTempTable("postpaid_summary_invoice_partitioned")
    
    val postpaidSummaryRowNumberedDf = sqlContext.sql("""
        select *, row_number() over (partition by invoice_num) row_number
        from postpaid_summary_invoice_partitioned
    """)
    postpaidSummaryRowNumberedDf.persist()
    postpaidSummaryRowNumberedDf.registerTempTable("postpaid_summary_row_numbered")
    
    val rmcsBillingPostpaidDf = sqlContext.sql("""
    select
    	date_format(cast(unix_timestamp(cast(start_dt as string), 'yyyyMMdd') as timestamp), 'dd-MMM-yyyy') bill_date,
    	date_format(cast(unix_timestamp(cast(start_dt as string), 'yyyyMMdd') as timestamp), 'dd-MMM-yyyy') bill_accounting_date,
    	invoice_num bill_id,
    	invoice_num bill_number,
    	concat_ws('', invoice_num, lpad(row_number, 3, '0')) bill_line_id,
    	row_number bill_line_number,
    	cast(quantity as decimal(18, 0)) billed_quantity,
    	cast(bill_amount as decimal(18,0)) bill_amount_in_entered_currency,
    	'' bill_line_id_int_1,
    	'' bill_line_id_int_2,
    	'' bill_line_id_int_3,
    	'' bill_line_id_int_4,
    	'' bill_line_id_int_5,
    	case po_group when 'PPU'
            --then hex(hash(concat_ws('_', customer_segment, po_group, revenue_code_name, prc_dt, start_dt, po_id, profit_center, currency_code)))
            --else hex(hash(concat_ws('_', customer_segment, contract_id, revenue_code_name, prc_dt, start_dt, po_id, profit_center,currency_code)))
            then concat_ws('',
                prc_dt,
                hex(hash(concat_ws('_', customer_segment, po_group, revenue_code_name, prc_dt, start_dt, po_id, profit_center, currency_code)))
            )
            else concat_ws('',
                prc_dt,
                hex(hash(concat_ws('_', customer_segment, contract_id, prc_dt, start_dt, po_id, profit_center, currency_code)))
            )
        end doc_line_id_char_1,
    	hex(hash(revenue_code_name)) doc_line_id_char_2,
    	start_dt doc_line_id_char_3,
    	hex(hash(po_id)) doc_line_id_char_4,
    	hex(hash(profit_center)) doc_line_id_char_5,
    	'' document_type_id,
    	'HADOOP' billing_application,
    	'HADOOP' source_system,
    	cast(bill_amount as decimal(18,0)) bill_amount_in_ledger_currency,
    	'HADOOP' document_type_code
    from postpaid_summary_row_numbered
    """)
    rmcsBillingPostpaidDf.registerTempTable("rmcs_billing_postpaid")
    
    val rmcsPostpaidOrderedDf = sqlContext.sql("""
    select order_id, content
        from (
            select concat_ws('_', bill_id, bill_line_id) order_id, concat('"', concat_ws('"|"', *), '"\r') content from rmcs_billing_postpaid
        ) rmcs_ordered
        order by order_id
    """).repartition(1).persist()
    rmcsPostpaidOrderedDf.registerTempTable("rmcs_billing_postpaid_ordered")
    
    val rmcsPostpaidCsvDf = sqlContext.sql("""
        select content from rmcs_billing_postpaid_ordered
    """)
    
    rmcsPostpaidCsvDf.coalesce(1)
        .write
        .mode("overwrite")
        .text(pathRmcsPostpaidBilling+"/PRC_DT=" + prcDt)
        
    rootLogger.info("====job ifrs RMCS postpaid finished====")
    
  }
  val postpaidEventReferenceMergeSchema = new StructType(Array(
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
      StructField("gl_account_b2b_desc_credit", StringType, true),
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
      StructField("vas_b2b", StringType, true),
      StructField("src_tp", StringType, true)))
  
}