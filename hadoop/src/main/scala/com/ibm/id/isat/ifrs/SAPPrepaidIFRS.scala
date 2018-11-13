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

object SAPPrepaidIFRS {
  def main(args: Array[String]): Unit = {
    val (prcDt, jobId, config, repcsFlag, env) = try {
      (args(0), args(1), args(2), args(3),args(4))
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
//    val prcDt = "20171125"
//    val pathRefRevcodeSid = "C:/Users/KharisSimon/Box Sync/Indosat/IFRS/Sample/spark/ref_revcode_sid"
//    val pathRefCustomerType = "C:/Users/KharisSimon/Box Sync/Indosat/IFRS/Sample/spark/ref_customer_type"
//    val pathRefDistribution = "C:/Users/KharisSimon/Box Sync/Indosat/IFRS/Sample/spark/ref_distribution"
    
    val pathRefRevcodeSid = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.REF_REVCODE_SID") + "/*.csv"
    val pathRefCustomerType = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.REF_CUSTOMER_TYPE") + "/*.csv"
    val pathRefDistribution = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.REF_DISTRIBUTION") + "/*.csv"
    val pathRefPrepaidEventReference = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.PREPAID_EVENT_REFERENCE") + "/*.csv"
    val pathRefSalmoEventReference = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.SALMO_EVENT_REFERENCE") + "/*.csv"
    val pathPrepaidReSummary = Common.getConfigDirectory(sc, config, "IFRS.OUTPUT.PREPAID_RE_SUMMARY")
    val pathOutputSap = Common.getConfigDirectory(sc, config, "IFRS.OUTPUT.SAP")
    val pathRefPrepaidEventReferenceMerge = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.MERGE_PREPAID_EVENT_REF") + "/*.csv"
    val pathRefGlAccount = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.REF_PREPAID_UNEARNED_GL_ACCOUNT") + "/*.csv"
    
    println("INFO: pathPrepaidReSummary=" + pathPrepaidReSummary)
    
//    val prepaidReSummaryDf = sqlContext.read.option("basePath", pathPrepaidReSummary)
//    .parquet(pathPrepaidReSummary+"/prc_dt="+prcDt)
//    prepaidReSummaryDf.registerTempTable("prepaid_re_summary")
    
    val prevMonth=Common.getMonthID(Common.getPreviousMonthDays(prcDt))
    val currMonth=Common.getMonthID(prcDt)
    if (repcsFlag=="REPCS"){
        val prepaidReSummaryDf = sqlContext.read.option("basePath", pathPrepaidReSummary)
        .parquet(pathPrepaidReSummary+"/PRC_DT="+currMonth+"*")
        prepaidReSummaryDf.registerTempTable("prepaid_re_summary_unfilter")
        
        val filterPostpaidReSummaryDf=sqlContext.sql("""
        select * from prepaid_re_summary_unfilter where PRC_DT>=%1$s04 and PRC_DT<%1$s25 and TRANSACTION_DATE>=%2$s01 and TRANSACTION_DATE<%1$s01
        """.format(currMonth,prevMonth))
        filterPostpaidReSummaryDf.registerTempTable("prepaid_re_summary")  
    } else {
        val prepaidReSummaryDf = sqlContext.read.option("basePath", pathPrepaidReSummary)
        .parquet(pathPrepaidReSummary+"/PRC_DT=*/TRANSACTION_DATE="+prevMonth+"*")
        prepaidReSummaryDf.registerTempTable("prepaid_re_summary_unfilter")
        
        val filterPostpaidReSummaryDf=sqlContext.sql("""
        select * from prepaid_re_summary_unfilter where PRC_DT<%1$s04
        """.format(currMonth))
        filterPostpaidReSummaryDf.registerTempTable("prepaid_re_summary")
    }
    
    val refPrepaidGlAccountDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(prepaidRefGlAccountSchema)
    .load(pathRefGlAccount).cache())
    refPrepaidGlAccountDf.registerTempTable("ref_prepaid_gl_account")
    
    val refPrepaidEventReferenceMergeDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(prepaidEventReferenceMergeSchema)
    .load(pathRefPrepaidEventReferenceMerge).cache())
    refPrepaidEventReferenceMergeDf.registerTempTable("ref_prepaid_event_reference")
    
    val refSidDistributionDf = sqlContext.sql("""
    select
        '' start_date,
        '' end_date,
        rev_code sid,
        ctr_desc description,
        voice_share / 100 share,
        voice_b2c b2c_gl,
        voice_b2b b2b_gl, src_tp
    from ref_prepaid_event_reference
    where nvl(voice_share, '') <> ''
    union all
    select
        '' start_date,
        '' end_date,
        rev_code sid,
        ctr_desc description,
        sms_share / 100 share,
        sms_b2c b2c_gl,
        sms_b2b b2b_gl, src_tp
    from ref_prepaid_event_reference
    where nvl(sms_share, '') <> ''
    union all
    select
        '' start_date,
        '' end_date,
        rev_code sid,
        ctr_desc description,
        data_share / 100 share,
        data_b2c b2c_gl,
        data_b2b b2b_gl, src_tp
    from ref_prepaid_event_reference
    where nvl(data_share, '') <> ''
    union all
    select
        '' start_date,
        '' end_date,
        rev_code sid,
        ctr_desc description,
        vas_share / 100 share,
        vas_b2c b2c_gl,
        vas_b2b b2b_gl, src_tp
    from ref_prepaid_event_reference
    where nvl(vas_share, '') <> ''
    """)
    refSidDistributionDf.filter("sid = '36300001014005'").show()
    refSidDistributionDf.registerTempTable("ref_sid_distribution")
    
    val sapPrepaidIfrsDf = sqlContext.sql("""
    select
        prc_dt,
        date_format(last_day(cast(unix_timestamp(cast(start_dt as string), 'yyyyMMdd') as timestamp)), 'yyyyMMdd') start_dt,
        prepaid_re_summary.customer_segment,
        profit_center,
        rev_code,
        allocated,
        sum(line_amt) line_amount,
        first(inst_month_1) inst_month_1,
        first(inst_month_2) inst_month_2,
        first(inst_month_3) inst_month_3,
        first(prepaid_re_summary.src_tp) src_tp,
        first(description) description,
        first(unearned_gl_account) unearned_gl_account,
        first(clearing_gl_account) clearing_gl_account,
        first(business_area) business_area
    from prepaid_re_summary
         left join ref_prepaid_gl_account on ref_prepaid_gl_account.source_type = prepaid_re_summary.src_tp
          and ref_prepaid_gl_account.customer_segment = prepaid_re_summary.customer_segment
    where allocated = 'Y'
    group by
        prc_dt,
        start_dt,
        prepaid_re_summary.customer_segment,
        profit_center,
        rev_code,
        allocated
    """)
    sapPrepaidIfrsDf.show()
    sapPrepaidIfrsDf.registerTempTable("sap_prepaid_ifrs")
    
    val sapBaseDf = sqlContext.sql("""
    select
        '1000' company_code,
        'YA' document_type,
        date_format(to_date(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp)), 'dd.MM.yyyy') document_date,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'dd.MM.yyyy') posting_date,
        'IDR' currency_key,
        customer_segment reference_reference,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'yyyyMM') document_text,
        case when sum(line_amount) < 0 then '40' else '50' end posting_key,
        first(clearing_gl_account) gl_account,
        case when sum(line_amount * case when share is not null then share else 1 end) < 0 then (- sum(line_amount * case when share is not null then share else 1 end )) else sum(line_amount * case when share is not null then share else 1 end ) end amount_currency,
        first(business_area) business_area,
        profit_center profit_center,
        'ID00' business_place,
        profit_center reference_key_1,
        case when nvl(inst_month_3, '') <> '' then '3 months installment'
            when nvl(inst_month_2, '') <> '' then '2 months installment'
            else ''
        end reference_key_2,
        '' reference_key_3,
        case customer_segment when 'B2C PREPAID' then b2c_gl
            when 'B2C SALMO' then b2c_gl
            when 'B2B PREPAID' then b2b_gl end assignment_assignment,
        case customer_segment when 'B2C PREPAID' then b2c_gl
            when 'B2C SALMO' then b2c_gl
            when 'B2B PREPAID' then b2b_gl end long_text,
        sap_prepaid_ifrs.src_tp,
        first(sap_prepaid_ifrs.description) description,
        first(unearned_gl_account) unearned_gl_account
    from sap_prepaid_ifrs
    left join ref_sid_distribution
    on lower(sap_prepaid_ifrs.rev_code) = lower(ref_sid_distribution.sid)
    and (case when sap_prepaid_ifrs.SRC_TP <> 'SALMO' then 'NONSALMO' else 'SALMO' end) = ref_sid_distribution.src_tp
    group by
        customer_segment,
        profit_center,
        b2c_gl,
        b2b_gl,
        inst_month_2,
        inst_month_3,
        sap_prepaid_ifrs.src_tp
    """)
    sapBaseDf.registerTempTable("sap_base")
    
    val distinctProfitCenterDf = sqlContext.sql("""
        select distinct profit_center, gl_account, reference_key_2, src_tp
        from sap_base
    """)
    .repartition(1)
    distinctProfitCenterDf.registerTempTable("distinct_profit_center")
    val profitCenterToDocIdDf = sqlContext.sql("""
        select
            profit_center,
            gl_account,
            reference_key_2,
            src_tp,
            row_number() over (partition by '' order by src_tp,gl_account,reference_key_2,profit_center) doc_id
        from distinct_profit_center
    """)
    .repartition(1)
    .persist() // Retain monotonically result
    profitCenterToDocIdDf.registerTempTable("profit_center_to_doc_id")
    
    
    val sapLineDf = sqlContext.sql("""
        select 
            profit_center_to_doc_id.doc_id,
            company_code company_code,
            document_type document_type,
            document_date document_date,
            posting_date posting_date,
            currency_key currency_key,
            reference_reference reference_reference,
            document_text document_text,
            posting_key posting_key,
            sap_base.gl_account gl_account,
            cast(amount_currency as decimal(18, 0)) amount_currency,
            business_area business_area,
            sap_base.profit_center profit_center,
            business_place business_place,
            reference_key_1 reference_key_1,
            sap_base.reference_key_2 reference_key_2,
            reference_key_3 reference_key_3,
            nvl(assignment_assignment,'4000000000') assignment_assignment,
            nvl(long_text,'4000000000') long_text,
            sap_base.src_tp,
            description,
            unearned_gl_account
        from sap_base
        left join profit_center_to_doc_id
          on sap_base.profit_center = profit_center_to_doc_id.profit_center
          and sap_base.gl_account = profit_center_to_doc_id.gl_account
          and sap_base.reference_key_2 = profit_center_to_doc_id.reference_key_2
          and sap_base.src_tp = profit_center_to_doc_id.src_tp
        where amount_currency > 0
    """)
    sapLineDf.persist()
    sapLineDf.registerTempTable("sap_line")
    
    val sapHeaderDf = sqlContext.sql("""
    select 
        doc_id doc_id,
        '1000' company_code,
        'YA' document_type,
        first(document_date) document_date,
        first(document_date) posting_date,
        'IDR' currency_key,
        reference_reference reference_reference,
        document_text document_text,
        case when cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0)) < 0
            then '50'
            else '40'
        end posting_key,
        first(unearned_gl_account) gl_account,
        case when cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0)) < 0
            then -cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0))
            else cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0))
        end amount_currency,
        first(business_area) business_area,
        profit_center profit_center,
        'ID00' business_place,
        profit_center reference_key_1,
        reference_key_2 reference_key_2,
        '' reference_key_3,
        profit_center assignment_assignment,
        --profit_center long_text
        concat_ws(' ', reference_reference, 'IFRS Impacted') long_text
    from sap_line
    group by
        document_text,
        reference_reference,
        doc_id,
        gl_account,
        profit_center,
        reference_key_2,
        src_tp
    """)
    sapHeaderDf.persist()
    sapHeaderDf.registerTempTable("sap_header")
    
    //need to show to make balance between header and line
    sqlContext.sql("select doc_id, sum(amount_currency) amount_currency from sap_line group by doc_id order by amount_currency desc").show()
    sqlContext.sql("select doc_id, amount_currency from sap_header order by amount_currency desc").show()
    
    val sapDf = sqlContext.sql("""
    select * from (
    select 
        doc_id,
        company_code,
        document_type,
        document_date,
        posting_date,
        currency_key,
        reference_reference,
        document_text,
        posting_key,
        gl_account,
        amount_currency amount_currency,
        business_area,
        profit_center,
        business_place,
        reference_key_1,
        reference_key_2,
        reference_key_3,
        assignment_assignment,
        long_text
    from sap_header
    union all
    select 
        doc_id,
        company_code,
        document_type,
        document_date,
        posting_date,
        currency_key,
        reference_reference,
        document_text,
        posting_key,
        gl_account,
        amount_currency,
        business_area,
        profit_center,
        business_place,
        reference_key_1,
        reference_key_2,
        reference_key_3,
        assignment_assignment,
        long_text
    from sap_line
    ) sap_all
    order by doc_id, posting_key, profit_center
    """)
    // Retain order
    sapDf.persist()
    sapDf.registerTempTable("sap")
    
    sapDf.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "|")
      .save(pathOutputSap + "/PRC_DT=" + prcDt + "/hdp_prepaid_YA")
    
    rootLogger.info("====job ifrs SAP prepaid IRFS finished====")
  }
  
 val prepaidEventReferenceSchema = new StructType(Array(
    StructField("rev_code", StringType, true),
    StructField("contract_template_id", StringType, true),
    StructField("level_1", StringType, true),
    StructField("level_2", StringType, true),
    StructField("level_3", StringType, true),
    StructField("level_4", StringType, true),
    StructField("level_5", StringType, true),
    StructField("level_6", StringType, true),
    StructField("level_7", StringType, true),
    StructField("level_8", StringType, true),
    StructField("level_9", StringType, true),
    StructField("level_10", StringType, true),
    StructField("level_11", StringType, true),
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
val prepaidReSummarySchema = new StructType(Array(
    StructField("PRC_DT", StringType, true),
    StructField("JOB_ID", StringType, true),
    StructField("SUBSCRIBER_TYPE", StringType, true),
    StructField("CUSTOMER_SEGMENT", StringType, true),
    StructField("SALMO_TRX_ID", StringType, true),
    StructField("REV_CODE", StringType, true),
    StructField("START_DT", StringType, true),
    StructField("END_DT", StringType, true),
    StructField("LINE_AMT_WITH_TAX", StringType, true),
    StructField("LINE_AMT", StringType, true),
    StructField("QTY", StringType, true),
    StructField("UOM", StringType, true),
    StructField("PROFIT_CENTER", StringType, true),
    StructField("CONTRACT_ID", StringType, true),
    StructField("CONTRACT_NAME", StringType, true),
    StructField("PO_ID", StringType, true),
    StructField("OFFER_ID", StringType, true),
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
    StructField("sub_line_type_code", StringType, true),
    StructField("PO_NAME", StringType, true),
    StructField("PO_GROUP", StringType, true),
    StructField("SATISFACTION_MEASURE_MODEL", StringType, true),
    StructField("PERF_SATISFACTION_PLAN", StringType, true),
    StructField("ALLOCATED", StringType, true)))
val prepaidEventReferenceMergeSchema = new StructType(Array(
    StructField("rev_code", StringType, true),
    StructField("contract_template_id", StringType, true),
    StructField("level_1", StringType, true),
    StructField("level_2", StringType, true),
    StructField("level_3", StringType, true),
    StructField("level_4", StringType, true),
    StructField("level_5", StringType, true),
    StructField("level_6", StringType, true),
    StructField("level_7", StringType, true),
    StructField("level_8", StringType, true),
    StructField("level_9", StringType, true),
    StructField("level_10", StringType, true),
    StructField("level_11", StringType, true),
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
    StructField("sp_data", StringType, true),
    StructField("src_tp", StringType, true)))
  val prepaidRefGlAccountSchema = new StructType(Array(
    StructField("source_type", StringType, true),
    StructField("description", StringType, true),
    StructField("unearned_gl_account", StringType, true),
    StructField("clearing_gl_account", StringType, true),
    StructField("business_area", StringType, true),
    StructField("customer_segment", StringType, true)))
 
}