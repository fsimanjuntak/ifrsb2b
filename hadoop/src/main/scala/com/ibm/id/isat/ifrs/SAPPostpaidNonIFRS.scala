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

object SAPPostpaidNonIFRS {
  def main(args: Array[String]): Unit = {
    val (prcDt,jobId, config , repcsFlag , env) = try {
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
    val pathRefPostpaidEventReference = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE") + "/*.csv"
    val pathRefPostpaidEventReferenceIphone= Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE_IPHONE") + "/*.csv"
    val pathPostpaidReSummary = Common.getConfigDirectory(sc, config, "IFRS.OUTPUT.POSTPAID_RE_SUMMARY")
    val pathOutputSap = Common.getConfigDirectory(sc, config, "IFRS.OUTPUT.SAP")
    val pathRefPostpaidEventReferenceMerge = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.MERGE_POSTPAID_EVENT_REF") + "/*.csv"
    
    
//    val postpaidReSummaryDf = sqlContext.read.option("basePath", pathPostpaidReSummary)
//    .parquet(pathPostpaidReSummary+"/prc_dt="+prcDt)
//    postpaidReSummaryDf.registerTempTable("postpaid_re_summary")
    
    val prevMonth=Common.getMonthID(Common.getPreviousMonthDays(prcDt))
    val currMonth=Common.getMonthID(prcDt)
    if (repcsFlag=="REPCS"){
        val postpaidReSummaryDf = sqlContext.read.option("basePath", pathPostpaidReSummary)
        .parquet(pathPostpaidReSummary+"/PRC_DT="+currMonth+"*")
        postpaidReSummaryDf.registerTempTable("postpaid_re_summary_unfilter")
        
        val filterPostpaidReSummaryDf=sqlContext.sql("""
        select * from postpaid_re_summary_unfilter where PRC_DT>=%1$s04 and PRC_DT<%1$s25 and TRANSACTION_DATE>=%2$s01 and TRANSACTION_DATE<%1$s01
        """.format(currMonth,prevMonth))
        filterPostpaidReSummaryDf.registerTempTable("postpaid_re_summary")  
    } else {
        val postpaidReSummaryDf = sqlContext.read.option("basePath", pathPostpaidReSummary)
        .parquet(pathPostpaidReSummary+"/*/start_dt="+prevMonth+"*")
        postpaidReSummaryDf.registerTempTable("postpaid_re_summary_unfilter")
        
        val filterPostpaidReSummaryDf=sqlContext.sql("""
        select * from postpaid_re_summary_unfilter where PRC_DT<%1$s04
        """.format(currMonth))
        filterPostpaidReSummaryDf.registerTempTable("postpaid_re_summary")
    }
    
    val refPostpaidEventReferenceMergeDf = broadcast(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", "|")
        .schema(postpaidEventReferenceMergeSchema)
        .load(pathRefPostpaidEventReferenceMerge))
    refPostpaidEventReferenceMergeDf.registerTempTable("ref_postpaid_event_reference")
    refPostpaidEventReferenceMergeDf.persist()
    refPostpaidEventReferenceMergeDf.first()
    
    val refSidDistributionDf = sqlContext.sql("""
    select
        '' start_date,
        '' end_date,
        rev_code sid,
        ctr_desc description,
        1 share,
        gl_account_b2c b2c_gl,
        gl_account_b2b b2b_gl,
        gl_account_b2b_credit b2b_credit_gl,
        gl_account_b2c_desc b2c_desc,
        gl_account_b2b_desc b2b_desc,
        gl_account_b2b_credit_desc b2b_credit_desc,
        src_tp
    from ref_postpaid_event_reference
    """)
    refSidDistributionDf.registerTempTable("ref_sid_distribution")
    
    val sapPostpaidNonIfrsDf = sqlContext.sql("""
    select
        prc_dt,
        job_id,
        date_format(last_day(cast(unix_timestamp(cast(nvl(start_dt, job_id) as string), 'yyyyMMdd') as timestamp)), 'yyyyMMdd') start_dt,
        customer_segment,
        customer_type_name,
        sap_customer_number,
        clearing_gl_account,
        business_area,
        prod_family_name,
        profit_center,
        currency_code,
        revenue_code_id,
        revenue_code_name,
        po_group,
        allocated,
        invoice_num,
        sum(revenue_mny) revenue_mny,
        invoice_tax_money,
        account_type_name
    from postpaid_re_summary
    where allocated = 'N' and revenue_mny <> 0
    group by
        prc_dt,
        job_id,
        start_dt,
        customer_segment,
        customer_type_name,
        sap_customer_number,
        clearing_gl_account,
        business_area,
        prod_family_name,
        profit_center,
        currency_code,
        revenue_code_id,
        revenue_code_name,
        po_group,
        allocated,
        invoice_num,
        invoice_tax_money,
        account_type_name
    """)
    sapPostpaidNonIfrsDf.registerTempTable("sap_postpaid_non_ifrs")
    
    val sapNonVatBaseDf = sqlContext.sql("""
    select
        '1000' company_code,
        'YD' document_type,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'dd.MM.yyyy') document_date,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'dd.MM.yyyy') posting_date,
        currency_code currency_key,
        customer_segment reference_reference,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'yyyyMM') document_text,
        case when sum(revenue_mny * share) < 0 then '40' else '50' end posting_key,
        clearing_gl_account gl_account,
        case when sum(revenue_mny * share) < 0 then (- sum(revenue_mny * share)) else sum(revenue_mny * share) end amount_currency,
        business_area,
        profit_center,
        'ID00' business_place,
        sap_customer_number reference_key_1,
        '' reference_key_2,
        '' reference_key_3,
        case customer_segment when 'B2C MOBILE' then b2c_gl else b2b_gl end assignment_assignment,
        case customer_segment when 'B2C MOBILE' then b2c_desc else b2b_desc end long_text
    from sap_postpaid_non_ifrs
    left join ref_sid_distribution on lower(sap_postpaid_non_ifrs.revenue_code_name) = lower(ref_sid_distribution.sid)
          and (case when customer_segment = 'B2B IPHONE' then 'IPHONE' else 'POSTPAID' end) = ref_sid_distribution.src_tp
    where 
        nvl(b2b_credit_gl,'') = ''
    group by
        customer_segment,
        profit_center,
        currency_code,
        sap_customer_number,
        clearing_gl_account,
        business_area,
        b2c_gl,
        b2b_gl,
        b2c_desc,
        b2b_desc
    """)
    sapNonVatBaseDf.registerTempTable("sap_non_vat_base")
    
    val sapVatBaseDf = sqlContext.sql("""
    select
        '1000' company_code,
        'YD' document_type,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'dd.MM.yyyy') document_date,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'dd.MM.yyyy') posting_date,
        currency_code currency_key,
        concat_ws('_', account_type_name, customer_type_name) reference_reference,
        date_format(cast(unix_timestamp(cast(first(start_dt) as string), 'yyyyMMdd') as timestamp), 'yyyyMM') document_text,
        case when sum(revenue_mny) < 0 then '40' else '50' end posting_key,
        b2b_credit_gl gl_account,
        case when sum(revenue_mny) < 0 then (- sum(revenue_mny)) else sum(revenue_mny) end amount_currency,
        business_area,
        '1000000000' profit_center,
        'ID00' business_place,
        nvl(sap_customer_number, '70000001') reference_key_1,
        '' reference_key_2,
        '' reference_key_3,
        case customer_segment when 'B2C POSTPAID' then b2c_gl else b2b_gl end assignment_assignment,
        concat_ws('_', account_type_name, customer_type_name) long_text
    from sap_postpaid_non_ifrs
    left join ref_sid_distribution on lower(sap_postpaid_non_ifrs.revenue_code_name) = lower(ref_sid_distribution.sid)
          and (case when customer_segment = 'B2B IPHONE' then 'IPHONE' else 'POSTPAID' end) = ref_sid_distribution.src_tp
    where nvl(b2b_credit_gl,'') <> ''
    group by
        customer_segment,
        currency_code,
        sap_customer_number,
        business_area,
        b2c_gl,
        b2b_gl,
        concat_ws('_', account_type_name, customer_type_name),
        b2b_credit_gl
    """)
    sapVatBaseDf.registerTempTable("sap_vat_base")
    
    val distinctReferenceDf = sqlContext.sql("""
    select distinct reference_reference, reference_key_1, document_date, profit_center, business_area
    from sap_non_vat_base
    union all
    select distinct reference_reference, reference_key_1, document_date, profit_center, business_area
    from sap_vat_base
    """)
    .repartition(1)
    distinctReferenceDf.registerTempTable("distinct_reference")
    val refReferenceToDocIdDf = broadcast(sqlContext.sql("""
    select
        reference_reference, reference_key_1, document_date, profit_center, business_area,
        row_number() over (partition by '' order by reference_reference,profit_center) doc_id
    from distinct_reference
    """)
    .repartition(1))
    .persist()
    refReferenceToDocIdDf.registerTempTable("reference_to_doc_id")
    
    val sapNonVatLineDf = sqlContext.sql("""
    select 
        reference_to_doc_id.doc_id,
        company_code company_code,
        document_type document_type,
        sap_non_vat_base.document_date document_date,
        posting_date posting_date,
        currency_key currency_key,
        sap_non_vat_base.reference_reference reference_reference,
        document_text document_text,
        posting_key posting_key,
        gl_account gl_account,
        cast(amount_currency as decimal(18, 0)) amount_currency,
        sap_non_vat_base.business_area business_area,
        sap_non_vat_base.profit_center profit_center,
        business_place business_place,
        sap_non_vat_base.reference_key_1 reference_key_1,
        reference_key_2 reference_key_2,
        reference_key_3 reference_key_3,
        assignment_assignment assignment_assignment,
        long_text long_text
    from sap_non_vat_base
    left join reference_to_doc_id
        on nvl(sap_non_vat_base.reference_key_1, '') = nvl(reference_to_doc_id.reference_key_1, '')
        and nvl(sap_non_vat_base.reference_reference, '') = nvl(reference_to_doc_id.reference_reference, '')
        and sap_non_vat_base.document_date = reference_to_doc_id.document_date
        and sap_non_vat_base.profit_center = reference_to_doc_id.profit_center
        and sap_non_vat_base.business_area = reference_to_doc_id.business_area
    """)
    sapNonVatLineDf.registerTempTable("sap_non_line")
    
    val sapVatLineDf = sqlContext.sql("""
    select 
        reference_to_doc_id.doc_id,
        company_code company_code,
        document_type document_type,
        sap_vat_base.document_date document_date,
        posting_date posting_date,
        currency_key currency_key,
        sap_vat_base.reference_reference reference_reference,
        document_text document_text,
        posting_key posting_key,
        gl_account gl_account,
        cast(amount_currency as decimal(18,0)) amount_currency,
        sap_vat_base.business_area business_area,
        sap_vat_base.profit_center profit_center,
        business_place business_place,
        sap_vat_base.reference_key_1 reference_key_1,
        reference_key_2 reference_key_2,
        reference_key_3 reference_key_3,
        case when assignment_assignment='' then gl_account else assignment_assignment end assignment_assignment,
        long_text long_text
    from sap_vat_base
    left join reference_to_doc_id
        on nvl(sap_vat_base.reference_key_1, '') = nvl(reference_to_doc_id.reference_key_1, '')
        and nvl(sap_vat_base.reference_reference, '') = nvl(reference_to_doc_id.reference_reference, '')
        and sap_vat_base.document_date = reference_to_doc_id.document_date
        and sap_vat_base.profit_center = reference_to_doc_id.profit_center
    """)
    sapVatLineDf.registerTempTable("sap_vat_line")
    
    val sapNonVatHeaderDf = sqlContext.sql("""
    select 
        doc_id doc_id,
        '1000' company_code,
        'YD' document_type,
        document_date document_date,
        document_date posting_date,
        'IDR' currency_key,
        reference_reference reference_reference,
        date_format(cast(unix_timestamp(document_date, 'dd.MM.yyyy') as timestamp), 'yyyyMM') document_text,
        case when cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0)) < 0
        then '11'
        else '01'
        end posting_key,
        reference_key_1 gl_account,
        case when cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0)) < 0
        then -cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0))
        else cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0))
        end amount_currency,
        business_area,
        profit_center profit_center,
        'ID00' business_place,
        reference_key_1 reference_key_1,
        '' reference_key_2,
        '' reference_key_3,
        profit_center assignment_assignment,
        concat_ws(' ', reference_reference, 'Non IFRS Impacted') long_text
    from sap_non_line
    group by
        doc_id,
        document_date,
        reference_reference,
        reference_key_1,
        business_area,
        profit_center
    """)
    sapNonVatHeaderDf.registerTempTable("sap_non_vat_header")
    
    val sapVatHeaderDf = sqlContext.sql("""
    select 
        doc_id doc_id,
        '1000' company_code,
        'YD' document_type,
        document_date document_date,
        document_date posting_date,
        'IDR' currency_key,
        reference_reference reference_reference,
        date_format(cast(unix_timestamp(document_date, 'dd.MM.yyyy') as timestamp), 'yyyyMM') document_text,
        case when cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0)) < 0
        then '11'
        else '01'
        end posting_key,
        reference_key_1 gl_account,
        case when cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0)) < 0
        then -cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0))
        else cast(sum(case posting_key when '40' then (- amount_currency) else amount_currency end) as decimal(18,0))
        end amount_currency,
        business_area,
        profit_center profit_center,
        'ID00' business_place,
        reference_key_1 reference_key_1,
        '' reference_key_2,
        '' reference_key_3,
        'POSTPAID VAT & NON REVENUE' assignment_assignment,
        concat_ws(' ', reference_reference, 'Non IFRS Impacted') long_text
    from sap_vat_line
    group by
        doc_id,
        document_date,
        reference_reference,
        reference_key_1,
        business_area,
        profit_center
    """)
    sapVatHeaderDf.registerTempTable("sap_vat_header")
    
    //need to show to make balance between header and line
    sqlContext.sql("select sum(amount_currency) amount_currency from sap_non_line").show()
    sqlContext.sql("select amount_currency from sap_non_vat_header").show()
    sqlContext.sql("select sum(amount_currency) amount_currency from sap_vat_line").show()
    sqlContext.sql("select amount_currency from sap_vat_header").show()
    
    val sapNonVatCsvDf = sqlContext.sql("""
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
    from sap_non_vat_header
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
    from sap_non_line
    ) sap_non_vat_all
    order by doc_id, reference_reference, posting_key 
    """)
    sapNonVatCsvDf.registerTempTable("sap_non_vat_csv")
    
    val sapVatCsvDf = sqlContext.sql("""
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
    from sap_vat_header
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
    from sap_vat_line
    ) sap_vat_all
    order by doc_id, reference_reference, posting_key
    """)
    .persist()
    sapVatCsvDf.registerTempTable("sap_vat_csv")
    
    sapNonVatCsvDf.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "|")
      .save(pathOutputSap+"/PRC_DT=" + prcDt + "/hdp_postpaid_YD")
      
    sapVatCsvDf.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "|")
      .save(pathOutputSap+"/PRC_DT=" + prcDt + "/hdp_postpaid_YD_v")
    
    
    rootLogger.info("====job ifrs SAP postpaid Non IFRS finished====")
  }
  
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
      StructField("vas_b2b", StringType, true),
      StructField("src_tp", StringType, true)))
}