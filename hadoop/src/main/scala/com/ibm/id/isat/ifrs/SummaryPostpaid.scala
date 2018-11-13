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

object SummaryPostpaid {
  def main(args: Array[String]): Unit = {
    
    val (prcDt, jobId, configDir, inputDir, env) = try {
      (args(0), args(1), args(2), args(3), args(4))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process date> <job identifier> <config directory> <input directory> <LOCAL|PRODUCTION>")
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
    
    val inputBillDetailIfrs = inputDir + "/bill_detail_ifrs"
    val inputBillSummary = inputDir + "/bill_summary"
    val inputBillAccountProduct = inputDir + "/bill_account_product"
    val pathRefPostpaidEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE") + "/*.csv"
    val pathRefPostpaidPoTemplate = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_PO_TEMPLATE") + "/*.csv"
    val pathRefPostpaidPoTemplateIphone = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_PO_TEMPLATE_IPHONE") + "/*.csv"
    val pathRefB2mBranchRegion = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.BRANCH_REGION") + "/*.txt"
    val pathRefProfitCenter = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PROFIT_CENTER") + "/*.csv"
    val pathPostpaidDetailUnidentified = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.IFRS_POSTPAID_UNIDENTIFIED")
    val pathPostpaidTrxSummary = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.POSTPAID_TRX_SUMMARY")
    val pathPostpaidReSummary = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.POSTPAID_RE_SUMMARY")
    val pathRefPostpaidEventReferenceIphone= Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE_IPHONE") + "/*.csv"
    val pathRefPostpaidEventReferenceMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_POSTPAID_EVENT_REF") + "/*.csv"
    val pathRefPostpaidPoTemplateMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_POSTPAID_PO_TEMPL") + "/*.csv"
    val pathRefCustomer=Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.REF_POSTPAID_CUSTOMER_NUMBER") + "/*.csv"
    
    val billAccountProductDf = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "~")
      .schema(billAccountProductSchema)
      .load(inputBillAccountProduct)
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
        case when nvl(first(account_type),'')='' then '3' else first(account_type) end account_type,
        first(npwp) npwp,
        first(billing_address) billing_address,
        first(postal_code) postal_code
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
        case when nvl(first(account_type),'')='' then '3' else first(account_type) end account_type,
        first(npwp) npwp,
        first(billing_address) billing_address,
        first(postal_code) postal_code
    from bill_account_product
    where nvl(customer_type_id, '') <> ''
    group by
        account_num
    """))
    refBillAccountProductDf.registerTempTable("ref_bill_account_product")
    
    val billDetailIfrsDf = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("header", "true")
        .schema(billDetailIfrsSchema)
        .load(inputBillDetailIfrs)
    billDetailIfrsDf.registerTempTable("bill_detail_ifrs")
    
    val billSummaryDf = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("header", "true")
        .schema(billSummarySchema)
        .load(inputBillSummary)
    billSummaryDf.registerTempTable("bill_summary")
    
    val refCustomerDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(customerSchema)
    .load(pathRefCustomer))
    refCustomerDf.registerTempTable("ref_customer_number")    
    refCustomerDf.persist()
    refCustomerDf.first()
    
    val refPostpaidEventReferenceMergeDf = broadcast(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", "|")
        .schema(postpaidEventReferenceMergeSchema)
        .load(pathRefPostpaidEventReferenceMerge))
    refPostpaidEventReferenceMergeDf.registerTempTable("ref_postpaid_event_reference")
    refPostpaidEventReferenceMergeDf.persist()
    refPostpaidEventReferenceMergeDf.first()
    
    val refPostpaidPoTemplateDf = broadcast(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .schema(postpaidPoTemplateMergeSchema)
        .load(pathRefPostpaidPoTemplateMerge))
    refPostpaidPoTemplateDf.cache()
    refPostpaidPoTemplateDf.count()
    refPostpaidPoTemplateDf.registerTempTable("ref_postpaid_po_template")
    
    val refPostpaidEventReferenceTransposeDf = sqlContext.sql("""
    select rev_code, contract_template_id, ctr_desc, ctr_po_1 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_1, '') <> ''
    union all
    select rev_code, contract_template_id, ctr_desc, ctr_po_2 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_2, '') <> ''
    union all
    select rev_code, contract_template_id, ctr_desc, ctr_po_3 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_3, '') <> ''
    union all
    select rev_code, contract_template_id, ctr_desc, ctr_po_4 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_4, '') <> ''
    union all
    select rev_code, contract_template_id, ctr_desc, ctr_po_5 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_5, '') <> ''
    union all
    select rev_code, contract_template_id, ctr_desc, ctr_po_6 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_6, '') <> ''
    union all
    select rev_code, contract_template_id, ctr_desc, ctr_po_7 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_7, '') <> ''
    union all
    select rev_code, contract_template_id, ctr_desc, ctr_po_8 ctr_po, offer_id, inst_month_1, inst_month_2, inst_month_3, inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12,
      voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share, src_tp
    from ref_postpaid_event_reference
    where nvl(ctr_po_8, '') <> ''
    """)
    refPostpaidEventReferenceTransposeDf.registerTempTable("ref_postpaid_event_reference_transpose")
    
    val postpaidEventReferencePoTemplateDf = sqlContext.sql("""
select
    ref_postpaid_event_reference_transpose.rev_code, 
    case when group='PPU' and nvl(contract_template_id, '') <> 'DROP'  then 'PPU' else contract_template_id end contract_template_id,
    ctr_desc, 
    ctr_po, 
    offer_id,
    inst_month_1,  
    inst_month_2,  
    inst_month_3,
    inst_month_4,
    inst_month_5,
    inst_month_6,
    inst_month_7,  
    inst_month_8,  
    inst_month_9,
    inst_month_10,
    inst_month_11,
    inst_month_12,
    po_template_id,
    group,
    po_template_name,
    allocated,
    satisfaction_measurement_model,
    performance_satisfaction_plan,
    sub_line_type_code,
    status,
    nvl(ref_prepaid_event_reference_transpose_first_po.first_ctr_po, '') <> '' first_po_flag,
    voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share
    ,ref_postpaid_event_reference_transpose.src_tp src_tp
from ref_postpaid_event_reference_transpose
left join ref_postpaid_po_template on ref_postpaid_event_reference_transpose.ctr_po = ref_postpaid_po_template.po_template_id
      and ref_postpaid_event_reference_transpose.src_tp=ref_postpaid_po_template.src_tp
left join (
    select rev_code,src_tp, min(ctr_po) first_ctr_po
    from ref_postpaid_event_reference_transpose
    group by rev_code,src_tp
) ref_prepaid_event_reference_transpose_first_po on lower(ref_postpaid_event_reference_transpose.rev_code) = lower(ref_prepaid_event_reference_transpose_first_po.rev_code)
    and ref_postpaid_event_reference_transpose.ctr_po = ref_prepaid_event_reference_transpose_first_po.first_ctr_po
    and ref_postpaid_event_reference_transpose.src_tp = ref_prepaid_event_reference_transpose_first_po.src_tp
    """)
    postpaidEventReferencePoTemplateDf.registerTempTable("ref_postpaid_event_reference_po_template")
    
    val refB2mBranchRegion = broadcast(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("header", "true")
        .load(pathRefB2mBranchRegion))
    refB2mBranchRegion.registerTempTable("ref_b2m_branch_region")
    refB2mBranchRegion.persist()
    
    val refProfitCenterDf = broadcast(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", "|")
        .schema(profitCenterSchema)
        .load(pathRefProfitCenter))
    refProfitCenterDf.registerTempTable("ref_profit_center")
    refProfitCenterDf.persist()
    refProfitCenterDf.first()
    
    val refCityToProfitCenterDf = broadcast(sqlContext.sql("""
        select distinct city, pc_id
        from ref_profit_center
    """))
    refCityToProfitCenterDf.registerTempTable("ref_city_to_profit_center")
    refCityToProfitCenterDf.persist()
    refCityToProfitCenterDf.first()
    
    val postpaidTransformDf = sqlContext.sql("""
select
    '""" + prcDt + """' prc_dt,
    '""" + jobId + """' job_id,
    'POSTPAID' subscriber_type,
    ref_bill_account_product.customer_type_id,
    ref_bill_account_product.customer_type_name,
    product_family_id prod_family_id,
    case when product_family_id in (1, 3, 4, 7, 25, 26, 57) then 'MOBILE'
        when product_family_id in (53, 54, 55, 56, 58) then 'MIDI'
        when product_family_id in (51, 52) then 'IPHONE'
        else product_family_id
    end prod_family_name,
    customer_name,
    ref_bill_account_product.account_name,
    ref_bill_account_product.account_type,
    npwp,
    billing_address,
    postal_code,
    bill_summary.vat_exchange_rate,
    bill_detail_ifrs.customer_ref,
    bill_detail_ifrs.account_num account_number,
    bill_detail_ifrs.bill_seq,
    service_id,
    service_class,
    sms_type,
    carrier,
    source_class_id,
    product_charge_type,
    revenue_code_id,
    trim(revenue_code_name) revenue_code_name,
    case when nvl(ref_postpaid_event_reference_po_template.group, '') <> 'PPU' and not first_po_flag
        then 0
        else revenue_mny
    end revenue_mny,
    product_seq_override product_seq,
    currency_code,
    event_count,
    volume,
    duration,
    ref_customer_number.account_type_name,
    ref_customer_number.sap_customer_number,
    ref_customer_number.customer_segment,
    ref_customer_number.clearing_gl_account,
    ref_customer_number.business_area,
    case ref_postpaid_event_reference_po_template.group 
        when 'PPU' then
            case
                when volume > 0 then case when volume > 99999999999 then 99999999999 else volume end
                when duration > 0 then duration
                when event_count > 0 then event_count
                else 1
            end
        else 1
    end qty,
    nvl(ref_b2m_branch_region.branch_name, 'Undefined') city,
    date_format(cast(unix_timestamp(bill_detail_ifrs.actual_bill_dtm, 'MM/dd/yyyy') as timestamp), 'yyyyMMdd') start_dt,
    date_format(cast(unix_timestamp(bill_detail_ifrs.actual_bill_dtm, 'MM/dd/yyyy') as timestamp), 'yyyyMMdd') end_dt,
    invoice_num,
    case when nvl(ref_postpaid_event_reference_po_template.group, '') <> 'PPU' and not first_po_flag
        then 0
        else bill_summary.invoice_net_mny
    end invoice_net_money,
    case when ref_postpaid_event_reference_po_template.group <> 'PPU' and not first_po_flag
        then 0
    else bill_summary.invoice_tax_mny
    end invoice_tax_money,
    'zzy' uom,
    nvl(ref_city_to_profit_center.pc_id, '1000000000') profit_center,
    contract_template_id contract_id_original,
    hex(hash(concat_ws('',contract_template_id,'""" + prcDt + """'))) contract_id,
    ctr_desc contract_name,
    ref_postpaid_event_reference_po_template.po_template_id po_id,
    offer_id, 
    inst_month_1,  
    inst_month_2,  
    inst_month_3,
    inst_month_4,
    inst_month_5,
    inst_month_6,
    inst_month_7,  
    inst_month_8,  
    inst_month_9,
    inst_month_10,
    inst_month_11,
    inst_month_12,
    po_template_name po_name,
    ref_postpaid_event_reference_po_template.group po_group,
    sub_line_type_code,
    satisfaction_measurement_model satisfaction_measure_model,
    performance_satisfaction_plan perf_satisfaction_plan,
    allocated,
    voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, voice_share, sms_share, data_share, vas_share,
    ref_bill_account_product.region_code,
    ref_bill_account_product.branch_code,
    case when nvl(ref_postpaid_event_reference_po_template.rev_code, '') = '' then 'REV_CODE not found' 
    when nvl(ref_postpaid_event_reference_po_template.po_template_id, '') = '' then 'PO_ID not found'
    else '' 
    end ERROR_MESSAGE
from bill_detail_ifrs
left join bill_summary on bill_detail_ifrs.account_num = bill_summary.account_num and bill_detail_ifrs.bill_seq = bill_summary.bill_seq
left join ref_bill_account_product on bill_detail_ifrs.account_num = ref_bill_account_product.account_num
    and bill_detail_ifrs.product_seq_override = ref_bill_account_product.product_seq
left join ref_customer_number on ref_bill_account_product.account_type=ref_customer_number.account_type
    and ref_bill_account_product.customer_type_name=ref_customer_number.customer_type_name
left join ref_postpaid_event_reference_po_template on upper(trim(bill_detail_ifrs.revenue_code_name)) = upper(ref_postpaid_event_reference_po_template.rev_code)
    and (case when ref_customer_number.customer_segment = 'B2B IPHONE' then 'IPHONE' else 'POSTPAID' end) = ref_postpaid_event_reference_po_template.src_tp
left join ref_b2m_branch_region on upper(ref_bill_account_product.branch_code) = upper(ref_b2m_branch_region.branch_code)
    and upper(ref_bill_account_product.region_code) = upper(ref_b2m_branch_region.region_code)
left join ref_city_to_profit_center on upper(ref_b2m_branch_region.branch_name) = upper(ref_city_to_profit_center.city)

where revenue_mny <> 0 and nvl(ref_bill_account_product.account_type,0) not in (2, 4, 5, 11, 12, 9, 10) and nvl(ref_bill_account_product.customer_type_id,0) not in (4, 5, 29)
and nvl(ref_postpaid_event_reference_po_template.contract_template_id, '') <> 'DROP'
    """)
    postpaidTransformDf.registerTempTable("postpaid_transform")
    
    val postpaidTransformWithErrorDf = sqlContext.sql("""
    select
        prc_dt,
        job_id,
        subscriber_type,
        customer_type_id,
        customer_type_name,
        prod_family_id,
        prod_family_name,
        customer_name,
        account_name,
        account_type,
        npwp,
        billing_address,
        postal_code,
        vat_exchange_rate,
        customer_ref,
        account_number,
        bill_seq,
        service_id,
        service_class,
        sms_type,
        carrier,
        source_class_id,
        product_charge_type,
        revenue_code_id,
        revenue_code_name,
        revenue_mny,
        product_seq,
        currency_code,
        event_count,
        volume,
        duration,
        account_type_name,
        sap_customer_number,
        customer_segment,
        clearing_gl_account,
        business_area,
        qty,
        city,
        start_dt,
        end_dt,
        invoice_num,
        invoice_net_money,
        invoice_tax_money,
        uom,
        profit_center,
        contract_id_original,
        contract_id,
        contract_name,
        po_id,
        offer_id,
        inst_month_1,  
        inst_month_2,  
        inst_month_3,
        inst_month_4,
        inst_month_5,
        inst_month_6,
        inst_month_7,  
        inst_month_8,  
        inst_month_9,
        inst_month_10,
        inst_month_11,
        inst_month_12,
        po_name,
        po_group,
        sub_line_type_code,
        satisfaction_measure_model,
        perf_satisfaction_plan,
        allocated,
        ERROR_MESSAGE
    from postpaid_transform
    """)
    postpaidTransformWithErrorDf.registerTempTable("postpaid_transform_with_error")

    val postpaidTrxSummaryDf = sqlContext.sql("""
    select
        prc_dt,
        job_id,
        subscriber_type,
        customer_type_id,
        customer_type_name,
        prod_family_id,
        prod_family_name,
        customer_name,
        account_name,
        account_type,
        npwp,
        billing_address,
        postal_code,
        vat_exchange_rate,
        customer_ref,
        account_number,
        bill_seq,
        service_id,
        service_class,
        sms_type,
        carrier,
        source_class_id,
        product_charge_type,
        revenue_code_id,
        revenue_code_name,
        revenue_mny,
        product_seq,
        currency_code,
        event_count,
        volume,
        duration,
        account_type_name,
        sap_customer_number,
        customer_segment,
        clearing_gl_account,
        business_area,
        qty,
        city,
        start_dt,
        end_dt,
        invoice_num,
        invoice_net_money,
        invoice_tax_money,
        uom,
        profit_center,
        contract_id_original,
        contract_id,
        contract_name,
        po_id,
        offer_id,
        inst_month_1,  
        inst_month_2,  
        inst_month_3,
        inst_month_4,
        inst_month_5,
        inst_month_6,
        inst_month_7,  
        inst_month_8,  
        inst_month_9,
        inst_month_10,
        inst_month_11,
        inst_month_12,
        po_name,
        po_group,
        sub_line_type_code,
        satisfaction_measure_model,
        perf_satisfaction_plan,
        allocated
    from postpaid_transform_with_error
    where nvl(ERROR_MESSAGE, '') = ''
    """)
    postpaidTrxSummaryDf.registerTempTable("postpaid_trx_summary")
    
    val postpaidUnidentifiedDf = sqlContext.sql("""
    select
        prc_dt,
        job_id,
        subscriber_type,
        customer_type_id,
        customer_type_name,
        prod_family_id,
        prod_family_name,
        customer_name,
        account_name,
        account_type,
        npwp,
        billing_address,
        postal_code,
        vat_exchange_rate,
        customer_ref,
        account_number,
        bill_seq,
        service_id,
        service_class,
        sms_type,
        carrier,
        source_class_id,
        product_charge_type,
        revenue_code_id,
        revenue_code_name,
        revenue_mny,
        product_seq,
        currency_code,
        event_count,
        volume,
        duration,
        account_type_name,
        sap_customer_number,
        customer_segment,
        clearing_gl_account,
        business_area,
        qty,
        city,
        start_dt,
        end_dt,
        invoice_num,
        invoice_net_money,
        invoice_tax_money,
        uom,
        profit_center,
        contract_id_original,
        contract_id,
        contract_name,
        po_id,
        offer_id,
        inst_month_1,  
        inst_month_2,  
        inst_month_3,
        inst_month_4,
        inst_month_5,
        inst_month_6,
        inst_month_7,  
        inst_month_8,  
        inst_month_9,
        inst_month_10,
        inst_month_11,
        inst_month_12,
        po_name,
        po_group,
        sub_line_type_code,
        satisfaction_measure_model,
        perf_satisfaction_plan,
        allocated,
        ERROR_MESSAGE
    from postpaid_transform_with_error
    where nvl(ERROR_MESSAGE, '') <> ''
    """)
    postpaidUnidentifiedDf.registerTempTable("postpaid_unidentified")
    
    val postpaidReSummaryDf = sqlContext.sql("""
    select
        prc_dt,
        job_id,
        subscriber_type,
        customer_type_id,
        customer_type_name,
        customer_name,
        account_name,
        npwp,
        billing_address,
        postal_code,
        vat_exchange_rate,
        customer_ref,
        account_number,
        revenue_code_id,
        revenue_code_name,
        cast(sum(revenue_mny) as decimal(24, 6)) revenue_mny,
        currency_code,
        account_type_name,
        sap_customer_number,
        prod_family_name,
        customer_segment,
        clearing_gl_account,
        business_area,
        cast(sum(qty) as decimal(18, 2)) qty,
        start_dt,
        end_dt,
        invoice_num,
        first(invoice_net_money) invoice_net_money,
        first(invoice_tax_money) invoice_tax_money,
        uom,
        profit_center,
        first(contract_id_original) contract_id_original,
        contract_id,
        first(contract_name) contract_name,
        po_id,
        offer_id,
        first(inst_month_1) inst_month_1,  
        first(inst_month_2) inst_month_2,  
        first(inst_month_3) inst_month_3,
        first(inst_month_4) inst_month_4,
        first(inst_month_5) inst_month_5,
        first(inst_month_6) inst_month_6,
        first(inst_month_7) inst_month_7,  
        first(inst_month_8) inst_month_8,  
        first(inst_month_9) inst_month_9,
        first(inst_month_10) inst_month_10,
        first(inst_month_11) inst_month_11,
        first(inst_month_12) inst_month_12,
        first(po_name) po_name,
        first(po_group) po_group,
        first(sub_line_type_code) sub_line_type_code,
        first(satisfaction_measure_model) satisfaction_measure_model,
        first(perf_satisfaction_plan) perf_satisfaction_plan,
        first(allocated) allocated
    from postpaid_trx_summary
    group by
        prc_dt,
        job_id,
        subscriber_type,
        customer_type_id,
        customer_type_name,
        customer_name,
        account_name,
        npwp,
        billing_address,
        postal_code,
        vat_exchange_rate,
        customer_ref,
        account_number,
        revenue_code_id,
        revenue_code_name,
        currency_code,
        account_type_name,
        sap_customer_number,
        prod_family_name,
        customer_segment,
        clearing_gl_account,
        business_area,
        city,
        start_dt,
        end_dt,
        invoice_num,
        uom,
        profit_center,
        contract_id,
        po_id,
        offer_id
    """)
//    postpaidReSummaryDf.cache()
    postpaidReSummaryDf.registerTempTable("postpaid_re_summary")
    
//    postpaidReSummaryDf.write
//        .partitionBy("prc_dt")
//        .mode("overwrite")
//        .parquet(pathPostpaidReSummaryParquet)
//    
//    postpaidTrxSummaryDf.repartition(1).write.format("com.databricks.spark.csv")
//        .mode("overwrite")
//        .option("delimiter", ";")
//        .option("header", "true")
//        .save(pathPostpaidTrxSummary+"/PRC_DT=" + prcDt)
    Common.cleanDirectory(sc, pathPostpaidDetailUnidentified + "/PRC_DT="+prcDt)
    postpaidUnidentifiedDf.repartition(1)
    .write.format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("delimiter", "|").option("header", "true")
    .save(pathPostpaidDetailUnidentified + "/PRC_DT="+prcDt)
    
    Common.cleanDirectoryWithPattern(sc, pathPostpaidTrxSummary, "/prc_dt="+prcDt+"/start_dt="+"*/job_id=" + jobId + "/*")
    postpaidTrxSummaryDf.repartition(50).write.partitionBy("PRC_DT", "START_DT", "JOB_ID")
    .mode("append").parquet(pathPostpaidTrxSummary)    
    
    Common.cleanDirectoryWithPattern(sc, pathPostpaidReSummary, "/prc_dt="+prcDt+"/start_dt="+"*/job_id=" + jobId + "/*")
    postpaidReSummaryDf.repartition(1).write.partitionBy("PRC_DT", "START_DT", "JOB_ID")
    .mode("append").parquet(pathPostpaidReSummary)
    
    
    rootLogger.info("====job ifrs summary postpaid finished====")
    
  }
  
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
      StructField("BRANCH_NAME", StringType, true),
      StructField("NPWP", StringType, true),
      StructField("BILLING_ADDRESS", StringType, true),
      StructField("POSTAL_CODE", StringType, true)))
  val billSummarySchema: StructType = StructType(Array(
      StructField("ACCOUNT_NUM", StringType, true),
      StructField("BILL_SEQ", StringType, true),
      StructField("BILL_DTM", StringType, true),
      StructField("BILL_TYPE_ID", StringType, true),
      StructField("BALANCE_FWD_MNY", StringType, true),
      StructField("INVOICE_NET_MNY", StringType, true),
      StructField("INVOICE_TAX_MNY", StringType, true),
      StructField("PAYMENTS_MNY", StringType, true),
      StructField("FAILED_PAYMENTS_MNY", StringType, true),
      StructField("ADJUSTMENTS_MNY", StringType, true),
      StructField("REFUNDS_MNY", StringType, true),
      StructField("BALANCE_OUT_MNY", StringType, true),
      StructField("EBD_EMAIL_STATUS", StringType, true),
      StructField("POINTS_FWD", StringType, true),
      StructField("POINTS_EARNED", StringType, true),
      StructField("POINTS_ADJUSTED", StringType, true),
      StructField("POINTS_REDEEMED", StringType, true),
      StructField("POINTS_OUT", StringType, true),
      StructField("TRANS_BILLED_TO_DTM", StringType, true),
      StructField("EVENT_BILLED_TO_DTM", StringType, true),
      StructField("TAX_POINT_DAT", StringType, true),
      StructField("OUTSTANDING_DEBT_MNY", StringType, true),
      StructField("CHASE_DEBT_MNY", StringType, true),
      StructField("CURRENT_DISPUTE_MNY", StringType, true),
      StructField("MODIFICATION_DAYS", StringType, true),
      StructField("CREDIT_ADJ_BILLED_TO_DTM", StringType, true),
      StructField("DEBIT_ADJ_BILLED_TO_DTM", StringType, true),
      StructField("LOYALTY_TRANS_BILLED_TO_DTM", StringType, true),
      StructField("EVENT_SEQ", StringType, true),
      StructField("INFO_INVOICE_MNY", StringType, true),
      StructField("INFO_CURRENCY_CODE", StringType, true),
      StructField("STATEMENT_BOO", StringType, true),
      StructField("BILL_SETTLED_BOO", StringType, true),
      StructField("EVENT_ARCHIVE_STATUS", StringType, true),
      StructField("INVOICE_NUM", StringType, true),
      StructField("ACTUAL_BILL_DTM", StringType, true),
      StructField("START_OF_BILL_DTM", StringType, true),
      StructField("BILL_VERSION", StringType, true),
      StructField("BILL_STATUS", StringType, true),
      StructField("CANCELLATION_REQUEST_DAT", StringType, true),
      StructField("CANCELLATION_DTM", StringType, true),
      StructField("REPLACES_INVOICE_NUM", StringType, true),
      StructField("BILLED_WITH_BILL_SEQ", StringType, true),
      StructField("CANCELLED_REASON_TXT", StringType, true),
      StructField("ARCHIVE_DAT", StringType, true),
      StructField("INVOICE_NUM_STATUS", StringType, true),
      StructField("INVOICE_REGISTER_ID", StringType, true),
      StructField("CHARGE_SEQ", StringType, true),
      StructField("DEBT_START_DAT", StringType, true),
      StructField("PAYMENT_DUE_DAT", StringType, true),
      StructField("BILL_HANDLING_CODE", StringType, true),
      StructField("OCR_NUM", StringType, true),
      StructField("BILL_SETTLED_DTM", StringType, true),
      StructField("EVENT_MASKED_BOO", StringType, true),
      StructField("BILL_LABEL", StringType, true),
      StructField("EVENT_DEL_NO_ARCHIVE_BOO", StringType, true),
      StructField("MIN_BILLED_EVENT_DTM", StringType, true),
      StructField("MAX_BILLED_EVENT_DTM", StringType, true),
      StructField("MIN_RERATED_EVENT_DTM", StringType, true),
      StructField("MAX_RERATED_EVENT_DTM", StringType, true),
      StructField("DISCOUNTED_RERATING_BOO", StringType, true),
      StructField("DOMAIN_ID", StringType, true),
      StructField("UST_CURRENCY_CODE", StringType, true),
      StructField("RANDOM_HASH", StringType, true),
      StructField("COLLECTION_NOTIFY_BOO", StringType, true),
      StructField("ADFG_PROCESSED_DTM", StringType, true),
      StructField("BILLING_OFFER_ID", StringType, true),
      StructField("OTC_MNY", StringType, true),
      StructField("OTC_BILLED_TO_DTM", StringType, true),
      StructField("OVERRIDE_BILL_DTM", StringType, true),
      StructField("LATE_FEE_MNY", StringType, true),
      StructField("DEFERRED_CHARGES_TOT", StringType, true),
      StructField("OVERWRITTEN_DEF_CHARGE_TOT", StringType, true),
      StructField("SETTLEUP_CHARGES_TOT", StringType, true),
      StructField("OEFR_CREATED_BOO", StringType, true),
      StructField("CURRENCY", StringType, true),
      StructField("VAT_EXCHANGE_RATE", StringType, true)))
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
      StructField("gl_account_b2b-credit", StringType, true),
      StructField("gl_account_b2b_desc-credit", StringType, true),
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
  val postpaidPoTemplateSchema = new StructType(Array(
      StructField("po_template_id", StringType, true),
      StructField("group", StringType, true),
      StructField("po_template_name", StringType, true),
      StructField("allocated", StringType, true),
      StructField("satisfaction_measurement_model", StringType, true),
      StructField("performance_satisfaction_plan", StringType, true),
      StructField("sub_line_type_code", StringType, true),
      StructField("status", StringType, true)))
  val postpaidPoTemplateMergeSchema = new StructType(Array(
      StructField("po_template_id", StringType, true),
      StructField("group", StringType, true),
      StructField("po_template_name", StringType, true),
      StructField("allocated", StringType, true),
      StructField("satisfaction_measurement_model", StringType, true),
      StructField("performance_satisfaction_plan", StringType, true),
      StructField("sub_line_type_code", StringType, true),
      StructField("status", StringType, true),
      StructField("src_tp", StringType, true)))
  val postpaidPoTemplateIphoneSchema = new StructType(Array(
      StructField("po_template_id", StringType, true),
      StructField("group", StringType, true),
      StructField("po_template_name", StringType, true),
      StructField("allocated", StringType, true),
      StructField("satisfaction_measurement_model", StringType, true),
      StructField("performance_satisfaction_plan", StringType, true),
      StructField("sub_line_type_code", StringType, true),
      StructField("status", StringType, true)))
  val refB2mBranchRegionSchema = new StructType(Array(
      StructField("region_code", StringType, true),
      StructField("region_name", StringType, true),
      StructField("branch_code", StringType, true),
      StructField("branch_name", StringType, true)))
  val profitCenterSchema = new StructType(Array(
      StructField("prefix", StringType, true),
      StructField("length", StringType, true),
      StructField("code", StringType, true),
      StructField("city", StringType, true),
      StructField("pc_name", StringType, true),
      StructField("pc_alias", StringType, true),
      StructField("pc_id", StringType, true),
      StructField("group", StringType, true)))
  val customerSchema = new StructType(Array(
      StructField("account_type_name", StringType, true),
      StructField("account_type", StringType, true),
      StructField("customer_type_name", StringType, true),
      StructField("group", StringType, true),
      StructField("customer_segment", StringType, true),
      StructField("sap_customer_number", StringType, true),
      StructField("clearing_gl_account", StringType, true),
      StructField("business_area", StringType, true)))
  
}