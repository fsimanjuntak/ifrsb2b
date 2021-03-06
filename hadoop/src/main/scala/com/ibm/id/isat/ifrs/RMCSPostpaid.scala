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

object RMCSPostpaid {
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
    val pathRmcsPostpaid = Common.getConfigDirectory(sc, config, "IFRS.OUTPUT.RMCS_POSTPAID")
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
    
    val postpaidReSummaryDf = sqlContext.read.option("basePath", pathPostpaidReSummary)
    .parquet(pathPostpaidReSummary+"/prc_dt="+prcDt)
    postpaidReSummaryDf.registerTempTable("postpaid_re_summary")
    
    val postpaidRmcsBaseDf = sqlContext.sql("""
      select
        customer_segment, 
        first(po_group) po_group, 
        prc_dt, 
        start_dt, 
        end_dt,
        po_id, 
        profit_center, 
        currency_code,
        contract_id,
        uom,
        sum(case when qty < 999999 then qty else 999999 end) qty,
        sum(revenue_mny) revenue_mny,
        allocated,
        first(sub_line_type_code) sub_line_type_code,
        contract_name,
        first(po_name) po_name,
        revenue_code_name,
        postpaid_re_summary.offer_id,
        first(perf_satisfaction_plan) perf_satisfaction_plan,
        first(satisfaction_measure_model) satisfaction_measure_model,
        first(postpaid_re_summary.inst_month_1) inst_month_1,  
        first(postpaid_re_summary.inst_month_2) inst_month_2,  
        first(postpaid_re_summary.inst_month_3) inst_month_3,
        first(postpaid_re_summary.inst_month_4) inst_month_4,
        first(postpaid_re_summary.inst_month_5) inst_month_5,
        first(postpaid_re_summary.inst_month_6) inst_month_6,
        first(postpaid_re_summary.inst_month_7) inst_month_7,  
        first(postpaid_re_summary.inst_month_8) inst_month_8,  
        first(postpaid_re_summary.inst_month_9) inst_month_9,
        first(postpaid_re_summary.inst_month_10) inst_month_10,
        first(postpaid_re_summary.inst_month_11) inst_month_11,
        first(postpaid_re_summary.inst_month_12) inst_month_12
    from postpaid_re_summary
    left join ref_postpaid_event_reference 
      on (case when customer_segment = 'B2B IPHONE' then 'IPHONE' else 'POSTPAID' end) = ref_postpaid_event_reference.src_tp
        and lower(postpaid_re_summary.revenue_code_name) = lower(ref_postpaid_event_reference.rev_code)
    where 
      nvl(ref_postpaid_event_reference.gl_account_b2b_credit,'') = ''
    group by 
        customer_segment, 
        prc_dt, 
        start_dt, 
        end_dt,
        po_id, 
        profit_center, 
        currency_code,
        contract_id,
        uom,
        allocated,
        contract_name,
        revenue_code_name,
        postpaid_re_summary.offer_id
    """)  
    
    val postpaidSummaryContractPartitionedDf = postpaidRmcsBaseDf
        .repartition(col("customer_segment"), col("contract_id"), col("prc_dt"), col("currency_code"))
        .persist()
    postpaidSummaryContractPartitionedDf.registerTempTable("postpaid_summary_contract_partitioned")
    
    val postpaidSummaryRowNumberedDf = sqlContext.sql("""
        select *, row_number() over (partition by customer_segment, contract_id, prc_dt, currency_code) row_number
        from postpaid_summary_contract_partitioned
    """)
    postpaidSummaryRowNumberedDf.persist()
    postpaidSummaryRowNumberedDf.registerTempTable("postpaid_summary_row_numbered")
    
    val rmcsSublineBasePostpaidDf = sqlContext.sql("""
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_1 inst_month, 1 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_1, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_2 inst_month, 2 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_2, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_3 inst_month, 3 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_3, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_4 inst_month, 4 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_4, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_5 inst_month, 5 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_5, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_6 inst_month, 6 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_6, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_7 inst_month, 7 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_7, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_8 inst_month, 8 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_8, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_9 inst_month, 9 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_9, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_10 inst_month, 10 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_10, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_11 inst_month, 11 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_11, '') <> ''
    union all
    select
        customer_segment, po_group, revenue_code_name, prc_dt, start_dt, contract_id, po_id, profit_center, currency_code, sub_line_type_code, inst_month_12 inst_month, 12 inst_month_num
    from postpaid_summary_row_numbered
    where nvl(inst_month_12, '') <> ''
    """)
    rmcsSublineBasePostpaidDf.registerTempTable("rmcs_subline_base_postpaid")
    
    val rmcsSublinePostpaidDf = sqlContext.sql("""
    select
        'S' record_type,
        '' document_sub_line_id,
        '' document_line_id,
        '' document_id,
        '' application_id,
        '' document_type_id,
        sub_line_type_code doc_sub_line_type,
        '' doc_sub_line_level,
        '' doc_sub_line_id_int_1,
        '' doc_sub_line_id_int_2,
        '' doc_sub_line_id_int_3,
        '' doc_sub_line_id_int_4,
        '' doc_sub_line_id_int_5,
        case po_group when 'PPU'
            then concat_ws('',
                prc_dt,
                hex(hash(concat_ws('_', customer_segment, po_group, revenue_code_name, prc_dt, start_dt, po_id, profit_center, currency_code))),
                '_',
                lpad(inst_month_num, 2, '0')
            )
        else concat_ws('',
                prc_dt,
                hex(hash(concat_ws('_', customer_segment, contract_id, prc_dt, start_dt, po_id, profit_center, currency_code))),
                '_',
                lpad(inst_month_num, 2, '0')
            )
        end doc_sub_line_id_char_1,
        hex(hash(revenue_code_name)) doc_sub_line_id_char_2,
        start_dt doc_sub_line_id_char_3,
        hex(hash(po_id)) doc_sub_line_id_char_4,
        hex(hash(profit_center)) doc_sub_line_id_char_5,
        '' doc_line_id_int_1,
        '' doc_line_id_int_2,
        '' doc_line_id_int_3,
        '' doc_line_id_int_4,
        '' doc_line_id_int_5,
        case po_group when 'PPU'
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
        '' doc_id_int_1,
        '' doc_id_int_2,
        '' doc_id_int_3,
        '' doc_id_int_4,
        '' doc_id_int_5,
        '' doc_id_char_1,
        '' doc_id_char_2,
        '' doc_id_char_3,
        '' doc_id_char_4,
        '' doc_id_char_5,
        '' line_number,
        '' org_id,
        '' ledger_id,
        '' sub_line_creation_date,
        '' sub_line_last_update_date,
        '' account_class,
        '' amount,
        '' acctd_amount,
        '' accounting_date,
        '' sla_posted_flag,
        '' sla_posted_date,
        '' event_id,
        '' code_combination_id,
        '' salesrep_id,
        '' salesrep_name,
        '' revenue_amount_split,
        '' revenue_percent_split,
        '' contingency_code,
        '' contingency_id,
        '' expiration_date,
        '' expiration_days,
        '' expired_flag,
        '' expiration_event_date,
        '' expired_by,
        '' comments,
        '' src_attribute_char1,
        '' src_attribute_char2,
        '' src_attribute_char3,
        '' src_attribute_char4,
        '' src_attribute_char5,
        '' src_attribute_char6,
        '' src_attribute_char7,
        '' src_attribute_char8,
        '' src_attribute_char9,
        '' src_attribute_char10,
        '' src_attribute_char11,
        '' src_attribute_char12,
        '' src_attribute_char13,
        '' src_attribute_char14,
        '' src_attribute_char15,
        '' src_attribute_char16,
        '' src_attribute_char17,
        '' src_attribute_char18,
        '' src_attribute_char19,
        '' src_attribute_char20,
        '' src_attribute_char21,
        '' src_attribute_char22,
        '' src_attribute_char23,
        '' src_attribute_char24,
        '' src_attribute_char25,
        '' src_attribute_char26,
        '' src_attribute_char27,
        '' src_attribute_char28,
        '' src_attribute_char29,
        '' src_attribute_char30,
        '' src_attribute_char31,
        '' src_attribute_char32,
        '' src_attribute_char33,
        '' src_attribute_char34,
        '' src_attribute_char35,
        '' src_attribute_char36,
        '' src_attribute_char37,
        '' src_attribute_char38,
        '' src_attribute_char39,
        '' src_attribute_char40,
        '' src_attribute_char41,
        '' src_attribute_char42,
        '' src_attribute_char43,
        '' src_attribute_char44,
        '' src_attribute_char45,
        '' src_attribute_char46,
        '' src_attribute_char47,
        '' src_attribute_char48,
        '' src_attribute_char49,
        '' src_attribute_char50,
        '' src_attribute_char51,
        '' src_attribute_char52,
        '' src_attribute_char53,
        '' src_attribute_char54,
        '' src_attribute_char55,
        '' src_attribute_char56,
        '' src_attribute_char57,
        '' src_attribute_char58,
        '' src_attribute_char59,
        '' src_attribute_char60,
        '' src_attribute_number1,
        '' src_attribute_number2,
        '' src_attribute_number3,
        '' src_attribute_number4,
        '' src_attribute_number5,
        '' src_attribute_number6,
        '' src_attribute_number7,
        '' src_attribute_number8,
        '' src_attribute_number9,
        '' src_attribute_number10,
        '' src_attribute_number11,
        '' src_attribute_number12,
        '' src_attribute_number13,
        '' src_attribute_number14,
        '' src_attribute_number15,
        '' src_attribute_number16,
        '' src_attribute_number17,
        '' src_attribute_number18,
        '' src_attribute_number19,
        '' src_attribute_number20,
        '' src_attribute_date1,
        '' src_attribute_date2,
        '' src_attribute_date3,
        '' src_attribute_date4,
        '' src_attribute_date5,
        '' src_attribute_date6,
        '' src_attribute_date7,
        '' src_attribute_date8,
        '' src_attribute_date9,
        '' src_attribute_date10,
        '' request_id,
        '' object_version_number,
        '' creation_date,
        '' created_by,
        '' last_update_date,
        '' last_updated_by,
        '' last_update_login,
        '' source_org_id,
        '' source_account_ccid,
        '' source_salesrep_name,
        '' source_contingency_name,
        '' contingency_name,
        '' account_code_segment1,
        '' account_code_segment2,
        '' account_code_segment3,
        '' account_code_segment4,
        '' account_code_segment5,
        '' account_code_segment6,
        '' account_code_segment7,
        '' account_code_segment8,
        '' account_code_segment9,
        '' account_code_segment10,
        '' account_code_segment11,
        '' account_code_segment12,
        '' account_code_segment13,
        '' account_code_segment14,
        '' account_code_segment15,
        '' account_code_segment16,
        '' account_code_segment17,
        '' account_code_segment18,
        '' account_code_segment19,
        '' account_code_segment20,
        '' account_code_segment21,
        '' account_code_segment22,
        '' account_code_segment23,
        '' account_code_segment24,
        '' account_code_segment25,
        '' account_code_segment26,
        '' account_code_segment27,
        '' account_code_segment28,
        '' account_code_segment29,
        '' account_code_segment30,
        '' data_transformation_status,
        'HADOOP' source_system,
        '' sp_first_name,
        '' sp_middle_name,
        '' sp_last_name,
        date_format(
            trunc(
                add_months(cast(unix_timestamp(cast(start_dt as string), 'yyyyMMdd') as timestamp), inst_month_num - 1), 
                'MM'
            ), 
            'dd-MMM-yy'
        ) satisfaction_measurement_date,
        '' satisfied_quantity,
        inst_month satisfaction_event_percent,
        'HADOOP' document_type_code
    from rmcs_subline_base_postpaid
    """)
    rmcsSublinePostpaidDf.persist()
    rmcsSublinePostpaidDf.registerTempTable("rmcs_subline_postpaid")
    
    val rmcsLinePostpaidDf = sqlContext.sql("""
    select 
        'L' record_type,
        '' document_line_id,
        '' document_id,
        '' application_id,
        '' document_type_id,
        '' doc_line_id_int_1,
        '' doc_line_id_int_2,
        '' doc_line_id_int_3,
        '' doc_line_id_int_4,
        '' doc_line_id_int_5,
        case po_group when 'PPU'
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
        '' doc_id_int_1,
        '' doc_id_int_2,
        '' doc_id_int_3,
        '' doc_id_int_4,
        '' doc_id_int_5,
        case po_group
            when 'PPU' then concat_ws('_', regexp_replace(customer_segment, ' POSTPAID', 'POST'), po_group, prc_dt, substr(currency_code, 1, 1))
            else concat_ws('_', regexp_replace(customer_segment, ' POSTPAID', 'POST'), contract_id, substr(currency_code, 1, 1))
        end doc_id_char_1,
        '' doc_id_char_2,
        '' doc_id_char_3,
        '' doc_id_char_4,
        '' doc_id_char_5,
        '' document_date,
        '' line_type,
        '' line_number,
        '' inventory_org_id,
        '' item_id,
        '' item_number,
        '' item_description,
        '' memo_line_id,
        uom uom_code,
        cast(qty as decimal(18, 0)) quantity,
        cast(cast(revenue_mny as decimal(18, 0)) / qty as decimal(23, 11)) unit_selling_price,
        '' unit_list_price,
        '' discount_percentage,
        '' unit_selling_pct_base_price,
        '' unit_list_pct_base_price,
        '' base_price,
        cast(revenue_mny as decimal(18, 0)) line_amount,
        '' bill_to_customer_id,
        '' ship_to_customer_id,
        '' bill_to_customer_party_id,
        '' bill_to_customer_party_num,
        '' bill_to_customer_party_name,
        '' bill_to_customer_num,
        '' ship_to_customer_num,
        '' bill_to_customer_name,
        '' ship_to_customer_name,
        '' bill_to_cust_site_name,
        '' bill_to_customer_site_id,
        '' ship_to_customer_site_id,
        '' ship_to_cust_site_name,
        '' bill_to_country,
        '' ship_to_country,
        '' bill_to_customer_state,
        '' bill_to_customer_county,
        '' bill_to_customer_city,
        '' bill_to_customer_postal_code,
        '' bill_to_customer_classification,
        '' delivered_flag,
        '' invoiced_flag,
        '' fulfilled_flag,
        '' cancelled_flag,
        '' delivery_status,
        '' salesrep_id,
        '' salesrep_name,
        '' line_creation_date,
        '' line_last_update_date,
        '' comments,
        nvl(allocated, '') src_attribute_char_1,
        case po_group when 'PPU' then 'PPU' else nvl(contract_name, 'N/A') end src_attribute_char_2,
        customer_segment src_attribute_char_3,
        nvl(po_name, '') src_attribute_char_4,
        nvl(profit_center, '') src_attribute_char_5,
        '' src_attribute_char_6,
        'N/A' src_attribute_char_7,
        'N/A' src_attribute_char_8,
        nvl(revenue_code_name, '') src_attribute_char_9,
        case po_group when 'PPU' then 'PPU' else nvl(offer_id, 'N/A') end src_attribute_char_10,
        '' src_attribute_char_11,
        '' src_attribute_char_12,
        '' src_attribute_char_13,
        '' src_attribute_char_14,
        '' src_attribute_char_15,
        '' src_attribute_char_16,
        '' src_attribute_char_17,
        '' src_attribute_char_18,
        '' src_attribute_char_19,
        '' src_attribute_char_20,
        '' src_attribute_char_21,
        '' src_attribute_char_22,
        '' src_attribute_char_23,
        '' src_attribute_char_24,
        '' src_attribute_char_25,
        '' src_attribute_char_26,
        '' src_attribute_char_27,
        '' src_attribute_char_28,
        '' src_attribute_char_29,
        '' src_attribute_char_30,
        '' src_attribute_char_31,
        '' src_attribute_char_32,
        '' src_attribute_char_33,
        '' src_attribute_char_34,
        '' src_attribute_char_35,
        '' src_attribute_char_36,
        '' src_attribute_char_37,
        '' src_attribute_char_38,
        '' src_attribute_char_39,
        '' src_attribute_char_40,
        '' src_attribute_char_41,
        '' src_attribute_char_42,
        '' src_attribute_char_43,
        '' src_attribute_char_44,
        '' src_attribute_char_45,
        '' src_attribute_char_46,
        '' src_attribute_char_47,
        '' src_attribute_char_48,
        '' src_attribute_char_49,
        '' src_attribute_char_50,
        '' src_attribute_char_51,
        '' src_attribute_char_52,
        '' src_attribute_char_53,
        '' src_attribute_char_54,
        '' src_attribute_char_55,
        '' src_attribute_char_56,
        '' src_attribute_char_57,
        '' src_attribute_char_58,
        '' src_attribute_char_59,
        '' src_attribute_char_60,
        '' src_attribute_number_1,
        '' src_attribute_number_2,
        '' src_attribute_number_3,
        '' src_attribute_number_4,
        '' src_attribute_number_5,
        '' src_attribute_number_6,
        '' src_attribute_number_7,
        '' src_attribute_number_8,
        '' src_attribute_number_9,
        '' src_attribute_number_10,
        '' src_attribute_number_11,
        '' src_attribute_number_12,
        '' src_attribute_number_13,
        '' src_attribute_number_14,
        '' src_attribute_number_15,
        '' src_attribute_number_16,
        '' src_attribute_number_17,
        '' src_attribute_number_18,
        '' src_attribute_number_19,
        '' src_attribute_number_20,
        '' src_attribute_date_1,
        '' src_attribute_date_2,
        '' src_attribute_date_3,
        '' src_attribute_date_4,
        '' src_attribute_date_5,
        '' src_attribute_date_6,
        '' src_attribute_date_7,
        '' src_attribute_date_8,
        '' src_attribute_date_9,
        '' src_attribute_date_10,
        '' request_id,
        '' object_version_number,
        '' creation_date,
        '' created_by,
        '' last_update_date,
        '' last_updated_by,
        '' last_update_login,
        '' memo_line_seq_id,
        '' payment_amount,
        '' quantity_cancelled,
        '' quantity_shipped,
        '' quantity_ordered,
        '' quantity_fulfilled,
        '' quantity_invoiced,
        '' open_flag,
        '' cust_po_number,
        '' project_id,
        '' task_id,
        '' payment_term_id,
        '' accounting_rule_id,
        case when nvl(inst_month_1, '') = '' 
              and nvl(inst_month_2, '') = ''
              and nvl(inst_month_3, '') = ''
              and nvl(inst_month_4, '') = '' 
              and nvl(inst_month_5, '') = '' 
              and nvl(inst_month_6, '') = '' 
              and nvl(inst_month_7, '') = ''
              and nvl(inst_month_8, '') = ''
              and nvl(inst_month_9, '') = '' 
              and nvl(inst_month_10, '') = '' 
              and nvl(inst_month_11, '') = ''
              and nvl(inst_month_12, '') = ''
            then nvl(date_format(cast(unix_timestamp(cast(start_dt as string), 'yyyyMMdd') as timestamp), 'dd-MMM-yy'), '')
            else ''
        end rule_start_date,
        case when nvl(inst_month_1, '') = '' 
              and nvl(inst_month_2, '') = ''
              and nvl(inst_month_3, '') = ''
              and nvl(inst_month_4, '') = '' 
              and nvl(inst_month_5, '') = '' 
              and nvl(inst_month_6, '') = '' 
              and nvl(inst_month_7, '') = ''
              and nvl(inst_month_8, '') = ''
              and nvl(inst_month_9, '') = '' 
              and nvl(inst_month_10, '') = '' 
              and nvl(inst_month_11, '') = ''
              and nvl(inst_month_12, '') = ''
            then nvl(date_format(cast(unix_timestamp(cast(nvl(end_dt, start_dt) as string), 'yyyyMMdd') as timestamp), 'dd-MMM-yy'), '')
            else ''
        end rule_end_date,
        '' actual_shipment_date,
        '' actual_arrival_date,
        '' fob_point_code,
        '' frieght_terms_code,
        '' scheduled_status_code,
        '' source_type_code,
        '' return_reason_code,
        '' shipping_interfaced_flag,
        '' credit_invoice_line_id,
        '' reference_customer_trx_line_id,
        '' shippable_flag,
        '' fulfillment_date,
        '' account_rule_duration,
        '' actual_fulfillment_date,
        '' contingency_id,
        '' revrec_event_code,
        '' revrec_expiration_days,
        '' accepted_quantity,
        '' accepted_by,
        '' revrec_comments,
        '' revrec_reference_document,
        '' revrec_signature,
        '' revrec_signature_date,
        '' revrec_implicit_flag,
        '' cost_amount,
        '' gross_margin_percent,
        '' src_attribute_category,
        '' source_org_id,
        '' reference_doc_line_id_int_1,
        '' reference_doc_line_id_int_2,
        '' reference_doc_line_id_int_3,
        '' reference_doc_line_id_int_4,
        '' reference_doc_line_id_int_5,
        '' reference_doc_line_id_char_1,
        '' reference_doc_line_id_char_2,
        '' reference_doc_line_id_char_3,
        '' reference_doc_line_id_char_4,
        '' reference_doc_line_id_char_5,
        '' override_auto_accounting_flag,
        '' source_inventory_org_id,
        '' reference_reversal_method,
        1 version_number,
        '' version_flag,
        row_number line_num,
        '' discount_amount,
        '' last_period_to_credit,
        '' memo_line_name,
        '' payment_term_name,
        nvl(perf_satisfaction_plan, '') accounting_rule_name,
        'ISAT' inventory_org_code,
        customer_segment orig_sys_bill_to_cust_site_ref,
        '' orig_sys_ship_to_cust_site_ref,
        customer_segment orig_sys_bill_to_cust_ref,
        '' orig_sys_ship_to_cust_ref,
        '' source_inventory_org_code,
        '' source_memo_line_name,
        po_id source_item_number,
        '' source_uom_code,
        '' source_salesrep_name,
        '' source_payment_term_name,
        '' source_accounting_rule_name,
        '' source_bill_to_cust_num,
        '' source_bill_to_cust_name,
        '' source_bill_to_cust_site_num,
        '' source_bill_to_cust_address,
        '' source_ship_to_cust_num,
        '' source_ship_to_cust_name,
        '' source_ship_to_cust_site_num,
        '' source_ship_to_cust_address,
        '' delivery_date,
        '' reference_document_type_id,
        'HADOOP' source_system,
        '' reference_source_system,
        '' sp_first_name,
        '' sp_middle_name,
        '' sp_last_name,
        nvl(satisfaction_measure_model, '') satisfaction_measurement_model,
        'HADOOP' source_document_type_code,
        '' document_type_code,
        '' contract_update_template_name,
        '' contract_update_template_id,
        '' contract_modification_date,
        '' initial_doc_line_id_int_1,
        '' initial_doc_line_id_int_2,
        '' initial_doc_line_id_int_3,
        '' initial_doc_line_id_int_4,
        '' initial_doc_line_id_int_5,
        '' initial_doc_line_id_char_1,
        '' initial_doc_line_id_char_2,
        '' initial_doc_line_id_char_3,
        '' initial_doc_line_id_char_4,
        '' initial_doc_line_id_char_5,
        '' initial_document_type_id,
        '' initial_document_type_code,
        '' initial_source_system,
        '' add_to_contract_flag,
        '' add_to_contract_action_code,
        '' manual_review_required,
        '' revision_intent_type_code,
        '' recurring_flag,
        '' recurring_frequency,
        '' recurring_patern_code,
        '' recurring_amount,
        '' termination_date,
        '' immaterial_change_code,
        '' unit_ssp
    from postpaid_summary_row_numbered
    """)
    rmcsLinePostpaidDf.registerTempTable("rmcs_line_postpaid")
    
    val rmcsHeaderPostpaidDf = sqlContext.sql("""
        select 
            'H' record_type,
            '' document_id,
            '' application_id,
            '' document_type_id,
            '' doc_id_int_1,
            '' doc_id_int_2,
            '' doc_id_int_3,
            '' doc_id_int_4,
            '' doc_id_int_5,
            case po_group
                when 'PPU' then concat_ws('_', regexp_replace(customer_segment, ' POSTPAID', 'POST'), po_group, prc_dt, substr(currency_code, 1, 1))
                else concat_ws('_', regexp_replace(customer_segment, ' POSTPAID', 'POST'), contract_id, substr(currency_code, 1, 1))
            end doc_id_char_1,
            '' doc_id_char_2,
            '' doc_id_char_3,
            '' doc_id_char_4,
            '' doc_id_char_5,
            nvl(date_format(cast(unix_timestamp(cast(prc_dt as string), 'yyyyMMdd') as timestamp), 'dd-MMM-yy'), '') document_date,
            case po_group
                when 'PPU' then concat_ws('_', regexp_replace(customer_segment, ' POSTPAID', 'POST'), po_group, prc_dt, substr(currency_code, 1, 1))
                else concat_ws('_', regexp_replace(customer_segment, ' POSTPAID', 'POST'), contract_id, substr(currency_code, 1, 1))
            end document_number,
            '' document_type,
            '' document_creation_date,
            '' document_update_date,
            currency_code currency_code,
            '' salesrep_id,
            '' salesrep_name,
            '' bill_to_customer_party_id,
            '' bill_to_customer_party_num,
            '' bill_to_customer_party_name,
            '' bill_to_customer_id,
            '' ship_to_customer_id,
            '' bill_to_customer_num,
            '' ship_to_customer_num,
            '' bill_to_customer_name,
            '' ship_to_customer_name,
            '' bill_to_customer_site_id,
            '' bill_to_cust_site_name,
            '' ship_to_customer_site_id,
            '' ship_to_cust_site_name,
            '' bill_to_country,
            '' ship_to_country,
            '' org_id,
            'INDOSAT BU' organization_name,
            '' ledger_id,
            '' ledger_name,
            '' legal_entity_id,
            'PT. Indosat Tbk.' legal_entity_name,
            '' cust_po_number,
            '' customer_contract_number,
            '' sales_agreement_number,
            '' quote_number,
            '' exchange_rate,
            '' exchange_rate_type,
            '' exchange_date,
            '' bill_to_customer_state,
            '' bill_to_customer_county,
            '' bill_to_customer_city,
            '' bill_to_customer_postal_code,
            '' bill_to_cust_classification,
            '' src_attribute_char_1,
            '' src_attribute_char_2,
            '' src_attribute_char_3,
            '' src_attribute_char_4,
            '' src_attribute_char_5,
            '' src_attribute_char_6,
            '' src_attribute_char_7,
            '' src_attribute_char_8,
            '' src_attribute_char_9,
            '' src_attribute_char_10,
            '' src_attribute_char_11,
            '' src_attribute_char_12,
            '' src_attribute_char_13,
            '' src_attribute_char_14,
            '' src_attribute_char_15,
            '' src_attribute_char_16,
            '' src_attribute_char_17,
            '' src_attribute_char_18,
            '' src_attribute_char_19,
            '' src_attribute_char_20,
            '' src_attribute_char_21,
            '' src_attribute_char_22,
            '' src_attribute_char_23,
            '' src_attribute_char_24,
            '' src_attribute_char_25,
            '' src_attribute_char_26,
            '' src_attribute_char_27,
            '' src_attribute_char_28,
            '' src_attribute_char_29,
            '' src_attribute_char_30,
            '' src_attribute_char_31,
            '' src_attribute_char_32,
            '' src_attribute_char_33,
            '' src_attribute_char_34,
            '' src_attribute_char_35,
            '' src_attribute_char_36,
            '' src_attribute_char_37,
            '' src_attribute_char_38,
            '' src_attribute_char_39,
            '' src_attribute_char_40,
            '' src_attribute_char_41,
            '' src_attribute_char_42,
            '' src_attribute_char_43,
            '' src_attribute_char_44,
            '' src_attribute_char_45,
            '' src_attribute_char_46,
            '' src_attribute_char_47,
            '' src_attribute_char_48,
            '' src_attribute_char_49,
            '' src_attribute_char_50,
            '' src_attribute_char_51,
            '' src_attribute_char_52,
            '' src_attribute_char_53,
            '' src_attribute_char_54,
            '' src_attribute_char_55,
            '' src_attribute_char_56,
            '' src_attribute_char_57,
            '' src_attribute_char_58,
            '' src_attribute_char_59,
            '' src_attribute_char_60,
            '' src_attribute_number_1,
            '' src_attribute_number_2,
            '' src_attribute_number_3,
            '' src_attribute_number_4,
            '' src_attribute_number_5,
            '' src_attribute_number_6,
            '' src_attribute_number_7,
            '' src_attribute_number_8,
            '' src_attribute_number_9,
            '' src_attribute_number_10,
            '' src_attribute_number_11,
            '' src_attribute_number_12,
            '' src_attribute_number_13,
            '' src_attribute_number_14,
            '' src_attribute_number_15,
            '' src_attribute_number_16,
            '' src_attribute_number_17,
            '' src_attribute_number_18,
            '' src_attribute_number_19,
            '' src_attribute_number_20,
            '' src_attribute_date_1,
            '' src_attribute_date_2,
            '' src_attribute_date_3,
            '' src_attribute_date_4,
            '' src_attribute_date_5,
            '' src_attribute_date_6,
            '' src_attribute_date_7,
            '' src_attribute_date_8,
            '' src_attribute_date_9,
            '' src_attribute_date_10,
            '' request_id,
            '' object_version_number,
            '' created_by,
            '' creation_date,
            '' last_updated_by,
            '' last_update_date,
            '' last_update_login,
            '' legal_entity_country,
            '' legal_entity_address,
            '' payment_term_id,
            '' invoicing_rule_id,
            '' accounting_rule_id,
            '' accounting_rule_duration,
            '' cancelled_flag,
            '' open_flag,
            '' return_reason_code,
            '' mea_flag,
            '' src_attribute_category,
            '' source_org_id,
            '' accounting_effect_flag,
            '' source_type_code,
            '' payment_term_name,
            customer_segment orig_sys_bill_to_cust_site_ref,
            '' orig_sys_ship_to_cust_site_ref,
            customer_segment orig_sys_bill_to_cust_ref,
            '' orig_sys_ship_to_cust_ref,
            '' source_legal_entity_name,
            '' source_exchange_rate_type,
            '' source_organization_name,
            '' source_salesrep_name,
            '' source_payment_term_name,
            '' source_bill_to_cust_num,
            '' source_bill_to_cust_name,
            '' source_bill_to_cust_site_num,
            '' source_bill_to_cust_address,
            '' source_ship_to_cust_num,
            '' source_ship_to_cust_name,
            '' source_ship_to_cust_site_num,
            '' source_ship_to_cust_address,
            'HADOOP' source_system,
            '' sp_first_name,
            '' sp_middle_name,
            '' sp_last_name,
            'HADOOP' source_document_type_code
        from postpaid_summary_row_numbered
        group by
            prc_dt,
            customer_segment,
            po_group,
            contract_id,
            currency_code
    """)
    rmcsHeaderPostpaidDf.registerTempTable("rmcs_header_postpaid")
    
    val rmcsPostpaidOrderedDf = sqlContext.sql("""
        select order_id, content
        from (
            select concat(doc_id_char_1, record_type, '000') order_id, concat('"', concat_ws('"|"', *), '"\r') content from rmcs_header_postpaid
            union all
            select concat(doc_id_char_1, record_type, lpad(line_num, 3, '0')) order_id, concat('"', concat_ws('"|"', *), '"\r') content from rmcs_line_postpaid
            union all
            select concat(doc_id_char_1, record_type, doc_sub_line_id_char_1) order_id, concat('"', concat_ws('"|"', *), '"\r') content from rmcs_subline_postpaid
        ) rmcs_ordered
        order by order_id
    """)
    rmcsPostpaidOrderedDf.repartition(1)
    rmcsPostpaidOrderedDf.persist()
    rmcsPostpaidOrderedDf.registerTempTable("rmcs_postpaid_ordered")
    
    val rmcsPostpaidCsvDf = sqlContext.sql("""
        select content from rmcs_postpaid_ordered
    """)
    rmcsPostpaidCsvDf.coalesce(1)
        .write
        .mode("overwrite")
        .text(pathRmcsPostpaid+"/PRC_DT=" + prcDt)
    
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