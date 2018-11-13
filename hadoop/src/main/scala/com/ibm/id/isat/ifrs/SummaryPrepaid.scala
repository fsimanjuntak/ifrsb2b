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
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat

object SummaryPrepaid {
  def main(args: Array[String]): Unit = {
    val (prcDt, jobId, configDir, inputDir, env) = try {
      (args(0), args(1), args(2), args(3), args(4))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process date> <job identifier> <config directory> <input directory> <LOCAL|PRODUCTION>")
      return
    };
    
    // Initialize Spark Context
    val sc = env match {
      case "LOCAL" => new SparkContext("local[*]", "local spark", new SparkConf())
      case _ => new SparkContext(new SparkConf())
    };
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false"); //Disable metadata parquet
    
    // Initialize Spark SQL
    val sqlContext = new HiveContext(sc)
    
    // Initialize Logging
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    
    // Paths
    val pathDailyUsage = inputDir + "/" + Common.getConfigDirectory(sc, configDir, "IFRS.USAGE.DAILY_PREPAID_CHILD_DIR") 
    val pathDailyNonusage = inputDir + "/" + Common.getConfigDirectory(sc, configDir, "IFRS.NONUSAGE.DAILY_PREPAID_CHILD_DIR")  
    val pathSpdata = inputDir + "/" + Common.getConfigDirectory(sc, configDir, "IFRS.USAGE.SPDATA_CHILD_DIR")
    val pathSalmo = inputDir + "/" + Common.getConfigDirectory(sc, configDir, "IFRS.SALMO.SALMO_CHILD_DIR")
    val pathDataRollover = inputDir + "/" + Common.getConfigDirectory(sc, configDir, "IFRS.DATAROLLOVER.IFRS_DATAROLLOVER_CHILD_DIR")
    val pathRefPrepaidPoTemplate = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PREPAID_PO_TEMPLATE") + "/*.csv"
    val pathRefSalmoPoTemplate = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.SALMO_PO_TEMPLATE") + "/*.csv"
    val pathRefPromocodeTemplate = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.REF_PROMOCODE_TEMPLATE") + "/*.csv"
    val pathRefPrepaidEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PREPAID_EVENT_REFERENCE") + "/*.csv"
    val pathRefSalmoEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.SALMO_EVENT_REFERENCE") + "/*.csv"
    val pathRefProfitCenter = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PROFIT_CENTER") + "/*.csv"
    val pathPrepaidRecon = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.IFRS_PREPAID_RECON_DIR")
    val pathPrepaidTrxSummary = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.PREPAID_TRX_SUMMARY")
    val pathPrepaidUnidentified = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.IFRS_PREPAID_UNIDENTIFIED")
    val pathPrepaidReSummary = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.PREPAID_RE_SUMMARY")
    val pathSpdataSor = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.SP_DATA_SOR")
    val pathBaseInputDir = Common.getConfigDirectory(sc, configDir, "IFRS.INPUT.IFRS_PREPAID_INPUT_DIR")
    val pathRefPrepaidEventReferenceMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_PREPAID_EVENT_REF") + "/*.csv"
    val pathRefPrepaidPoTemplateMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_PREPAID_PO_TEMPL") + "/*.csv"
    
    val refPrepaidPoTemplateMergeDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
	.option("header", "true")
	.option("delimiter", ";")
	.schema(prepaidPoTemplateMergeSchema)
	.load(pathRefPrepaidPoTemplateMerge).cache())
	refPrepaidPoTemplateMergeDf.persist()
	refPrepaidPoTemplateMergeDf.show()
	refPrepaidPoTemplateMergeDf.registerTempTable("ref_prepaid_po_template")
    
    val refPrepaidEventReferenceMergeDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(prepaidEventReferenceMergeSchema)
    .load(pathRefPrepaidEventReferenceMerge).cache())
    refPrepaidEventReferenceMergeDf.show()
    refPrepaidEventReferenceMergeDf.registerTempTable("ref_prepaid_event_reference")
    
    val refPrepaidEventReferenceTransposeDf = sqlContext.sql("""
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_1 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_1,'') <> ''
      union all
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_2 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_2,'') <> ''
      union all
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_3 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_3,'') <> ''
      union all
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_4 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_4,'') <> ''
      union all
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_5 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_5,'') <> ''
      union all
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_6 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_6,'') <> ''
      union all
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_7 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_7,'') <> ''
      union all
      select rev_code, offer_id, contract_template_id, level_4, level_5, level_10, level_11, ctr_desc, ctr_po_8 ctr_po, inst_month_1, inst_month_2, inst_month_3,
           inst_month_4, inst_month_5, inst_month_6, inst_month_7, inst_month_8, inst_month_9, inst_month_10, inst_month_11, inst_month_12, voice_b2c, sms_b2c, data_b2c, vas_b2c, voice_b2b, sms_b2b, data_b2b, vas_b2b, 
           voice_share, sms_share, data_share, vas_share, sp_data, src_tp
      from ref_prepaid_event_reference
      where nvl(ctr_po_8,'') <> ''
        """)
    refPrepaidEventReferenceTransposeDf.filter("rev_code = '12300001143339'").show(false)
    refPrepaidEventReferenceTransposeDf.registerTempTable("ref_prepaid_event_reference_transpose")
    
    val eventReferencePoTemplateDf = broadcast(sqlContext.sql("""
        select
          ref_prepaid_event_reference_transpose.rev_code,
          offer_id, 
          case group when 'PPU' then 'PPU' else contract_template_id end contract_template_id,
          level_4,
          level_5,
          level_10,
          level_11,
          ctr_desc,
          ctr_po, 
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
          voice_b2c, 
          sms_b2c, 
          data_b2c, 
          vas_b2c,
          voice_b2b, 
          sms_b2b, 
          data_b2b, 
          vas_b2b,
          voice_share, 
          sms_share, 
          data_share, 
          vas_share,
          po_template_id,
          group,
          po_template_name,
          allocated,
          satisfaction_measurement_model,
          performance_satisfaction_plan,
          status,
          sub_line_type_code,
          nvl(ref_prepaid_event_reference_transpose_first_po.first_ctr_po, '') <> '' first_po_flag, 
          sp_data,
          ref_prepaid_event_reference_transpose.src_tp src_tp
      from ref_prepaid_event_reference_transpose
      left join ref_prepaid_po_template on ref_prepaid_event_reference_transpose.ctr_po = ref_prepaid_po_template.po_template_id
        and ref_prepaid_event_reference_transpose.src_tp = ref_prepaid_po_template.src_tp
      left join (
          select rev_code,src_tp, min(ctr_po) first_ctr_po
          from ref_prepaid_event_reference_transpose
          group by rev_code,src_tp
      ) ref_prepaid_event_reference_transpose_first_po on ref_prepaid_event_reference_transpose.rev_code = ref_prepaid_event_reference_transpose_first_po.rev_code
          and ref_prepaid_event_reference_transpose.ctr_po = ref_prepaid_event_reference_transpose_first_po.first_ctr_po
          and ref_prepaid_event_reference_transpose.src_tp=ref_prepaid_event_reference_transpose_first_po.src_tp
        """))
    eventReferencePoTemplateDf.persist()
    eventReferencePoTemplateDf.show()
    eventReferencePoTemplateDf.registerTempTable("ref_event_reference_po_template")
    
    val refProfitCenterDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(profitCenterSchema)
    .load(pathRefProfitCenter))
    refProfitCenterDf.persist()
    refProfitCenterDf.show()
    refProfitCenterDf.registerTempTable("ref_profit_center")
    
    val refCityToProfitCenterCityDf = broadcast(sqlContext.sql("""
        select distinct city, pc_id
        from ref_profit_center
        """))
    refCityToProfitCenterCityDf.persist()
    refCityToProfitCenterCityDf.show()
    refCityToProfitCenterCityDf.registerTempTable("ref_city_to_profit_center_city")
    
    val refCityToProfitCenterAliasDf = broadcast(sqlContext.sql("""
        select distinct pc_alias, pc_id
        from ref_profit_center
        """).cache())
    refCityToProfitCenterAliasDf.show()
    refCityToProfitCenterAliasDf.registerTempTable("ref_city_to_profit_center_alias")
    
    val refCityToProfitCenterDf = broadcast(sqlContext.sql("""
        select city, pc_id
        from ref_city_to_profit_center_city
        union all
        select concat_ws('_','pc_alias', pc_alias) city, pc_id
        from ref_city_to_profit_center_alias
        """).cache())
    refCityToProfitCenterDf.show()
    refCityToProfitCenterDf.registerTempTable("ref_city_to_profit_center")
    
    val dailyUsageDf = sqlContext.read
    .option("basePath", pathDailyUsage)
    .schema(schemaDailyUsage)
    .parquet(pathDailyUsage)
    dailyUsageDf.registerTempTable("daily_usage")
    
    val dailyNonusageDf = sqlContext.read
    .option("basePath", pathDailyNonusage)
    .schema(schemaDailyNonusage)
    .parquet(pathDailyNonusage)
    dailyNonusageDf.registerTempTable("daily_nonusage")
    
    val spdataDf = sqlContext.read.format("com.databricks.spark.csv")
    .option("delimiter", ";")
    .schema(schemaSpdata)
    .load(pathSpdata)
    spdataDf.show()
    spdataDf.registerTempTable("spdata")
    
    val salmoDf = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("delimiter", "|")
    .schema(salmoSchema)
    .load(pathSalmo)
	salmoDf.registerTempTable("salmo_source")
    
    val salmoDfFormat = sqlContext.sql("""
      select b_msisdn,
            case when datetime like '%JAN%' then date_format(cast(unix_timestamp(datetime, 'dd-MMM-yy') as timestamp), 'yyyyMMdd') 
              else date_format(cast(unix_timestamp(substr(datetime, 1, 10), 'yyyy-MM-dd') as timestamp), 'yyyyMMdd') 
            end datetime,
            case when reversed_datetime like '%JAN%' then date_format(cast(unix_timestamp(reversed_datetime, 'dd-MMM-yy') as timestamp), 'yyyyMMdd') 
              else date_format(cast(unix_timestamp(substr(reversed_datetime, 1, 10), 'yyyy-MM-dd') as timestamp), 'yyyyMMdd') 
            end reversed_datetime,
            case when reversed_datetime like '%JAN%' then date_format(cast(unix_timestamp(reversed_datetime, 'dd-MMM-yy') as timestamp), 'yyyyMMdd') 
              else date_format(cast( unix_timestamp(reversed_datetime,'yyyy-MM-dd HH:mm:ss')-300 as timestamp), 'yyyyMMdd') 
            end reversed_datetime_cross_day,
            cast(main_price as double) main_price,
            final_transaction_status,
            status_description,
            reversed_status,
            transaction_id,
            --regexp_replace(product_name,unbase64('wqA='),'') product_name,
            case when product_name like 'Internet Unlimited+%7GB' then 'Internet Unlimited+7GB' 
              else product_name
            end product_name,
            nvl(area,'') area
        from salmo_source where
        transaction_type in ('Purchase Data Package','Bulk Purchase Package Transaction')
        and l1_parent_id not like 'D5001%'
      """)
      salmoDfFormat.registerTempTable("salmo_format")
    
    //get yesterday date
    val inputFormat = new SimpleDateFormat("yyyyMMdd")
    val  givenDate = inputFormat.parse(prcDt);
    val cal = Calendar.getInstance();
    cal.setTime(givenDate);
    cal.add(Calendar.DATE, -1);
    val yesterdate = inputFormat.format(cal.getTime());
    
    val salmoDfSuccess = sqlContext.sql("""
      select b_msisdn,
            product_name,
            area,
            datetime,
            datetime original_date,
            main_price,
            transaction_id
        from salmo_format where
        lower(final_transaction_status) = 'completed'
      """)
      salmoDfSuccess.registerTempTable("salmo_success")
    val salmoDfReversed = sqlContext.sql("""
      select b_msisdn,
            product_name,
            area,
            --datetime,
            reversed_datetime datetime,
            datetime original_date,
            -main_price main_price,
            transaction_id
        from salmo_format where
        lower(final_transaction_status) = 'failed' and lower(status_description) = 'reversed'
        and lower(reversed_status)='completed' and datetime<>reversed_datetime
        and datetime<>reversed_datetime_cross_day
        and reversed_datetime<='"""+yesterdate+"""'
      """)
      salmoDfReversed.registerTempTable("salmo_reversed")
    val salmoDfUnion = sqlContext.sql("""
      select * from salmo_success
      union all
      select * from salmo_reversed
      """)
      salmoDfUnion.show()
      salmoDfUnion.registerTempTable("salmo")
    
    val dataRolloverDf = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("delimiter", ";")
    .schema(dataRolloverSchema)
    .load(pathDataRollover)
	  dataRolloverDf.show()
    dataRolloverDf.registerTempTable("data_rollover")
    
    val prepaidDf = sqlContext.sql("""
        select
            prc_dt,
            job_id,
            MSISDN,
            REVENUE_CODE,
            PROMO_PACKAGE_NAME,
            SERVICE_CITY_NAME,
            TRANSACTION_DATE,
            TOTAL_AMOUNT,
            TOTAL_DURATION,
            TOTAL_HIT,
            TOTAL_VOLUME,
            SERVICE_USAGE_TYPE,
            REVENUE_FLAG,
            SERVICE_CLASS_ID,
            OFFER_ATTR_VALUE,
            CUSTOMER_SEGMENT,
            '' SAMO_TXN_ID,
            'CHARGING' SRC_TP
        from daily_usage
        where
            (
                revenue_flag = 'Yes' 
                and (
                    account_id like 'DA%' 
                    or (
                        account_id = 'MA'
                        and substring(revenue_code, 1, 1) not in (0,1,2,3,4,5,6,7,8,9)
                    )
                    or ((account_id is null or account_id = 'REGULER') and revenue_code = 'MMSLOCAL')
                )
            )
            or (
                substring(account_id, 1, 2) in ('MA', 'DA')
                and revenue_flag = 'Yes'
                and substring(revenue_code, 1, 1) in (0,1,2,3,4,5,6,7,8,9)
            )
        union all
        select
            prc_dt,
            job_id,
            '' MSISDN,
            revenue_code REVENUE_CODE,
            promo_package_name PROMO_PACKAGE_NAME,
            city SERVICE_CITY_NAME,
            TRANSACTION_DATE,
            total_amount TOTAL_AMOUNT,
            0 TOTAL_DURATION,
            total_hit TOTAL_HIT,
            0 TOTAL_VOLUME,
            service_type SERVICE_USAGE_TYPE,
            REVENUE_FLAG,
            '' SERVICE_CLASS_ID,
            '' OFFER_ATTR_VALUE,
            '' CUSTOMER_SEGMENT,
            '' SAMO_TXN_ID,
            'CHARGING' SRC_TP
        from daily_nonusage
        where revenue_flag = 'Yes' and total_amount <> 0
        union all
        select
            '' prc_dt,
            '' job_id,
            '' MSISDN,
            rev_code REVENUE_CODE,
            '' PROMO_PACKAGE_NAME,
            city SERVICE_CITY_NAME,
            start_dt TRANSACTION_DATE,
            cast(regexp_replace(amount, '[ ,]', '') as double) TOTAL_AMOUNT,
            0 TOTAL_DURATION,
            cast(regexp_replace(total_hit, '[ ,]', '') as double) TOTAL_HIT,
            0 TOTAL_VOLUME,
            '' SERVICE_USAGE_TYPE,
            '' REVENUE_FLAG,
            '' SERVICE_CLASS_ID,
            '' OFFER_ATTR_VALUE,
            CUSTOMER_SEGMENT,
            '' SAMO_TXN_ID,
            'SPDATA' SRC_TP
        from spdata
        union all
        select
            '' prc_dt,
            '' job_id,
            b_msisdn MSISDN,
            product_name REVENUE_CODE,
            '' PROMO_PACKAGE_NAME,
            nvl(area,'') SERVICE_CITY_NAME,
            datetime TRANSACTION_DATE,
            cast(main_price as double) TOTAL_AMOUNT,
            0 TOTAL_DURATION,
            1 TOTAL_HIT,
            0 TOTAL_VOLUME,
            '' SERVICE_USAGE_TYPE,
            '' REVENUE_FLAG,
            '' SERVICE_CLASS_ID,
            '' OFFER_ATTR_VALUE,
            'B2C SALMO' CUSTOMER_SEGMENT,
            transaction_id SAMO_TXN_ID,
            'SALMO' SRC_TP
        from salmo
        union all
        select
            '' prc_dt,
            '' job_id,
            '' MSISDN,
            rev_code REVENUE_CODE,
            package_name PROMO_PACKAGE_NAME,
            city SERVICE_CITY_NAME,
            start_dt TRANSACTION_DATE,
            TOTAL_AMOUNT,
            0 TOTAL_DURATION,
            0 TOTAL_HIT,
            TOTAL_VOLUME TOTAL_VOLUME,
            'DATA' SERVICE_USAGE_TYPE,
            '' REVENUE_FLAG,
            '' SERVICE_CLASS_ID,
            '' OFFER_ATTR_VALUE,
            CUSTOMER_SEGMENT,
            '' SAMO_TXN_ID,
            'DATARO' SRC_TP  
        from data_rollover
        """)
    prepaidDf.persist()
    prepaidDf.registerTempTable("daily_prepaid")
    
    val prepaidReconDf = sqlContext.sql("""
    select
      SRC_TP,
      TRANSACTION_DATE,
      REVENUE_CODE,
      '"""+prcDt+"""' PRC_DT,
      case when nvl(JOB_ID, '') = '' then '"""+jobId+"""' else JOB_ID end JOB_ID,
      sum(total_amount) REVENUE_WITH_TAX,
      sum(total_amount) / 1.1 REVENUE,
      sum(total_hit) TOTAL_HIT,
      'HADOOP_INPUT' SRC_SYS,
      '""" + prcDt + "_" + jobId + """' PROCESS_ID,
      '' TRANSACTION_DATE_ORIGINAL,
      '' STATUS
    from daily_prepaid where src_tp<>'SALMO'
    group by
      SRC_TP,
      TRANSACTION_DATE,
      REVENUE_CODE,
      PRC_DT,
      JOB_ID
    """)
    
    val prepaidReconInputSalmoDf = sqlContext.sql("""
    select
      'SALMO' SRC_TP,
      datetime TRANSACTION_DATE,
      product_name REVENUE_CODE,
      '""" + prcDt + """' PRC_DT,
      '""" + jobId + """' JOB_ID,
      sum(cast(main_price as double)) REVENUE_WITH_TAX,
      sum(cast(main_price as double)) / 1.1 REVENUE,
      0 TOTAL_HIT,
      'HADOOP_INPUT' SRC_SYS,
      '""" + prcDt + "_" + jobId + """' PROCESS_ID,
      original_date TRANSACTION_DATE_ORIGINAL,
      case when main_price < 0 then 'REVERSED' else 'SUCCESS' end STATUS
    from salmo
    group by
      datetime,
      product_name,
      original_date,
      case when main_price < 0 then 'REVERSED' else 'SUCCESS' end
    """)
    
    val prepaidTransformDf = sqlContext.sql("""
      select
          '""" + prcDt + """' PRC_DT,
          '""" + jobId + """' JOB_ID,
          daily_prepaid.SRC_TP,
          'PREPAID' SUBSCRIBER_TYPE,
          case when nvl(CUSTOMER_SEGMENT, '') = '' then 'B2C PREPAID' else CUSTOMER_SEGMENT end CUSTOMER_SEGMENT,
          SAMO_TXN_ID SALMO_TRX_ID,
          MSISDN MSISDN,
          REVENUE_CODE REV_CODE,
          SERVICE_CITY_NAME CITY,
          TRANSACTION_DATE START_DT,
          TRANSACTION_DATE END_DT,
          case when ref_event_reference_po_template.first_po_flag = false then 0
              else TOTAL_AMOUNT
          end LINE_AMT_WITH_TAX,
          case when ref_event_reference_po_template.first_po_flag = false then 0
              else TOTAL_AMOUNT / 1.1
          end LINE_AMT,
          case when not ref_event_reference_po_template.first_po_flag then 0
          else TOTAL_DURATION
          end DURATION,
          case when not ref_event_reference_po_template.first_po_flag then 0
          else TOTAL_VOLUME
          end VOLUME,
          case when not ref_event_reference_po_template.first_po_flag then 0
          else TOTAL_HIT
          end HITS,
          case when not ref_event_reference_po_template.first_po_flag then 1
          else
              case SERVICE_USAGE_TYPE
                  when 'VOICE' THEN case TOTAL_DURATION when 0 then 1 else TOTAL_DURATION end
                  when 'SMS' THEN case TOTAL_HIT when 0 then 1 else TOTAL_HIT end
                  when 'DATA' THEN case TOTAL_VOLUME when 0 then 1 else TOTAL_VOLUME end
              else case TOTAL_HIT when 0 then 1 else TOTAL_HIT end
              end
          end QTY,
          PROMO_PACKAGE_NAME,
          'zzy' UOM,
          nvl(pc_id, '1000000000') PROFIT_CENTER,
          ref_event_reference_po_template.contract_template_id CONTRACT_ID_ORIGINAL,
          hex(hash(concat_ws('',ref_event_reference_po_template.contract_template_id,'""" + prcDt + """'))) CONTRACT_ID,
          ref_event_reference_po_template.ctr_desc CONTRACT_NAME,
          ref_event_reference_po_template.po_template_id PO_ID,
          OFFER_ID,
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
          sub_line_type_code,
          ref_event_reference_po_template.po_template_name PO_NAME,
          ref_event_reference_po_template.group PO_GROUP,
          ref_event_reference_po_template.satisfaction_measurement_model SATISFACTION_MEASURE_MODEL,
          ref_event_reference_po_template.performance_satisfaction_plan PERF_SATISFACTION_PLAN,
          ref_event_reference_po_template.allocated ALLOCATED,
          ref_event_reference_po_template.sp_data SP_DATA,
          TOTAL_AMOUNT SOURCE_TOTAL_AMOUNT,
          voice_b2c, 
          sms_b2c, 
          data_b2c, 
          vas_b2c,
          voice_b2b, 
          sms_b2b, 
          data_b2b, 
          vas_b2b,
          voice_share, 
          sms_share, 
          data_share, 
          vas_share,
          case when (
              allocated = 'Y' 
              and ((
                    nvl(voice_b2c, '') = ''
                    and nvl(sms_b2c, '') = ''
                    and nvl(data_b2c, '') = ''
                    and nvl(vas_b2c, '') = '')
                  or (
                    nvl(voice_b2b, '') = ''
                    and nvl(sms_b2b, '') = ''
                    and nvl(data_b2b, '') = ''
                    and nvl(vas_b2b, '') = '')
                  or (
                    nvl(voice_share, '') = ''
                    and nvl(sms_share, '') = ''
                    and nvl(data_share, '') = ''
                    and nvl(vas_share, '') = ''))
              ) then 'Share not found'
            when nvl(ref_event_reference_po_template.rev_code, '') = '' then 'REV_CODE not found'
            when nvl(ref_event_reference_po_template.po_template_id, '') = '' then 'PO_ID not found'
            else ''
          end ERROR_MESSAGE
      from daily_prepaid
      left join ref_city_to_profit_center
        on upper(
            case when daily_prepaid.SRC_TP='SALMO' then concat_ws('_','pc_alias', (
					  case when daily_prepaid.service_city_name='' then 'DEFAULT' 
					  else daily_prepaid.service_city_name 
                      end
                      )
                    ) 
            else daily_prepaid.service_city_name
            end
            ) = upper(ref_city_to_profit_center.city)
      left join ref_event_reference_po_template
        on upper(daily_prepaid.revenue_code) = upper(ref_event_reference_po_template.rev_code)
        and (
          (case when daily_prepaid.SRC_TP <> 'SALMO' then 'NONSALMO' else 'SALMO' end) = ref_event_reference_po_template.src_tp
        )
        """)
    prepaidTransformDf.registerTempTable("prepaid_transform")
    
    val prepaidTrxSummaryDf = sqlContext.sql("""
      select
          PRC_DT,
          JOB_ID,
          SRC_TP,
          SUBSCRIBER_TYPE,
          CUSTOMER_SEGMENT,
          SALMO_TRX_ID,
          MSISDN,
          REV_CODE,
          CITY,
          START_DT,
          END_DT,
          LINE_AMT_WITH_TAX,
          LINE_AMT,
          DURATION,
          VOLUME,
          HITS,
          QTY,
          PROMO_PACKAGE_NAME,
          UOM,
          PROFIT_CENTER,
          CONTRACT_ID_ORIGINAL,
          CONTRACT_ID,
          CONTRACT_NAME,
          PO_ID,
          OFFER_ID,
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
          sub_line_type_code,
          PO_NAME,
          PO_GROUP,
          SATISFACTION_MEASURE_MODEL,
          PERF_SATISFACTION_PLAN,
          ALLOCATED,
          START_DT TRANSACTION_DATE
      from prepaid_transform
      where ERROR_MESSAGE = '' and SOURCE_TOTAL_AMOUNT <> 0
        """);
    prepaidTrxSummaryDf.registerTempTable("prepaid_trx_summary")
    
    val detailUnidentifiedDf = sqlContext.sql("""
        select 
            PRC_DT,
            JOB_ID,
            SRC_TP,
            SUBSCRIBER_TYPE,
            CUSTOMER_SEGMENT,
            SALMO_TRX_ID,
            MSISDN,
            REV_CODE,
            CITY,
            START_DT,
            END_DT,
            LINE_AMT_WITH_TAX,
            LINE_AMT,
            DURATION,
            VOLUME,
            HITS,
            QTY,
            PROMO_PACKAGE_NAME,
            UOM,
            PROFIT_CENTER,
            CONTRACT_ID_ORIGINAL,
            CONTRACT_ID,
            CONTRACT_NAME,
            PO_ID,
            OFFER_ID,
            PO_NAME,
            PO_GROUP,
            SATISFACTION_MEASURE_MODEL,
            PERF_SATISFACTION_PLAN,
            ALLOCATED,
            ERROR_MESSAGE
        from prepaid_transform
        where ERROR_MESSAGE <> ''
        """);
    detailUnidentifiedDf.registerTempTable("detail_unidentified")
    
    val prepaidReSummaryDf = sqlContext.sql("""
      select
          PRC_DT,
          JOB_ID,
          first(SRC_TP) SRC_TP,
          SUBSCRIBER_TYPE,
          CUSTOMER_SEGMENT,
          REV_CODE,
          START_DT,
          END_DT,
          sum(LINE_AMT_WITH_TAX) LINE_AMT_WITH_TAX,
          cast(sum(LINE_AMT) as decimal(18,0)) LINE_AMT,
          sum(QTY) QTY,
          UOM,
          PROFIT_CENTER,
          first(CONTRACT_ID_ORIGINAL) CONTRACT_ID_ORIGINAL,
          CONTRACT_ID,
          CONTRACT_NAME,
          PO_ID,
          OFFER_ID,
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
          first(sub_line_type_code) sub_line_type_code,
          PO_NAME,
          PO_GROUP,
          SATISFACTION_MEASURE_MODEL,
          PERF_SATISFACTION_PLAN,
          ALLOCATED,
          START_DT TRANSACTION_DATE
      from prepaid_trx_summary
      group by
          PRC_DT,
          JOB_ID,
          SUBSCRIBER_TYPE,
          CUSTOMER_SEGMENT,
          REV_CODE,
          START_DT,
          END_DT,
          UOM,
          PROFIT_CENTER,
          CONTRACT_ID,
          CONTRACT_NAME,
          PO_ID,
          OFFER_ID,
          PO_NAME,
          PO_GROUP,
          SATISFACTION_MEASURE_MODEL,
          PERF_SATISFACTION_PLAN,
          ALLOCATED
        """)
    prepaidReSummaryDf.persist()
    prepaidReSummaryDf.registerTempTable("prepaid_re_summary")
    
    val reSmyReconDf = sqlContext.sql("""
    select
      SRC_TP,
      start_dt TRANSACTION_DATE,
      REV_CODE REVENUE_CODE,
      PRC_DT,
      JOB_ID,
      sum(line_amt_with_tax) REVENUE_WITH_TAX,
      sum(line_amt) REVENUE,
      0 TOTAL_HIT,
      'HADOOP_OUTPUT' SRC_SYS,
      '""" + prcDt + "_" + jobId + """' PROCESS_ID,
      '' TRANSACTION_DATE_ORIGINAL,
      '' STATUS
    from prepaid_re_summary
    group by
      SRC_TP,
      START_DT,
      REV_CODE,
      PRC_DT,
      JOB_ID
    """)
    

    Common.cleanDirectoryWithPattern(sc, pathPrepaidRecon, "/TRANSACTION_DATE="+"*"+"/PRC_DT="+prcDt+"/SRC_SYS=HADOOP_INPUT")
    Common.cleanDirectoryWithPattern(sc, pathPrepaidRecon, "/TRANSACTION_DATE="+"*"+"/PRC_DT="+prcDt+"/SRC_SYS=HADOOP_OUTPUT")
    prepaidReconDf.unionAll(reSmyReconDf).unionAll(prepaidReconInputSalmoDf).repartition(20)
    .write.partitionBy("TRANSACTION_DATE", "PRC_DT", "SRC_SYS")
    .mode("append")
    .parquet(pathPrepaidRecon)
    
    Common.cleanDirectoryWithPattern(sc, pathPrepaidTrxSummary, "/PRC_DT="+prcDt+"/TRANSACTION_DATE="+"*")
    prepaidTrxSummaryDf.repartition(20).write.partitionBy("PRC_DT","TRANSACTION_DATE")
    .mode("append")
    .save(pathPrepaidTrxSummary)
    
    Common.cleanDirectory(sc, pathPrepaidUnidentified + "/PRC_DT="+prcDt)
    detailUnidentifiedDf.repartition(1)
    .write.format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("delimiter", "|").option("header", "true")
    .save(pathPrepaidUnidentified + "/PRC_DT="+prcDt)
    
//    Common.cleanDirectory(sc, pathPrepaidReSummary+"/PRC_DT="+prcDt)
    Common.cleanDirectoryWithPattern(sc, pathPrepaidReSummary, "/PRC_DT="+prcDt+"/TRANSACTION_DATE="+"*")
    prepaidReSummaryDf.repartition(1).write.partitionBy("PRC_DT","TRANSACTION_DATE")
    .mode("append").save(pathPrepaidReSummary)
    
    if (detailUnidentifiedDf.count() != 0) {
      rootLogger.info("====job ifrs summary prepaid failed====")
    }
    else {
      rootLogger.info("====job ifrs summary prepaid finished====")
    }
    
  }
  
  val profitCenterSchema: StructType = new StructType(Array(
    StructField("prefix", StringType, true),
    StructField("length", StringType, true),
    StructField("code", StringType, true),
    StructField("city", StringType, true),
    StructField("pc_name", StringType, true),
    StructField("pc_alias", StringType, true),
    StructField("pc_id", StringType, true),
    StructField("group", StringType, true)))
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
    StructField("vas_b2b", StringType, true),
    StructField("sp_data", StringType, true)))
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
val prepaidPoTemplateSchema = new StructType(Array(
    StructField("po_template_id", StringType, true),
    StructField("group", StringType, true),
    StructField("po_template_name", StringType, true),
    StructField("allocated", StringType, true),
    StructField("satisfaction_measurement_model", StringType, true),
    StructField("performance_satisfaction_plan", StringType, true),
    StructField("sub_line_type_code", StringType, true),
    StructField("status", StringType, true)))
val prepaidPoTemplateMergeSchema = new StructType(Array(
    StructField("po_template_id", StringType, true),
    StructField("group", StringType, true),
    StructField("po_template_name", StringType, true),
    StructField("allocated", StringType, true),
    StructField("satisfaction_measurement_model", StringType, true),
    StructField("performance_satisfaction_plan", StringType, true),
    StructField("sub_line_type_code", StringType, true),
    StructField("status", StringType, true),
    StructField("src_tp", StringType, true)))
val intcctSchema = StructType(Array(
    StructField("intcctPfxStart", StringType, true),
    StructField("intcctPfxEnd", StringType, true),
    StructField("intcctPfx", StringType, true),
    StructField("intcctCty", StringType, true),
    StructField("intcctOpr", StringType, true),
    StructField("locName", StringType, true),
    StructField("effDt", StringType, true),
    StructField("endDt", StringType, true),
    StructField("intcctTp", StringType, true)))
  val svcClassOfrSchema: StructType = StructType(Array(
    StructField("svcClassCode", StringType, true),
    StructField("svcClassCodeOld", StringType, true),
    StructField("svcClassName", StringType, true),
    StructField("offerId", StringType, true),
    StructField("offerAttrName", StringType, true),
    StructField("offerAttrValue", StringType, true),
    StructField("areaName", StringType, true),
    StructField("prmPkgCode", StringType, true),
    StructField("prmPkgName", StringType, true),
    StructField("brndSCName", StringType, true),
    StructField("effDt", StringType, true),
    StructField("endDt", StringType, true),
    StructField("customerTp", StringType, true)))
  val schemaDailyUsage: StructType = StructType(Array(
    StructField("MSISDN", StringType, true),
    StructField("PREFIX_NUMBER", StringType, true),
    StructField("SERVICE_CITY_NAME", StringType, true),
    StructField("SERVICE_PROVIDER_ID", StringType, true),
    StructField("COUNTRY_NAME", StringType, true),
    StructField("HLR_BRANCH_NM", StringType, true),
    StructField("HLR_REGION_NM", StringType, true),
    StructField("OTHER_PREFIX_NO", StringType, true),
    StructField("DESTINATION_CITY_NAME", StringType, true),
    StructField("DESTINATION_PROVIDER_ID", StringType, true),
    StructField("DESTINATION_COUNTRY_NAME", StringType, true),
    StructField("SERVICE_CLASS_ID", StringType, true),
    StructField("PROMO_PACKAGE_CODE", StringType, true),
    StructField("PROMO_PACKAGE_NAME", StringType, true),
    StructField("BRAND_NAME", StringType, true),
    StructField("MSC_ADDRESS", StringType, true),
    StructField("ORIGINAL_REALM", StringType, true),
    StructField("ORIGINAL_HOST", StringType, true),
    StructField("MCCMNC", StringType, true),
    StructField("LAC", StringType, true),
    StructField("CI", StringType, true),
    StructField("LACI_CLUSTER_ID", StringType, true),
    StructField("LACI_CLUSTER_NM", StringType, true),
    StructField("LACI_REGION_ID", StringType, true),
    StructField("LACI_REGION_NM", StringType, true),
    StructField("LACI_AREA_ID", StringType, true),
    StructField("LACI_AREA_NM", StringType, true),
    StructField("LACI_SALESAREA_ID", StringType, true),
    StructField("LACI_SALESAREA_NM", StringType, true),
    StructField("IMSI", StringType, true),
    StructField("APN", StringType, true),
    StructField("SERVICE_SCENARIO", StringType, true),
    StructField("ROAMING_POSITION", StringType, true),
    StructField("FAF", StringType, true),
    StructField("RATING_GROUP", StringType, true),
    StructField("CONTENT_TYPE", StringType, true),
    StructField("IMEI", StringType, true),
    StructField("GGSN_ADDRESS", StringType, true),
    StructField("SGSN_ADDRESS", StringType, true),
    StructField("RAT_TYPE", StringType, true),
    StructField("SERVICE_OFFERING_ID", StringType, true),
    StructField("SUBSCRIBER_TYPE", StringType, true),
    StructField("ACCOUNT_ID", StringType, true),
    StructField("ACCOUNT_GROUPID", StringType, true),
    StructField("TRAFFIC_CASE", StringType, true),
    StructField("REVENUE_CODE", StringType, true),
    StructField("DIRECTION_TYPE", StringType, true),
    StructField("DISTANCE_TYPE", StringType, true),
    StructField("SERVICE_TYPE", StringType, true),
    StructField("SERVICE_USAGE_TYPE", StringType, true),
    StructField("SVC_USG_DIRECTION", StringType, true),
    StructField("SVC_USG_DESTINATION", StringType, true),
    StructField("TRAFFIC_FLAG", StringType, true),
    StructField("REVENUE_FLAG", StringType, true),
    StructField("TOTAL_VOLUME", DoubleType, true),
    StructField("TOTAL_AMOUNT", DoubleType, true),
    StructField("TOTAL_DURATION", DoubleType, true),
    StructField("TOTAL_HIT", IntegerType, true),
    StructField("COMMUNITY_ID_1", StringType, true),
    StructField("COMMUNITY_ID_2", StringType, true),
    StructField("COMMUNITY_ID_3", StringType, true),
    StructField("ACCUMULATED_COST", DoubleType, true),
    StructField("RECORD_TYPE", StringType, true),
    StructField("ECI", StringType, true),
    StructField("SITE_TECH", StringType, true),
    StructField("SITE_OPERATOR", StringType, true),
    StructField("SVC_USG_TRAFFIC", StringType, true),
    StructField("MGR_SVCCLSS_ID", StringType, true),
    StructField("OFFER_ID", StringType, true),
    StructField("OFFER_ATTR_KEY", StringType, true),
    StructField("OFFER_ATTR_VALUE", StringType, true),
    StructField("OFFER_AREA_NAME", StringType, true),
    StructField("PRC_DT", StringType, true),
    StructField("MICROCLUSTER_NAME", StringType, true),
    StructField("CUSTOMER_SEGMENT", StringType, true),
    StructField("transaction_date", IntegerType, true),
    StructField("job_id", IntegerType, true),
    StructField("src_tp", StringType, true)))
  val schemaDailyNonusage: StructType = StructType(Array(
    StructField("MSISDN", StringType, true),
    StructField("PREFIX", StringType, true),
    StructField("CITY", StringType, true),
    StructField("PROVIDER_ID", StringType, true),
    StructField("COUNTRY", StringType, true),
    StructField("SERVICECLASSID", StringType, true),
    StructField("PROMO_PACKAGE_CODE", StringType, true),
    StructField("PROMO_PACKAGE_NAME", StringType, true),
    StructField("BRAND_NAME", StringType, true),
    StructField("TOTAL_AMOUNT", DoubleType, true),
    StructField("TOTAL_HIT", DoubleType, true),
    StructField("REVENUE_CODE", StringType, true),
    StructField("SERVICE_TYPE", StringType, true),
    StructField("GL_CODE", StringType, true),
    StructField("GL_NAME", StringType, true),
    StructField("REAL_FILENAME", StringType, true),
    StructField("HLR_BRANCH_NM", StringType, true),
    StructField("HLR_REGION_NM", StringType, true),
    StructField("REVENUE_FLAG", StringType, true),
    StructField("MCCMNC", StringType, true),
    StructField("LAC", StringType, true),
    StructField("CI", StringType, true),
    StructField("LACI_CLUSTER_ID", StringType, true),
    StructField("LACI_CLUSTER_NM", StringType, true),
    StructField("LACI_REGION_ID", StringType, true),
    StructField("LACI_REGION_NM", StringType, true),
    StructField("LACI_AREA_ID", StringType, true),
    StructField("LACI_AREA_NM", StringType, true),
    StructField("LACI_SALESAREA_ID", StringType, true),
    StructField("LACI_SALESAREA_NM", StringType, true),
    StructField("ORIGIN_TIMESTAMP", StringType, true),
    StructField("MGR_SVCCLS_ID", StringType, true),
    StructField("OFFER_ID", StringType, true),
    StructField("OFFER_AREA_NAME", StringType, true),
    StructField("RECORD_ID", StringType, true),
    StructField("PRC_DT", StringType, true),
    StructField("FILE_DT", StringType, true),
    StructField("CUSTOMER_SEGMENT", StringType, true),
    StructField("SRC_TP", StringType, true),
    StructField("TRANSACTION_DATE", IntegerType, true),
    StructField("JOB_ID", IntegerType, true)))
  val schemaSpdata: StructType = StructType(Array(
    StructField("PRC_DT", StringType, true),
    StructField("JOB_ID", StringType, true),
    StructField("START_DT", StringType, true),
    StructField("REV_CODE", StringType, true),
    StructField("CITY", StringType, true),
    StructField("TOTAL_HIT", StringType, true),
    StructField("CUSTOMER_SEGMENT", StringType, true),
    StructField("PRICE", StringType, true),
    StructField("AMOUNT", StringType, true)))
  val salmoSchema: StructType = new StructType(Array(
  	StructField("datetime",StringType, true),
    StructField("initiatetime",StringType, true),
    StructField("transaction_type",StringType, true),
    StructField("channel",StringType, true),
    StructField("region",StringType, true),
    StructField("area",StringType, true),
    StructField("sales_area",StringType, true),
    StructField("cluster",StringType, true),
    StructField("additional_territory",StringType, true),
    StructField("transaction_id",StringType, true),
    StructField("l1_parent_id",StringType, true),
    StructField("l1_parent_name",StringType, true),
    StructField("additional_parent_id",StringType, true),
    StructField("additional_parent_name",StringType, true),
    StructField("transaction_channel",StringType, true),
    StructField("organization_type",StringType, true),
    StructField("organization_id",StringType, true),
    StructField("organization_name",StringType, true),
    StructField("operator_id",StringType, true),
    StructField("operator_name",StringType, true),
    StructField("operator_type",StringType, true),
    StructField("msisdn",StringType, true),
    StructField("user_name",StringType, true),
    StructField("a_longitude",StringType, true),
    StructField("a_latitude",StringType, true),
    StructField("a_lac_dec",StringType, true),
    StructField("a_ci_dec",StringType, true),
    StructField("a_real_rime_territory",StringType, true),
    StructField("b_msisdn",StringType, true),
    StructField("dompetku_msisdn",StringType, true),
    StructField("b_lac_dec",StringType, true),
    StructField("b_ci_dec",StringType, true),
    StructField("b_real_rime_territory",StringType, true),
    StructField("bill_number",StringType, true),
    StructField("voucher_type",StringType, true),
    StructField("product_group",StringType, true),
    StructField("product_name",StringType, true),
    StructField("main_price",StringType, true),
    StructField("discount",StringType, true),
    StructField("amount_debit",StringType, true),
    StructField("tax",StringType, true),
    StructField("final_transaction_status",StringType, true),
    StructField("status_description",StringType, true),
    StructField("pre_balance",StringType, true),
    StructField("post_balance",StringType, true),
    StructField("additional_parameter1",StringType, true),
    StructField("additional_parameter2",StringType, true),
    StructField("additional_parameter3",StringType, true),
    StructField("additional_parameter4",StringType, true),
    StructField("additional_parameter5",StringType, true),
    StructField("reversal_transaction_id",StringType, true),
    StructField("reversed_datetime",StringType, true),
    StructField("reversed_status",StringType, true),
    StructField("acommissionresult",StringType, true),
    StructField("bcommissionresult",StringType, true),
    StructField("adiscountresult",StringType, true),
    StructField("bdiscountresult",StringType, true),
    StructField("discountrulename",StringType, true),
    StructField("commissionschema",StringType, true),
    StructField("msisdnterritoryverificationresult",StringType, true)))
  val dataRolloverSchema: StructType = new StructType(Array(
  	StructField("PRC_DT",StringType, true),
  	StructField("JOB_ID",StringType, true),
  	StructField("START_DT",StringType, true),
  	StructField("PURCHASE_TYPE",StringType, true),
  	StructField("PACKAGE_NAME",StringType, true),
  	StructField("TOTAL_MSISDN",StringType, true),
  	StructField("TOTAL_VOLUME",StringType, true),
  	StructField("CITY",StringType, true),
  	StructField("CUSTOMER_SEGMENT",StringType, true),
  	StructField("REV_CODE",StringType, true),
  	StructField("ARKB",StringType, true),
  	StructField("TOTAL_AMOUNT",StringType, true)))
}