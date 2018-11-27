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

object B2bPostpaidInitialRunning {
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
    val sqlContext = new HiveContext(sc)
    
    // Initialize File System (for renameFile)
    val fs = FileSystem.get(sc.hadoopConfiguration);
    
    // Initialize Logging
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    rootLogger.setLevel(Level.OFF)
    
    //TODO Input Paths
    val pathCweoDailyAsset = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_ASSET")
    val pathCweoDailyAgreement = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_AGREEMENT")
    val pathCweoDailyCstacc = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_CSTACC")
    val pathCweoDailyOrder = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_ORDER")
    val pathIfrsBillCharge = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.RBM_IFRS_BILL_CHARGE")
    
    val pathB2bIfrsBillingTransform = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_TRANSFORM_BILLING_SOR")
    val pathB2bIfrsRevenueTransform = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_TRANSFORM_REVENUE_SOR")
    val pathB2bIfrsRevenueCsv = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_REVENUE_CSV")
    val pathB2bIfrsBilingCsv = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_BILING_CSV")
    val pathB2bIfrsReconcilliationCsv = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_RECONCILLIATION_CSV")
    
    val pathB2bIfrsTransformEventTypeTempCsv = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.TEMP.B2B_IFRS_EVENT_TYPE")
    
    val pathRefGroupOfServices = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.REFERENCE.IFRS_B2B_REF_GROUP_OF_SERVICES")
    val pathRefB2mBranchRegion = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.REFERENCE.IFRS_B2B_REF_B2M_BRANCH_REGION")
    val pathRefProfitCenter = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.REFERENCE.IFRS_B2B_REF_PROFIT_CENTER")
    val pathRefCustomerGroup = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.REFERENCE.IFRS_B2B_REF_CUSTOMER_GROUP")
    
    var hh = Calendar.getInstance().get(Calendar.HOUR_OF_DAY).toString()
    var mm=""
    if (Calendar.getInstance().get(Calendar.MINUTE) > 10)
      mm = Calendar.getInstance().get(Calendar.MINUTE).toString()
    else
      mm = ("0" + Calendar.getInstance().get(Calendar.MINUTE)).toString()
    var ss=""
    if (Calendar.getInstance().get(Calendar.SECOND) > 10)
      ss = Calendar.getInstance().get(Calendar.SECOND).toString()
    else
      ss = "0" + Calendar.getInstance().get(Calendar.SECOND).toString()
    
    val inputDate = "20180804"
    
    //TODO Main Transformation
    val dailyAssetDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathCweoDailyAsset + "/file_date="+inputDate))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathCweoDailyAsset)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(cweoDailyAssetSchema)
      .load(p + "/*.txt")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    dailyAssetDf.registerTempTable("daily_asset")
    
    
    val dailyAgreementDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathCweoDailyAgreement + "/file_date="+inputDate))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathCweoDailyAgreement)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(cweoDailyAgreementSchema)
      .load(p + "/*.txt")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    dailyAgreementDf.registerTempTable("daily_agreement")
    
    
    val dailyCstaccDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathCweoDailyCstacc + "/file_date="+inputDate))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathCweoDailyCstacc)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(cweoDailyCstaccSchema)
      .load(p + "/*.txt")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    .distinct()
    dailyCstaccDf.registerTempTable("daily_cstacc")
    
    val dailyOrderDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathCweoDailyOrder + "/file_date="+inputDate))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathCweoDailyOrder)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(cweoDailyOrderSchema)
      .load(p + "/*.txt")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    dailyOrderDf.registerTempTable("daily_order")
    /*
    val evenTypeTempFile = fs.globStatus(
    new Path(pathB2bIfrsTransformEventTypeTempCsv + "/ifrs*"))(0).getPath().getName()*/
    val eventTypeTempDf = broadcast(
       sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter","|")
      .load(pathB2bIfrsTransformEventTypeTempCsv)//+"/"+evenTypeTempFile)
      .cache())
      .distinct()
      eventTypeTempDf.registerTempTable("event_type_temp")
      
      
    val billChargeDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathIfrsBillCharge + "/file_date="+inputDate))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathIfrsBillCharge)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(ifrsBillChargeSchema)
      .load(p + "/*.dat")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    //.withColumn("PRODUCT_END", when(col("CHRG_TP") === "INSTALLATION", col("PRODUCT_START")).otherwise(col("PRODUCT_END")))
    .filter("SUBSCRIPTION_TYPE not in ('MOBILE','IPHONE','BULK','STARONE')") //filter data, update from 5530 to 5319
    billChargeDf.registerTempTable("bill_charge")
    
    
    //TODO
    // References
    val refGroupOfServicesDf = ReferenceDF.getGroupOfServicesDF(sqlContext, pathRefGroupOfServices + "/*.csv")
    refGroupOfServicesDf.registerTempTable("ref_group_of_services_logic")
    sqlContext.sql("""
    select 
    business_area_name,
    substring_index(concat_ws(',',collect_list(group_of_services)),',', 1) group_of_services
    from (
      select distinct business_area_name,group_of_services
      from ref_group_of_services_logic) a
    group by business_area_name

    """).registerTempTable("ref_group_of_services")
    
    
    val refB2mBranchRegion = broadcast(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("header", "true")
        .load(pathRefB2mBranchRegion)
        .cache())
    refB2mBranchRegion.registerTempTable("ref_b2m_branch_region")
    
    val refProfitCenterDf = broadcast(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", "|")
        .schema(SummaryPostpaid.profitCenterSchema)
        .load(pathRefProfitCenter)
        .cache())
    refProfitCenterDf.registerTempTable("ref_profit_center")
    
    
    val refCityToProfitCenterDf = broadcast(sqlContext.sql("""
        select distinct city, pc_id
        from ref_profit_center
    """).cache())
    refCityToProfitCenterDf.registerTempTable("ref_city_to_profit_center")
    
    val refCustomerGroupDf = ReferenceDF.getCustomerGroupDF(sqlContext, pathRefCustomerGroup + "/*.csv")
    refCustomerGroupDf.registerTempTable("ref_customer_group")
    
    
    //TODO Output
    // - PRODUCT_END
    // - PROFIT_CENTER
    val b2bIfrsTransform = sqlContext.sql("""
        select
        '"""+prcDt+"""' PRC_DT,
        '"""+jobId+"""' JOB_ID,
        bill_charge.ACCOUNT_NUM ACCOUNT_NUM,
        bill_charge.CUST_REF CUSTOMER_REF,
        
        
        --customer_account
        daily_cstacc.CA_NM CUSTOMER_NAME,--v1 concat(daily_cstacc.CUST_SALUTATION, '_', daily_cstacc.CA_NM) CUSTOMER_NAME,
        daily_cstacc.CUST_SALUTATION CA_SALUTATION,
        daily_cstacc.CA_NM CA_NM,

        --agreement
        --case when parent_asset.AGRMT_NUM = '' then bill_charge.AGRMT_NUM else parent_asset.AGRMT_NUM end AGREEMENT_NUM,
        --substr(daily_agreement.AGRMT_NM, 1, 50) AGREEMENT_NAME,
        parent_asset.AGRMT_NUM AGREEMENT_NUM,
        case when daily_agreement.AGRMT_NM = '' then bill_charge.AGRMT_NUM else daily_agreement.AGRMT_NM end AGREEMENT_NAME,
        nvl(daily_agreement.CONTRACT_PERIOD, '0') CONTRACT_PERIOD,
        cast(daily_agreement.CONTRACT_INITIAL_AMT as DOUBLE) CONTRACT_INITIAL_AMT,
        date_format(from_unixtime(unix_timestamp(daily_agreement.AGRMT_START, 'mm/dd/yyyy')), 'yyyy-mm-dd') AGREEMENT_START, -- v1 'yyyy-MM-dd'
        date_format(from_unixtime(unix_timestamp(daily_agreement.AGRMT_END, 'mm/dd/yyyy')), 'yyyy-mm-dd') AGREEMENT_END, -- v1 'yyyy-MM-dd'

    
        bill_charge.SUBS_PD_ID SUBS_PRODUCT_ID,
        bill_charge.SUBSCRIPTION_TYPE SUBSCRIPTION_TYPE,
        bill_charge.SUBS_REF SUBSCRIPTION_REF,

        daily_asset_product_type.PD_TP PRODUCT_TYPE,
        parent_asset.PD_CGY PRODUCT_CATEGORY,
        case when nvl(bill_charge.SVC_ID, '') = '' then parent_asset.SVC_ID
          else bill_charge.SVC_ID
        end SERVICE_ID,
        
        case when bill_charge.PRODUCT_ID = '1' and (parent_asset.PD_CGY = 'Mobile' or  parent_asset.PD_CGY = 'Corporate Mobile Bulk') then bill_charge.PRODUCT_NM
          else case when bill_charge.CHRG_TP in ('MRC', 'INSTALLATION') then
              service_asset.PD_NM
            when bill_charge.CHRG_TP = 'ONE TIME CHARGE' then parent_asset_otc_ast_id.PD_NM
            end
          end BUSINESS_AREA_NAME,
         
        date_format(substr(bill_charge.SVC_START_DT,0,10),'yyyy-MM-dd') SERVICE_START,
        date_format(substr(bill_charge.SVC_END_DT,0,10),'yyyy-MM-dd') SERVICE_END,

       customer_segment.PRODUCT_CATEGORY CUSTOMER_SEGMENT,

        bill_charge.PD_SEQ PRODUCT_SEQ,
        bill_charge.PRODUCT_ID PRODUCT_ID,
        bill_charge.PRODUCT_NM PRODUCT_NAME,
        bill_charge.OTC_ID OTC_ID,
        bill_charge.OTC_SEQ OTC_SEQ,
        bill_charge.CURRENCY_CODE CURRENCY_CODE,
        bill_charge.CHRG_TP CHARGE_TYPE,

        cast(bill_charge.CHARGE_ORI as DOUBLE) CHARGE_ORIGINAL,
        cast(bill_charge.CHARGE_IDR as DOUBLE) CHARGE_IDR,
        date_format(substr(bill_charge.CHRG_START,0,10),'yyyy-MM-dd') CHARGE_START_DT,
        date_format(substr(
          case 
              when bill_charge.CHRG_TP = 'ONE TIME CHARGE' then bill_charge.CHRG_START 
              else bill_charge.CHRG_END 
          end
        ,0,10),'yyyy-MM-dd') CHARGE_END_DT,
        date_format(substr(
        case 
            when bill_charge.CHRG_TP = 'ONE TIME CHARGE' then bill_charge.CHRG_START
            else bill_charge.PRODUCT_START
        end
        ,0,10),'yyyy-MM-dd') PRODUCT_START,
        date_format(substr(
          case when bill_charge.CHRG_TP = 'INSTALLATION' and if(bill_charge.PRODUCT_END = '', null,bill_charge.PRODUCT_END) is null then date_add(add_months(bill_charge.PRODUCT_START,nvl(daily_agreement.CONTRACT_PERIOD, '0')),-1)
               when bill_charge.CHRG_TP = 'MRC' and if(bill_charge.PRODUCT_END = '', null,bill_charge.PRODUCT_END) is null then date_add(add_months(bill_charge.PRODUCT_START,nvl(daily_agreement.CONTRACT_PERIOD, '0')),-1)
               when bill_charge.CHRG_TP = 'ONE TIME CHARGE' then bill_charge.CHRG_START
               else bill_charge.PRODUCT_END
          end
        ,0,10),'yyyy-MM-dd') PRODUCT_END,
        nvl(concat(bill_charge.PD_SEQ, '_', bill_charge.SUBS_REF, '_', bill_charge.CHRG_TP), '') PO_ID,
        bill_charge.INVOICE_NUM,
        date_format(substr(bill_charge.ACTUAL_BILL_DTM,0,10),'yyyy-MM-dd') ACTUAL_BILL_DTM,
        case 
          when (nvl(bill_charge.PRODUCT_END, '') != '' and 
                nvl(bill_charge.SVC_END_DT, '') != '') or bill_charge.CHRG_START = bill_charge.PRODUCT_START then 'Y' 
          else 'N'
        end EVENT_FLAG,
        case 
          when parent_asset.PD_CGY in ('Mobile', 'Mobile Bulk Offer', 'Corporate Mobile Bulk') THEN 'Mobile'
          when parent_asset.PD_CGY = 'IPhone' THEN 'IPhone'
          when parent_asset.PD_CGY <> 'MOBILE' OR  
               parent_asset.PD_CGY <> 'Mobile Bulk Offer' OR
               parent_asset.PD_CGY <> 'Corporate Mobile Bulk' OR
               parent_asset.PD_CGY <> 'IPhone' THEN ref_group_of_services.GROUP_OF_SERVICES
          else ref_group_of_services.GROUP_OF_SERVICES
        end SERVICE_GROUP,
        case
          when parent_asset.PD_CGY = 'Mobile' or 
               parent_asset.PD_CGY = 'Mobile Bulk Offer' or
               parent_asset.PD_CGY = 'Corporate Mobile Bulk' or
               parent_asset.PD_CGY = 'IPhone' THEN ref_city_to_profit_center.pc_id
          else '1000000000'
        end PROFIT_CENTER,
        ref_customer_group.CUSTOMER_GROUP CUSTOMER_GROUP,
        midi_attr.MIDI_ATTRIBUTE MIDI_ATTRIBUTE,
        case
          when daily_asset_product_type.AST_ST = 'Inactive' then null
          else 'Creation'
        end EVENT_TYPE
          
        from bill_charge

          -----------------------------------------------GET PRODUCT_TYPE-------------------------------------------------------------
          left join daily_asset daily_asset_product_type
            on bill_charge.SUBS_REF = daily_asset_product_type.SUBSCRIPTION_REF
        		and bill_charge.PD_SEQ = daily_asset_product_type.PRODUCT_SEQUENCE
            --v1 and bill_charge.PRODUCT_ID = daily_asset_product_type.BILLING_PRODUCT_ID

          -----------------------------------------------GET CUSTOMER_ACCOUNT---------------------------------------------------------
          left join daily_cstacc
            on bill_charge.CUST_REF = daily_cstacc.CA_REFR

          -----------------------------------------------BUSINESS_AREA_NAME = MRC OR INSTALLATION-------------------------------------
          left join daily_asset service_asset
            on (bill_charge.CHRG_TP in ('MRC', 'INSTALLATION')
            and bill_charge.PRODUCT_ID = service_asset.BILLING_PRODUCT_ID
            and bill_charge.SUBS_REF = service_asset.SUBSCRIPTION_REF
      		  and bill_charge.PD_SEQ = service_asset.PRODUCT_SEQUENCE) 
            or (bill_charge.CHRG_TP not in ('MRC', 'INSTALLATION')
            and bill_charge.OTC_ID = service_asset.BILLING_PRODUCT_ID
            and bill_charge.SUBS_REF = service_asset.SUBSCRIPTION_REF
      		  and bill_charge.PD_SEQ = service_asset.PRODUCT_SEQUENCE)

          -----------------------------------------------BUSINESS_AREA_NAME = OTC-----------------------------------------------------
          left join daily_asset parent_asset_otc
            on bill_charge.OTC_ID = parent_asset_otc.BILLING_PRODUCT_ID
              and bill_charge.SUBS_REF = parent_asset_otc.SUBSCRIPTION_REF
              and bill_charge.CHRG_TP='ONE TIME CHARGE'
          left join daily_asset parent_asset_otc_ast_id
            on parent_asset_otc.PRN_AST_REFR = parent_asset_otc_ast_id.AST_ID

          -----------------------------------------------GET SERVICE_ID---------------------------------------------------------------
          left join (select distinct SUBSCRIPTION_REF from daily_asset where daily_asset.PD_TP = 'Baseline Root Product' ) daily_asset
            on bill_charge.SUBS_REF = daily_asset.SUBSCRIPTION_REF
          left join daily_asset parent_asset                                       
            on (parent_asset.SUBSCRIPTION_REF = daily_asset.SUBSCRIPTION_REF                                                      
            and bill_charge.SUBSCRIPTION_TYPE <> 'BUNDLE'
            and parent_asset.PD_TP = 'Baseline Root Product'
            and case when nvl(bill_charge.SVC_ID, '') = '' then parent_asset.SVC_ID else bill_charge.SVC_ID end = parent_asset.SVC_ID)
            or (parent_asset.SUBSCRIPTION_REF = daily_asset.SUBSCRIPTION_REF
            and bill_charge.SUBSCRIPTION_TYPE = 'BUNDLE'
            and parent_asset.PD_TP = 'Baseline Root Product'
            and service_asset.PRN_AST_REFR = parent_asset.AST_ID
            and case when nvl(bill_charge.SVC_ID, '') = '' then parent_asset.SVC_ID else bill_charge.SVC_ID end = parent_asset.SVC_ID)
          
          -----------------------------------------------GET CUSTOMER SEGMENT---------------------------------------------------------
          left join (
                 select distinct a.SUBSCRIPTION_REF,AST_ID,SVC_ID,PD_TP,
                  case 
                    when total_PD_CGY>1 then 'B2B HYBRID'
                    else 'B2B MIDI'--concat('B2B ',a.PD_CGY)
                  end PRODUCT_CATEGORY
                  from daily_asset a
                  inner join (
                   select SUBSCRIPTION_REF,count(distinct PD_CGY)total_PD_CGY
                          from daily_asset 
                          where PD_TP = 'Baseline Root Product' --and SUBSCRIPTION_REF in ('8169221','8936641')
                          group by  SUBSCRIPTION_REF) b on a.SUBSCRIPTION_REF = b.SUBSCRIPTION_REF
                  where PD_TP = 'Baseline Root Product' and a.PD_CGY != 'IT Services'
          ) customer_segment 
           on (customer_segment.SUBSCRIPTION_REF = daily_asset.SUBSCRIPTION_REF                                                      
            and bill_charge.SUBSCRIPTION_TYPE <> 'BUNDLE'
            and customer_segment.PD_TP = 'Baseline Root Product'
            and case when nvl(bill_charge.SVC_ID, '') = '' then customer_segment.SVC_ID else bill_charge.SVC_ID end = customer_segment.SVC_ID)
            or (customer_segment.SUBSCRIPTION_REF = daily_asset.SUBSCRIPTION_REF
            and bill_charge.SUBSCRIPTION_TYPE = 'BUNDLE'
            and customer_segment.PD_TP = 'Baseline Root Product'
            and service_asset.PRN_AST_REFR = customer_segment.AST_ID
            and case when nvl(bill_charge.SVC_ID, '') = '' then customer_segment.SVC_ID else bill_charge.SVC_ID end = customer_segment.SVC_ID)

          ------------------------------------------------GET AGREEMENT---------------------------------------------------------------
          left join daily_agreement
            on daily_agreement.AGRMT_NUM = parent_asset.AGRMT_NUM
        --  left join daily_agreement daily_agreement_backup
        --    on daily_agreement_backup.AGRMT_NM = bill_charge.AGRMT_NUM --for test : input data in bill charge, when AGRMT_NUM is null

          -----------------------------------GET REFERENCE GROUP OF SERVICE---------------------------------------------------------
          left join ref_group_of_services
          -- Use the derived field: BUSINESS_AREA_NAME
          on case when daily_asset_product_type.PD_NM in ('Mobile', 'Mobile Bulk Offer') then 'MOBILE'
                  when daily_asset_product_type.PD_NM = 'Iphone' then 'IPHONE'
                  else case when bill_charge.CHRG_TP in ('MRC', 'INSTALLATION') then parent_asset.PD_NM
                            when bill_charge.CHRG_TP = 'ONE TIME CHARGE' then parent_asset_otc_ast_id.PD_NM
                       end
              end = ref_group_of_services.BUSINESS_AREA_NAME

          -----------------------------------GET REFERENCE B2M_BRANCH_REGION---------------------------------------------------------
          left join ref_b2m_branch_region
            on lpad(bill_charge.BRANCH_CODE, 2, '0') = ref_b2m_branch_region.BRANCH_CODE
            and lpad(bill_charge.REGION_CODE, 2, '0') = ref_b2m_branch_region.REGION_CODE

          -----------------------------------GET REFERENCE PROFIT_CENTER-------------------------------------------------------------
          left join ref_city_to_profit_center
            on upper(ref_b2m_branch_region.branch_name) = upper(ref_city_to_profit_center.city)

          -----------------------------------GET REFERENCE CUSTOMER_GROUP------------------------------------------------------------
          left join ref_customer_group
            on daily_cstacc.CA_NM = ref_customer_group.CA_NAME

          ----------------------------------GET MIDI ATTRIBUTE-----------------------------------------------------------------------
          left join (
            select
            a.SUBSCRIPTION_REF,
            concat_ws(' and ',collect_list(concat(trim(MIDI_ATTRIBUTE)))) MIDI_ATTRIBUTE
            from (select DISTINCT bill_charge.SUBS_REF SUBSCRIPTION_REF from bill_charge) a
            left join (select distinct SUBSCRIPTION_REF,case when MIDI_ATTRIBUTE = '' then null else MIDI_ATTRIBUTE end MIDI_ATTRIBUTE from daily_asset) b
            on a.SUBSCRIPTION_REF = b.SUBSCRIPTION_REF
            group by a.SUBSCRIPTION_REF
            ) midi_attr
            on bill_charge.SUBS_REF = midi_attr.SUBSCRIPTION_REF
      """)
    b2bIfrsTransform.persist(StorageLevel.DISK_ONLY)
    b2bIfrsTransform.registerTempTable("b2b_transform")
    b2bIfrsTransform.show()
    sqlContext.sql("select count(*),'b2b_transform' from b2b_transform").show(false)
    
   // Save b2bTransform to CSV file as billing datamart
    val billingDataMart = sqlContext.sql("""
        select * from b2b_transform
    """);
    
    billingDataMart.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", "|")
    .save(pathB2bIfrsBillingTransform + "/process_id=" + prcDt + "_" + jobId)
    val data_mart_billing_file = fs.globStatus(
    new Path(pathB2bIfrsBillingTransform + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsBillingTransform + "/process_id=" + prcDt + "_" + jobId + "/"+ data_mart_billing_file),
    new Path(pathB2bIfrsBillingTransform + "/process_id=" + prcDt + "_" + jobId + "/initial_ifrs_billing_data_mart"+prcDt+"_"+hh+mm+ss+".dat"))

      
    sqlContext.sql("select count(*),'event_type_temp_before_running' from event_type_temp").show(false)
    //dailyRunning running
    val eventType = sqlContext.sql("""
        select 
        *
        from (
          select 
          b.SERVICE_ID,
          b.AGREEMENT_NUM,
          b.PO_ID,
          b.AGREEMENT_NAME,
          row_number() over (partition by b.AGREEMENT_NUM order by b.PO_ID asc) row_number
          from (
            select distinct
            a.SERVICE_ID,
            a.AGREEMENT_NUM,
            a.PO_ID,
            a.AGREEMENT_NAME
            from (
              select distinct
              event_type_temp.SERVICE_ID,
              event_type_temp.AGREEMENT_NUM,
              event_type_temp.PO_ID,
              event_type_temp.AGREEMENT_NAME
              from event_type_temp
              union all
              select distinct
              b2b_transform.SERVICE_ID,
              b2b_transform.AGREEMENT_NUM,
              b2b_transform.PO_ID,
              b2b_transform.AGREEMENT_NAME
              from
              b2b_transform
              where b2b_transform.EVENT_TYPE = 'Creation'
              ) a
            ) b
          ) c where c.row_number = 1
      """)
    eventType.registerTempTable("event_type_after_running")
    eventType.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", "|")
    .save(pathB2bIfrsTransformEventTypeTempCsv)
    sqlContext.sql("select count(*),'event_type_temp_after_running' from event_type_temp").show(false) 
   
    
    val productDescription = sqlContext.sql("""
        select 
        AGREEMENT_NUM,
        SUBSCRIPTION_REF,
        SERVICE_ID,
        concat_ws(' and ', collect_list(concat(BUSINESS_AREA_NAME))) PRODUCT_DESCRIPTION 
        from (
          select distinct
          b2b_transform.AGREEMENT_NUM,
          b2b_transform.SUBSCRIPTION_REF,
          b2b_transform.SERVICE_ID,
          b2b_transform.BUSINESS_AREA_NAME
          from b2b_transform
        ) as a
        group by
        AGREEMENT_NUM,
        SUBSCRIPTION_REF,
        SERVICE_ID
      """)
    productDescription.registerTempTable("product_description")
    
    
    val b2bIfrsTransformPd = sqlContext.sql("""
         select
         b2b_transform.*,
         product_description.PRODUCT_DESCRIPTION

         from b2b_transform
           left join product_description
           on b2b_transform.AGREEMENT_NUM = product_description.AGREEMENT_NUM
           and b2b_transform.SUBSCRIPTION_REF = product_description.SUBSCRIPTION_REF
           and b2b_transform.SERVICE_ID = product_description.SERVICE_ID
      """)
    b2bIfrsTransformPd.registerTempTable("b2b_transform_pd")
      
    sqlContext.sql("select count(*),'b2b_transform_pd' from b2b_transform_pd").show()
      
    
    b2bIfrsTransformPd.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", "|")
    .save(pathB2bIfrsRevenueTransform + "/process_id=" + prcDt + "_" + jobId)
    val data_mart_revenue_file = fs.globStatus(
    new Path(pathB2bIfrsRevenueTransform + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsRevenueTransform + "/process_id=" + prcDt + "_" + jobId + "/"+ data_mart_revenue_file),
    new Path(pathB2bIfrsRevenueTransform + "/process_id=" + prcDt + "_" + jobId + "/initial_ifrs_data_mart_revenue"+prcDt+"_"+hh+mm+ss+".dat"))
    
     
    //TODO
    val headerDf = sqlContext.sql("""
        select 
            'H' record_type,
            nvl(null, '') DOCUMENT_ID,
            nvl(null, '') APPLICATION_ID,
            nvl(null, '') DOCUMENT_TYPE_ID,
            nvl(null, '') DOC_ID_INT_1,
            nvl(null, '') DOC_ID_INT_2,
            nvl(null, '') DOC_ID_INT_3,
            nvl(null, '') DOC_ID_INT_4,
            nvl(null, '') DOC_ID_INT_5,--v1 CA_NM DOC_ID_INT_5,
            nvl(substr(AGREEMENT_NAME,1,30), '') DOC_ID_CHAR_1, --done
            nvl(substr(AGREEMENT_NAME,31,60), '') DOC_ID_CHAR_2, --done
            nvl(null, '') DOC_ID_CHAR_3,
            nvl(null, '') DOC_ID_CHAR_4,
            nvl(null, '') DOC_ID_CHAR_5,
            nvl(date_format(from_unixtime(unix_timestamp('"""+prcDt+"""', 'yyyyMMdd')), 'dd-MMM-yyyy'), '') DOCUMENT_DATE,
            nvl(substr(AGREEMENT_NAME,1,30), '') DOCUMENT_NUMBER,--v1 nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'),1,30), '') DOCUMENT_NUMBER,
            nvl(null, '') DOCUMENT_TYPE,
            nvl(null, '') DOCUMENT_CREATION_DATE,
            nvl(null, '') DOCUMENT_UPDATE_DATE,
            'IDR' CURRENCY_CODE,
            '' SALESREP_ID,
            '' SALESREP_NAME,
            '' BILL_TO_CUSTOMER_PARTY_ID,
            '' BILL_TO_CUSTOMER_PARTY_NUM,
            '' BILL_TO_CUSTOMER_PARTY_NAME,
            '' BILL_TO_CUSTOMER_ID,
            '' SHIP_TO_CUSTOMER_ID,
            '' BILL_TO_CUSTOMER_NUM,
            '' SHIP_TO_CUSTOMER_NUM,
            '' BILL_TO_CUSTOMER_NAME,
            '' SHIP_TO_CUSTOMER_NAME,
            '' BILL_TO_CUSTOMER_SITE_ID,
            '' BILL_TO_CUST_SITE_NAME,
            '' SHIP_TO_CUSTOMER_SITE_ID,
            '' SHIP_TO_CUST_SITE_NAME,
            '' BILL_TO_COUNTRY,
            '' SHIP_TO_COUNTRY,
            '' ORG_ID,
            'INDOSAT BU' ORGANIZATION_NAME,
            '' LEDGER_ID,
            '' LEDGER_NAME,
            '' LEGAL_ENTITY_ID,
            'PT. Indosat Tbk.' LEGAL_ENTITY_NAME,
            '' CUST_PO_NUMBER,
            '' CUSTOMER_CONTRACT_NUMBER,
            '' SALES_AGREEMENT_NUMBER,
            '' QUOTE_NUMBER,
            '' EXCHANGE_RATE,
            '' EXCHANGE_RATE_TYPE,
            '' EXCHANGE_DATE,
            '' BILL_TO_CUSTOMER_STATE,
            '' BILL_TO_CUSTOMER_COUNTY,
            '' BILL_TO_CUSTOMER_CITY,
            '' BILL_TO_CUSTOMER_POSTAL_CODE,
            '' BILL_TO_CUST_CLASSIFICATION,
            '' SRC_ATTRIBUTE_CHAR_1,
            '' SRC_ATTRIBUTE_CHAR_2,
            '' SRC_ATTRIBUTE_CHAR_3,
            '' SRC_ATTRIBUTE_CHAR_4,
            '' SRC_ATTRIBUTE_CHAR_5,
            '' SRC_ATTRIBUTE_CHAR_6,
            '' SRC_ATTRIBUTE_CHAR_7,
            '' SRC_ATTRIBUTE_CHAR_8,
            '' SRC_ATTRIBUTE_CHAR_9,
            '' SRC_ATTRIBUTE_CHAR_10,
            '' SRC_ATTRIBUTE_CHAR_11,
            '' SRC_ATTRIBUTE_CHAR_12,
            '' SRC_ATTRIBUTE_CHAR_13,
            '' SRC_ATTRIBUTE_CHAR_14,
            '' SRC_ATTRIBUTE_CHAR_15,
            '' SRC_ATTRIBUTE_CHAR_16,
            '' SRC_ATTRIBUTE_CHAR_17,
            '' SRC_ATTRIBUTE_CHAR_18,
            '' SRC_ATTRIBUTE_CHAR_19,
            '' SRC_ATTRIBUTE_CHAR_20,
            '' SRC_ATTRIBUTE_CHAR_21,
            '' SRC_ATTRIBUTE_CHAR_22,
            '' SRC_ATTRIBUTE_CHAR_23,
            '' SRC_ATTRIBUTE_CHAR_24,
            '' SRC_ATTRIBUTE_CHAR_25,
            '' SRC_ATTRIBUTE_CHAR_26,
            '' SRC_ATTRIBUTE_CHAR_27,
            '' SRC_ATTRIBUTE_CHAR_28,
            '' SRC_ATTRIBUTE_CHAR_29,
            '' SRC_ATTRIBUTE_CHAR_30,
            '' SRC_ATTRIBUTE_CHAR_31,
            '' SRC_ATTRIBUTE_CHAR_32,
            '' SRC_ATTRIBUTE_CHAR_33,
            '' SRC_ATTRIBUTE_CHAR_34,
            '' SRC_ATTRIBUTE_CHAR_35,
            '' SRC_ATTRIBUTE_CHAR_36,
            '' SRC_ATTRIBUTE_CHAR_37,
            '' SRC_ATTRIBUTE_CHAR_38,
            '' SRC_ATTRIBUTE_CHAR_39,
            '' SRC_ATTRIBUTE_CHAR_40,
            '' SRC_ATTRIBUTE_CHAR_41,
            '' SRC_ATTRIBUTE_CHAR_42,
            '' SRC_ATTRIBUTE_CHAR_43,
            '' SRC_ATTRIBUTE_CHAR_44,
            '' SRC_ATTRIBUTE_CHAR_45,
            '' SRC_ATTRIBUTE_CHAR_46,
            '' SRC_ATTRIBUTE_CHAR_47,
            '' SRC_ATTRIBUTE_CHAR_48,
            '' SRC_ATTRIBUTE_CHAR_49,
            '' SRC_ATTRIBUTE_CHAR_50,
            '' SRC_ATTRIBUTE_CHAR_51,
            '' SRC_ATTRIBUTE_CHAR_52,
            '' SRC_ATTRIBUTE_CHAR_53,
            '' SRC_ATTRIBUTE_CHAR_54,
            '' SRC_ATTRIBUTE_CHAR_55,
            '' SRC_ATTRIBUTE_CHAR_56,
            '' SRC_ATTRIBUTE_CHAR_57,
            '' SRC_ATTRIBUTE_CHAR_58,
            '' SRC_ATTRIBUTE_CHAR_59,
            '' SRC_ATTRIBUTE_CHAR_60,
            '' SRC_ATTRIBUTE_NUMBER_1,
            '' SRC_ATTRIBUTE_NUMBER_2,
            '' SRC_ATTRIBUTE_NUMBER_3,
            '' SRC_ATTRIBUTE_NUMBER_4,
            '' SRC_ATTRIBUTE_NUMBER_5,
            '' SRC_ATTRIBUTE_NUMBER_6,
            '' SRC_ATTRIBUTE_NUMBER_7,
            '' SRC_ATTRIBUTE_NUMBER_8,
            '' SRC_ATTRIBUTE_NUMBER_9,
            '' SRC_ATTRIBUTE_NUMBER_10,
            '' SRC_ATTRIBUTE_NUMBER_11,
            '' SRC_ATTRIBUTE_NUMBER_12,
            '' SRC_ATTRIBUTE_NUMBER_13,
            '' SRC_ATTRIBUTE_NUMBER_14,
            '' SRC_ATTRIBUTE_NUMBER_15,
            '' SRC_ATTRIBUTE_NUMBER_16,
            '' SRC_ATTRIBUTE_NUMBER_17,
            '' SRC_ATTRIBUTE_NUMBER_18,
            '' SRC_ATTRIBUTE_NUMBER_19,
            '' SRC_ATTRIBUTE_NUMBER_20,
            '' SRC_ATTRIBUTE_DATE_1,
            '' SRC_ATTRIBUTE_DATE_2,
            '' SRC_ATTRIBUTE_DATE_3,
            '' SRC_ATTRIBUTE_DATE_4,
            '' SRC_ATTRIBUTE_DATE_5,
            '' SRC_ATTRIBUTE_DATE_6,
            '' SRC_ATTRIBUTE_DATE_7,
            '' SRC_ATTRIBUTE_DATE_8,
            '' SRC_ATTRIBUTE_DATE_9,
            '' SRC_ATTRIBUTE_DATE_10,
            '' REQUEST_ID,
            '' OBJECT_VERSION_NUMBER,
            '' CREATED_BY,
            '' CREATION_DATE,
            '' LAST_UPDATED_BY,
            '' LAST_UPDATE_DATE,
            '' LAST_UPDATE_LOGIN,
            '' LEGAL_ENTITY_COUNTRY,
            '' LEGAL_ENTITY_ADDRESS,
            '' PAYMENT_TERM_ID,
            '' INVOICING_RULE_ID,
            '' ACCOUNTING_RULE_ID,
            '' ACCOUNTING_RULE_DURATION,
            '' CANCELLED_FLAG,
            '' OPEN_FLAG,
            '' RETURN_REASON_CODE,
            '' MEA_FLAG,
            '' SRC_ATTRIBUTE_CATEGORY,
            '' SOURCE_ORG_ID,
            '' ACCOUNTING_EFFECT_FLAG,
            '' SOURCE_TYPE_CODE,
            '' PAYMENT_TERM_NAME,
            nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_SITE_REF,
            nvl(NULL, '') ORIG_SYS_SHIP_TO_CUST_SITE_REF,
            nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_REF,
            '' ORIG_SYS_SHIP_TO_CUST_REF,
            '' SOURCE_LEGAL_ENTITY_NAME,
            '' SOURCE_EXCHANGE_RATE_TYPE,
            '' SOURCE_ORGANIZATION_NAME,
            '' SOURCE_SALESREP_NAME,
            '' SOURCE_PAYMENT_TERM_NAME,
            '' SOURCE_BILL_TO_CUST_NUM,
            '' SOURCE_BILL_TO_CUST_NAME,
            '' SOURCE_BILL_TO_CUST_SITE_NUM,
            '' SOURCE_BILL_TO_CUST_ADDRESS,
            '' SOURCE_SHIP_TO_CUST_NUM,
            '' SOURCE_SHIP_TO_CUST_NAME,
            '' SOURCE_SHIP_TO_CUST_SITE_NUM,
            '' SOURCE_SHIP_TO_CUST_ADDRESS,
            'HADOOP' SOURCE_SYSTEM,
            '' SP_FIRST_NAME,
            '' SP_MIDDLE_NAME,
            '' SP_LAST_NAME,
            'HADOOP' SOURCE_DOCUMENT_TYPE_CODE
        from b2b_transform_pd
        where CA_NM != "null"
              and AGREEMENT_NUM != "null"
              and EVENT_TYPE != "null"
              and EVENT_TYPE = 'Creation' 
        group by
          AGREEMENT_NAME, CUSTOMER_NAME, CA_NM


      """)
    headerDf.show(false)
    headerDf.registerTempTable("header")
    headerDf.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", "|")
    .save("/user/hdp-rev_dev/ifrs_b2b/output/testrevenue/H")
    
    //TODO
    val lineDf = sqlContext.sql("""
        select
            'L' RECORD_TYPE,
            nvl(null, '') DOCUMENT_LINE_ID,
            nvl(null, '') DOCUMENT_ID,
            nvl(null, '') APPLICATION_ID,
            nvl(null, '') DOCUMENT_TYPE_ID,
            nvl(null, '') DOC_LINE_ID_INT_1,
            nvl(null, '') DOC_LINE_ID_INT_2,
            nvl(null, '') DOC_LINE_ID_INT_3,
            nvl(null, '') DOC_LINE_ID_INT_4,
            nvl(null, '') DOC_LINE_ID_INT_5,
            nvl(substr(AGREEMENT_NAME,1,30), '') DOC_LINE_ID_CHAR_1, --done
            nvl(substr(AGREEMENT_NAME,31,60), '') DOC_LINE_ID_CHAR_2, --done
            nvl(substr(SERVICE_ID,1,30), '') DOC_LINE_ID_CHAR_3,
            nvl(substr(SERVICE_ID,31,60), '') DOC_LINE_ID_CHAR_4,
            nvl(concat(PRODUCT_SEQ, '_', SUBSCRIPTION_REF, '_', CHARGE_TYPE), '') DOC_LINE_ID_CHAR_5,
            nvl(null, '') DOC_ID_INT_1,
            nvl(null, '') DOC_ID_INT_2,
            nvl(null, '') DOC_ID_INT_3,
            nvl(null, '') DOC_ID_INT_4,
            nvl(null, '') DOC_ID_INT_5, --v1 nvl(CA_NM, '') DOC_ID_INT_5,
            nvl(substr(AGREEMENT_NAME,1,30), '') DOC_ID_CHAR_1, --done
            nvl(substr(AGREEMENT_NAME,31,60), '') DOC_ID_CHAR_2, --done
            nvl(null, '') DOC_ID_CHAR_3,
            nvl(null, '') DOC_ID_CHAR_4,
            nvl(null, '') DOC_ID_CHAR_5,
            nvl(null, '') DOCUMENT_DATE,
            nvl(null, '') LINE_TYPE,
            nvl(null, '') LINE_NUMBER,--v1 row_number() over (partition by AGREEMENT_NAME) LINE_NUMBER,
            nvl(null, '') INVENTORY_ORG_ID,
            nvl(null, '') ITEM_ID,
            nvl(null, '') ITEM_NUMBER,
            nvl(null, '') ITEM_DESCRIPTION,
            nvl(null, '') MEMO_LINE_ID,
            'zzy' UOM_CODE,
            case
              when (CHARGE_TYPE = 'INSTALLATION' or CHARGE_TYPE = 'ONE TIME CHARGE') and EVENT_TYPE != 'Termination' then 1
              when (CHARGE_TYPE = 'MRC' and EVENT_TYPE != 'Termination') then cast(contract_period as DOUBLE)
              when EVENT_TYPE = 'Termination' then cast(months_between(date_add(PRODUCT_END,1),PRODUCT_START) as DOUBLE)
              else 0
            end QUANTITY,
            case when CHARGE_IDR = '' then 0 else cast(nvl(CHARGE_IDR,0) as DOUBLE) end UNIT_SELLING_PRICE,
            nvl(null, '') UNIT_LIST_PRICE,
            nvl(null, '') DISCOUNT_PERCENTAGE,
            nvl(null, '') UNIT_SELLING_PCT_BASE_PRICE,
            nvl(null, '') UNIT_LIST_PCT_BASE_PRICE,
            nvl(null, '') BASE_PRICE,
            nvl(
             case
              --when CHARGE_TYPE = 'MRC' then cast(CHARGE_IDR * contract_period as DOUBLE)
              --when CHARGE_TYPE = 'INSTALLATION' or CHARGE_TYPE='ONE TIME CHARGE' then cast(CHARGE_IDR as DOUBLE) 

              when (CHARGE_TYPE = 'INSTALLATION' or CHARGE_TYPE = 'ONE TIME CHARGE') and EVENT_TYPE != 'Termination' then cast(CHARGE_IDR as DOUBLE)
              when (CHARGE_TYPE = 'MRC' and EVENT_TYPE != 'Termination') then cast(CHARGE_IDR * contract_period as DOUBLE)
              when EVENT_TYPE = 'Termination' then cast(months_between(date_add(PRODUCT_END,1),PRODUCT_START) as DOUBLE)*cast(CHARGE_IDR as DOUBLE)
            else 0 end 
            ,0) LINE_AMOUNT,
            nvl(null, '') BILL_TO_CUSTOMER_ID,
            nvl(null, '') SHIP_TO_CUSTOMER_ID,
            nvl(null, '') BILL_TO_CUSTOMER_PARTY_ID,
            nvl(null, '') BILL_TO_CUSTOMER_PARTY_NUM,
            nvl(null, '') BILL_TO_CUSTOMER_PARTY_NAME,
            nvl(null, '') BILL_TO_CUSTOMER_NUM,
            nvl(null, '') SHIP_TO_CUSTOMER_NUM,
            nvl(null, '') BILL_TO_CUSTOMER_NAME,
            nvl(null, '') SHIP_TO_CUSTOMER_NAME,
            nvl(null, '') BILL_TO_CUST_SITE_NAME,
            nvl(null, '') BILL_TO_CUSTOMER_SITE_ID,
            nvl(null, '') SHIP_TO_CUSTOMER_SITE_ID,
            nvl(null, '') SHIP_TO_CUST_SITE_NAME,
            nvl(null, '') BILL_TO_COUNTRY,
            nvl(null, '') SHIP_TO_COUNTRY,
            nvl(null, '') BILL_TO_CUSTOMER_STATE,
            nvl(null, '') BILL_TO_CUSTOMER_COUNTY,
            nvl(null, '') BILL_TO_CUSTOMER_CITY,
            nvl(null, '') BILL_TO_CUSTOMER_POSTAL_CODE,
            nvl(null, '') BILL_TO_CUSTOMER_CLASSIFICATION,
            nvl(null, '') DELIVERED_FLAG,
            nvl(null, '') INVOICED_FLAG,
            nvl(null, '') FULFILLED_FLAG,
            nvl(null, '') CANCELLED_FLAG,
            nvl(null, '') DELIVERY_STATUS,
            nvl(null, '') SALESREP_ID,
            nvl(null, '') SALESREP_NAME,
            nvl(null, '') LINE_CREATION_DATE,
            nvl(null, '') LINE_LAST_UPDATE_DATE,
            nvl(null, '') COMMENTS,
            'Y' SRC_ATTRIBUTE_CHAR_1,--v1 case when b.cnt <= 1 then 'N' else 'Y' end SRC_ATTRIBUTE_CHAR_1,
            nvl(substr(PRODUCT_DESCRIPTION,0,150), '') SRC_ATTRIBUTE_CHAR_2,
            nvl(concat(SERVICE_GROUP, '-', CUSTOMER_GROUP), '') SRC_ATTRIBUTE_CHAR_3,
            nvl(concat(SERVICE_GROUP, '-', CUSTOMER_GROUP, '-', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR_4,
            nvl(PROFIT_CENTER, '') SRC_ATTRIBUTE_CHAR_5,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_6,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_7,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_8,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_9,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_10,
            nvl(AGREEMENT_NAME, '') SRC_ATTRIBUTE_CHAR_11,--nvl(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'), '') SRC_ATTRIBUTE_CHAR_11,
            nvl(SERVICE_ID, '') SRC_ATTRIBUTE_CHAR_12,
            nvl(concat(PRODUCT_SEQ, '_', SUBSCRIPTION_REF, '_', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR_13,
            nvl(BUSINESS_AREA_NAME, '') SRC_ATTRIBUTE_CHAR_14,
            nvl(SERVICE_GROUP, '') SRC_ATTRIBUTE_CHAR_15,
            nvl(CUSTOMER_GROUP, '') SRC_ATTRIBUTE_CHAR_16,
            nvl(CHARGE_TYPE, '') SRC_ATTRIBUTE_CHAR_17,
            nvl(concat(SERVICE_GROUP, '-', CUSTOMER_GROUP, '-', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR_18,
            nvl(concat(PRODUCT_NAME, '-', SERVICE_GROUP, '-', CUSTOMER_GROUP, '-', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR_19,
            nvl(concat(date_format(AGREEMENT_START, 'dd-MMM-yy'), '-', date_format(AGREEMENT_END, 'dd-MMM-yy')), '') SRC_ATTRIBUTE_CHAR_20,
            nvl(CONTRACT_INITIAL_AMT, 0) SRC_ATTRIBUTE_CHAR_21,
            nvl(AGREEMENT_NAME, '') SRC_ATTRIBUTE_CHAR_22,
            'Y' SRC_ATTRIBUTE_CHAR_23,
            'B2B MIDI' SRC_ATTRIBUTE_CHAR_24,
            nvl(EVENT_TYPE, '') SRC_ATTRIBUTE_CHAR_25,
            nvl(MIDI_ATTRIBUTE,''),  --changed to bandwith
            nvl(null, '') SRC_ATTRIBUTE_CHAR_27,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_28,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_29,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_30,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_31,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_32,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_33,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_34,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_35,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_36,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_37,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_38,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_39,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_40,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_41,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_42,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_43,  
            nvl(null, '') SRC_ATTRIBUTE_CHAR_44,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_45,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_46,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_47,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_48,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_49,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_50,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_51,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_52,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_53,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_54,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_55,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_56,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_57,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_58,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_59,
            nvl(null, '') SRC_ATTRIBUTE_CHAR_60,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_1,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_2,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_3,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_4,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_5,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_6,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_7,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_8,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_9,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_10,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_11,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_12,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_13,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_14,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_15,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_16,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_17,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_18,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_19,
            nvl(null, '') SRC_ATTRIBUTE_NUMBER_20,
            nvl(null, '') SRC_ATTRIBUTE_DATE_1,
            nvl(null, '') SRC_ATTRIBUTE_DATE_2,
            nvl(null, '') SRC_ATTRIBUTE_DATE_3,
            nvl(null, '') SRC_ATTRIBUTE_DATE_4,
            nvl(null, '') SRC_ATTRIBUTE_DATE_5,
            nvl(null, '') SRC_ATTRIBUTE_DATE_6,
            nvl(null, '') SRC_ATTRIBUTE_DATE_7,
            nvl(null, '') SRC_ATTRIBUTE_DATE_8,
            nvl(null, '') SRC_ATTRIBUTE_DATE_9,
            nvl(null, '') SRC_ATTRIBUTE_DATE_10,
            nvl(null, '') REQUEST_ID,
            nvl(null, '') OBJECT_VERSION_NUMBER,
            nvl(null, '') CREATION_DATE,
            nvl(null, '') CREATED_BY,
            nvl(null, '') LAST_UPDATE_DATE,
            nvl(null, '') LAST_UPDATED_BY,
            nvl(null, '') LAST_UPDATE_LOGIN,
            nvl(null, '') MEMO_LINE_SEQ_ID,
            nvl(null, '') PAYMENT_AMOUNT,
            nvl(null, '') QUANTITY_CANCELLED,
            nvl(null, '') QUANTITY_SHIPPED,
            nvl(null, '') QUANTITY_ORDERED,
            nvl(null, '') QUANTITY_FULFILLED,
            nvl(null, '') QUANTITY_INVOICED,
            nvl(null, '') OPEN_FLAG,
            nvl(null, '') CUST_PO_NUMBER,
            nvl(null, '') PROJECT_ID,
            nvl(null, '') TASK_ID,
            nvl(null, '') PAYMENT_TERM_ID,
            nvl(null, '') ACCOUNTING_RULE_ID,
            nvl(case
              when EVENT_TYPE = 'Modification' then nvl(date_format(date_add(PRODUCT_START,1), 'dd-MMM-yy'), '') --different logic with daily running
              when CHARGE_TYPE = 'ONE TIME CHARGE' then nvl(date_format(CHARGE_START_DT, 'dd-MMM-yy'), '') 
              else nvl(date_format(PRODUCT_START, 'dd-MMM-yy'), '') 
            end,'') RULE_START_DATE,
            nvl(date_format(PRODUCT_END, 'dd-MMM-yy'), '') RULE_END_DATE,
            nvl(null, '') ACTUAL_SHIPMENT_DATE,
            nvl(null, '') ACTUAL_ARRIVAL_DATE,
            nvl(null, '') FOB_POINT_CODE,
            nvl(null, '') FRIEGHT_TERMS_CODE,
            nvl(null, '') SCHEDULED_STATUS_CODE,
            nvl(null, '') SOURCE_TYPE_CODE,
            nvl(null, '') RETURN_REASON_CODE,
            nvl(null, '') SHIPPING_INTERFACED_FLAG,
            nvl(null, '') CREDIT_INVOICE_LINE_ID,
            nvl(null, '') REFERENCE_CUSTOMER_TRX_LINE_ID,
            nvl(null, '') SHIPPABLE_FLAG,
            nvl(null, '') FULFILLMENT_DATE,
            nvl(null, '') ACCOUNT_RULE_DURATION,
            nvl(null, '') ACTUAL_FULFILLMENT_DATE,
            nvl(null, '') CONTINGENCY_ID,
            nvl(null, '') REVREC_EVENT_CODE,
            nvl(null, '') REVREC_EXPIRATION_DAYS,
            nvl(null, '') ACCEPTED_QUANTITY,
            nvl(null, '') ACCEPTED_BY,
            nvl(null, '') REVREC_COMMENTS,
            nvl(null, '') REVREC_REFERENCE_DOCUMENT,
            nvl(null, '') REVREC_SIGNATURE,
            nvl(null, '') REVREC_SIGNATURE_DATE,
            nvl(null, '') REVREC_IMPLICIT_FLAG,
            nvl(null, '') COST_AMOUNT,
            nvl(null, '') GROSS_MARGIN_PERCENT,
            nvl(null, '') SRC_ATTRIBUTE_CATEGORY,
            nvl(null, '') SOURCE_ORG_ID,
            nvl(null, '') REFERENCE_DOC_LINE_ID_INT_1,
            nvl(null, '') REFERENCE_DOC_LINE_ID_INT_2,
            nvl(null, '') REFERENCE_DOC_LINE_ID_INT_3,
            nvl(null, '') REFERENCE_DOC_LINE_ID_INT_4,
            nvl(null, '') REFERENCE_DOC_LINE_ID_INT_5,
            nvl(null, '') REFERENCE_DOC_LINE_ID_CHAR_1,
            nvl(null, '') REFERENCE_DOC_LINE_ID_CHAR_2,
            nvl(null, '') REFERENCE_DOC_LINE_ID_CHAR_3,
            nvl(null, '') REFERENCE_DOC_LINE_ID_CHAR_4,
            nvl(null, '') REFERENCE_DOC_LINE_ID_CHAR_5,
            nvl(null, '') OVERRIDE_AUTO_ACCOUNTING_FLAG,
            nvl(null, '') SOURCE_INVENTORY_ORG_ID,
            nvl(null, '') REFERENCE_REVERSAL_METHOD,
            nvl(case
              when EVENT_TYPE = 'Termination'  then '2'
              when EVENT_TYPE in ('Creation','Modification') then '1'
              else nvl(null, '')
            end,'') VERSION_NUMBER,
            nvl(case
              when EVENT_TYPE in ('Termination','Modification') then 'Y'
              else nvl(null, '')
            end,'') VERSION_FLAG,
            nvl(row_number() over (partition by AGREEMENT_NAME),'') LINE_NUM, --v1 '' LINE_NUM,
            nvl(null, '') DISCOUNT_AMOUNT,
            nvl(null, '') LAST_PERIOD_TO_CREDIT,
            nvl(null, '') MEMO_LINE_NAME,
            nvl(null, '') PAYMENT_TERM_NAME,
            'Daily Rate Partial Periods' ACCOUNTING_RULE_NAME,
            'ISAT' INVENTORY_ORG_CODE,
            nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_SITE_REF,
            nvl(null, '') ORIG_SYS_SHIP_TO_CUST_SITE_REF,
            nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_REF,
            nvl(null, '') ORIG_SYS_SHIP_TO_CUST_REF,
            nvl(null, '') SOURCE_INVENTORY_ORG_CODE,
            nvl(null, '') SOURCE_MEMO_LINE_NAME,
            nvl(concat(BUSINESS_AREA_NAME, '-', SERVICE_GROUP,'-',CUSTOMER_GROUP,'-',CHARGE_TYPE), '') SOURCE_ITEM_NUMBER,
            nvl(null, '') SOURCE_UOM_CODE,
            nvl(null, '') SOURCE_SALESREP_NAME,
            nvl(null, '') SOURCE_PAYMENT_TERM_NAME,
            nvl(null, '') SOURCE_ACCOUNTING_RULE_NAME,
            nvl(null, '') SOURCE_BILL_TO_CUST_NUM,
            nvl(null, '') SOURCE_BILL_TO_CUST_NAME,
            nvl(null, '') SOURCE_BILL_TO_CUST_SITE_NUM,
            nvl(null, '') SOURCE_BILL_TO_CUST_ADDRESS,
            nvl(null, '') SOURCE_SHIP_TO_CUST_NUM,
            nvl(null, '') SOURCE_SHIP_TO_CUST_NAME,
            nvl(null, '') SOURCE_SHIP_TO_CUST_SITE_NUM,
            nvl(null, '') SOURCE_SHIP_TO_CUST_ADDRESS,
            nvl(null, '') DELIVERY_DATE,
            nvl(null, '') REFERENCE_DOCUMENT_TYPE_ID,
            'HADOOP' SOURCE_SYSTEM,
            nvl(null, '') REFERENCE_SOURCE_SYSTEM,
            nvl(null, '') SP_FIRST_NAME,
            nvl(null, '') SP_MIDDLE_NAME,
            nvl(null, '') SP_LAST_NAME,
            'ORA_MEASURE_PERIOD_SATISFIED' SATISFACTION_MEASUREMENT_MODEL,
            'HADOOP' SOURCE_DOCUMENT_TYPE_CODE,
            nvl(null, '') DOCUMENT_TYPE_CODE,
            nvl(null, '') CONTRACT_UPDATE_TEMPLATE_NAME,
            nvl(null, '') CONTRACT_UPDATE_TEMPLATE_ID,
            nvl(case
              when EVENT_TYPE = 'Modification' then nvl(date_format(PRODUCT_START, 'dd-MMM-yy'), '') 
              when EVENT_TYPE = 'Termination' then nvl(date_format(date_add(PRODUCT_END,1), 'dd-MMM-yy'), '') 
              else nvl(null,'')
            end,'') CONTRACT_MODIFICATION_DATE, --SAME AS PRODUCT_START
            nvl(null, '') INITIAL_DOC_LINE_ID_INT_1, 
            nvl(null, '') INITIAL_DOC_LINE_ID_INT_2,
            nvl(null, '') INITIAL_DOC_LINE_ID_INT_3,
            nvl(null, '') INITIAL_DOC_LINE_ID_INT_4,
            nvl(null, '') INITIAL_DOC_LINE_ID_INT_5,
            nvl(case
              when EVENT_TYPE = 'Modification' then substr(atfr.et_anum,0,30)
              else nvl(null,'')
            end,'') INITIAL_DOC_LINE_ID_CHAR_1, --if event_type modification dari salah satu line nya modificiation ,contrac_id
            nvl(case
              when EVENT_TYPE = 'Modification' then substr(atfr.et_anum,31,60)
              else nvl(null,'')
            end,'') INITIAL_DOC_LINE_ID_CHAR_2, --kalau lebih dari 30
            nvl(case
              when EVENT_TYPE = 'Modification' then substr(atfr.et_si,0,60)
              else nvl(null,'')
            end,'')  INITIAL_DOC_LINE_ID_CHAR_3,--if event_type modification dari salah satu line nya modificiation, service_id
            nvl(case
              when EVENT_TYPE = 'Modification' then substr(atfr.et_si,31,60)
              else nvl(null,'')
            end,'')  INITIAL_DOC_LINE_ID_CHAR_4, --kalau lebih dari 30
            nvl(case
              when EVENT_TYPE = 'Modification' then atfr.et_p_i
              else nvl(null,'')
            end,'') INITIAL_DOC_LINE_ID_CHAR_5, --if event_type modification dari salah satu line nya modificiation,po_id
            '' INITIAL_DOCUMENT_TYPE_ID,
            nvl(case 
              when EVENT_TYPE = 'Modification' then 'HADOOP' 
            else nvl(null,'')
            end,'')  INITIAL_DOCUMENT_TYPE_CODE, --v1 'HADOOP'  INITIAL_DOCUMENT_TYPE_CODE
            nvl(case 
              when EVENT_TYPE = 'Modification' then 'HADOOP' 
            else nvl(null,'')
            end,'') INITIAL_SOURCE_SYSTEM, --v1 'HADOOP' INITIAL_SOURCE_SYSTEM,
            nvl(case 
              when EVENT_TYPE = 'Modification' then 'Y' 
            else nvl(null,'')
            end,'') ADD_TO_CONTRACT_FLAG, --v1 'Y' ADD_TO_CONTRACT_FLAG,
            nvl(case 
              when EVENT_TYPE = 'Modification' then 'CREATE NEW PO' 
            else nvl(null,'')
            end,'') ADD_TO_CONTRACT_ACTION_CODE, --v1 'CREATE NEW PO' ADD_TO_CONTRACT_ACTION_CODE,
            nvl(null, '') MANUAL_REVIEW_REQUIRED,
            nvl(null, '') REVISION_INTENT_TYPE_CODE,
            nvl(null, '') RECURRING_FLAG,
            nvl(null, '') RECURRING_FREQUENCY,
            nvl(null, '') RECURRING_PATERN_CODE,
            nvl(null, '') RECURRING_AMOUNT,
            nvl(case
              when EVENT_TYPE = 'Termination' then nvl(date_format(PRODUCT_END, 'dd-MMM-yy'), '')
              else ''
            end,'') TERMINATION_DATE,
            nvl(case
              when EVENT_TYPE = 'Modification' or EVENT_TYPE = 'Termination' then 'Immaterial' 
              else nvl(null, '')
            end,'') IMMATERIAL_CHANGE_CODE, -- "immaterial"
        case when CHARGE_IDR = '' then 0 else cast(nvl(CHARGE_IDR,0) as DOUBLE) end UNIT_SSP
        from b2b_transform_pd
          left join (
            select 
            AGREEMENT_NUM a_n, 
            CUSTOMER_NAME c_n,
            count(*) cnt
            from b2b_transform_pd
            where CA_NM != "null"
                  and AGREEMENT_NUM != "null"
            group by AGREEMENT_NUM, CUSTOMER_NAME
            ) b
            on b2b_transform_pd.AGREEMENT_NUM = b.a_n
            and b2b_transform_pd.CUSTOMER_NAME = b.c_n
          left join (select AGREEMENT_NUM et_anum ,SERVICE_ID et_si,PO_ID et_p_i from event_type_after_running) atfr
            on b2b_transform_pd.AGREEMENT_NUM = atfr.et_anum 
            and b2b_transform_pd.SERVICE_ID = atfr.et_si 
            and b2b_transform_pd.PO_ID = atfr.et_p_i
        where CA_NM != "null"
              and AGREEMENT_NUM != "null"
              and EVENT_TYPE = "Creation"
      """)
    lineDf.show(false)
    lineDf.registerTempTable("line")
    lineDf.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", "|")
    .save("/user/hdp-rev_dev/ifrs_b2b/output/testrevenue/L")
    
    
    
    val billingDf = sqlContext.sql("""
      select
        date_format(ACTUAL_BILL_DTM,'dd-MMM-yy') BILL_DATE,     
        date_format(ACTUAL_BILL_DTM,'dd-MMM-yy') BILL_ACCOUNTING_DATE,   
        INVOICE_NUM BILL_ID,
        concat(AGREEMENT_NAME,'_',lpad(row_number() over (partition by AGREEMENT_NAME), 2, '0')) BILL_NUMBER,
        concat(INVOICE_NUM,lpad(row_number() over (partition by AGREEMENT_NAME), 3, '0')) BILL_LINE_ID,
        row_number() over (partition by AGREEMENT_NAME) BILL_LINE_NUMBER,
        nvl(null, '') BILL_QUANTITY,
        CHARGE_IDR BILL_AMOUNT,
        nvl(null, '') DOC_LINE_ID_INT_1,
        nvl(null, '') DOC_LINE_ID_INT_2,
        nvl(null, '') DOC_LINE_ID_INT_3,
        nvl(null, '') DOC_LINE_ID_INT_4,
        nvl(null, '') DOC_LINE_ID_INT_5,
        nvl(substr(AGREEMENT_NAME, 1, 30), '') DOC_LINE_ID_CHAR_1, --takeout prc_dt
        nvl(substr(AGREEMENT_NAME, 31, 60), '') DOC_LINE_ID_CHAR_2, --takeout prc_dt
        nvl(substr(SERVICE_ID, 1, 30), '') DOC_LINE_ID_CHAR_3,
        nvl(substr(SERVICE_ID, 31, 60), '') DOC_LINE_ID_CHAR_4,
        nvl(concat(PRODUCT_SEQ, '_', SUBSCRIPTION_REF, '_', CHARGE_TYPE), '') DOC_LINE_ID_CHAR_5,
        nvl(null, '') DOCUMENT_TYPE_ID,
        'HADOOP' BILLING_APPLICATION,
        'HADOOP' SOURCE_SYSTEM,
        CHARGE_IDR BILL_ACCTD_AMOUNT,
        'HADOOP' DOCUMENT_TYPE_CODE--v1,SUBSCRIPTION_REF,PRODUCT_ID,PRODUCT_SEQ
      from b2b_transform --get original b2b_transform table without any duplicate data
      """)
      billingDf.registerTempTable("b2b_biling")
      sqlContext.sql("select count(*),'b2b_biling' from b2b_biling").show(false)
    
      
     val revenueDf = sqlContext.sql("""
      select value, ord
      from (
        select concat_ws('|', *) value, concat(LPAD(concat(DOC_ID_CHAR_1, ORIG_SYS_BILL_TO_CUST_SITE_REF), 100, '0'), RECORD_TYPE) ord from header
        union all
        select concat_ws('|', *) value, concat(LPAD(concat(DOC_ID_CHAR_1, ORIG_SYS_BILL_TO_CUST_SITE_REF), 100, '0'), RECORD_TYPE) ord from line
      ) t
      order by ord asc
      """)
    
    revenueDf.select("value").coalesce(1).write.format("com.databricks.spark.csv") //changed from repartition to coalesce, because when used repartition the data not sorting oredered
    .mode("overwrite")
    .save(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId)
    val revenue_file = fs.globStatus(
    new Path(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId + "/"+ revenue_file),
    new Path(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId + "/initial_revenuedataimport_"+prcDt+"_"+hh+mm+ss+".dat"))//v1 ".csv"))
    
    
    billingDf.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "false").option("delimiter", "|").option("quoteMode","ALL")
    .save(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId)
    val billing_file = fs.globStatus(
    new Path(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId +"/"+ billing_file),
    new Path(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId + "/initial_billingdataimport_"+prcDt+"_"+hh+mm+ss+".dat"))
   
    // Reconcilliation file
    val reconcilliationDf = sqlContext.sql("""
        select t.* 
        	from
        		(
        		select '"""+prcDt+"""' PRC_DT,
             ACTUAL_BILL_DTM ACTUAL_BILL_DTM, 
             CUST_REF CUSTOMER_REF, 
             AGRMT_NUM CONTRACT_NUMBER, 
             SUBSCRIPTION_TYPE, 
             ACCOUNT_NUM, 
             SVC_ID SERVICE_ID, 
             cast(CHARGE_ORI as DOUBLE) CHARGE_ORIGINAL, 
             cast(CHARGE_IDR as DOUBLE) CHARGE_IDR,
             'BILLING' RECON_TYPE 
        			from bill_charge
        		union all
        		select b2b_transform.PRC_DT,
              b2b_transform.ACTUAL_BILL_DTM,
              b2b_transform.CUSTOMER_REF,       			 
        			b2b_transform.AGREEMENT_NAME,
              b2b_transform.SUBSCRIPTION_TYPE,
              b2b_transform.ACCOUNT_NUM,
              b2b_transform.SERVICE_ID,
        			b2b_transform.CHARGE_ORIGINAL,
        			b2b_transform.CHARGE_IDR,
        			'DATAMART' RECON_TYPE
        			from b2b_transform
        		) t
        order by t.RECON_TYPE  asc
      """)
    reconcilliationDf.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", "|")
    .save(pathB2bIfrsReconcilliationCsv + "/process_id=" + prcDt + "_" + jobId)
    val reconcilliation_file = fs.globStatus(
    new Path(pathB2bIfrsReconcilliationCsv + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsReconcilliationCsv + "/process_id=" + prcDt + "_" + jobId + "/"+ reconcilliation_file),
    new Path(pathB2bIfrsReconcilliationCsv + "/process_id=" + prcDt + "_" + jobId + "/initial_reconcilliationdataimport_"+prcDt+"_"+hh+mm+ss+".dat"))

    //fs.delete(new Path("mydata.csv-temp"), true)
    
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
    StructField("CONTRACT_SLA", StringType),
    StructField("MIDI_ATTRIBUTE", StringType, true)))
  
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