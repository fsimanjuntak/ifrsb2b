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
      return 
    
    
    //TODO Input Paths
    val pathCweoDailyAsset = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_ASSET")
    val pathCweoDailyAgreement = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_AGREEMENT")
    val pathCweoDailyCstacc = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_CSTACC")
    val pathCweoDailyOrder = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.CATALIST_DAILY_ORDER")
    val pathIfrsBillCharge = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.INPUT.RBM_IFRS_BILL_CHARGE")
    
    val pathB2bIfrsTransform = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_TRANSFORM_SOR")
    val pathB2bIfrsRevenueCsv = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_REVENUE_CSV")
    val pathB2bIfrsBilingCsv = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.OUTPUT.B2B_IFRS_BILING_CSV")
    
    //val pathB2bIfrsTransformEventTypeTempCsv = Common.getConfigDirectory(sc, configDir, "IFRS_B2B.TEMP.B2B_IFRS_EVENT_TYPE")
    
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
    
    sqlContext.read.format("com.databricks.spark.csv").option("delimiter","|")
    .option("header", "true").load("C:/Users/YosuaSimanjuntak/Downloads/kharis/Development/test/output/b2b_ifrs_transform/process_id=20180912_888001")
    .registerTempTable("b2b_transform_pd")
    
    val eventTypeTempDf = broadcast(
       sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter","|")
      .load("C:/Users/YosuaSimanjuntak/Downloads/kharis/Development/test/temp/b2b_ifrs_event_type_temp_csv")//+"/"+evenTypeTempFile)
      .cache())
      .distinct()
      eventTypeTempDf.registerTempTable("event_type_after_running")
      
    val lineDf1 = sqlContext.sql("""
        select 
            b2b_transform_pd.AGREEMENT_NUM,
            atfr.et_si,
            atfr.et_anum,
            atfr.et_p_i,
            atfr.row_number_et
            
        from b2b_transform_pd
          left join (
            select 
            AGREEMENT_NAME a_n, 
            CUSTOMER_NAME c_n,
            count(*) cnt
            from b2b_transform_pd
            where CA_NM is not null
                  and AGREEMENT_NAME is not null
            group by AGREEMENT_NAME, CUSTOMER_NAME) b
            on b2b_transform_pd.AGREEMENT_NAME = b.a_n
            and b2b_transform_pd.CUSTOMER_NAME = b.c_n
          left join (
              select 
              distinct event_type_after_running.SERVICE_ID et_si,
              event_type_after_running.AGREEMENT_NUM et_anum,
              event_type_after_running.PO_ID et_p_i,
              event_type_after_running.AGREEMENT_NAME et_aname,
              row_number() over (partition by event_type_after_running.SERVICE_ID,event_type_after_running.AGREEMENT_NUM,event_type_after_running.PO_ID) row_number_et
              from b2b_transform_pd
              left join event_type_after_running
              on b2b_transform_pd.AGREEMENT_NUM = event_type_after_running.AGREEMENT_NUM
              ) atfr
              on b2b_transform_pd.AGREEMENT_NUM = atfr.et_anum 
              and b2b_transform_pd.SERVICE_ID = atfr.et_si 
              and b2b_transform_pd.PO_ID = atfr.et_p_i
              and atfr.row_number_et = 1
            where CA_NM != "null"
                  and AGREEMENT_NAME != "null"
                  and EVENT_TYPE != "null"
           
      """)
    lineDf1.show(false)
    lineDf1.registerTempTable("line")
    
    
    sqlContext.sql("select count(*) from line").show(false)
    
    lineDf1.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "|").option("header", "true")
    .save("C:/Users/YosuaSimanjuntak/Downloads/kharis/Development/test/output/testing_b2b_ifrs_transofrm")
      
      return
    
    //TODO Main Transformation
    val dailyAssetDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathCweoDailyAsset + "/file_date=*"))
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
    .globStatus(new Path(pathCweoDailyAgreement + "/file_date=*"))
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
    .globStatus(new Path(pathCweoDailyCstacc + "/file_date=*"))
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
    .globStatus(new Path(pathCweoDailyOrder + "/file_date=*"))
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
    
    
   /* val eventTypeTempDf = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(pathB2bIfrsTransformEventTypeTempCsv)
      .cache())
    eventTypeTempDf.registerTempTable("event_type_temp")*/
    
    
    val billChargeDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathIfrsBillCharge + "/file_date=*"))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathIfrsBillCharge)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(ifrsBillChargeSchema)
      .load(p + "/*.txt")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    .withColumn("PRODUCT_END", when(col("CHRG_TP") === "INSTALLATION", col("PRODUCT_START")).otherwise(col("PRODUCT_END")))
    billChargeDf.registerTempTable("bill_charge")
    
    
    
    // References
    val refGroupOfServicesDf = ReferenceDF.getGroupOfServicesDF(sqlContext, pathRefGroupOfServices + "/*.csv")
    refGroupOfServicesDf.registerTempTable("ref_group_of_services")
    
    
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
        concat(daily_cstacc.CUST_SALUTATION, '_', daily_cstacc.CA_NM) CUSTOMER_NAME,
        daily_cstacc.CUST_SALUTATION CA_SALUTATION,
        daily_cstacc.CA_NM CA_NM,

        --agreement
        case when parent_asset.AGRMT_NUM = '' then bill_charge.AGRMT_NUM else parent_asset.AGRMT_NUM end AGREEMENT_NUM,
        substr(daily_agreement.AGRMT_NM, 1, 50) AGREEMENT_NAME,
        nvl(daily_agreement.CONTRACT_PERIOD, '0') CONTRACT_PERIOD,
        daily_agreement.CONTRACT_INITIAL_AMT CONTRACT_INITIAL_AMT,
        date_format(from_unixtime(unix_timestamp(daily_agreement.AGRMT_START, 'dd-MMM-yy')), 'yyyy-MM-dd') AGREEMENT_START,
        date_format(from_unixtime(unix_timestamp(daily_agreement.AGRMT_END, 'dd-MMM-yy')), 'yyyy-MM-dd') AGREEMENT_END,

    
        bill_charge.SUBS_PD_ID SUBS_PRODUCT_ID,
        bill_charge.SUBSCRIPTION_TYPE SUBSCRIPTION_TYPE,
        bill_charge.SUBS_REF SUBSCRIPTION_REF,

        daily_asset_product_type.PD_TP PRODUCT_TYPE,
        parent_asset.PD_CGY PRODUCT_CATEGORY,
        case when nvl(bill_charge.SVC_ID, '') = '' then parent_asset.SVC_ID
          else bill_charge.SVC_ID
        end SERVICE_ID,
        
        case when bill_charge.PRODUCT_ID = '1' and parent_asset.PD_CGY = 'Mobile' and  parent_asset.PD_CGY = 'Corporate Mobile Bulk' then bill_charge.PRODUCT_NM
          else case when bill_charge.CHRG_TP in ('MRC', 'INSTALLATION') then
              service_asset.PD_NM
            when bill_charge.CHRG_TP = 'ONE TIME CHARGE' then parent_asset_otc_ast_id.PD_NM
            end
          end BUSINESS_AREA_NAME,
         
        date_format(substr(bill_charge.SVC_START_DT,0,10),'yyyy-MM-dd') SERVICE_START,
        date_format(substr(bill_charge.SVC_END_DT,0,10),'yyyy-MM-dd') SERVICE_END,

        case parent_asset.PD_CGY
          when 'Mobile' then 'B2B Mobile'
          when 'Mobile Bulk' then 'B2B Mobile'
          when 'Corporate Mobile Bulk' then 'B2B Mobile'
          when 'IPhone' then 'B2B IPhone'
          else 'B2B MIDI'
          end CUSTOMER_SEGMENT,

        bill_charge.PD_SEQ PRODUCT_SEQ,
        bill_charge.PRODUCT_ID PRODUCT_ID,
        bill_charge.PRODUCT_NM PRODUCT_NAME,
        bill_charge.OTC_ID OTC_ID,
        bill_charge.OTC_SEQ OTC_SEQ,
        bill_charge.CURRENCY_CODE CURRENCY_CODE,
        bill_charge.CHRG_TP CHARGE_TYPE,

        cast(bill_charge.CHARGE_ORI as int) CHARGE_ORIGINAL,
        cast(bill_charge.CHARGE_IDR as int) CHARGE_IDR,
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
          case when bill_charge.CHRG_TP = 'INSTALLATION' and if(bill_charge.PRODUCT_END = '', null,bill_charge.PRODUCT_END) is null then add_months(bill_charge.PRODUCT_START,nvl(daily_agreement.CONTRACT_PERIOD, '0'))
               when bill_charge.CHRG_TP = 'MRC' and if(bill_charge.PRODUCT_END = '', null,bill_charge.PRODUCT_END) is null then add_months(bill_charge.PRODUCT_START,nvl(daily_agreement.CONTRACT_PERIOD, '0'))
               when bill_charge.CHRG_TP = 'ONE TIME CHARGE' then bill_charge.CHRG_START
               else bill_charge.PRODUCT_END
          end
        ,0,10),'yyyy-MM-dd') PRODUCT_END,
        concat(bill_charge.PD_SEQ, '_', bill_charge.SUBS_REF, '_', bill_charge.CHRG_TP) PO_ID,
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
         case 
          when (daily_order.ORDER_TYPE = 'New Registration' 
               or daily_order.ORDER_TYPE = 'Migrate Post Ind to Post Corp'
               or daily_order.ORDER_TYPE = 'Migrate Prepaid to Post Corp')
               and cast(months_between(date_format(current_timestamp(),'yyyy-MM'),date_format(from_unixtime(unix_timestamp(daily_order.ORDER_COMPLETION_DATE, 'dd-MM-yyyy')), 'yyyy-MM'))as int) = 3
               and daily_order.ORDER_STATUS = 'Complete' then 'Creation'
          when (daily_order.ORDER_TYPE = 'Change Package' and daily_order.ACTION = 'Add')
               or (daily_order.ORDER_TYPE = 'Change Ownership' and daily_order.ACTION = 'Add')
               and cast(months_between(date_format(current_timestamp(),'yyyy-MM'),date_format(from_unixtime(unix_timestamp(daily_order.ORDER_COMPLETION_DATE, 'dd-MM-yyyy')), 'yyyy-MM'))as int) = 3
               and daily_order.ORDER_STATUS = 'Complete' then case when dense_rank() over(partition by bill_charge.SUBS_REF order by date_format(from_unixtime(unix_timestamp(daily_order.ORDER_COMPLETION_DATE, 'dd-MM-yyyy')), 'yyyy-MM-dd') asc ) > 1 then 'Modification' else 'Creation' end  --PUT JOIN HEREEEEEEEEEEEEE ELSE = 'CREATION'
          when daily_order.ORDER_TYPE = 'Modify'
               and (daily_order.ACTION = 'Add' or daily_order.ACTION = 'Delete')
               and cast(months_between(date_format(current_timestamp(),'yyyy-MM'),date_format(from_unixtime(unix_timestamp(daily_order.ORDER_COMPLETION_DATE, 'dd-MM-yyyy')), 'yyyy-MM'))as int) = 3
               and daily_order.ORDER_STATUS = 'Complete' then 'Modification'
          when daily_order.ORDER_TYPE = 'Migrate Post Corp to Post Ind'
               or daily_order.ORDER_TYPE = 'Terminate'
               or (daily_order.ORDER_TYPE = 'Change Package' and daily_order.ACTION = 'Delete')
               or (daily_order.ORDER_TYPE = 'Change Ownership' and daily_order.ACTION = 'Delete')
               and cast(months_between(date_format(current_timestamp(),'yyyy-MM'),date_format(from_unixtime(unix_timestamp(daily_order.ORDER_COMPLETION_DATE, 'dd-MM-yyyy')), 'yyyy-MM'))as int) = 3
               and daily_order.ORDER_STATUS = 'Complete' then 'Termination'
          else null
        end EVENT_TYPE,
        daily_order.ORDER_TYPE,
        daily_order.ORDER_NUM,
        date_format(from_unixtime(unix_timestamp(daily_order.ORDER_COMPLETION_DATE, 'dd-MM-yyyy')), 'yyyy-MM-dd') ORDER_COMPLETION_DATE
        
  
        from bill_charge

          -----------------------------------------------GET PRODUCT_TYPE-------------------------------------------------------------
          left join daily_asset daily_asset_product_type
            on bill_charge.SUBS_REF = daily_asset_product_type.SUBSCRIPTION_REF
        		and bill_charge.PD_SEQ = daily_asset_product_type.PRODUCT_SEQUENCE
            and bill_charge.PRODUCT_ID = daily_asset_product_type.BILLING_PRODUCT_ID

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
          
          ------------------------------------------------GET AGREEMENT---------------------------------------------------------------
          left join daily_agreement
            on daily_agreement.AGRMT_NUM = parent_asset.AGRMT_NUM
          left join daily_agreement daily_agreement_backup
            on daily_agreement_backup.AGRMT_NM = bill_charge.AGRMT_NUM --for test : input data in bill charge, when AGRMT_NUM is null

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

          -------------------------------------------GET EVENT_TYPE------------------------------------------------------------------
          left join daily_order
            on (bill_charge.SUBS_REF = daily_order.SUBSCRIPTION_REF
            and bill_charge.PRODUCT_ID = daily_order.BILLING_PRODUCT_ID
            and bill_charge.PD_SEQ = daily_order.PRODUCT_SEQ
            and daily_order.ORDER_TYPE in ('New Registration','Migrate Prepaid to Post Corp','Migrate Post Ind to Post Corp','Change Package','Change Ownership','Modify','Terminate','Migrate Post Corp to Post Ind'))
            or (bill_charge.SUBS_REF = daily_order.SUBSCRIPTION_REF
            and bill_charge.PRODUCT_ID = 1
            and bill_charge.SUBSCRIPTION_TYPE = 'MOBILE'
            and bill_charge.PD_SEQ = daily_order.PRODUCT_SEQ
            and daily_order.ORDER_TYPE in ('New Registration','Migrate Prepaid to Post Corp','Migrate Post Ind to Post Corp','Change Package','Change Ownership','Modify','Terminate','Migrate Post Corp to Post Ind'))

      """)
    b2bIfrsTransform.persist(StorageLevel.DISK_ONLY)
    b2bIfrsTransform.registerTempTable("b2b_transform")
    b2bIfrsTransform.show();
   
    sqlContext.sql("select count(*) from b2b_transform").show(false)
   
    /*
    b2bIfrsTransform.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", "|")
    .save("C:/Users/YosuaSimanjuntak/Downloads/kharis/Development/test/output/testing_b2b_ifrs_transofrm" + "/process_id=" + prcDt + "_" + jobId)
		*/
    
   /* sqlContext.sql("select * from event_type_temp").show(100)
    
    //dailyRunning running
    val eventType = sqlContext.sql("""
        select
        distinct 
        a.SERVICE_ID,
        a.AGREEMENT_NUM
        from (
          select 
          distinct event_type_temp.SERVICE_ID,
          event_type_temp.AGREEMENT_NUM
          from event_type_temp
          union all
          select
          distinct b2b_transform.SERVICE_ID,
          b2b_transform.AGREEMENT_NUM
          from
          b2b_transform
          where b2b_transform.EVENT_TYPE = 'Creation'
          ) a
      """)
    eventType.registerTempTable("event_type")*/
    
    //initialRunning
    /*val eventType = sqlContext.sql("""
        
          
        select
        distinct b2b_transform.SERVICE_ID,
        b2b_transform.AGREEMENT_NUM
        from
        b2b_transform
        where b2b_transform.EVENT_TYPE = 'Creation'
          
      """)
    eventType.registerTempTable("event_type")*/
    
    /*eventType.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", "|")
    .save(pathB2bIfrsTransformEventTypeTempCsv)
    fs.rename(
    new Path(pathB2bIfrsTransformEventTypeTempCsv+"/part-00000"),
    new Path(pathB2bIfrsTransformEventTypeTempCsv+"/ifrs_b2b_eventype_"+prcDt+".csv"))
      */
     
   
   /* val b2bIfrsTransformEt = sqlContext.sql("""

        select
        b2b_transform.PRC_DT,
        b2b_transform.JOB_ID,
        b2b_transform.ACCOUNT_NUM,
        b2b_transform.CUSTOMER_REF,
        b2b_transform.CUSTOMER_NAME,
        b2b_transform.CA_SALUTATION,
        b2b_transform.CA_NM,
        b2b_transform.AGREEMENT_NUM,
        b2b_transform.AGREEMENT_NAME,
        b2b_transform.CONTRACT_PERIOD,
        b2b_transform.CONTRACT_INITIAL_AMT,
        b2b_transform.AGREEMENT_START,
        b2b_transform.AGREEMENT_END,
        b2b_transform.SUBS_PRODUCT_ID,
        b2b_transform.SUBSCRIPTION_TYPE,
        b2b_transform.SUBSCRIPTION_REF,
        b2b_transform.PRODUCT_TYPE,
        b2b_transform.PRODUCT_CATEGORY,
        b2b_transform.SERVICE_ID,
        b2b_transform.BUSINESS_AREA_NAME,
        b2b_transform.SERVICE_START,
        b2b_transform.SERVICE_END,
        b2b_transform.CUSTOMER_SEGMENT,
        b2b_transform.PRODUCT_SEQ,
        b2b_transform.PRODUCT_ID,
        b2b_transform.PRODUCT_NAME,
        b2b_transform.OTC_ID,
        b2b_transform.OTC_SEQ,
        b2b_transform.CURRENCY_CODE,
        b2b_transform.CHARGE_TYPE,
        b2b_transform.CHARGE_ORIGINAL,
        b2b_transform.CHARGE_IDR,
        b2b_transform.CHARGE_START_DT,
        b2b_transform.CHARGE_END_DT,
        b2b_transform.PRODUCT_START,
        b2b_transform.PRODUCT_END,
        b2b_transform.PO_ID,
        b2b_transform.INVOICE_NUM,
        b2b_transform.ACTUAL_BILL_DTM,
        b2b_transform.EVENT_FLAG,
        b2b_transform.SERVICE_GROUP,
        b2b_transform.PROFIT_CENTER,
        b2b_transform.CUSTOMER_GROUP,
        case
          when b2b_transform.SERVICE_ID = event_type.SERVICE_ID and b2b_transform.AGREEMENT_NUM = event_type.AGREEMENT_NUM then 'Modification'
          else b2b_transform.EVENT_TYPE
        end EVENT_TYPE,
        b2b_transform.ORDER_TYPE,
        b2b_transform.ORDER_NUM,
        b2b_transform.ORDER_COMPLETION_DATE
        
        from b2b_transform
        left join event_type
          on b2b_transform.SERVICE_ID = event_type.SERVICE_ID
          and b2b_transform.AGREEMENT_NUM = event_type.AGREEMENT_NUM
      """).registerTempTable("b2bIfrsTransformEt")
      */
   
    
    
    val productDescription = sqlContext.sql("""

        select 
        AGREEMENT_NUM,
        SUBSCRIPTION_REF,
        SERVICE_ID,
        concat_ws('and', collect_list(concat(BUSINESS_AREA_NAME))) PRODUCT_DESCRIPTION 
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
      
    sqlContext.sql("select count(*) from b2b_transform_pd").show()
      
    
    b2bIfrsTransformPd.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", "|")
    .save(pathB2bIfrsTransform + "/process_id=" + prcDt + "_" + jobId)
    val data_mart_file = fs.globStatus(
    new Path(pathB2bIfrsTransform + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsTransform + "/process_id=" + prcDt + "_" + jobId + "/"+ data_mart_file),
    new Path(pathB2bIfrsTransform + "/process_id=" + prcDt + "_" + jobId + "/ifrs_data_mart"+prcDt+"_"+hh+mm+ss+".dat"))
    
     
      
    val headerDf = sqlContext.sql("""
      select
        'H' RECORD_TYPE,
        'HADOOP' DOCUMENT_TYPE_CODE,
        nvl(null, '') DOC_ID_INT_1,
        nvl(null, '') DOC_ID_INT_2,
        nvl(null, '') DOC_ID_INT_3,
        nvl(null, '') DOC_ID_INT_4,
        CA_NM DOC_ID_INT_5,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'),1,30), '') DOC_ID_CHAR_1,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'),31,60), '') DOC_ID_CHAR_2,
        nvl(null, '') DOC_ID_CHAR_3,
        nvl(null, '') DOC_ID_CHAR_4,
        nvl(null, '') DOC_ID_CHAR_5,
        'HADOOP' SOURCE_SYSTEM,
        nvl(date_format(from_unixtime(unix_timestamp('"""+prcDt+"""', 'yyyyMMdd')), 'dd-MMM-yy'), '') DOCUMENT_DATE,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'), ''),30) DOCUMENT_NUMBER,
        'IDR' CURRENCY_CODE,
        'INDOSAT BU' ORGANIZATION_NAME,
        'PT.Indosat Tbk.' LEGAL_ENTITY_NAME,
        nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_SITE_REF,
        nvl(null, '') ORIG_SYS_SHIP_TO_CUST_SITE_REF,
        nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_REF,
        nvl(null, '') ORIG_SYS_SHIP_TO_CUST_REF,
        nvl(null, '') CUST_PO_NUMBER,
        nvl(null, '') SRC_ATTRIBUTE_CHAR1,
        nvl(null, '') SRC_ATTRIBUTE_CHAR2,
        nvl(null, '') SRC_ATTRIBUTE_CHAR3,
        nvl(null, '') SRC_ATTRIBUTE_CHAR4,
        nvl(null, '') SRC_ATTRIBUTE_CHAR5,
        nvl(null, '') SRC_ATTRIBUTE_CHAR6,
        nvl(null, '') SRC_ATTRIBUTE_CHAR7,
        nvl(null, '') SRC_ATTRIBUTE_CHAR8,
        nvl(null, '') SRC_ATTRIBUTE_CHAR9,
        nvl(null, '') SRC_ATTRIBUTE_CHAR10,
        nvl(null, '') SRC_ATTRIBUTE_CHAR11,
        nvl(null, '') SRC_ATTRIBUTE_CHAR12,
        nvl(null, '') SRC_ATTRIBUTE_CHAR13,
        nvl(null, '') SRC_ATTRIBUTE_CHAR14,
        nvl(null, '') SRC_ATTRIBUTE_CHAR15,
        nvl(null, '') SRC_ATTRIBUTE_CHAR16,
        nvl(null, '') SRC_ATTRIBUTE_CHAR17,
        nvl(null, '') SRC_ATTRIBUTE_CHAR18,
        nvl(null, '') SRC_ATTRIBUTE_CHAR19,
        nvl(null, '') SRC_ATTRIBUTE_CHAR20,
        nvl(null, '') SRC_ATTRIBUTE_CHAR21,
        nvl(null, '') SRC_ATTRIBUTE_CHAR22,
        nvl(null, '') SRC_ATTRIBUTE_CHAR23,
        nvl(null, '') SRC_ATTRIBUTE_CHAR24,
        nvl(null, '') SRC_ATTRIBUTE_CHAR25,
        nvl(null, '') SRC_ATTRIBUTE_CHAR26,
        nvl(null, '') SRC_ATTRIBUTE_CHAR27,
        nvl(null, '') SRC_ATTRIBUTE_CHAR28,
        nvl(null, '') SRC_ATTRIBUTE_CHAR29,
        nvl(null, '') SRC_ATTRIBUTE_CHAR30,
        nvl(null, '') SRC_ATTRIBUTE_CHAR31,
        nvl(null, '') SRC_ATTRIBUTE_CHAR32,
        nvl(null, '') SRC_ATTRIBUTE_CHAR33,
        nvl(null, '') SRC_ATTRIBUTE_CHAR34,
        nvl(null, '') SRC_ATTRIBUTE_CHAR35,
        nvl(null, '') SRC_ATTRIBUTE_CHAR36,
        nvl(null, '') SRC_ATTRIBUTE_CHAR37,
        nvl(null, '') SRC_ATTRIBUTE_CHAR38,
        nvl(null, '') SRC_ATTRIBUTE_CHAR39,
        nvl(null, '') SRC_ATTRIBUTE_CHAR40,
        nvl(null, '') SRC_ATTRIBUTE_CHAR41,
        nvl(null, '') SRC_ATTRIBUTE_CHAR42,
        nvl(null, '') SRC_ATTRIBUTE_CHAR43,
        nvl(null, '') SRC_ATTRIBUTE_CHAR44,
        nvl(null, '') SRC_ATTRIBUTE_CHAR45,
        nvl(null, '') SRC_ATTRIBUTE_CHAR46,
        nvl(null, '') SRC_ATTRIBUTE_CHAR47,
        nvl(null, '') SRC_ATTRIBUTE_CHAR48,
        nvl(null, '') SRC_ATTRIBUTE_CHAR49,
        nvl(null, '') SRC_ATTRIBUTE_CHAR50,
        nvl(null, '') SRC_ATTRIBUTE_CHAR51,
        nvl(null, '') SRC_ATTRIBUTE_CHAR52,
        nvl(null, '') SRC_ATTRIBUTE_CHAR53,
        nvl(null, '') SRC_ATTRIBUTE_CHAR54,
        nvl(null, '') SRC_ATTRIBUTE_CHAR55,
        nvl(null, '') SRC_ATTRIBUTE_CHAR56,
        nvl(null, '') SRC_ATTRIBUTE_CHAR57,
        nvl(null, '') SRC_ATTRIBUTE_CHAR58,
        nvl(null, '') SRC_ATTRIBUTE_CHAR59,
        nvl(null, '') SRC_ATTRIBUTE_CHAR60,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER1,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER2,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER3,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER4,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER5,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER6,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER7,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER8,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER9,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER10,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER11,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER12,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER13,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER14,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER15,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER16,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER17,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER18,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER19,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER20,
        nvl(null, '') SRC_ATTRIBUTE_DATE1,
        nvl(null, '') SRC_ATTRIBUTE_DATE2,
        nvl(null, '') SRC_ATTRIBUTE_DATE3,
        nvl(null, '') SRC_ATTRIBUTE_DATE4,
        nvl(null, '') SRC_ATTRIBUTE_DATE5,
        nvl(null, '') SRC_ATTRIBUTE_DATE6,
        nvl(null, '') SRC_ATTRIBUTE_DATE7,
        nvl(null, '') SRC_ATTRIBUTE_DATE8,
        nvl(null, '') SRC_ATTRIBUTE_DATE9,
        nvl(null, '') SRC_ATTRIBUTE_DATE10
      from b2b_transform_pd
      where CA_NM is not null
        and AGREEMENT_NAME is not null 
        and EVENT_TYPE is not null
      group by
        AGREEMENT_NAME, CUSTOMER_NAME, CA_NM
      """)
    headerDf.show(false)
    headerDf.registerTempTable("header")
    
    val lineDf = sqlContext.sql("""
      select
        'L' RECORD_TYPE,
        'HADOOP' DOCUMENT_TYPE_CODE,
        nvl(null, '') DOC_LINE_ID_INT_1,
        nvl(null, '') DOC_LINE_ID_INT_2,
        nvl(null, '') DOC_LINE_ID_INT_3,
        nvl(null, '') DOC_LINE_ID_INT_4,
        nvl(null, '') DOC_LINE_ID_INT_5,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'),1,30), '') DOC_LINE_ID_CHAR_1,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'),31,60), '') DOC_LINE_ID_CHAR_2,
        substr(nvl(SERVICE_ID, ''),1,30) DOC_LINE_ID_CHAR_3,
        substr(nvl(SERVICE_ID, ''),31,60) DOC_LINE_ID_CHAR_4,
        nvl(concat(PRODUCT_SEQ, '_', SUBSCRIPTION_REF, '_', CHARGE_TYPE), '') DOC_LINE_ID_CHAR_5,
        'HADOOP' SOURCE_SYSTEM,
        row_number() over (partition by AGREEMENT_NAME) LINE_NUM,
        nvl(null, '') DOC_ID_INT_1,
        nvl(null, '') DOC_ID_INT_2,
        nvl(null, '') DOC_ID_INT_3,
        nvl(null, '') DOC_ID_INT_4,
        CA_NM DOC_ID_INT_5,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'),1,30), '') DOC_ID_CHAR_1,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'),31,60), '') DOC_ID_CHAR_2,
        nvl(null, '') DOC_ID_CHAR_3,
        nvl(null, '') DOC_ID_CHAR_4,
        nvl(null, '') DOC_ID_CHAR_5,
        'zzy' UOM_CODE,
        '1' QUANTITY,
        nvl(CHARGE_IDR, '') UNIT_SELLING_PRICE,
        nvl(1 * CHARGE_IDR, '') LINE_AMOUNT,
        nvl(CHARGE_IDR, '') UNIT_SSP,
        nvl(concat(BUSINESS_AREA_NAME, '-', SERVICE_GROUP,'-',CUSTOMER_GROUP,'-',CHARGE_TYPE), '') SOURCE_ITEM_NUMBER,
        'ISAT' INVENTORY_ORG_CODE,
        nvl(null, '') MEMO_LINE_NAME,
        'ORA_MEASURE_PERIOD_SATISFIED' SATISFACTION_MEASUREMENT_MODEL,
        'Daily Rate Partial Periods' ACCOUNTING_RULE_NAME,
        nvl(upper(date_format(PRODUCT_START, 'dd-MMM-yy')), '') RULE_START_DATE,
        nvl(upper(date_format(PRODUCT_END, 'dd-MMM-yy')), '') RULE_END_DATE,
        nvl(null, '') ACCOUNTING_RULE_DURATION,
        nvl(null, '') UNIT_LIST_PRICE,
        nvl(null, '') BASE_PRICE,
        nvl(null, '') COST_AMOUNT,
        nvl(null, '') RECURRING_FLAG,
        nvl(null, '') RECURRING_FREQUENCY,
        nvl(null, '') RECURRING_PATTERN_CODE,
        nvl(null, '') RECURRING_AMOUNT,
        nvl(null, '') TERMINATION_DATE,
        nvl(null, '') MANUAL_REVIEW_REQUIRED_FLAG,
        nvl(null, '') IMMATERIAL_CHANGE_CODE,
        nvl(null, '') CONTRACT_MODIFICATION_DATE,
        nvl(null, '') BILL_TO_CUST_CLASSIFICATION,
        nvl(null, '') CUST_PO_NUMBER,
        nvl(null, '') DOCUMENT_TYPE_CODE,
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
        nvl(null, '') REFERENCE_SOURCE_SYSTEM,
        nvl(null, '') REVENUE_REVERSAL_METHOD,
        nvl(null, '') VERSION_FLAG,
        '1' VERSION_NUMBER,
        nvl(null, '') ADD_TO_CONTRACT_FLAG,
        nvl(null, '') ADD_TO_CONTRACT_ACTION_CODE,
        nvl(null, '') INITIAL_DOCUMENT_TYPE_CODE,
        nvl(null, '') INITIAL_DOC_LINE_ID_INT_1,
        nvl(null, '') INITIAL_DOC_LINE_ID_INT_2,
        nvl(null, '') INITIAL_DOC_LINE_ID_INT_3,
        nvl(null, '') INITIAL_DOC_LINE_ID_INT_4,
        nvl(null, '') INITIAL_DOC_LINE_ID_INT_5,
        nvl(null, '') INITIAL_DOC_LINE_ID_CHAR_1,
        nvl(null, '') INITIAL_DOC_LINE_ID_CHAR_2,
        nvl(null, '') INITIAL_DOC_LINE_ID_CHAR_3,
        nvl(null, '') INITIAL_DOC_LINE_ID_CHAR_4,
        nvl(null, '') INITIAL_DOC_LINE_ID_CHAR_5,
        nvl(null, '') INITIAL_SOURCE_SYSTEM,
        nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_SITE_REF,
        nvl(null, '') ORIG_SYS_SHIP_TO_CUST_SITE_REF,
        nvl(CUSTOMER_NAME, '') ORIG_SYS_BILL_TO_CUST_REF,
        nvl(null, '') ORIG_SYS_SHIP_TO_CUST_REF,
        case when b.cnt <= 1 then 'N' else 'Y' end SRC_ATTRIBUTE_CHAR1,
        nvl(PRODUCT_DESCRIPTION, '') SRC_ATTRIBUTE_CHAR2,
        nvl(concat(SERVICE_GROUP, '-', CUSTOMER_GROUP), '') SRC_ATTRIBUTE_CHAR3,
        nvl(concat(SERVICE_GROUP, '-', CUSTOMER_GROUP, '-', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR4,
        PROFIT_CENTER SRC_ATTRIBUTE_CHAR5,
        nvl(null, '') SRC_ATTRIBUTE_CHAR6,
        nvl(null, '') SRC_ATTRIBUTE_CHAR7,
        nvl(null, '') SRC_ATTRIBUTE_CHAR8,
        nvl(null, '') SRC_ATTRIBUTE_CHAR9,
        nvl(null, '') SRC_ATTRIBUTE_CHAR10,
        nvl(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'), '') SRC_ATTRIBUTE_CHAR11,
        nvl(SERVICE_ID, '') SRC_ATTRIBUTE_CHAR12,
        nvl(concat(PRODUCT_SEQ, '_', SUBSCRIPTION_REF, '_', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR13,
        nvl(BUSINESS_AREA_NAME, '') SRC_ATTRIBUTE_CHAR14,
        nvl(SERVICE_GROUP, '') SRC_ATTRIBUTE_CHAR15,
        nvl(CUSTOMER_GROUP, '') SRC_ATTRIBUTE_CHAR16,
        nvl(CHARGE_TYPE, '') SRC_ATTRIBUTE_CHAR17,
        nvl(concat(SERVICE_GROUP, '-', CUSTOMER_GROUP, '-', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR18,
        nvl(concat(PRODUCT_NAME, '-', SERVICE_GROUP, '-', CUSTOMER_GROUP, '-', CHARGE_TYPE), '') SRC_ATTRIBUTE_CHAR19,
        nvl(concat(date_format(AGREEMENT_START, 'dd-MMM-yyyy'), '_', date_format(AGREEMENT_END, 'dd-MMM-yyyy')), '') SRC_ATTRIBUTE_CHAR20,
        CONTRACT_INITIAL_AMT SRC_ATTRIBUTE_CHAR21,
        nvl(AGREEMENT_NAME, '') SRC_ATTRIBUTE_CHAR22,
        'Y' SRC_ATTRIBUTE_CHAR23,
        'B2B MIDI' SRC_ATTRIBUTE_CHAR24,
        EVENT_TYPE SRC_ATTRIBUTE_CHAR25,
        nvl(null, '') SRC_ATTRIBUTE_CHAR26,
        nvl(null, '') SRC_ATTRIBUTE_CHAR27,
        nvl(null, '') SRC_ATTRIBUTE_CHAR28,
        nvl(null, '') SRC_ATTRIBUTE_CHAR29,
        nvl(null, '') SRC_ATTRIBUTE_CHAR30,
        nvl(null, '') SRC_ATTRIBUTE_CHAR31,
        nvl(null, '') SRC_ATTRIBUTE_CHAR32,
        nvl(null, '') SRC_ATTRIBUTE_CHAR33,
        nvl(null, '') SRC_ATTRIBUTE_CHAR34,
        nvl(null, '') SRC_ATTRIBUTE_CHAR35,
        nvl(null, '') SRC_ATTRIBUTE_CHAR36,
        nvl(null, '') SRC_ATTRIBUTE_CHAR37,
        nvl(null, '') SRC_ATTRIBUTE_CHAR38,
        nvl(null, '') SRC_ATTRIBUTE_CHAR39,
        nvl(null, '') SRC_ATTRIBUTE_CHAR40,
        nvl(null, '') SRC_ATTRIBUTE_CHAR41,
        nvl(null, '') SRC_ATTRIBUTE_CHAR42,
        nvl(null, '') SRC_ATTRIBUTE_CHAR43,
        nvl(null, '') SRC_ATTRIBUTE_CHAR44,
        nvl(null, '') SRC_ATTRIBUTE_CHAR45,
        nvl(null, '') SRC_ATTRIBUTE_CHAR46,
        nvl(null, '') SRC_ATTRIBUTE_CHAR47,
        nvl(null, '') SRC_ATTRIBUTE_CHAR48,
        nvl(null, '') SRC_ATTRIBUTE_CHAR49,
        nvl(null, '') SRC_ATTRIBUTE_CHAR50,
        nvl(null, '') SRC_ATTRIBUTE_CHAR51,
        nvl(null, '') SRC_ATTRIBUTE_CHAR52,
        nvl(null, '') SRC_ATTRIBUTE_CHAR53,
        nvl(null, '') SRC_ATTRIBUTE_CHAR54,
        nvl(null, '') SRC_ATTRIBUTE_CHAR55,
        nvl(null, '') SRC_ATTRIBUTE_CHAR56,
        nvl(null, '') SRC_ATTRIBUTE_CHAR57,
        nvl(null, '') SRC_ATTRIBUTE_CHAR58,
        nvl(null, '') SRC_ATTRIBUTE_CHAR59,
        nvl(null, '') SRC_ATTRIBUTE_CHAR60,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER1,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER2,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER3,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER4,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER5,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER6,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER7,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER8,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER9,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER10,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER11,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER12,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER13,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER14,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER15,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER16,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER17,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER18,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER19,
        nvl(null, '') SRC_ATTRIBUTE_NUMBER20,
        nvl(null, '') SRC_ATTRIBUTE_DATE1,
        nvl(null, '') SRC_ATTRIBUTE_DATE2,
        nvl(null, '') SRC_ATTRIBUTE_DATE3,
        nvl(null, '') SRC_ATTRIBUTE_DATE4,
        nvl(null, '') SRC_ATTRIBUTE_DATE5,
        nvl(null, '') SRC_ATTRIBUTE_DATE6,
        nvl(null, '') SRC_ATTRIBUTE_DATE7,
        nvl(null, '') SRC_ATTRIBUTE_DATE8,
        nvl(null, '') SRC_ATTRIBUTE_DATE9,
        nvl(null, '') SRC_ATTRIBUTE_DATE10,
        nvl(null, '') REVISION_INTENT_TYPE_CODE
      from b2b_transform_pd
      join (select AGREEMENT_NAME a_n, CUSTOMER_NAME c_n, count(*) cnt
          from b2b_transform_pd
          where CA_NM is not null
            and AGREEMENT_NAME is not null 
            and EVENT_FLAG = 'Y'
          group by AGREEMENT_NAME, CUSTOMER_NAME) b
        on b2b_transform_pd.AGREEMENT_NAME = b.a_n
          and b2b_transform_pd.CUSTOMER_NAME = b.c_n
      where CA_NM is not null
        and AGREEMENT_NAME is not null 
        and EVENT_TYPE is not null
      """)
    lineDf.show(false)
    lineDf.registerTempTable("line")
    
   
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
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'), 1, 30), '') DOC_LINE_ID_CHAR_1,
        nvl(substr(concat(AGREEMENT_NAME, '_', '"""+prcDt+"""'), 31, 60), '') DOC_LINE_ID_CHAR_2,
        nvl(substr(SERVICE_ID, 1, 30), '') DOC_LINE_ID_CHAR_3,
        nvl(substr(SERVICE_ID, 31, 60), '') DOC_LINE_ID_CHAR_4,
        nvl(concat(PRODUCT_SEQ, '_', SUBSCRIPTION_REF, '_', CHARGE_TYPE), '') DOC_LINE_ID_CHAR_5,
        nvl(null, '') DOCUMENT_TYPE_ID,
        'HADOOP' BILLING_APPLICATION,
        'HADOOP' SOURCE_SYSTEM,
        CHARGE_IDR BILL_ACCTD_AMOUNT,
        'HADOOP' DOCUMENT_TYPE_CODE
      from b2b_transform_pd
      """)
      
    val revenueDf = sqlContext.sql("""
      select value, ord
      from (
        select concat_ws('|', *) value, concat(LPAD(concat(DOC_ID_CHAR_1, ORIG_SYS_BILL_TO_CUST_SITE_REF), 100, '0'), RECORD_TYPE) ord from header
        union all
        select concat_ws('|', *) value, concat(LPAD(concat(DOC_ID_CHAR_1, ORIG_SYS_BILL_TO_CUST_SITE_REF), 100, '0'), RECORD_TYPE) ord from line
      ) t
      order by ord
      """).repartition(1)
    revenueDf.persist()
    revenueDf.show(false)
    
    revenueDf.select("value").write.mode("overwrite").option("header", "true").option("quoteMode","ALL")
    .text(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId)       
    val revenue_file = fs.globStatus(
    new Path(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId + "/"+ revenue_file),
    new Path(pathB2bIfrsRevenueCsv + "/process_id=" + prcDt + "_" + jobId + "/revenuedataimport_"+prcDt+"_"+hh+mm+ss+".csv"))
    
    
    billingDf.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "false").option("delimiter", "|").option("quoteMode","ALL")
    .save(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId)
    val billing_file = fs.globStatus(
    new Path(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId + "/part*"))(0).getPath().getName()
    fs.rename(
    new Path(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId +"/"+ billing_file),
    new Path(pathB2bIfrsBilingCsv + "/process_id=" + prcDt + "_" + jobId + "/billingdataimport_"+prcDt+"_"+hh+mm+ss+".dat"))
   
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