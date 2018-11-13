package com.ibm.id.isat.spcashback

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.types.IntegerType

object SPCashback {
  def main(args: Array[String]): Unit = {
    
    /*
     * Retrieve arguments and print usage if incomplete
     */
    val (prcDt, jobId, tgtDt, numDays, numDaysForward, configDir, env) = try {
      (args(0), args(1), args(2), args(3), args(4), args(5), args(6))
    } catch {
      case t: ArrayIndexOutOfBoundsException => println("Usage: <prc_dt> <job_id> <target date> <salmo number of days> <number of days forward to consider> <conf> <LOCAL|PRODUCTION>")
        return
    }
    
    /*
     * Initiate Spark Context
     */
    val sc = env match {
      case "LOCAL" => new SparkContext("local", "testing spark", new SparkConf())
      case _ => new SparkContext(new SparkConf())
    }
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false") // Disable metadata parquet
    val storeLev = org.apache.spark.storage.StorageLevel.DISK_ONLY
    
    /*
     * Initiate SQL Context
     */
    val sqlContext = new HiveContext(sc)

    /*
     * Get paths from config
     */
    val pathSalmo = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_SALDOMOBO")
    val pathChurn = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_CHURN")
    val pathAirRefill = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_AIRREFILL")
    val pathSdpSubscriber = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_SDPSUBSCRIBER")
    val pathActHistory = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_ACTDATAHISTORY")
    val pathRebuyHistory = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_REBUYHISTORY")
    val pathVoUsed = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_VO_USED")
    val pathFdvmobopmjoin = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.INPUT.SPCASHBACK_INPUT_FDV_MOBO_PM_JOIN")
    val pathRefInitDataPkg = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.REFERENCE.SPCASHBACK_REFERENCE_REF_INIT_DATA_PKG")
    val pathRefRebuyDataPkg = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.REFERENCE.SPCASHBACK_REFERENCE_REF_REBUY_DATA_PKG")
    val pathRefRebuyDataVoucher = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.REFERENCE.SPCASHBACK_REFERENCE_REF_REBUY_DATA_VOUCHER")
    val pathRefSc = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.REFERENCE.SPCASHBACK_REFERENCE_REF_SC")
    val pathBuypackage = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.OUTPUT.SPCASHBACK_OUTPUT_BUYPACKAGE")
    val pathReportCsv = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.OUTPUT.SPCASHBACK_OUTPUT_REPORTCSV")
    val pathReportSor = Common.getConfigDirectory(sc, configDir, "SPCASHBACK.OUTPUT.SPCASHBACK_OUTPUT_SOR")

    FileSystem.newInstance(sc.hadoopConfiguration)
    .globStatus(new Path(pathSalmo+"/*/salmo/*"))
    .map(glob=>glob.getPath.toString)
    .map(pathString=>{
      val reg = raw".*prepaid\/(.*)_(.*)\/salmo.*".r
      val (prcDt: String, jobId: String) = pathString match {
        case reg(prcDt, jobId) => (prcDt, jobId)
      } 
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathSalmo).option("delimiter", "|")
      .schema(salmoSchema).load(pathString).withColumn("prc_dt", lit(prcDt))
      .withColumn("job_id", lit(jobId))
    })
    .reduce((a,b)=>a.unionAll(b))
    .registerTempTable("all_salmo")
    
    val salmoDf = sqlContext.sql("""
    select * 
    from all_salmo 
    where prc_dt between date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -"""+numDays+"""), 'yyyyMMdd') 
        and date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +"""+numDaysForward+"""), 'yyyyMMdd') 
    """)
    //salmoDf.persist()
    salmoDf.registerTempTable("salmo")

    val regisDf = sqlContext.sql("""
    select *, rank() over (partition by b_msisdn order by datetime desc) rn
    from salmo
    where transaction_type = 'Prepaid Registration' 
        and final_transaction_status = 'Completed' 
        and status_description = 'Success'
    """).filter("rn = 1").drop("rn")
    //regisDf.show()
    //regisDf.persist()
    regisDf.registerTempTable("regis")

    val purcDf = sqlContext.sql("""
    select *
    from salmo
    where transaction_type not in ('Prepaid Registration', '401565401.00', '')
        and final_transaction_status = 'Completed' 
        and status_description = 'Success'
    """)
    //purcDf.show()
    //purcDf.persist()
    purcDf.registerTempTable("purc")
    
    val sourceSdpSubsDf = sqlContext.read.option("basePath", pathSdpSubscriber).parquet(pathSdpSubscriber+"/*").filter("""
    dt_id between date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), 0), 'yyyyMMdd') and date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +1), 'yyyyMMdd') 
    and unix_timestamp(account_activated_date) >= unix_timestamp('2018-06-08 00:00:00')
    --    and date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +"""+numDaysForward+"""), 'yyyyMMdd') 
    """)
    //sourceSdpSubsDf.show()
    sourceSdpSubsDf.registerTempTable("source_sdpsubs")
    
    val refScDf = broadcast(sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").schema(RefScSchema).load(pathRefSc+"/*.csv").cache())
    refScDf.registerTempTable("ref_sc")
    
    val sdpSubsDf = sqlContext.sql("""
    select source_sdpsubs.subscriber_id, last_value(source_sdpsubs.account_activated_date) account_activated_date
    from source_sdpsubs join ref_sc
        on source_sdpsubs.svc_class_id = ref_sc.service_class_id
    group by source_sdpsubs.subscriber_id
    """)
    //sdpSubsDf.persist()
    //sdpSubsDf.count()
    sdpSubsDf.registerTempTable("sdpsubs")
    
    val prevActdataHistDf = sqlContext.read.parquet(pathActHistory).filter("""
    dt_id = date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -1), 'yyyyMMdd')
    """)
    prevActdataHistDf.drop("dt_id").registerTempTable("actdatahist")
    
    val buypackDf = sqlContext.sql("""
    select regis.datetime regis_datetime, regis.initiatetime regis_initiatetime, regis.transaction_type regis_transaction_type, regis.channel regis_channel, regis.region regis_region, regis.area regis_area, regis.sales_area regis_sales_area, regis.cluster regis_cluster, regis.additional_territory regis_additional_territory, regis.transaction_id regis_transaction_id, regis.l1_parent_id regis_l1_parent_id, regis.l1_parent_name regis_l1_parent_name, regis.additional_parent_id regis_additional_parent_id, regis.additional_parent_name regis_additional_parent_name, regis.transaction_channel regis_transaction_channel, regis.organization_type regis_organization_type, regis.organization_id regis_organization_id, regis.organization_name regis_organization_name, regis.operator_id regis_operator_id, regis.operator_name regis_operator_name, regis.operator_type regis_operator_type, regis.msisdn regis_msisdn, regis.user_name regis_user_name, regis.a_longitude regis_a_longitude, regis.a_latitude regis_a_latitude, regis.a_lac_dec regis_a_lac_dec, regis.a_ci_dec regis_a_ci_dec, regis.a_real_rime_territory regis_a_real_rime_territory, regis.b_msisdn regis_b_msisdn, regis.dompetku_msisdn regis_dompetku_msisdn, regis.b_lac_dec regis_b_lac_dec, regis.b_ci_dec regis_b_ci_dec, regis.b_real_rime_territory regis_b_real_rime_territory, regis.bill_number regis_bill_number, regis.voucher_type regis_voucher_type, regis.product_group regis_product_group, regis.product_name regis_product_name, regis.main_price regis_main_price, regis.discount regis_discount, regis.amount_debit regis_amount_debit, regis.tax regis_tax, regis.final_transaction_status regis_final_transaction_status, regis.status_description regis_status_description, regis.pre_balance regis_pre_balance, regis.post_balance regis_post_balance, regis.additional_parameter1 regis_additional_parameter1, regis.additional_parameter2 regis_additional_parameter2, regis.additional_parameter3 regis_additional_parameter3, regis.additional_parameter4 regis_additional_parameter4, regis.additional_parameter5 regis_additional_parameter5, regis.reversal_transaction_id regis_reversal_transaction_id, regis.reversed_datetime regis_reversed_datetime, regis.reversed_status regis_reversed_status, regis.acommissionresult regis_acommissionresult, regis.bcommissionresult regis_bcommissionresult, regis.adiscountresult regis_adiscountresult, regis.bdiscountresult regis_bdiscountresult, regis.discountrulename regis_discountrulename, regis.commissionschema regis_commissionschema, regis.msisdnterritoryverificationresult regis_msisdnterritoryverificationresult, regis.prc_dt regis_prc_dt,  regis.job_id regis_job_id,
    purc.datetime purc_datetime, purc.initiatetime purc_initiatetime, purc.transaction_type purc_transaction_type, purc.channel purc_channel, purc.region purc_region, purc.area purc_area, purc.sales_area purc_sales_area, purc.cluster purc_cluster, purc.additional_territory purc_additional_territory, purc.transaction_id purc_transaction_id, purc.l1_parent_id purc_l1_parent_id, purc.l1_parent_name purc_l1_parent_name, purc.additional_parent_id purc_additional_parent_id, purc.additional_parent_name purc_additional_parent_name, purc.transaction_channel purc_transaction_channel, purc.organization_type purc_organization_type, purc.organization_id purc_organization_id, purc.organization_name purc_organization_name, purc.operator_id purc_operator_id, purc.operator_name purc_operator_name, purc.operator_type purc_operator_type, purc.msisdn purc_msisdn, purc.user_name purc_user_name, purc.a_longitude purc_a_longitude, purc.a_latitude purc_a_latitude, purc.a_lac_dec purc_a_lac_dec, purc.a_ci_dec purc_a_ci_dec, purc.a_real_rime_territory purc_a_real_rime_territory, purc.b_msisdn purc_b_msisdn, purc.dompetku_msisdn purc_dompetku_msisdn, purc.b_lac_dec purc_b_lac_dec, purc.b_ci_dec purc_b_ci_dec, purc.b_real_rime_territory purc_b_real_rime_territory, purc.bill_number purc_bill_number, purc.voucher_type purc_voucher_type, purc.product_group purc_product_group, purc.product_name purc_product_name, purc.main_price purc_main_price, purc.discount purc_discount, purc.amount_debit purc_amount_debit, purc.tax purc_tax, purc.final_transaction_status purc_final_transaction_status, purc.status_description purc_status_description, purc.pre_balance purc_pre_balance, purc.post_balance purc_post_balance, purc.additional_parameter1 purc_additional_parameter1, purc.additional_parameter2 purc_additional_parameter2, purc.additional_parameter3 purc_additional_parameter3, purc.additional_parameter4 purc_additional_parameter4, purc.additional_parameter5 purc_additional_parameter5, purc.reversal_transaction_id purc_reversal_transaction_id, purc.reversed_datetime purc_reversed_datetime, purc.reversed_status purc_reversed_status, purc.acommissionresult purc_acommissionresult, purc.bcommissionresult purc_bcommissionresult, purc.adiscountresult purc_adiscountresult, purc.bdiscountresult purc_bdiscountresult, purc.discountrulename purc_discountrulename, purc.commissionschema purc_commissionschema, purc.msisdnterritoryverificationresult purc_msisdnterritoryverificationresult, purc.prc_dt purc_prc_dt,  purc.job_id purc_job_id,
    sdpsubs.subscriber_id, sdpsubs.account_activated_date
    from purc left join regis
        on purc.b_msisdn = concat('62', regis.b_msisdn)
    join sdpsubs
        on purc.b_msisdn = concat('62', sdpsubs.subscriber_id)
    where date_format(from_unixtime(unix_timestamp(purc.datetime)), 'yyyyMMdd') = """+tgtDt+"""
    union all
    select * from actdatahist
    """)
    //buypackDf.write.mode("overwrite").parquet(pathBuypackage+"/dt_id="+tgtDt)
    //val buypackDf = sqlContext.read.parquet(pathBuypackage+"/dt_id="+tgtDt)
    //buypackDf.persist(storeLev)
    buypackDf.registerTempTable("buypack")
    
    val actvAndRebuyDf = sqlContext.sql("""
    select
        buypack.*,
        case rank() over (partition by regis_b_msisdn order by purc_datetime) when 1 then 'ACT_DATA' else 'REBUY' end regis_flag
    from buypack
    """)
    actvAndRebuyDf.persist(storeLev)
    //actvAndRebuyDf.count()
    //actvAndRebuyDf.write.mode("overwrite").parquet(pathBuypackage+"/dt_id="+tgtDt)
    actvAndRebuyDf.registerTempTable("actvrebuy")
    
    val refInitDataPkgDf = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").schema(RefInitDataPkgSchema).load(pathRefInitDataPkg+"/*.csv").cache()
    refInitDataPkgDf.registerTempTable("ref_init_data_pkg")
    
    val actvDf = sqlContext.sql("""
    select actvrebuy.*, ref_init_data_pkg.subscription_price, case when purc_organization_id = regis_organization_id then 'TRUE' else 'FALSE' end cashback_flag
    from actvrebuy join ref_init_data_pkg
        on actvrebuy.purc_product_name = ref_init_data_pkg.package_name
    where regis_flag = 'ACT_DATA'""")
    actvDf.registerTempTable("actv")
    
    val actvHistory = sqlContext.sql("""
    select *
    from actv
    where purc_datetime between date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -180+1) 
            and date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +1)
    """).drop("subscription_price").drop("cashback_flag")
    
    FileSystem.newInstance(sc.hadoopConfiguration)
    .globStatus(new Path(pathVoUsed+"/vo_Used*")).filter(d=>d.getLen > 0).map(glob=>glob.getPath.toString)
    .map(pathString=>{val reg = raw".*vo_Used_(.{8}).*".r; val prcDt: String = pathString match {case reg(prcDt) => prcDt}; sqlContext.read.format("com.databricks.spark.csv").option("basePath", pathVoUsed).option("delimiter", ";").option("header", "true").schema(voUsedSchema).load(pathString).withColumn("prc_dt", lit(prcDt))}).reduce((a,b)=>a.unionAll(b)).registerTempTable("all_vo_used")
    
    val voUsedDf = sqlContext.sql("""
    select *
    from all_vo_used
    where prc_dt between date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -0), 'yyyyMMdd')
            and date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +"""+numDaysForward+"""), 'yyyyMMdd')
    """)
    voUsedDf.registerTempTable("vo_used")
    
    sqlContext.read.parquet(pathFdvmobopmjoin).registerTempTable("all_fdv_mobo_pm_join")
    
    val fdvMoboPmJoinDf = sqlContext.sql("""
    select
        PM_VOUCHER_SN pm_voucher_sn,
        PM_RED_MSISDN pm_red_msisdn,
        PM_VOUCHER_RED_DTTM pm_voucher_red_dttm,
        MOBO_INJ_PACK_ID mobo_inj_pack_id,
        MOBO_INJ_PACK_NAME mobo_inj_pack_name,
        MOBO_INJ_SALES_PRICE mobo_inj_sales_price
    from all_fdv_mobo_pm_join
    where PRC_DT between date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -0), 'yyyyMMdd')
            and date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +"""+numDaysForward+"""), 'yyyyMMdd')
        and PM_VOUCHER_GROUP_ID = '9901'
    """)
    fdvMoboPmJoinDf.registerTempTable("fdv_mobo_pm_join")
    
    val refRebuyDataPkgDf = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").schema(RefRebuyDataPkgSchema).load(pathRefRebuyDataPkg+"/*.csv").cache()
    refRebuyDataPkgDf.registerTempTable("ref_rebuy_data_pkg")
    
    val refRebuyDataVoucherDf = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").schema(RefRebuyDataVoucherSchema).load(pathRefRebuyDataVoucher+"/*.csv").cache()
    refRebuyDataVoucherDf.registerTempTable("ref_rebuy_data_voucher")
    
    val prevRebuyHistDf = sqlContext.read.schema(rebuyHistSchema).parquet(pathRebuyHistory).filter("""
    dt_id = date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -1), 'yyyyMMdd')
    """)
    prevRebuyHistDf.drop("dt_id").registerTempTable("rebuy_hist")
    
    val rebuyUnionDf = sqlContext.sql("""
    select 
        purc_b_msisdn msisdn,
        regis_flag transaction_flag,
        purc_datetime timestamp_rebuy,
        cast(purc_amount_debit as int) package_price,
        purc_transaction_type transaction_type,
        purc_transaction_id transaction_id,
        purc_channel channel,
        case when ref_rebuy_data_pkg.mobo_package_name is null then 'FALSE' else 'TRUE' end mobo_package_flag
    from actvrebuy left join ref_rebuy_data_pkg
        on actvrebuy.purc_product_name = ref_rebuy_data_pkg.mobo_package_name
    where regis_flag = 'REBUY'
    union all
    select
        msisdn,
        'REBUY' transaction_flag,
        used_date timestamp_rebuy,
        cast(case when nvl(recharge_value, '') = '' then ref_rebuy_data_voucher.main_price else recharge_value end as int) package_price,
        'VO_USED' transaction_type,
        'N/A' transaction_id,
        'N/A' channel,
        'TRUE' mobo_package_flag
    from vo_used join ref_rebuy_data_voucher
        on voucher_type_id = voucher_group_id
    union all
    select
        pm_red_msisdn msisdn,
        'REBUY' transaction_flag,
        pm_voucher_red_dttm timestamp_rebuy,
        cast(mobo_inj_sales_price as int) package_price,
        'FDV' transaction_type,
        'N/A' transaction_id,
        'N/A' channel,
        'TRUE' mobo_package_flag
    from fdv_mobo_pm_join
    union all
    select msisdn, transaction_flag, timestamp_rebuy, package_price, transaction_type, transaction_id, nvl(channel, 'N/A') channel, mobo_package_flag
    from rebuy_hist
    """)
    rebuyUnionDf.registerTempTable("rebuy")
    
    val rebuyDf = sqlContext.sql("""
    select rebuy.*, actv.regis_organization_id outlet_id, actv.account_activated_date timestamp_activation, actv.purc_datetime timestamp_inject_package,
      case when mobo_package_flag = 'TRUE' and rank() over (partition by msisdn, mobo_package_flag order by timestamp_rebuy) <= 5 then actv.cashback_flag else 'FALSE' end cashback_flag
    from rebuy join actv 
        on rebuy.msisdn = actv.purc_b_msisdn
    """)
    //rebuyDf.count()
    rebuyDf.registerTempTable("rebuy")
    
    val rebuyHistDf = rebuyDf.filter("""
    timestamp_rebuy between date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -180+1) 
    and date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +1)
    """).drop("outlet_id").drop("timestamp_activation").drop("timestamp_inject_package").drop("cashback_flag").repartition(50)
    
    val arChurnDf = sqlContext.read.option("basePath", pathChurn).parquet(pathChurn).filter("""
    dt_id between date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), 0), 'yyyyMMdd') 
        and date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +"""+numDaysForward+"""), 'yyyyMMdd') 
    """).cache()
    //arChurnDf.show()
    //arChurnDf.persist()
    arChurnDf.registerTempTable("ar_churn")
    
    FileSystem.newInstance(sc.hadoopConfiguration)
    .globStatus(new Path(pathAirRefill+"/*")).map(glob=>glob.getPath.toString)
    .map(pathString=>{val reg = raw".*\/(.*)_(.*)".r; val (prcDt: String, jobId: String) = pathString match {case reg(prcDt, jobId) => (prcDt, jobId)}; sqlContext.read.format("com.databricks.spark.csv").option("basePath", pathAirRefill).option("delimiter", "|").schema(airrefillSchema).load(pathString).withColumn("prc_dt", lit(prcDt)).withColumn("job_id", lit(jobId))}).reduce((a,b)=>a.unionAll(b))
    .registerTempTable("all_refill")
    
    val airRefillDf = sqlContext.sql("""
    select all_refill.*
    from all_refill left join ar_churn
        on all_refill.msisdn = ar_churn.msisdn
    where all_refill.prc_dt between date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), -"""+numDaysForward+"""), 'yyyyMMdd') 
        and date_format(date_add(from_unixtime(unix_timestamp('"""+tgtDt+"""', 'yyyyMMdd')), +"""+numDaysForward+"""), 'yyyyMMdd')
        and ar_churn.msisdn is null
    """)
    //airRefillDf.filter("reload_type = 'VOUCHER PHYSIC'").show()
    //airRefillDf.persist()
    airRefillDf.registerTempTable("airrefill")
    
    val reload1nDf = sqlContext.sql("""
    select
        airrefill.key,
        airrefill.msisdn,
        airrefill.transactiontimestamp,
        airrefill.subscribertype,
        airrefill.amount,
        airrefill.da_detail,
        airrefill.currentserviceclass,
        airrefill.activationdate,
        airrefill.serviceclassid,
        airrefill.serviceclassname,
        airrefill.promopackagename,
        airrefill.brandname,
        airrefill.newserviceclass,
        airrefill.newserviceclassname,
        airrefill.newpromopackagename,
        airrefill.newbrandname,
        airrefill.originnodetype,
        airrefill.originhostname,
        airrefill.externaldata1,
        airrefill.programname,
        airrefill.programowner,
        airrefill.programcategory,
        airrefill.bankname,
        airrefill.bankdetail,
        airrefill.mainaccountbalancebefore,
        airrefill.mainaccountbalanceafter,
        airrefill.lac,
        airrefill.cellid,
        airrefill.transactiontype,
        airrefill.filenameinput,
        airrefill.voucher_type,
        airrefill.reload_type,
        airrefill.jobid,
        airrefill.cdr_tp,
        actv.*,
        case when datediff(from_unixtime(unix_timestamp(transactiontimestamp, 'yyyyMMddHHmmss+0700'), 'yyyy-MM-dd HH:mm:ss'), account_activated_date) > 7
            or amount < 10000
            then 'RELOAD_N'
        else 
            case rank() over (partition by 
                regis_b_msisdn, 
                datediff(from_unixtime(unix_timestamp(transactiontimestamp, 'yyyyMMddHHmmss+0700'), 'yyyy-MM-dd HH:mm:ss'), account_activated_date) > 7 or amount < 10000 
            order by transactiontimestamp) when 1 then 'RELOAD_1' else 'RELOAD_N' end
        end reloadtype
    from airrefill 
    join actv
        on airrefill.msisdn = concat(actv.purc_b_msisdn)
    """)
    //reload1nDf.filter("reload_type = 'VOUCHER PHYSIC'").show()
    //reload1nDf.cache()
    //reload1nDf.persist()
    reload1nDf.registerTempTable("reload1n")
    
    val spcashbackDf = sqlContext.sql("""
    select 
      '"""+prcDt+"""' dt_id,
      purc_organization_id outlet_id,
      purc_b_msisdn msisdn,
      regis_flag transaction_flag,
      cashback_flag cashback,
      cast(account_activated_date as string) timestamp_activation,
      purc_datetime timestamp_inject_package,
      'N/A' timestamp_rebuy,
      'N/A' timestamp_reload,
      'N/A' reload_denom,
      cast(purc_amount_debit as int) package_price,
      purc_transaction_type transaction_type,
      purc_transaction_id transaction_id,
      purc_channel channel,
      '"""+prcDt+"""' prc_dt,
      '"""+jobId+"""' job_id,
      '"""+tgtDt+"""' file_date
    from actv
    where date_format(purc_datetime, 'yyyyMMdd') = '"""+tgtDt+"""'
    union all
    select 
        '"""+prcDt+"""' dt_id,
        outlet_id,
        msisdn,
        transaction_flag,
        cashback_flag cashback,
        cast(timestamp_activation as string) timestamp_activation,
        timestamp_inject_package,
        timestamp_rebuy,
        'N/A' timestamp_reload,
        'N/A' reload_denom,
        package_price,
        transaction_type,
        transaction_id transaction_id,
        channel channel,
        '"""+prcDt+"""' prc_dt,
        '"""+jobId+"""' job_id,
        '"""+tgtDt+"""' file_date
    from rebuy
    where date_format(timestamp_rebuy, 'yyyyMMdd') = '"""+tgtDt+"""'
    union all
    select 
      '"""+prcDt+"""' dt_id,
      regis_organization_id outlet_id,
      purc_b_msisdn msisdn,
      reloadtype transaction_flag,
      case when from_unixtime(unix_timestamp(transactiontimestamp, 'yyyyMMddHHmmss+0700'), 'yyyy-MM-dd HH:mm:ss') > purc_datetime THEN cashback_flag ELSE 'FALSE' end cashback,
      cast(account_activated_date as string) timestamp_activation,
      purc_datetime timestamp_inject_package,
      'N/A' timestamp_rebuy,
      from_unixtime(unix_timestamp(transactiontimestamp, 'yyyyMMddHHmmss+0700'), 'yyyy-MM-dd HH:mm:ss') timestamp_reload,
      cast(amount as int) reload_denom,
      'N/A' package_price,
      nvl(case reload_type when 'null' then null else reload_type end, case when originhostname = 'SALDOMOBO' then originhostname when originhostname like '%ngssp%' then 'NGSSP' else '' end) transaction_type,
      'N/A' transaction_id,
      'N/A' channel,
      '"""+prcDt+"""' prc_dt,
      '"""+jobId+"""' job_id,
      '"""+tgtDt+"""' file_date
    from reload1n
    where date_format(from_unixtime(unix_timestamp(transactiontimestamp, 'yyyyMMddHHmmss+0700'), 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd') = '"""+tgtDt+"""'
    """)
    //spcashbackDf.persist()
    spcashbackDf.registerTempTable("spcashback")
    
    /*
     * Output
     */
    
    actvHistory.drop("regis_flag").repartition(1).write
    .mode("overwrite")
    .parquet(pathActHistory+"/dt_id="+tgtDt)
    
    rebuyHistDf.write
    .mode("overwrite")
    .parquet(pathRebuyHistory+"/dt_id="+tgtDt)
    
    val spcashbackSorDf = spcashbackDf.orderBy("msisdn", "transaction_flag").repartition(1).persist(storeLev)
    
    spcashbackSorDf.write.mode("overwrite").parquet(pathReportSor+"/target_date="+tgtDt)
    
    spcashbackSorDf.filter("cashback = 'TRUE'")
    .write.mode("overwrite").format("com.databricks.spark.csv")
    .option("delimiter", "|").option("header", "true")
    .save(pathReportCsv+"/dt_id="+tgtDt)
    
  }
  
  val airrefillSchema = new StructType(Array(
  StructField("key", StringType, true),
  StructField("msisdn", StringType, true),
  StructField("transactiontimestamp", StringType, true),
  StructField("subscribertype", StringType, true),
  StructField("amount", StringType, true),
  StructField("da_detail", StringType, true),
  StructField("currentserviceclass", StringType, true),
  StructField("activationdate", StringType, true),
  StructField("serviceclassid", StringType, true),
  StructField("serviceclassname", StringType, true),
  StructField("promopackagename", StringType, true),
  StructField("brandname", StringType, true),
  StructField("newserviceclass", StringType, true),
  StructField("newserviceclassname", StringType, true),
  StructField("newpromopackagename", StringType, true),
  StructField("newbrandname", StringType, true),
  StructField("originnodetype", StringType, true),
  StructField("originhostname", StringType, true),
  StructField("externaldata1", StringType, true),
  StructField("programname", StringType, true),
  StructField("programowner", StringType, true),
  StructField("programcategory", StringType, true),
  StructField("bankname", StringType, true),
  StructField("bankdetail", StringType, true),
  StructField("mainaccountbalancebefore", StringType, true),
  StructField("mainaccountbalanceafter", StringType, true),
  StructField("lac", StringType, true),
  StructField("cellid", StringType, true),
  StructField("transactiontype", StringType, true),
  StructField("filenameinput", StringType, true),
  StructField("voucher_type", StringType, true),
  StructField("reload_type", StringType, true),
  StructField("jobid", StringType, true),
  StructField("cdr_tp", StringType, true)))
  
  val RefInitDataPkgSchema = new StructType(Array(
  StructField("keyword", StringType, true),
  StructField("package_name", StringType, true),
  StructField("subscription_price", StringType, true)))
  
  val RefRebuyDataPkgSchema = new StructType(Array(
  StructField("paket", StringType, true),
  StructField("keyword", StringType, true),
  StructField("mobo_package_name", StringType, true)))
  
  val RefRebuyDataVoucherSchema = new StructType(Array(
  StructField("voucher_group_id", StringType, true),
  StructField("keyword", StringType, true),
  StructField("package_name", StringType, true),
  StructField("fixed_tariff_mobo", StringType, true),
  StructField("main_price", StringType, true)))

  val RefScSchema = new StructType(Array(
  StructField("service_class_id", StringType, true),
  StructField("service_class_name", StringType, true)))
  
  val salmoSchema = new StructType(Array(
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

  val voUsedSchema = new StructType(Array(
  StructField("serial_no", StringType, true),
  StructField("recharge_value", StringType, true),
  StructField("recharge_period", StringType, true),
  StructField("expiration_date", StringType, true),
  StructField("voucher_type_id", StringType, true),
  StructField("package_id", StringType, true),
  StructField("dealer_id", StringType, true),
  StructField("voucher_state", StringType, true),
  StructField("state_change_user", StringType, true),
  StructField("msisdn", StringType, true),
  StructField("provider_id", StringType, true),
  StructField("voucher_provider_id", StringType, true),
  StructField("disabled_date", StringType, true),
  StructField("delivered_date", StringType, true),
  StructField("enabled_date", StringType, true),
  StructField("usage_req_date", StringType, true),
  StructField("used_date", StringType, true),
  StructField("blocked_date", StringType, true),
  StructField("deleted_date", StringType, true)))
  
  val rebuyHistSchema = new StructType(Array(
  StructField("msisdn", StringType, true),
  StructField("transaction_flag", StringType, true),
  StructField("timestamp_rebuy", StringType, true),
  StructField("package_price", IntegerType, true),
  StructField("transaction_type", StringType, true),
  StructField("transaction_id", StringType, true),
  StructField("channel", StringType, true),
  StructField("mobo_package_flag", StringType, true)))
  
}