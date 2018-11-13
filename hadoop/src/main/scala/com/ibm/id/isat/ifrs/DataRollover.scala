package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.SQLContext
import com.ibm.id.isat.utils.ReferenceDF
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object DataRollover {
  def main(args: Array[String]): Unit = {
    val (prcDt, jobId, configFile, monthId, env) = try {
      (args(0), args(1), args(2), args(3), args(4))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process date> <job identifier> <config file> <month identifier> <LOCAL|PRODUCTION>")
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
    val pathSdpDedicatedAccountShadow = Common.getConfigDirectory(sc, configFile, "IFRS.INPUT.IFRS_SDP_DEDICATED_ACCOUNT_SHADOW") + "/" + monthId
    val pathSdpOfr = Common.getConfigDirectory(sc, configFile, "IFRS.INPUT.IFRS_SDP_OFR") + "/" + monthId
    val pathSaReofferidrollover = Common.getConfigDirectory(sc, configFile, "IFRS.INPUT.IFRS_SA_REOFFERIDROLLOVER") + "/" + monthId
    val pathRefSidRollover = Common.getConfigDirectory(sc, configFile, "IFRS.INPUT.IFRS_SA_REFSIDROLLOVER")
    val pathSorPrepaid = Common.getConfigDirectory(sc, configFile, "IFRS.USAGE.CCN_CS5_PREPAID")
    val pathDataRolloverCcn = "/user/hdp-rev/spark/output/ifrs/datarollover_ccn"
    val pathDataRolloverCsv = Common.getConfigDirectory(sc, configFile, "IFRS.OUTPUT.DATA_ROLLOVER_CSV")
    
    // Input Data Frames
    val sdpDedicatedAccountShadowDf=sqlContext.read.parquet(pathSdpDedicatedAccountShadow)
    sdpDedicatedAccountShadowDf.registerTempTable("sdp_dedicated_account_shadow")
    
    val sdpOfrDf=sqlContext.read.parquet(pathSdpOfr)
    sdpOfrDf.registerTempTable("sdp_ofr")
    
    val saReofferidrolloverDf=broadcast(sqlContext.read.parquet(pathSaReofferidrollover).cache())
    saReofferidrolloverDf.registerTempTable("sa_reofferidrollover")
    
    val saRefSidRolloverDf=broadcast(sqlContext.read.parquet(pathRefSidRollover).cache())
    saRefSidRolloverDf.registerTempTable("sa_refsidrollover")

    Common.cleanDirectory(sc, pathDataRolloverCcn + "/TGT_MTH=" + monthId)
    FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathSorPrepaid+"/TRANSACTION_DATE="+monthId+"*"))
    .map{fileStatus => fileStatus.getPath().toString()}
    .map{pathDaily => {
      sqlContext.read.option("basePath", pathSorPrepaid)
      .parquet(pathDaily)
      .filter("service_scenario = '2' and (account_id like 'DA%' or account_id like 'MA%')")
      .select("a_party_number", "b_party_number", "trigger_time", "revenue_code", "account_id", "usage_amount", "service_scenario")
      .repartition(200).write.mode("append")
      .parquet(pathDataRolloverCcn + "/TGT_MTH=" + monthId)
    }}
    
    val ccnDf=sqlContext.read.parquet(pathDataRolloverCcn + "/TGT_MTH=" + monthId)
    ccnDf.registerTempTable("ccn")
    
    val ccnSidRolloverDf=sqlContext.sql("""
    select
      a_party_number msisdn, 
      trigger_time ccr_trigger_time, 
      a_party_number anumber, 
      revenue_code sid,
      case when account_id like 'DA%' then usage_amount else 0 end da_amount,
      case when account_id like 'MA%' then usage_amount else 0 end ma_amount
    from ccn
    join sa_refsidrollover
      on ccn.service_scenario = '2'
      and (account_id like 'DA%' or account_id like 'MA%') 
      and ccn.revenue_code = sa_refsidrollover.sid
    """)
    ccnSidRolloverDf.registerTempTable("sa_ccnsidrollover")
    
    val saDmmsisdnsidrolloverDf=sqlContext.sql("""
    select 
      msisdn, ccr_trigger_time, anumber, sid, da_amount, ma_amount, pkg_name, pkg_definition, offer_id, tarif
    from (
      select 
        msisdn, ccr_trigger_time, anumber, a.sid, da_amount, ma_amount, pkg_name, pkg_definition, offer_id, tarif, row_number() over (partition by msisdn, ccr_trigger_time, anumber, a.sid, da_amount, ma_amount, pkg_name, pkg_definition, offer_id, tarif) rnk
      from (
        select 
          a.*, row_number() over (partition by msisdn order by ccr_trigger_time desc) rnk
        from sa_ccnsidrollover a
        having rnk = 1 and da_amount >= 0 and ma_amount >= 0
      ) a
      left join sa_refsidrollover
        on a.sid = sa_refsidrollover.sid
        having rnk = 1
    ) non_duplicate
    """)
    saDmmsisdnsidrolloverDf.registerTempTable("sa_dmmsisdnsidrollover")

    // Transform
    val saDarolloverDf=sqlContext.sql("""
select * 
from sdp_dedicated_account_shadow 
where da_id in (
  '20204051',
  '20205071',
  '20206141',
  '20206151',
  '20210271',
  '20210281',
  '20216221',
  '20216231',
  '20216241'
)
    """)
    saDarolloverDf.cache()
    saDarolloverDf.registerTempTable("sa_darollover")
    
    val subsRolloverNTDf= sqlContext.sql("""
select
  a.*,
  rank() over (
    partition by
      a.account_id, 
      a.offer_id 
    order by 
      a.start_date desc, 
      a.dt_id desc, 
      a.ev_id
  ) ranks
from sdp_ofr a
join (
    select distinct account_id from sa_darollover
  ) b
on a.account_id = b.account_id
join (
    select distinct offer_id
    from sa_reofferidrollover
    where offer_id is not null
  ) c
on a.offer_id = c.offer_id
having ranks = 1
    """)
    subsRolloverNTDf.persist()
    subsRolloverNTDf.registerTempTable("subs_rollover_nt")
    
    val subsRolloverCLNDf= sqlContext.sql("""
select
  account_id,
  offer_id,
  start_date,
  expiry_date,
  start_time,
  expiry_time,
  offer_type,
  pam_service_id,
  product_id,
  src_stm_id,
  ev_id,
  prc_dt,
  seq_id,
  ppn_dttm,
  source_dim_key,
  rank() over (partition by account_id order by start_date desc, expiry_date desc, dt_id desc, ev_id desc) ranks2
from subs_rollover_nt
having ranks2 = 1
    """)
    subsRolloverCLNDf.persist()
    subsRolloverCLNDf.registerTempTable("sa_subsrollover_cln")
    
    val sidOfferIdRolloverDFt1= sqlContext.sql("""
select
  a.dt_id,
  concat('62', a.account_id) msisdn,
  b.offer_id,
  b.start_date,
  expiry_date,
  a.da_id, 
  concat(a.da_id, b.offer_id) key_id,
  a.da_unit_balance 
from sa_darollover a
left join sa_subsrollover_cln b
  on (a.account_id=b.account_id)
where a.da_unit_balance > 0
    """)
    sidOfferIdRolloverDFt1.registerTempTable("rollover_t1")
    
    val sidOfferIdRolloverT2DF= sqlContext.sql("""
select
  a.dt_id,
  a.msisdn,
  a.offer_id,
  a.start_date,
  a.expiry_date,
  a.da_id,
  a.da_unit_balance
from rollover_t1 a 
left outer join (
  select distinct
    offer_id,
    concat(da, offer_id) key_id,
    group_category
  from sa_reofferidrollover
) b
  on (a.key_id=b.key_id)
    """)
    sidOfferIdRolloverT2DF.registerTempTable("rollover_t2")
    
    val sidOfferIdRolloverDf= sqlContext.sql("""
select
  a.dt_id,
  a.msisdn,
  a.offer_id,
  nvl(date_format(cast(a.start_date/1000 as timestamp), 'yyyyMMdd'), '') start_date,
  nvl(date_format(cast(a.expiry_date/1000 as timestamp), 'yyyyMMdd'), '') expiry_date,
  a.da_id,
  b.ccr_trigger_time,
  b.sid,
  b.pkg_name,
  b.pkg_definition,
  b.offer_id offeridsid,
  b.tarif,
  b.da_amount,
  b.ma_amount,
  a.da_unit_balance,
  count(*) over (
    partition by 
      a.msisdn
    order by a.start_date desc)
  as duplicate_rank
from rollover_t2 a 
left outer join sa_dmmsisdnsidrollover b
  on a.msisdn=b.msisdn
    """)
    sidOfferIdRolloverDf.persist()
    
    val intcctRef = ReferenceDF.getIntcctAllRef(sc)
    val sidOfferIdRolloverWithCityDF = Common.getIntcctWithSID4(
        sqlContext, sidOfferIdRolloverDf.filter("duplicate_rank = 1"), intcctRef, "msisdn", 
        "msisdn", "dt_id", "''", "APrefix", 
        "ServiceCountryName", "ServiceCityName", "ServiceProviderId", 
        "BPrefix", "DestinationCountryName", "DestinationCityName", 
        "DestinationProviderId", "'1'")
    sidOfferIdRolloverWithCityDF.registerTempTable("sid_offer_id_rollover_with_city")
    
    val dataRolloverDf = sqlContext.sql("""
select
  '"""+prcDt+"""' PRC_DT,
  '"""+jobId+"""' JOB_ID,
  dt_id START_DT, -- DT_ID will be used instead of incomplete start_date
  case when lower(pkg_definition) like '%renewal%' then 'Renewal' else 'Buy/Rebuy' end PURCHASE_TYPE,
  pkg_name PACKAGE_NAME,
  count(distinct nvl(msisdn, '')) TOTAL_MSISDN,
  cast(sum(da_unit_balance) / 1024 as decimal(18, 2)) TOTAL_VOLUME,
  ServiceCityName CITY,
  '' CUSTOMER_SEGMENT,
  '' REV_CODE,
  '' ARKB,
  '' TOTAL_AMOUNT
from
  sid_offer_id_rollover_with_city
group by
  dt_id,
  case when lower(pkg_definition) like '%renewal%' then 'Renewal' else 'Buy/Rebuy' end,
  pkg_name,
  servicecityname
    """)
    dataRolloverDf.registerTempTable("data_rollover")
    
    Common.cleanDirectory(sc, pathDataRolloverCsv + "/MTH_ID=" + monthId)
    dataRolloverDf.coalesce(1)
    .write.format("com.databricks.spark.csv")
    .mode("append")
    .option("delimiter", ";")
    .option("header", "true")
    .save(pathDataRolloverCsv + "/MTH_ID=" + monthId)
  }
}