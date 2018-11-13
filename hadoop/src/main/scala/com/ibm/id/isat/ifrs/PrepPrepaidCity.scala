package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import com.ibm.id.isat.utils.ReferenceDF
import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object PrepPrepaidCity {
  def main(args: Array[String]): Unit = {
    
    val (trxDate, input, configDir) = try {
      (args(0), args(1), args(2))  
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    val sc = new SparkContext(new SparkConf())
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

    val sqlContext=new HiveContext(sc)
    
    val pathAggDailyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_DAILY_PREPAID.OUTPUT_SPARK_AGG_DAILY_PREPAID")
    
    val refIntcctDf = ReferenceDF.getIntcctAllDF(sqlContext)
    refIntcctDf.registerTempTable("intcct")
    refIntcctDf.persist()
    refIntcctDf.count()
    
    val dailyPrepaid = sqlContext.read.option("basePath",pathAggDailyOutput).parquet(input)
    dailyPrepaid.registerTempTable("daily_usage_prepaid")
    
    val distinctMsisdn = sqlContext.sql("""
select distinct msisdn from daily_usage_prepaid
    """)
    distinctMsisdn.registerTempTable("distinct_msisdn")
    
    val prefixLookup = sqlContext.sql("""
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 1) = intcct.intcctPfx and intcct.length = 1 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 2) = intcct.intcctPfx and intcct.length = 2 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 3) = intcct.intcctPfx and intcct.length = 3 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 4) = intcct.intcctPfx and intcct.length = 4 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 5) = intcct.intcctPfx and intcct.length = 5 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 6) = intcct.intcctPfx and intcct.length = 6 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 7) = intcct.intcctPfx and intcct.length = 7 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 8) = intcct.intcctPfx and intcct.length = 8 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 9) = intcct.intcctPfx and intcct.length = 9 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 10) = intcct.intcctPfx and intcct.length = 10 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 11) = intcct.intcctPfx and intcct.length = 11 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 12) = intcct.intcctPfx and intcct.length = 12 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 13) = intcct.intcctPfx and intcct.length = 13 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 14) = intcct.intcctPfx and intcct.length = 14 union all
select msisdn, locName, length from distinct_msisdn a left join intcct on substr(msisdn, 1, 17) = intcct.intcctPfx and intcct.length = 17
    """)
    .repartition(col("msisdn"))
    prefixLookup.registerTempTable("prefix_lookup")
    
    val msisdnCity = sqlContext.sql("""
select 
  msisdn, 
  first_value(locName) over (partition by msisdn order by length desc) city
from prefix_lookup
""")
    msisdnCity.registerTempTable("msisdn_city")
    
    val distinctMsisdnCity = sqlContext.sql("""
select distinct msisdn, city from msisdn_city
    """)
    distinctMsisdnCity.registerTempTable("distinct_msisdn_city")
    
    val dailyPrepaidCity = sqlContext.sql("""
select
    daily_usage_prepaid.MSISDN MSISDN,
    PREFIX_NUMBER,
    case
      when service_scenario = '2' then c.city
      else SERVICE_CITY_NAME 
    end SERVICE_CITY_NAME,
    SERVICE_PROVIDER_ID,
    COUNTRY_NAME,
    HLR_BRANCH_NM,
    HLR_REGION_NM,
    OTHER_PREFIX_NO,
    case when service_scenario = '2' then SERVICE_CITY_NAME
    else c.city end DESTINATION_CITY_NAME,
    DESTINATION_PROVIDER_ID,
    DESTINATION_COUNTRY_NAME,
    SERVICE_CLASS_ID,
    PROMO_PACKAGE_CODE,
    PROMO_PACKAGE_NAME,
    BRAND_NAME,
    MSC_ADDRESS,
    ORIGINAL_REALM,
    ORIGINAL_HOST,
    MCCMNC,
    LAC,
    CI,
    LACI_CLUSTER_ID,
    LACI_CLUSTER_NM,
    LACI_REGION_ID,
    LACI_REGION_NM,
    LACI_AREA_ID,
    LACI_AREA_NM,
    LACI_SALESAREA_ID,
    LACI_SALESAREA_NM,
    IMSI,
    APN,
    SERVICE_SCENARIO,
    ROAMING_POSITION,
    FAF,
    RATING_GROUP,
    CONTENT_TYPE,
    IMEI,
    GGSN_ADDRESS,
    SGSN_ADDRESS,
    RAT_TYPE,
    SERVICE_OFFERING_ID,
    SUBSCRIBER_TYPE,
    ACCOUNT_ID,
    ACCOUNT_GROUPID,
    TRAFFIC_CASE,
    REVENUE_CODE,
    DIRECTION_TYPE,
    DISTANCE_TYPE,
    SERVICE_TYPE,
    SERVICE_USAGE_TYPE,
    SVC_USG_DIRECTION,
    SVC_USG_DESTINATION,
    TRAFFIC_FLAG,
    REVENUE_FLAG,
    TOTAL_VOLUME,
    TOTAL_AMOUNT,
    TOTAL_DURATION,
    TOTAL_HIT,
    COMMUNITY_ID_1,
    COMMUNITY_ID_2,
    COMMUNITY_ID_3,
    ACCUMULATED_COST,
    RECORD_TYPE,
    ECI,
    SITE_TECH,
    SITE_OPERATOR,
    SVC_USG_TRAFFIC,
    MGR_SVCCLSS_ID,
    OFFER_ID,
    OFFER_ATTR_KEY,
    OFFER_ATTR_VALUE,
    OFFER_AREA_NAME,
    PRC_DT,
    MICROCLUSTER_NAME,
    case 
      when promo_package_name in (
        'BTPN Wow',
        'Bundling Corporate',
        'Bundling 60GB',
        'LE Pintar2',
        'SP Bluebird',
        'SP CUG Large Enterprise',
        'SP CUG Large Interprise',
        'SP CUG Prepaid SME'
      )
      then 'B2B PREPAID'
      else ''
    end CUSTOMER_SEGMENT,
    transaction_date,
    job_id,
    src_tp 
from daily_usage_prepaid
left join distinct_msisdn_city c
on daily_usage_prepaid.msisdn = c.msisdn
    """)
    
    Common.cleanDirectory(sc, pathAggDailyOutput + "_tmp/transaction_date="+trxDate)
    dailyPrepaidCity
    .write.partitionBy("transaction_date", "job_id", "src_tp")
    .mode("append")
    .parquet(pathAggDailyOutput + "_tmp")
  }
}