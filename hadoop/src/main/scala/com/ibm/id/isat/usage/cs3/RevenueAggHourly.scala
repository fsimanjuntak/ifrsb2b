package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveContext

object RevenueAggHourly {
  def main(args: Array[String]): Unit = {
    
   /* //val sc = new SparkContext("local", "testing spark ", new SparkConf());
    val sc = new SparkContext(new SparkConf())
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._  
  
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    hc.setConf("hive.exec.dynamic.partition", "true") 
    hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict") 
    
    val results = hc.sql("select trgrdate, sum(usgAmount) from revenue.CS3TransformPrepaid group by trgrdate");
    
    val aggHourly = hc.sql("""
                    insert into revenue.AGGHOURLY partition (TRANSACTION_DATE)
                    select TRANSACTION_HOUR, A_PARTY_NUMBER, A_PREFIX, SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM, HLR_BRANCH_NM, HLR_REGION_NM, 
                    B_PREFIX, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM, SERVICE_CLASS_ID, PROMO_PACKAGE_CODE, PROMO_PACKAGE_NAME, BRAND_NAME, MSC_ADDRESS, ORIGINAL_REALM, ORIGINAL_HOST, 
                    MNC_MCC, LAC, CI, LACI_CLUSTER_ID, LACI_CLUSTER_NM, LACI_REGION_ID, LACI_REGION_NM, LACI_AREA_ID, LACI_AREA_NM, 
                    LACI_SALESAREA_ID, LACI_SALESAREA_NM, IMSI, APN, SERVICE_SCENARIO, ROAMING_POSITION, FAF,RATING_GROUP, 
                    CONTENT_TYPE, IMEI, GGSN_ADDRESS, SGSN_ADDRESS, RAT_TYPE, SERVICE_OFFER_ID, 
                    PAYMENT_CATEGORY, ACCOUNT_ID, ACCOUNT_GROUP_ID, TRAFFIC_CASE, REVENUE_CODE, DIRECTION_TYPE, 
                    DISTANCE_TYPE, SERVICE_TYPE, SERVICE_USG_TYPE, SVC_USG_DIRECTION, SVC_USG_DESTINATION, TRAFFIC_FLAG, REVENUE_FLAG, 
                    sum(USAGE_VOLUME) TOTAL_VOLUME, 
                    sum(USAGE_AMOUNT) TOTAL_AMOUNT, 
                    sum(USAGE_DURATION) TOTAL_DURATION,  
                    sum(HIT) TOTAL_HIT, 
                    COMMUNITY_ID_1, COMMUNITY_ID_2, COMMUNITY_ID_3, sum(ACCUMULATED_COST), 
                    RECORD_TYPE, ECI, SITE_TECH, SITE_OPERATOR, REVENUE_CODE_L7, MGR_SVCCLSS_ID, OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME, TRANSACTION_DATE 
                    from revenue.CS3TransformPrepaid3 group by  
                    TRANSACTION_HOUR, A_PARTY_NUMBER, A_PREFIX, SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM, HLR_BRANCH_NM, HLR_REGION_NM, 
                    B_PREFIX, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM, SERVICE_CLASS_ID, PROMO_PACKAGE_CODE, PROMO_PACKAGE_NAME, BRAND_NAME, MSC_ADDRESS, ORIGINAL_REALM, ORIGINAL_HOST, 
                    MNC_MCC, LAC, CI, LACI_CLUSTER_ID, LACI_CLUSTER_NM, LACI_REGION_ID, LACI_REGION_NM, LACI_AREA_ID, LACI_AREA_NM, 
                    LACI_SALESAREA_ID, LACI_SALESAREA_NM, IMSI, APN, SERVICE_SCENARIO, ROAMING_POSITION, FAF,RATING_GROUP, 
                    CONTENT_TYPE, IMEI, GGSN_ADDRESS, SGSN_ADDRESS, RAT_TYPE, SERVICE_OFFER_ID, 
                    PAYMENT_CATEGORY, ACCOUNT_ID, ACCOUNT_GROUP_ID, TRAFFIC_CASE, REVENUE_CODE, DIRECTION_TYPE, 
                    DISTANCE_TYPE, SERVICE_TYPE, SERVICE_USG_TYPE, SVC_USG_DIRECTION, SVC_USG_DESTINATION, TRAFFIC_FLAG, REVENUE_FLAG,
                    COMMUNITY_ID_1, COMMUNITY_ID_2, COMMUNITY_ID_3,RECORD_TYPE, ECI, SITE_TECH, SITE_OPERATOR, REVENUE_CODE_L7, MGR_SVCCLSS_ID, 
                    OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME, TRANSACTION_DATE
                    """);
    
    //results.write.partitionBy("trgrdate").mode("append").save("/user/apps/CS3/output/testagg") 
    results.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/1")
    
    sc.stop();*/
    
    
  }
}