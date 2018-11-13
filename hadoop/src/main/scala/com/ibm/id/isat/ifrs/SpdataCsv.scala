package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.ibm.id.isat.utils.Common
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object SpdataCsv {
  def main(args: Array[String]): Unit = {
    val (prcMth, tgtMth, configDir, env) = try {
      (args(0), args(1), args(2), args(3))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process month> <target month> <config directory> <LOCAL|PRODUCTION>")
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
    val pathSpdataSor = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.SP_DATA_SOR")
    val pathSpdataCsv = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.SP_DATA_CSV")
    val pathSpdataMentariDir = Common.getConfigDirectory(sc, configDir, "IFRS.INPUT.IFRS_SPDATA_MENTARI_DIR") + "/" + tgtMth
    val pathUsageSor = "/user/hdp-rev/spark/output/transform/CS5_prepaid"
    val pathSpdataSorOutput = pathSpdataSor + "/PRC_MTH=" + prcMth
    val pathRefPrepaidEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PREPAID_EVENT_REFERENCE") + "/*.csv"
    
    val refOriPrepaidEventReferenceDf = broadcast(sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(SummaryPrepaid.prepaidEventReferenceSchema)
    .load(pathRefPrepaidEventReference)
    .cache())
    refOriPrepaidEventReferenceDf.registerTempTable("ref_ori_prepaid_event_reference")
    
    Common.cleanDirectory(sc, pathSpdataSorOutput)
    FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(pathUsageSor + "/TRANSACTION_DATE=" + tgtMth + "*"))
    .map{fileStatus => fileStatus.getPath().toString()}
    .take(2)
    .map{pathDaily => {
      val trxDate = pathDaily.replaceAll(".*TRANSACTION_DATE=", ""); 
      val tableName = "usage_sor_" + trxDate;
      sqlContext.read.option("basePath", pathUsageSor).parquet(pathDaily).registerTempTable(tableName);
      sqlContext.sql("""
      select 
        TRANSACTION_DATE, 
        TRIGGER_TIME, 
        A_PARTY_NUMBER, 
        REVENUE_CODE, 
        SERVICE_CITY_NAME 
      from """+tableName+""" a
      join ref_ori_prepaid_event_reference b
      on a.REVENUE_CODE = b.REV_CODE
        and a.REVENUE_FLAG = 'Yes' 
        and nvl(b.SP_DATA, '') <> '' 
      """)
      .repartition(200).write.mode("append")
      .parquet(pathSpdataSorOutput)
    }}
    
    sqlContext.read.parquet(pathSpdataSorOutput).registerTempTable("spdata")
    sqlContext.read.parquet(pathSpdataMentariDir).registerTempTable("spdata_mentari")
    
    val spdataCsvDf = sqlContext.sql("""
    select 
      '' PRC_DT, 
      '' JOB_ID, 
      TRANSACTION_DATE START_DT, 
      REVENUE_CODE REV_CODE, 
      SERVICE_CITY_NAME CITY, 
      count(*) TOTAL_HIT, 
      '' CUSTOMER_SEGMENT, 
      '' PRICE, 
      '' AMOUNT 
    from (
      select 
        *, 
        row_number() over (
          partition by A_PARTY_NUMBER 
          order by TRIGGER_TIME desc
        ) rnk 
      from spdata having rnk = 1
    ) non_duplicate 
    group by 
      TRANSACTION_DATE, 
      REVENUE_CODE, 
      SERVICE_CITY_NAME
    union all
    select
      '' PRC_DT, 
      '' JOB_ID, 
      `Date` START_DT, 
      PROMO_PACKAGE_NAME REV_CODE, 
      `City` CITY, 
      `MentariSuperDataActivation` TOTAL_HIT, 
      '' CUSTOMER_SEGMENT, 
      '' PRICE, 
      '' AMOUNT 
    from spdata_mentari
    """)
    .repartition(1)
    
    Common.cleanDirectory(sc, pathSpdataCsv+"/PRC_MTH="+prcMth)
    spdataCsvDf.write.format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true").option("delimiter", ";")
    .save(pathSpdataCsv+"/PRC_MTH="+prcMth)
    
  }
  
}