package com.ibm.id.isat.fdv

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

object FdvDailyPackage {
  def main(args: Array[String]): Unit = {
    val (prcDt,jobId, configDir, env, inputDir, targetDir) = try {
      (args(0), args(1), args(2),args(3),args(4),args(5))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process month> <job identifier> <config directory>  <LOCAL|PRODUCTION> <input directory> <target directory>")
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
    
    // Initialize Logging
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    
    // Paths
    val fdvMoboPmJoin = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_MOBO_PM_JOIN")
    val fdvDailyPackage  = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_DAILY_PACKAGE")
   
     sqlContext.read.parquet(fdvMoboPmJoin+"/PRC_DT="+prcDt).registerTempTable("FDV_MOBO_PM_JOIN")
    
    val fdvDailyPackageDf = sqlContext.sql("""
        SELECT  MOBO_TRX_DTTM,
                PM_VOUCHER_STATUS,
                MOBO_INJ_PACK_ID,
                MOBO_INJ_PACK_NAME,
                COUNT(MOBO_VOUCHER_SN) NUM_VOUCHER,
                SUM(MOBO_INJ_MAIN_PRICE) TOTAL_AMOUNT_MAIN_PRICE,
                SUM(MOBO_INJ_DISC_PRICE) TOTAL_AMOUNT_DISC_PRICE,
                SUM(MOBO_INJ_SALES_PRICE) TOTAL_AMOUNT_SALES_PRICE,
                """+prcDt+""" PRC_DT
         FROM FDV_MOBO_PM_JOIN
         GROUP BY 
                MOBO_TRX_DTTM,
                PM_VOUCHER_STATUS,
                MOBO_INJ_PACK_ID,
                MOBO_INJ_PACK_NAME
    """)
    Common.cleanDirectory(sc, fdvDailyPackage+"/PRC_DT="+prcDt)
     fdvDailyPackageDf.repartition(10)
    .write.partitionBy("PRC_DT")
    .mode("append")
    .parquet(fdvDailyPackage)
    
   
      
      
    
  }
}