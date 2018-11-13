package com.ibm.id.isat.fdv

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

object FdvMoboPmRevenue {
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
    
    //input
    val fdvMoboPmJoin = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_MOBO_PM_JOIN")
    val fdvMoboPmRevenue  = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_MOBO_PM_REVENUE")
    
    sqlContext.read.parquet(fdvMoboPmJoin+"/PRC_DT="+prcDt).registerTempTable("FDV_MOBO_PM_JOIN")
  
    val fdvMoboPmRevenueDf = 
    sqlContext.sql("""
                     SELECT 
                       MOBO_TRX_ID, 
                       MOBO_TRX_DTTM,
                       MOBO_VOUCHER_SN,
                       MOBO_VOUCHER_EXP_DT,
                       MOBO_INJ_DEALER_ID,
                       MOBO_INJ_DEALER_NAME,
                       MOBO_INJ_OUTLET_ID,
                       MOBO_INJ_OUTLET_NAME,
                       MOBO_INJ_OUTLET_AREA,
                       MOBO_INJ_STATUS,
                       MOBO_INJ_PACK_ID,
                       MOBO_INJ_PACK_NAME,
                       MOBO_INJ_MAIN_PRICE,
                       MOBO_INJ_DISC_PRICE,
                       MOBO_INJ_SALES_PRICE,
                       PM_VOUCHER_SN,
                       PM_RECHARGE_VAL,
                       PM_VOUCHER_EXP_DTTM,
                       PM_VOUCHER_GROUP_ID,
                       PM_VOUCHER_STATUS,
                       PM_RED_MSISDN,
                       PM_VOUCHER_RED_DTTM,
                       """+prcDt+""" PRC_DT
                     FROM FDV_MOBO_PM_JOIN WHERE PM_VOUCHER_STATUS = 'U'  or PM_VOUCHER_STATUS = 'X'
    """)
    Common.cleanDirectory(sc, fdvMoboPmRevenue+"/PRC_DT="+prcDt)
    fdvMoboPmRevenueDf.repartition(10)
    .write.partitionBy("PRC_DT")
    .mode("append")
    .parquet(fdvMoboPmRevenue)
 
  }
}