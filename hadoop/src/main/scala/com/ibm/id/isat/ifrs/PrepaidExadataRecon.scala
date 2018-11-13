package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object PrepaidExadataRecon {
  def main(args: Array[String]): Unit = {
    val (prcDt, inputDir, configDir, env) = try {
      (args(0), args(1), args(2), args(3))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <trx date> <input dir> <config file> <LOCAL|PRODUCTION>")
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
    val pathDailyUsage = inputDir 
    val pathPrepaidRecon = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.IFRS_PREPAID_RECON_DIR")
    
    
    val prepaidReconDf = sqlContext.read.parquet(inputDir)
    .filter("nvl(prc_dt, '') <> '' and transaction_date rlike '^[0-9]+$'")
    .withColumn("REVENUE_WITH_TAX", col("REVENUE_WITH_TAX").cast(DoubleType))
    .withColumn("REVENUE", col("REVENUE").cast(DoubleType))
    .withColumn("TOTAL_HIT", col("TOTAL_HIT").cast(DoubleType))
    
    Common.cleanDirectoryWithPattern(sc, pathPrepaidRecon, "/TRANSACTION_DATE=*/PRC_DT="+prcDt+"/SRC_SYS=EXADATA")
    prepaidReconDf.repartition(20)
    .write.partitionBy("TRANSACTION_DATE", "PRC_DT", "SRC_SYS")
    .mode("append")
    .parquet(pathPrepaidRecon)
    
  }
}