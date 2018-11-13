package com.ibm.id.isat.ifrs
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.DateType

object Recap {
  def main(args: Array[String]): Unit = {
    
    var prcDt = ""
    var configDir = ""
    try {
      configDir = args(0)
      prcDt = args(1)      
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    
    val sqlContext=new SQLContext(sc)
    
    val pathPostpaidReSummary = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.POSTPAID_RE_SUMMARY")
    val pathPrepaidReSummary = Common.getConfigDirectory(sc, configDir, "IFRS.OUTPUT.PREPAID_RE_SUMMARY")
    
    
    val smyRePrepaid=sqlContext.read.option("basePath",pathPrepaidReSummary).parquet(pathPrepaidReSummary)
    smyRePrepaid.registerTempTable("RE_PREPAID")
    val smyRePostpaid=sqlContext.read.option("basePath",pathPostpaidReSummary).parquet(pathPostpaidReSummary)
    smyRePostpaid.registerTempTable("RE_POSTPAID")
    
    val genPrepaid=sqlContext.sql("select PRC_DT,count(*) ROWCOUNT, sum(LINE_AMT) TOTAL_LINE_AMT from RE_PREPAID group by PRC_DT")
//    genPrepaid.write.format("com.databricks.spark.csv")
//    .option("delimiter", "|").option("header", "true")
//    .save("/user/hdp-rev/spark/output/ifrs/recap/prepaid")
    genPrepaid.write.partitionBy("PRC_DT").save("/user/hdp-rev/spark/output/ifrs/recap/prepaid")
    
    val genPostpaid=sqlContext.sql("select PRC_DT,count(*) ROWCOUNT, sum(REVENUE_MNY) TOTAL_REVENUE_MNY from RE_POSTPAID group by PRC_DT")
//    genPostpaid.write.format("com.databricks.spark.csv")
//    .option("delimiter", "|").option("header", "true")
//    .save("/user/hdp-rev/spark/output/ifrs/recap/postpaid")
    genPostpaid.write.partitionBy("PRC_DT").save("/user/hdp-rev/spark/output/ifrs/recap/postpaid")
  }
}