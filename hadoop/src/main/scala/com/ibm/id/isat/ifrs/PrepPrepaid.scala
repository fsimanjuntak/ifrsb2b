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

object PrepPrepaid {
  def main(args: Array[String]): Unit = {
    
    var prcDt = ""
    var config = ""
    try {
      config = args(0)
      prcDt = args(1)      
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    
    val sqlContext=new SQLContext(sc)
    
    val pathRefPromocodeTemplate = Common.getConfigDirectory(sc, config, "IFRS.REFERENCE.REF_PROMOCODE_TEMPLATE")
    val pathDailyUsage = Common.getConfigDirectory(sc, config, "IFRS.USAGE.DAILY_PREPAID_TMP")
    val pathDailyNonusage = Common.getConfigDirectory(sc, config, "IFRS.NONUSAGE.DAILY_PREPAID_TMP")
    val pathDailyUsageOutput = Common.getConfigDirectory(sc, config, "IFRS.USAGE.DAILY_PREPAID")
    val pathDailyNonusageOutput = Common.getConfigDirectory(sc, config, "IFRS.NONUSAGE.DAILY_PREPAID")
    
    val promoCodeDf = broadcast(sqlContext.read.format("com.databricks.spark.csv")
    .option("delimiter", "|").option("header", "true").load(pathRefPromocodeTemplate))
    promoCodeDf.persist()
    promoCodeDf.count()
    promoCodeDf.registerTempTable("ref_promocode_template")
    
    val dailyUsageDf = sqlContext.read
    .parquet(pathDailyUsage + "/transaction_date=" + prcDt)
    dailyUsageDf.registerTempTable("daily_usage")
    
    val dailyNonusageDf = sqlContext.read.option("basePath", pathDailyNonusage)
    .parquet(pathDailyNonusage+ "/*/TRANSACTION_DATE=" + prcDt)
    dailyNonusageDf.registerTempTable("daily_nonusage")
    
    val dailyUsageOutDf=sqlContext.sql("""
    select *, case when CUSTOMER_GROUP is null then 'B2C PREPAID' else CUSTOMER_GROUP end CUSTOMER_SEGMENT, """+prcDt +""" transaction_date
    from daily_usage left outer join ref_promocode_template
    on daily_usage.PROMO_PACKAGE_NAME=ref_promocode_template.PROMO_CODE_ID
""")
    
    val dailyNonUsageOutDf=sqlContext.sql("""
    select daily_nonusage.*, case when CUSTOMER_GROUP is null then 'B2C PREPAID' else CUSTOMER_GROUP end CUSTOMER_SEGMENT--, """+prcDt +""" TRANSACTION_DATE 
    from daily_nonusage left outer join ref_promocode_template
    on daily_nonusage.PROMO_PACKAGE_NAME=ref_promocode_template.PROMO_CODE_ID
""")

    
    Common.cleanDirectory(sc, pathDailyUsageOutput+"/transaction_date="+prcDt)
    dailyUsageOutDf.write.partitionBy("transaction_date")
    .mode("append").save(pathDailyUsageOutput)
    
    Common.cleanDirectory(sc, pathDailyNonusageOutput+"/SRC_TP=ALL"+"/TRANSACTION_DATE="+prcDt)
    dailyNonUsageOutDf.write.partitionBy("TRANSACTION_DATE")
    .mode("append").save(pathDailyNonusageOutput+"/SRC_TP=ALL")
    
  }
}