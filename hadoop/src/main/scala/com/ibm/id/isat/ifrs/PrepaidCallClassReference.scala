package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object PrepaidCallClassReference {
  def main(args: Array[String]): Unit = {
    val (refCallClassDir, refLevelToDirectionDir, configFile, env) = try {
      (args(0), args(1), args(2), args(3))
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
    val pathRefPrepaidEventReference = Common.getConfigDirectory(sc, configFile, "IFRS.REFERENCE.PREPAID_EVENT_REFERENCE")
    
    // Input Data Frames
    val refCallClassDf=sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .schema(SummaryPrepaid.prepaidEventReferenceSchema)
        .load(refCallClassDir + "/*.*")
    refCallClassDf.registerTempTable("ref_call_class")
    
    val refLevelToDirection=sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .schema(refLevelToDirectionSchema)
        .load(refLevelToDirectionDir + "/*.*")
    refLevelToDirection.registerTempTable("ref_level_to_direction")
    
    // Transform
    val refRevcode=sqlContext.sql("""
select 
    rev_code,
    contract_template_id,
    level_1,
    level_2,
    level_3,
    level_4,
    a.level_5,
    level_6,
    level_7,
    level_8,
    level_9,
    level_10,
    a.level_11,
    ctr_desc,
    ctr_po_1,
    ctr_po_2,
    ctr_po_3,
    ctr_po_4,
    ctr_po_5,
    ctr_po_6,
    ctr_po_7,
    ctr_po_8,
    offer_id,
    inst_month_1,
    inst_month_2,
    inst_month_3,
    inst_month_4,
    inst_month_5,
    inst_month_6,
    inst_month_7,
    inst_month_8,
    inst_month_9,
    inst_month_10,
    inst_month_11,
    inst_month_12,
    voice_share,
    sms_share,
    data_share,
    vas_share,
    voice_b2c,
    sms_b2c,
    data_b2c,
    vas_b2c,
    voice_b2b,
    sms_b2b,
    data_b2b,
    vas_b2b,
    sp_data,
    SVC_USG_TP,
    DRC_TP
from ref_call_class a
join ref_level_to_direction b
on a.level_5 = b.level_5 and a.level_11 = b.level_11
    """)
    refRevcode.registerTempTable("sa_darollover")
    

    refRevcode.coalesce(1)
    .write.format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("delimiter", "|")
    .save(pathRefPrepaidEventReference)
  }
  
  val refLevelToDirectionSchema = new StructType(Array(
    StructField("LEVEL_5", StringType, true),
    StructField("LEVEL_11", StringType, true),
    StructField("SVC_USG_TP", StringType, true),
    StructField("DRC_TP", StringType, true)))
}