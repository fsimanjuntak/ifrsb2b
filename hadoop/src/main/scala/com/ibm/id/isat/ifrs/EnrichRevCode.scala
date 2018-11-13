package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.ibm.id.isat.utils.Common
import com.ibm.id.isat.utils.ReferenceDF
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object EnrichRevCode {
  def main(args: Array[String]): Unit = {
    
    var configDir = ""
    try {
      configDir = args(0)
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    
    val pathRefPrepaidEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PREPAID_EVENT_REFERENCE") + "/*.csv"
    val pathRefPrepaidEventReferenceEnrich = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.ENRICH_PREPAID_EVENT_REF")
    val pathRefLevelToDirection = "/user/apps/hadoop_spark/reference/ref_level_to_direction/*.txt"
    
    // Initialize Spark SQL
    val sqlContext = new SQLContext(sc)
    
    
    val refPrepaidEventReferenceDf = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", "|")
    .schema(prepaidEventReferenceSchema)
    .load(pathRefPrepaidEventReference)
    refPrepaidEventReferenceDf.registerTempTable("ref_prepaid_event_reference")
    
    val refLevelToDirectionDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|")
        .option("header", "true").schema(refLevelToDirectionSchema).load(pathRefLevelToDirection)
    refLevelToDirectionDF.registerTempTable("refLevelToDirection")
    
    val refPrepaidEventReferenceEnrichDf = sqlContext.sql("""
      select ref_prepaid_event_reference.*,
        refLevelToDirection.svc_usg_tp,
        refLevelToDirection.drc_tp,
        '' dist_tp,
        '' rev_sign
      from ref_prepaid_event_reference 
      left outer join refLevelToDirection 
      on (ref_prepaid_event_reference.level_5=refLevelToDirection.level_5 and ref_prepaid_event_reference.level_11=refLevelToDirection.level_11)
    """)
    refPrepaidEventReferenceEnrichDf.repartition(1)
    .write.format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("delimiter", "|").option("header", "true")
    .save(pathRefPrepaidEventReferenceEnrich + "/working_dir")
  }
  val refLevelToDirectionSchema = new StructType(Array(
    StructField("level_5",StringType,true),
    StructField("level_11",StringType,true),
    StructField("svc_usg_tp",StringType,true),
    StructField("drc_tp",StringType,true)
  ))
  val prepaidEventReferenceSchema = new StructType(Array(
    StructField("rev_code", StringType, true),
    StructField("contract_template_id", StringType, true),
    StructField("level_1", StringType, true),
    StructField("level_2", StringType, true),
    StructField("level_3", StringType, true),
    StructField("level_4", StringType, true),
    StructField("level_5", StringType, true),
    StructField("level_6", StringType, true),
    StructField("level_7", StringType, true),
    StructField("level_8", StringType, true),
    StructField("level_9", StringType, true),
    StructField("level_10", StringType, true),
    StructField("level_11", StringType, true),
    StructField("ctr_desc", StringType, true),
    StructField("ctr_po_1", StringType, true),
    StructField("ctr_po_2", StringType, true),
    StructField("ctr_po_3", StringType, true),
    StructField("ctr_po_4", StringType, true),
    StructField("ctr_po_5", StringType, true),
    StructField("ctr_po_6", StringType, true),
    StructField("ctr_po_7", StringType, true),
    StructField("ctr_po_8", StringType, true),
    StructField("offer_id", StringType, true),
    StructField("inst_month_1", StringType, true),
    StructField("inst_month_2", StringType, true),
    StructField("inst_month_3", StringType, true),
    StructField("inst_month_4", StringType, true),
    StructField("inst_month_5", StringType, true),
    StructField("inst_month_6", StringType, true),
    StructField("inst_month_7", StringType, true),
    StructField("inst_month_8", StringType, true),
    StructField("inst_month_9", StringType, true),
    StructField("inst_month_10", StringType, true),
    StructField("inst_month_11", StringType, true),
    StructField("inst_month_12", StringType, true),
    StructField("voice_share", StringType, true),
    StructField("sms_share", StringType, true),
    StructField("data_share", StringType, true),
    StructField("vas_share", StringType, true),
    StructField("voice_b2c", StringType, true),
    StructField("sms_b2c", StringType, true),
    StructField("data_b2c", StringType, true),
    StructField("vas_b2c", StringType, true),
    StructField("voice_b2b", StringType, true),
    StructField("sms_b2b", StringType, true),
    StructField("data_b2b", StringType, true),
    StructField("vas_b2b", StringType, true),
    StructField("sp_data", StringType, true)))
}