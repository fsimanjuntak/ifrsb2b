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

object MergeCallClass {
  def main(args: Array[String]): Unit = {
    val (configDir, classType, mergeType) = try {
      (args(0), args(1), args(2))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage:<config path> <PREPAID/POSTPAID> <EVENT/PO>")
      return
    };
    
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    
    val pathRefPrepaidEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PREPAID_EVENT_REFERENCE") + "/*.csv"
    val pathRefSalmoEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.SALMO_EVENT_REFERENCE") + "/*.csv"
    val pathRefPrepaidEventReferenceMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_PREPAID_EVENT_REF")
    
    val pathRefPrepaidPoTemplate = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.PREPAID_PO_TEMPLATE") + "/*.csv"
    val pathRefSalmoPoTemplate = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.SALMO_PO_TEMPLATE") + "/*.csv"
    val pathRefPrepaidPoTemplateMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_PREPAID_PO_TEMPL")
    
    val pathRefPostpaidEventReference = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE") + "/*.csv"
    val pathRefPostpaidEventReferenceIphone= Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_EVENT_REFERENCE_IPHONE") + "/*.csv"
    val pathRefPostpaidEventReferenceMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_POSTPAID_EVENT_REF")
    
    val pathRefPostpaidPoTemplate = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_PO_TEMPLATE") + "/*.csv"
    val pathRefPostpaidPoTemplateIphone = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.POSTPAID_PO_TEMPLATE_IPHONE") + "/*.csv"
    val pathRefPostpaidPoTemplateMerge = Common.getConfigDirectory(sc, configDir, "IFRS.REFERENCE.MERGE_POSTPAID_PO_TEMPL")
    
    // Initialize Spark SQL
    val sqlContext = new SQLContext(sc)
    
    if (classType=="PREPAID"){
      if(mergeType=="EVENT"){
        val refPrepaidEventReferenceDf = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", "|")
        .schema(eventReferenceSchema)
        .load(pathRefPrepaidEventReference)
        refPrepaidEventReferenceDf.registerTempTable("ref_prepaid_event_reference")
        
        val refSalmoEventReferenceDf = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", "|")
        .schema(eventReferenceSchema)
        .load(pathRefSalmoEventReference)
        refSalmoEventReferenceDf.registerTempTable("ref_salmo_event_reference")
        
        val refPrepaidEventReferenceMergeDf = sqlContext.sql("""
            select distinct rev_code,contract_template_id,
            level_1,level_2,level_3,level_4,level_5,level_6,level_7,level_8,level_9,level_10,level_11,
            ctr_desc,ctr_po_1,ctr_po_2,ctr_po_3,ctr_po_4,ctr_po_5,ctr_po_6,ctr_po_7,ctr_po_8,offer_id,
            inst_month_1,inst_month_2,inst_month_3,inst_month_4,inst_month_5,inst_month_6,inst_month_7,
            inst_month_8,inst_month_9,inst_month_10,inst_month_11,inst_month_12,
            voice_share,sms_share,data_share,vas_share,voice_b2c,sms_b2c,data_b2c,vas_b2c,
            voice_b2b,sms_b2b,data_b2b,vas_b2b,sp_data, 'NONSALMO' src_tp
            from ref_prepaid_event_reference
            union all
            select distinct rev_code,contract_template_id,
            level_1,level_2,level_3,level_4,level_5,level_6,level_7,level_8,level_9,level_10,level_11,
            ctr_desc,ctr_po_1,ctr_po_2,ctr_po_3,ctr_po_4,ctr_po_5,ctr_po_6,ctr_po_7,ctr_po_8,offer_id,
            inst_month_1,inst_month_2,inst_month_3,inst_month_4,inst_month_5,inst_month_6,inst_month_7,
            inst_month_8,inst_month_9,inst_month_10,inst_month_11,inst_month_12,
            voice_share,sms_share,data_share,vas_share,voice_b2c,sms_b2c,data_b2c,vas_b2c,
            voice_b2b,sms_b2b,data_b2b,vas_b2b,sp_data, 'SALMO' src_tp
            from ref_salmo_event_reference
        """)
        refPrepaidEventReferenceMergeDf.repartition(1)
        .write.format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("delimiter", "|").option("header", "true")
        .save(pathRefPrepaidEventReferenceMerge + "/working_dir")
      }else if(mergeType=="PO"){
        val refOriPrepaidPoTemplateDf = broadcast(sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", ";")
          .schema(prepaidPoTemplateSchema)
          .load(pathRefPrepaidPoTemplate).cache())
          refOriPrepaidPoTemplateDf.registerTempTable("ref_ori_prepaid_po_template")
    
        val refSalmoPoTemplateDf = broadcast(sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", ";")
          .schema(prepaidPoTemplateSchema)
          .load(pathRefSalmoPoTemplate))
          refSalmoPoTemplateDf.persist()
          refSalmoPoTemplateDf.registerTempTable("ref_salmo_po_template")
          
        val refPrepaidPoTemplateDf = broadcast(sqlContext.sql("""
        select *, 'NONSALMO' src_tp
        from ref_ori_prepaid_po_template
        union all
        select *, 'SALMO' src_tp
        from ref_salmo_po_template
        """))
        refPrepaidPoTemplateDf.repartition(1)
        .write.format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("delimiter", ";").option("header", "true")
        .save(pathRefPrepaidPoTemplateMerge + "/working_dir")
          
      }
    }else if (classType=="POSTPAID"){
      if(mergeType=="EVENT"){
        val refPostpaidEventReferenceDf = broadcast(sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", "|")
          .schema(postpaidEventReferenceSchema)
          .load(pathRefPostpaidEventReference))
          refPostpaidEventReferenceDf.registerTempTable("ref_postpaid_event_reference")
        
        val refPostpaidEventReferenceIphoneDf = broadcast(sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", "|")
          .load(pathRefPostpaidEventReferenceIphone))
          refPostpaidEventReferenceIphoneDf.registerTempTable("ref_postpaid_event_reference_iphone")
        
        val refPostpaidEventReferenceMergeDf = broadcast(sqlContext.sql("""
          select *, 'POSTPAID' src_tp
          from ref_postpaid_event_reference
          union all
          select *, 'IPHONE' src_tp
          from ref_postpaid_event_reference_iphone
          """))
          refPostpaidEventReferenceMergeDf.repartition(1)
          .write.format("com.databricks.spark.csv")
          .mode("overwrite")
          .option("delimiter", "|").option("header", "true")
          .save(pathRefPostpaidEventReferenceMerge + "/working_dir")
          
      }else if(mergeType=="PO"){
        val refPostpaidPoTemplateDf = broadcast(sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", ";")
          .schema(postpaidPoTemplateSchema)
          .load(pathRefPostpaidPoTemplate))
          refPostpaidPoTemplateDf.registerTempTable("ref_postpaid_po_template")
        
        val refPostpaidPoTemplateIphoneDf = broadcast(sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .schema(postpaidPoTemplateSchema)
            .load(pathRefPostpaidPoTemplateIphone))
          refPostpaidPoTemplateIphoneDf.registerTempTable("ref_postpaid_po_template_iphone")
        
        val refPostpaidPoTemplateMergeDf = broadcast(sqlContext.sql("""
          select *, 'POSTPAID' src_tp
          from ref_postpaid_po_template
          union all
          select *, 'IPHONE' src_tp
          from ref_postpaid_po_template_iphone
          """))
          refPostpaidPoTemplateMergeDf.repartition(1)
          .write.format("com.databricks.spark.csv")
          .mode("overwrite")
          .option("delimiter", ";").option("header", "true")
          .save(pathRefPostpaidPoTemplateMerge + "/working_dir")
        
      }
    }
  }
  val postpaidEventReferenceSchema = new StructType(Array(
      StructField("rev_code", StringType, true),
      StructField("contract_template_id", StringType, true),
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
      StructField("sfc", StringType, true),
      StructField("duration", StringType, true),
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
      StructField("gl_account_b2c", StringType, true),
      StructField("gl_account_b2c_desc", StringType, true),
      StructField("gl_account_b2b", StringType, true),
      StructField("gl_account_b2b_desc", StringType, true),
      StructField("gl_account_b2b-credit", StringType, true),
      StructField("gl_account_b2b_desc-credit", StringType, true),
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
      StructField("vas_b2b", StringType, true)))
  val postpaidPoTemplateSchema = new StructType(Array(
      StructField("po_template_id", StringType, true),
      StructField("group", StringType, true),
      StructField("po_template_name", StringType, true),
      StructField("allocated", StringType, true),
      StructField("satisfaction_measurement_model", StringType, true),
      StructField("performance_satisfaction_plan", StringType, true),
      StructField("sub_line_type_code", StringType, true),
      StructField("status", StringType, true)))
  val eventReferenceSchema = new StructType(Array(
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
  val prepaidPoTemplateSchema = new StructType(Array(
    StructField("po_template_id", StringType, true),
    StructField("group", StringType, true),
    StructField("po_template_name", StringType, true),
    StructField("allocated", StringType, true),
    StructField("satisfaction_measurement_model", StringType, true),
    StructField("performance_satisfaction_plan", StringType, true),
    StructField("sub_line_type_code", StringType, true),
    StructField("status", StringType, true)))
}