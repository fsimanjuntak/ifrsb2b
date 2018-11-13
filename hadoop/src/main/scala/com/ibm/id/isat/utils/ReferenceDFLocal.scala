package com.ibm.id.isat.utils


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.ibm.id.isat.usage.cs5.CS5Reference

object ReferenceDFLocal {
  
  def getServiceClassDF(sqlContext: SQLContext) : DataFrame =
  {
    val svcClassDF = broadcast(sqlContext.read
       .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ReferenceSchema.svcClassSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/NONTDWM.REF_SERVICE_CLASS_MV_*.txt"));
    
    return svcClassDF;
  }
  
  def getRegionBranchDF(sqlContext: SQLContext) : DataFrame =
  {
    val regionBranchDF = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ReferenceSchema.regionBranchSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/ref_region_branch.csv"));
    
    return regionBranchDF;
  }
  
  def getRevenueCodeDF(sqlContext: SQLContext) : DataFrame =
  {
    val revcodeDF = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(ReferenceSchema.revcodeSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/SOR.REF_REVCODE_*.txt"));
    
    return revcodeDF;
  }
  
  def getMaDaDF(sqlContext: SQLContext) : DataFrame =
  {
    val madaDF = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(ReferenceSchema.madaSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/NONTDWM.REF_MA_DA_*.txt"));
    
    return madaDF;
  }
  
  def getRecordTypeDF(sqlContext: SQLContext) : DataFrame =
  {
    val recTypeDF = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ReferenceSchema.recTypeSchema)
      .load("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/all_ref_record_type_CCN.txt"));
    

    return recTypeDF;
  }
  
  def getSDPOfferDF(sqlContext: SQLContext) : DataFrame =
  {
    val sdpOfferDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(ReferenceSchema.sdpOfferSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/sdp_offer.txt");
    

    return sdpOfferDF;
  }
  
  def getServiceClassOfferDF(sqlContext: SQLContext) : DataFrame =
  {
    val svcClassOfferDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ReferenceSchema.svcClassOfrSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/NONTDWM.REF_SERVICE_CLASS_OFR*.txt");
    

    return svcClassOfferDF;
  }
  
  def getIntcctDF(sqlContext: SQLContext) : DataFrame =
  {
    val intcctDF = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(ReferenceSchema.intcctAllSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/HA1.REF_INTCCT_MV_ALL_*.txt"));
    

    return intcctDF;
  }
  
  def getFossDF(sqlContext: SQLContext) : DataFrame =
  {
    val fossDF = broadcast(sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .schema(ReferenceSchema.fossSchema)
      .load("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/Tertio/foss_to_hadoop/spool_dir/*.txt"));
    
    return fossDF;
  }
}