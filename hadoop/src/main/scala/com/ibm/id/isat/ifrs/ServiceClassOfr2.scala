package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

// Utility for create service class ofr 2
object ServiceClassOfr2 {
  def main(args: Array[String]): Unit = {
    val (svcOriginDir, refPromocodeDir, outputDir, env) = try {
      (args(0), args(1), args(2), args(3))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <origin svcClassOfr dir> <refPromocode dir> <output dir> <LOCAL|PRODUCTION>")
      return
    };
    // Initialize Spark Context
    val sc = env match {
      case "PRODUCTION" => new SparkContext(new SparkConf())
      case "LOCAL" => new SparkContext("local[*]", "local spark", new SparkConf())
      case _ => return
    };
    
    val sqlContext = new HiveContext(sc)
    
    val svcClassOfrDf = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .load(svcOriginDir)
    svcClassOfrDf.registerTempTable("service_class_ofr")
    svcClassOfrDf.show()
    val refPromocodeDf = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .option("header", "true")
        .load(refPromocodeDir)
    refPromocodeDf.registerTempTable("ref_promocode")
    
    sqlContext.sql("""
select c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, nvl(customer_group, '') c12
from service_class_ofr a left join ref_promocode b on a.c8 = b.promo_code_id
    """)
    .coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .option("delimiter", ",")
    .mode("overwrite")
    .save(outputDir)
  }
}