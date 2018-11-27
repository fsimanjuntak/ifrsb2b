package com.ibm.id.isat.ifrs

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import com.ibm.id.isat.utils.Common
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
import com.ibm.id.isat.utils.ReferenceDF
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SQLContext

object B2bPostpaidTestSatu {
  def main(args: Array[String]): Unit = {
    val (prcDt, jobId, configDir, env) = try {
      (args(0), args(1), args(2), args(3))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process date> <job identifier> <input directory> <config directory> <LOCAL|PRODUCTION>")
      return
    }
    
    // Initialize Spark Context
    val sc = env match {
      case "PRODUCTION" => new SparkContext(new SparkConf())
      case "LOCAL" => new SparkContext("local[*]", "local spark", new SparkConf())
      case _ => return
    }
    
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false") //Disable metadata parquet
    
    // Initialize SQL Context
    val sqlContext = new SQLContext(sc)
    
    // Initialize File System (for renameFile)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = "C:/Users/FransSimanjuntak/sparksource/test/"
    
    // Initialize Logging
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    rootLogger.setLevel(Level.OFF)
    
    val pathsource1 = path+"source/identity"    
    val pathsource2 = path+"source/detail"
    val inputDate = "20180804"
    
    val identityDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(path + "source/identity/file_date="+inputDate))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathsource1)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(identitySchema)
      .load(p + "/*.txt")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    identityDf.registerTempTable("identity")
    
    val detailDf = FileSystem.get(sc.hadoopConfiguration)
    .globStatus(new Path(path + "source/identity/file_date="+inputDate))
    .map(f => f.getPath.toString)
    .map(p => {
      val pattern = ".*file_date=(.*)".r
      val pattern(fileDate) = p
      sqlContext.read.format("com.databricks.spark.csv")
      .option("basePath", pathsource2)
      .option("header", "true")
      .option("delimiter", "|")
      .schema(detailSchema)
      .load(p + "/*.txt")
      .withColumn("file_date", lit(fileDate))
    })
    .reduce((a, b) => a.unionAll(b))
    detailDf.registerTempTable("detail")
    
    println("test")
    
//    sqlContext.read.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("delimiter", "|")
//      .load(path+"source1.txt")
//      .registerTempTable("source_1")
//      
//    sqlContext.read.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("delimiter", "|")
//      .load(path+"source2.txt")
//      .registerTempTable("source_2")
//    
    
    sqlContext.sql("""select A.USERNAME,A.NAME,B.ADDRESS from identity A inner join 
       detail B on A.USERNAME = B.USERNAME
       """).show()
    
    val resultDF = sqlContext.sql("""select A.USERNAME,A.NAME,B.ADDRESS from identity A inner join 
       detail B on A.USERNAME = B.USERNAME
       """)
       
    resultDF.repartition(1).write.format("com.databricks.spark.csv")
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", "|")
    .save(path+"result/output")
  
    val data_mart_file = fs.globStatus(
    new Path(path + "/result/output/part*"))(0).getPath().getName()
    fs.rename(
    new Path(path + "/result/output/"+ data_mart_file),
    new Path(path + "/result/output/test.dat"))

    
    println("[INFO] Process done.")
    

  
  
  }
  
  val identitySchema = new StructType(Array(
    StructField("USERNAME", StringType, true),
    StructField("NAME", StringType, true)))
  
  val detailSchema = new StructType(Array(
    StructField("USERNAME", StringType, true),
    StructField("ADDRESS", StringType, true)))
  
}