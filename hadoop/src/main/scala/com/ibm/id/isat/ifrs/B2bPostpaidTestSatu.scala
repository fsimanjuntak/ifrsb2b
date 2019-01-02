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
import org.apache.spark.sql.types.DataTypes
import scala.collection.mutable.ListBuffer

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
    
    var identityDf = FileSystem.get(sc.hadoopConfiguration)
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
//    identityDf.registerTempTable("identity")
    
    
//    val inputFormat = new java.text.SimpleDateFormat("yyyyMMdd")
//    val convertedDate = inputFormat.parse(inputDate)
    
    
    var lstTransactionDate = new ListBuffer[String]()
    
    val totaltransdate= -60
    val format="yyyyMMdd"
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(format).parse(inputDate))
    dateAux.add(Calendar.DATE, totaltransdate)
    lstTransactionDate += new SimpleDateFormat(format).format(dateAux.getTime()).toString();
    
    for( days <- 1 to 59 ){
//      val newdate = Calendar.getInstance()
//      newdate.setTime(dateAux.getTime())
//      newdate.add(Calendar.DATE, days)
      dateAux.add(Calendar.DATE, 1)
      val dt_order = new SimpleDateFormat(format).format(dateAux.getTime()).toString()
      lstTransactionDate += dt_order;   
   }
    
   
    
//    val lstTransaction = List("20180803","20180802")
//    
    for (tr_date <- lstTransactionDate){
     println(pathsource1+"/file_date="+tr_date + "/*.txt")
     try {
       
       
//       if (fs.exists(new Path(pathsource1+"/file_date="+tr_date + "/*.txt"))){
         val newDF =  sqlContext.read.format("com.databricks.spark.csv")
                      .option("basePath", pathsource1)
                      .option("header", "true")
                      .option("delimiter", "|")
                      .schema(identitySchema)
                      .load(path + "source/identity/file_date="+tr_date + "/*.txt")
                      .withColumn("file_date", lit(tr_date))
//         identityDf = identityDf.unionAll(newDF);
         
         if (!newDF.rdd.isEmpty){
             identityDf = identityDf.unionAll(newDF);
          }
         else{
           println("Empty")
         }
         
//       }
//       else {
//         println("File does not exist")
//       }
//       
       
       
      
//      newDF.saveAsTable("identity", "append");
//      newDF.write.insertInto("identity");
//      mode("append").saveAsTable("identity");
      
//      if (newDF.take(1).length>0){
//        identityDf = identityDf.unionAll(newDF);
//      }
  
     }catch 
     {
       case e: Exception => println("File does not exist"+e.getMessage)
     }
         
      
  
//      identityDf = identityDf.unionAll(newDF);
    }
    
//    identityDf.show()
    identityDf.registerTempTable("identity")
    
    val resultDF = sqlContext.sql("""select * from identity A 
       """).show()
    
   
//    val identityDf = FileSystem.get(sc.hadoopConfiguration)
//    .listStatus(new Path(path + "source/identity/"))
//    .map(f => f.getPath.toString)
//    .map(p => {
//        for (tr_date <- lstTransactionDate){
//          val newpath = p+"/file_date="+tr_date;
//          print()
//          sqlContext.read.format("com.databricks.spark.csv")
//          .option("basePath", pathsource1)
//          .option("header", "true")
//          .option("delimiter", "|")
//          .schema(identitySchema)
//          .load(p + "/*.txt")
//          .withColumn("file_date", lit(tr_date))
//        }
//    })
//    .reduce((a, b) => a.unionAll(b))
//    identityDf.registerTempTable("identity")
      
//val identityDf = FileSystem.get(sc.hadoopConfiguration ).globStatus(new Path(path + "source/identity/"))

     
//identityDf.foreach( filename => {
//  val folder_name = filename.getPath.toString()
//  val splitted_folder_name = folder_name.split("/")
//  val folder_prcdate = splitted_folder_name(splitted_folder_name.length-1).split("=")
//  val prc_date = folder_prcdate(1)
//  
//  if (lstTransactionDate contains prc_date){
//      doSomething(folder_name,prc_date)
//  }
//});


//identityDf.registerTempTable("identity")
    
    
//        val resultDF = sqlContext.sql("""select * from identity A 
//       """).show()
       
    
//   val strDate="20180801"
   
    
//    val detailDf = FileSystem.get(sc.hadoopConfiguration)
//    .globStatus(new Path(path + "source/identity/file_date="+inputDate))
//    .map(f => f.getPath.toString)
//    .map(p => {
//      val pattern = ".*file_date=(.*)".r
//      val pattern(fileDate) = p
//      sqlContext.read.format("com.databricks.spark.csv")
//      .option("basePath", pathsource2)
//      .option("header", "true")
//      .option("delimiter", "|")
//      .option("dateFormat", "dd-MM-yyyy HH:mm:ss")
//      .schema(detailSchema)
//      .load(p + "/*.txt")
//      .withColumn("file_date", lit(fileDate))
//    })
//    .reduce((a, b) => a.unionAll(b))
//    .filter(("ORDER_COMPLETION_DATE >= add_months(from_unixtime(UNIX_TIMESTAMP('"+strDate+"', 'yyyyMMdd')),-3)"))
//    detailDf.registerTempTable("detail")
//    
//     sqlContext.sql("""select * from detail
//       """).show()

    
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
    
//    sqlContext.sql("""select A.USERNAME,A.NAME,B.ADDRESS from identity A inner join 
//       detail B on A.USERNAME = B.USERNAME
//       """).show()
    
//    val resultDF = sqlContext.sql("""select A.USERNAME,A.NAME,B.ADDRESS from identity A inner join 
//       detail B on A.USERNAME = B.USERNAME
//       """)
//       
//    resultDF.repartition(1).write.format("com.databricks.spark.csv")
//    .mode("overwrite")
//    .option("header", "true")
//    .option("delimiter", "|")
//    .save(path+"result/output")
//  
//    val data_mart_file = fs.globStatus(
//    new Path(path + "/result/output/part*"))(0).getPath().getName()
//    fs.rename(
//    new Path(path + "/result/output/"+ data_mart_file),
//    new Path(path + "/result/output/test.dat"))

    
    println("[INFO] Process done.")
   
  }
  
  val identitySchema = new StructType(Array(
    StructField("USERNAME", StringType, true),
    StructField("NAME", StringType, true)))
  
  val detailSchema = new StructType(Array(
    StructField("USERNAME", StringType, true),
    StructField("ADDRESS", StringType, true),
    StructField("ORDER_COMPLETION_DATE", DataTypes.DateType, true)))
  
}