package com.ibm.id.isat.usage.split

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import com.ibm.id.isat.utils._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.conf.Configuration
import java.io._

object SplitUsage   {
  
  def main(args: Array[String]): Unit = {
    
    var prcDt = ""
    var jobID = ""
    var configDir = ""
    var inputDir = ""
     
    try {
        prcDt = args(0)
        jobID = args(1)
        configDir = args(2)
        inputDir = args(3)
    }
    catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return 
    }
    
    /*--- Get Population Date Time ---*/
    val dateFormat = new SimpleDateFormat("yyyyMMdd");    //("yyyy/MM/dd HH:mm:ss");    
    
    println("Start Usage Split with Process Date and Job ID : " + prcDt + ", " + jobID);    
    
    //val sc = new SparkContext("local", "Split Usage", new SparkConf());
    val sc = new SparkContext(new SparkConf())
    Logger.getRootLogger().setLevel(Level.ERROR)
    val log = Logger.getLogger(SplitUsage.getClass)
        
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    /*--- Get Output Directory Path ---*/
    val OUTPUT_SPLIT_CS5 = Common.getConfigDirectory(sc, configDir, "USAGE.SPLIT.OUTPUT_SPLIT_CS5")
    val OUTPUT_SPLIT_CS3 = Common.getConfigDirectory(sc, configDir, "USAGE.SPLIT.OUTPUT_SPLIT_CS3") 
    
    /*--- Set Child Directory ---*/
    val childDir = Common.getChildDirectory(prcDt, jobID)
    
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

     
    val inputRDD = sc.textFile(inputDir)
    inputRDD.persist()
    
    val CS5RDD = inputRDD.filter(line => line.split("\t")(0) == "CS5")
    val CS3RDD = inputRDD.filter(line => line.split("\t")(0) == "CS3")
    
    inputRDD.unpersist()
    
    
    /*--- Save CS5 records ---*/
    Common.cleanDirectory(sc, OUTPUT_SPLIT_CS5 + "/" + childDir)
    CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir); 
    
        
    /*--- Save CS3 records ---*/
    Common.cleanDirectory(sc, OUTPUT_SPLIT_CS3 +"/" + childDir)
    CS3RDD.repartition(3).saveAsTextFile(OUTPUT_SPLIT_CS3 + "/" + childDir);  
    
        
    sc.stop(); 
  }
}