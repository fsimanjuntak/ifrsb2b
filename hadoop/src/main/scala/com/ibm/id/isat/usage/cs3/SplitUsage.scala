package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import java.util.Calendar
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
        //configDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/USAGE_SPLIT.conf"
        configDir = args(2)
        //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/conversion"
        inputDir = args(3)
    }
    catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return 
    }
    
    /*--- Get Population Date Time ---*/
    val dateFormat = new SimpleDateFormat("yyyyMMdd");    //("yyyy/MM/dd HH:mm:ss");    
    //val prcDt = dateFormat.format(Calendar.getInstance.getTime)
    
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
    val OUTPUT_SPLIT_LOG_APP_ID = Common.getConfigDirectory(sc, configDir, "USAGE.SPLIT.OUTPUT_SPLIT_LOG_APP_ID")
    
    
    /*--- Set Child Directory ---*/
    val childDir = Common.getChildDirectory(prcDt, jobID)
    
    /*--- Write Application ID to HDFS ---*/
    val app_id = sc.applicationId    
    Common.cleanDirectory(sc, OUTPUT_SPLIT_LOG_APP_ID +"/" + childDir + ".log") 
    Common.writeApplicationID(sc, OUTPUT_SPLIT_LOG_APP_ID + "/" + childDir + ".log", app_id)
    
    //val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration); 
    //val output = fs.create(new org.apache.hadoop.fs.Path(OUTPUT_SPLIT_LOG_APP_ID + "/" + childDir + ".log" ));
    //val os = new BufferedOutputStream(output)
    //os.write(sc.applicationId.getBytes("UTF-8"))
    //os.close()

    val inputRDD = sc.textFile(inputDir)
    inputRDD.persist()
    
    val CS5RDD = inputRDD.filter(line => line.split("\t")(0) == "CS5")
    val CS3RDD = inputRDD.filter(line => line.split("\t")(0) == "CS3")
    
    inputRDD.unpersist()
    
    
    /*--- Save CS5 records ---*/
    Common.cleanDirectory(sc, OUTPUT_SPLIT_CS5 + "/" + childDir)
    CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir); 
    
    /*try{
      CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir);    
    }
    catch {
	    case e: Exception => println("CS5 folder already exists") 
	  } */  
        
    /*--- Save CS3 records ---*/
    Common.cleanDirectory(sc, OUTPUT_SPLIT_CS3 +"/" + childDir)
    CS3RDD.saveAsTextFile(OUTPUT_SPLIT_CS3 + "/" + childDir);  
    
    /*try{
      CS3RDD.saveAsTextFile(OUTPUT_SPLIT_CS3 + "/" + childDir);   
    }
    catch {
	    case e: Exception => println("CS3 folder already exists") 
	  } */  
    
    
    
    //CS5RDD.take(5) foreach {case (a) => println (a)}
    //CS3RDD.take(5) foreach {case (a) => println (a)}
    
    //formatRDD.collect foreach {case (a) => println (a)};
        
    sc.stop(); 
  }
}