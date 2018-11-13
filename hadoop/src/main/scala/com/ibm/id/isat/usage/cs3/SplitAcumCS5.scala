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
import com.ibm.id.isat.usage.cs5._
import org.apache.hadoop.io.compress.Compressor
import org.apache.hadoop.io.compress.Lz4Codec
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.io.compress.DeflateCodec
//import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.apache.hadoop.io.compress.CompressionCodecFactory;
//import com.hadoop.compression.lzo.LzoCodec

object SplitAcumCS5   {
  
  def main(args: Array[String]): Unit = {
    
    var prcDt = ""
    var jobID = ""
    var configDir = ""
    var inputDir = ""    
    var outputDir = ""
    
    //var compressionTp = new org.apache.hadoop.io.compress.BZip2Codec
    
    try {
        //prcDt = args(0)
        //prcDt = "20160907"
        //jobID = "7000"
        //jobID = args(1)
        //configDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/USAGE.conf"
        //configDir = args(2)
        //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/OFFLOADING _DWH_TO_HDP/sample_tdwsmy_cs5.txt"
        inputDir = args(0)
        outputDir = args(1)
        //compressionTp = args(4)
    }
    catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return 
    }
    
    /*--- Get Population Date Time ---*/
    val dateFormat = new SimpleDateFormat("yyyyMMdd");    //("yyyy/MM/dd HH:mm:ss");    
    //val prcDt = dateFormat.format(Calendar.getInstance.getTime)
    
    //println("Start Usage Split with Process Date and Job ID : " + prcDt + ", " + jobID);    
    
    //val sc = new SparkContext("local", "Split Usage", new SparkConf());
    val sc = new SparkContext(new SparkConf())
    Logger.getRootLogger().setLevel(Level.ERROR)
    val log = Logger.getLogger(SplitUsage.getClass)
        
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    /*--- Get Output Directory Path ---*/
    //val OUTPUT_SPLIT_CS5 = Common.getConfigDirectory(sc, configDir, "USAGE.SPLIT.OUTPUT_SPLIT_CS5")
    //val OUTPUT_SPLIT_CS3 = Common.getConfigDirectory(sc, configDir, "USAGE.SPLIT.OUTPUT_SPLIT_CS3") 
    
    
    /*--- Set Child Directory ---*/
    val childDir = Common.getChildDirectory(prcDt, jobID)
    
    /*--- Write Application ID to HDFS ---*/
    //val app_id = sc.applicationId    
    //Common.cleanDirectory(sc, OUTPUT_SPLIT_LOG_APP_ID +"/" + childDir + ".log") 
    //Common.writeApplicationID(sc, OUTPUT_SPLIT_LOG_APP_ID + "/" + childDir + ".log", app_id)
    
    //val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration); 
    //val output = fs.create(new org.apache.hadoop.fs.Path(OUTPUT_SPLIT_LOG_APP_ID + "/" + childDir + ".log" ));
    //val os = new BufferedOutputStream(output)
    //os.write(sc.applicationId.getBytes("UTF-8"))
    //os.close()
    
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val inputRDD = sc.textFile(inputDir)
    inputRDD.persist()
    
    
    
    val splitRDD = inputRDD.map(line => line.concat("|" + concatDA(Common.getTupleInTuple(line.split("\\|")(58), "^", 0, "~", 0)) + "|" + 
        Common.getTupleInTuple(line.split("\\|")(58), "^", 0, "~", 3) + "|" + 
        concatDA(Common.getTupleInTuple(line.split("\\|")(58), "^", 1, "~", 0)) + "|" + 
        Common.getTupleInTuple(line.split("\\|")(58), "^", 1, "~", 3) + "|" + 
        Common.getTuple(line.split("\\|")(54), ",", 0) + "|" + 
        Common.getTuple(line.split("\\|")(54), ",", 1) + "|" + 
        Common.getTuple(line.split("\\|")(54), ",", 2) + "|" + 
        Common.getTuple(line.split("\\|")(54), ",", 3) + "|" + 
        Common.getTuple(line.split("\\|")(54), ",", 4) 
        
    ))
    
    
    //splitRDD.collect foreach {case (a) => println (a)};
        
    
    Common.cleanDirectory(sc, outputDir)
    splitRDD.saveAsTextFile(outputDir); 
    
    //sc.hadoopConfiguration.set("spark.io.compression.codec", "lz4")
    
    //val compress = new org.apache.hadoop.io.compress.CompressionCodec //. .Lz4Codec
    //val codecFactory = new CompressionCodecFactory(getConf());
    //val x = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec")
    
    /*--- Save CS5 records ---*/
    //Common.cleanDirectory(sc, OUTPUT_SPLIT_CS5 + "/" + childDir)
    //CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir); 
    
    
    //CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir, classOf[LzoCodec]); 
    //CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir, classOf[Lz4Codec]); 
    //CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir, classOf[SnappyCodec])
    //CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
    
    /*try{
      CS5RDD.saveAsTextFile(OUTPUT_SPLIT_CS5 + "/" + childDir);    
    }
    catch {
	    case e: Exception => println("CS5 folder already exists") 
	  } */  
        
    /*--- Save CS3 records ---*/
    //Common.cleanDirectory(sc, OUTPUT_SPLIT_CS3 +"/" + childDir)
    //CS3RDD.saveAsTextFile(OUTPUT_SPLIT_CS3 + "/" + childDir); 
    //CS3RDD.saveAsTextFile(OUTPUT_SPLIT_CS3 + "/" + childDir, classOf[LzoCodec])
    //CS3RDD.saveAsTextFile(OUTPUT_SPLIT_CS3 + "/" + childDir, classOf[Lz4Codec])
    //CS3RDD.saveAsTextFile(OUTPUT_SPLIT_CS3 + "/" + childDir, classOf[SnappyCodec])
    
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
   
  
  def concatDA(daInfo: String) : String = {
    
    var result = ""
    
    if(daInfo != "")    
      result = "DA" + daInfo
    else
      result = daInfo
      
      return result
  }
  
}
  
 

      