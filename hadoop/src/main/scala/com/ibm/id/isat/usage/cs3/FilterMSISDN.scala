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
import org.apache.hadoop.io.compress.GzipCodec
//import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.apache.hadoop.io.compress.CompressionCodecFactory;
//import com.hadoop.compression.lzo.LzoCodec

object FilterMSISDN   {
  
  def main(args: Array[String]): Unit = {
    
    var inputDir = ""
    var outputDir = ""
    //var compressionTp = new org.apache.hadoop.io.compress.BZip2Codec
    
    try {
        //configDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/USAGE.conf"
        inputDir = args(0)
        //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/conversion"
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
      
    
    //val sc = new SparkContext("local", "Split Usage", new SparkConf());
    val sc = new SparkContext(new SparkConf())
    Logger.getRootLogger().setLevel(Level.ERROR)
    val log = Logger.getLogger(SplitUsage.getClass)
        
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val inputRDD = sc.textFile(inputDir)
    inputRDD.persist()
    
    
    //val smsRDD = inputRDD.filter(line => line.contains("628567181391") && line.contains("SCAP_V.2"))
    
    //val smsRDD = inputRDD.filter(line => (line.contains("81532647782") || line.contains("8567181391")) && line.contains("SCAP_V.2"))
    
    //val gprsRDD = inputRDD.filter(line => (line.contains("85649694036") || line.contains("81584218577") || line.contains("8563029131") || line.contains("8159180223") || line.contains("85792046629")) && line.contains("3gpp"))
    
    //val gprsRDD = inputRDD.filter(line => (line.contains("85770000600") || line.contains("85645479710") || line.contains("85775757720") || line.contains("85696465698") || line.contains("85814502368")) && line.contains("CAP"))
    
    //val googlePlayRDD = inputRDD.filter(line =>  line.contains("3939303931303031303134303031"))
    //val googlePlayRDD = inputRDD.filter(line =>  line.contains("8561873256") && line.contains("20160908124249"))
    
    //val googlePlayRDD = inputRDD.filter(line => line.contains("85821184410") || line.contains("85733635311") || line.contains("85733750999") )
    
    //val googlePlayRDD = inputRDD.filter(line => line.contains("99231074014107") )
    
    //val googlePlayRDD = inputRDD.filter(line => line.contains("SCAP_V.2") && line.contains("DWSCallClassCategory*237") )
    
    val googlePlayRDD = inputRDD.filter(line => line.contains("20160908") || line.contains("20160909") || line.contains("20160910") || line.contains("20160911") )
    
    
    
    
    //sc.hadoopConfiguration.set("spark.io.compression.codec", "lz4")
    
    //val compress = new org.apache.hadoop.io.compress.CompressionCodec //. .Lz4Codec
    //val codecFactory = new CompressionCodecFactory(getConf());
    //val x = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec")
    
    /*--- Save CS5 records ---*/
    Common.cleanDirectory(sc, outputDir)
    googlePlayRDD.saveAsTextFile(outputDir, classOf[GzipCodec]); 
    
   
        
    sc.stop(); 
  }
}