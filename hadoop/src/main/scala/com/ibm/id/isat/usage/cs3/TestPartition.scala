package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner
import com.ibm.id.isat.utils._
import java.util.Calendar
import java.text.SimpleDateFormat

object TestPartition {
  def main(args: Array[String]): Unit = {
    
    val jobID = args(0)
    //val jobID = "1237"
    //val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3.txt"
    //val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/rej ref/*"
    //val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3_SIT.txt"
    //val inputDir = "/user/apps/CS3/input/sample_conv_output_CS3_SIT.txt"
    //val inputDir = "/user/apps/CS3/input/sample_conv_output_CS3.txt"
    val inputDir = args(1)
    val refDir = args(2)
    //val refDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/sdp_offer.txt"
    
    println("Start Job ID : ");    
    
    //val sc = new SparkContext("local", "CS3 Transform", new SparkConf());
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    /*--- Get Population Date Time ---*/
    val dateFormat = new SimpleDateFormat("yyyyMMdd");    //("yyyy/MM/dd HH:mm:ss");    
    val prcDt = dateFormat.format(Calendar.getInstance.getTime)
    
    val refRDD = sc.textFile(refDir).persist()
    val ref2 = refRDD.map(line => line.split("\\|")).map(col => (col(0), (col(1),col(2),col(3)))).partitionBy(new HashPartitioner(10))
    
    val inputRDD = sc.textFile(inputDir)    
    val splitRDD = inputRDD.map(line => line.split("\t")).map(col => col(1).concat("|" + col(2) + "|" + col(3)))
    
    val formatRDD = splitRDD.filter(line => ! line.split("\\|")(6).isEmpty() && ! line.split("\\|")(21).isEmpty() && ! line.split("\\|")(9).isEmpty()).
                    map(line => line.split("\\|")).map(col => (Common.normalizeMSISDN(col(6)), col.mkString("|")))//.partitionBy(new HashPartitioner(10))
    
    //val resultRDD = formatRDD.leftOuterJoin(ref2)
    val resultRDD2 = ref2.rightOuterJoin(formatRDD)
    //val formatRDD = splitRDD.map(line => line.split("\\|")).map(col => (Common.normalizeMSISDN(col(6)), col.mkString("|"))).partitionBy(new HashPartitioner(10))
    
    //formatRDD.take(5) foreach {case (a) => println (a)}
    //resultRDD.collect foreach {case (a) => println (a)};
        
    //resultRDD.saveAsTextFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Output/"+prcDt+jobID)
    resultRDD2.saveAsTextFile("/user/apps/CS3/test_join/"+prcDt+jobID)
    sc.stop(); 
  }
}