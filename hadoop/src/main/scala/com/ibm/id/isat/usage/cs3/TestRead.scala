package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import scala.io.Source
import scala.util.control.Breaks._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner
import com.ibm.id.isat.utils._
import java.util.Calendar
import java.io._
import java.io.OutputStream
import java.text.SimpleDateFormat
import com.ibm.id.isat.usage.cs3._
import com.ibm.id.isat.utils._
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object TestRead {
  
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext("local", "CS3 Transform", new SparkConf());
    //val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
   // FileSystem fileSystem = FileSystem.get(sc);
    
    ///user/apps/CS3/reference/ref_service_class.csv
    val intcct13List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_13.txt").getLines.toList
   
    
    
  }
}