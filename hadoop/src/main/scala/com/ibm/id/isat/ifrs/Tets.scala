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

object Test {
  def main(args: Array[String]): Unit = {
    println("test")
  }
}