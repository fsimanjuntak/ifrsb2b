package com.ibm.id.isat.ifrs

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.DateType

object PrepPostpaid {
  def main(args: Array[String]): Unit = {
    
    var prcDt = ""
    var config = ""
    try {
      config = args(0)
      prcDt = args(1)      
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    
    val sqlContext=new SQLContext(sc)
    
    
    val jobId = prcDt
    val input = Common.getConfigDirectory(sc, config, "IFRS.INPUT.RBM") + "/JOB_ID=" + prcDt 
    val inputBillDetailIfrs = input + "/bill_detail_ifrs"
    val inputBillSummary = input + "/bill_summary"
    val inputBillAccountProduct = input + "/bill_account_product"
    
    val billAccountProductRDD = sc.textFile(inputBillAccountProduct);
    val billAccountProductRDDFilter=billAccountProductRDD.filter ( x => x.count (_ == '~') == 97).map(line => line.split("~",-1))
          .map { p => Row(p(0),p(1),p(19),p(38),p(45),p(66),p(67),p(68),p(83),p(92),p(94)) }
//		print(dukCapilRDDFilter.foreach(println))
    val billAccountProductDF=sqlContext.createDataFrame(billAccountProductRDDFilter,billAccountProductSchema)
//    billAccountProductDF.registerTempTable("DUKCAPIL_UNFILTER")
    billAccountProductDF.write
      .mode("overwrite")
      .save(inputBillAccountProduct+"_parquet")
    
  }
  val billAccountProductSchema=new StructType(Array(
    StructField("customer_ref", StringType, true),
    StructField("account_num", StringType, true),
    StructField("account_name", StringType, true),
    StructField("product_seq", StringType, true),
    StructField("product_family_id", StringType, true),
    StructField("region_code", StringType, true),
    StructField("branch_code", StringType, true),
    StructField("account_type", StringType, true),
    StructField("customer_type_id", StringType, true),
    StructField("customer_type_name", StringType, true),
    StructField("customer_name", StringType, true)))
}