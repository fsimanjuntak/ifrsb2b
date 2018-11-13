package com.ibm.id.isat.IPCNRecon

import java.util.Date
import java.text.SimpleDateFormat
import java.text.ParseException
import java.text._
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.commons.lang3.time.DateUtils
import scala.util.Try


object ReconFunc { 
    def main(args: Array[String]): Unit = {
      val x= "20161430"
      println(reconDateValidation(x))
    }
  
    def reconDateValidation(x:String) : Boolean = {   
        try {
            val df = new SimpleDateFormat("yyyyMMdd");
            df.setLenient(false);
            df.parse(x);
            return true;
        } catch {
        case e: ParseException => return false;
      }
}

    
    def reconDateChangeFormat(x:String) : String = {
     var  format1 = new SimpleDateFormat("yyyyMMdd");
     var  oldDate= format1.parse(x);
     val newdate = new SimpleDateFormat("yyyy-MM-dd").format(oldDate); 
     return newdate.toString();
    }
     
}