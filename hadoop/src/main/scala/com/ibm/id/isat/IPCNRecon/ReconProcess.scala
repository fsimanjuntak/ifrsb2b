package com.ibm.id.isat.IPCNRecon

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.types._
import org.apache.spark
import com.ibm.id.isat.utils._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window


object ReconProcess {
    def main(args: Array[String]): Unit = {
    //val sc = new SparkContext("local", "testing spark ", new SparkConf())
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    
    //PREPARATION STAGES
    var reconDt = ""
    var prcDt = ""
    var jobID = ""
    var inputDir = ""
    var usageDir = ""
    var targetReconDir = "" 
    var targetUsageDir = ""
    var newReconDt = ""
    
    //check parameters validation
     try {
      reconDt = args(0)
      prcDt = args(1)
      jobID = args(2)
      inputDir = args(3)
      usageDir = args(4)
      targetReconDir = args(5)
      targetUsageDir = args(6)
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
       
    //check reconDt validation
    if (ReconFunc.reconDateValidation(reconDt) == true) {
    } else {
      println("reconDt is not valid!")
      return
    }
    
    //create new format for reconDT
    newReconDt = ReconFunc.reconDateChangeFormat(reconDt)
        
    //prepare directories and prepare Dataframes
    val childDir = "/RECON_DT=" + reconDt + "/JOB_ID=" + jobID
    val UsageDF = sqlContext.read.load(usageDir+"/TRANSACTION_DATE="+reconDt+"/JOB_ID="+jobID+"/*parquet*");
    
    val ipcnLogDF = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false") 
    .option("delimiter",";")
    .schema(ReconSchema.IPCNLogSchema)
    .load(inputDir+"/*"+newReconDt+"*") 
    
    UsageDF.registerTempTable("usage")
    ipcnLogDF.registerTempTable("ipcnLog")
    
    //val UsageRDD = sc.textFile(usageDir+"/TRANSACTION_DATE="+reconDt+"/*/*.txt*");
    //val InputRDD = sc.textFile(inputDir+"/*"+newReconDt+"*");
    
    //val UsageDF = sqlContext.read
    //.format("com.databricks.spark.csv")
    //.option("header", "false") 
    //.option("delimiter","|")
    //.schema(ReconSchema.AddonSchema)
    //.load(usageDir+"/TRANSACTION_DATE="+reconDt+"/*/*.txt*") 

    //TRANSFORM STAGES
    val TransformTest = sqlContext.sql("""
    select a.*, b.* from
    (
        select * from ipcnLog
    ) a join 
    (
      select
      * 
      from usage
    ) b 
    on a.SSPTransactionID = b.SSP_TRANSACTION_ID
    """)

    //TransformTest.show()
    
    /*
    //prepare the logs and usage
    val PairLogRDD = InputRDD.filter(line => line.split(";")(8) != "").map(line => (line.split(";")(8),line ));
    val PairUsageRDD = UsageRDD.map(line => (line.split("\\|")(6), line));
    
    //join the logs and usage
    val joinResult = PairLogRDD.leftOuterJoin(PairUsageRDD)
    
    //additional transformation regarding delimiter
    val tmpFinalRDD = joinResult.map{case (k,v) => v}
    val tmpFinalRDD2 = tmpFinalRDD.map{case(k,v) => k.toString().replaceAll(";","|").concat("|").concat(v.getOrElse("||||||||||||||||||||||||||||||"))};
    
    //prepare records which have no ssp transaction id
    val EmptyRDD = InputRDD.filter(line => line.split(";")(8) == "").map(line => line.toString().replaceAll(";", "|").concat("||||||||||||||||||||||||||||||||"))
    
    //union between reconciliation result with records which have no ssp transaction id
    val FinalRDD = tmpFinalRDD2.union(EmptyRDD);
    
    */
    
    //APPLY STAGES
    Common.cleanDirectory(sc, targetReconDir + "" + childDir );
    Common.cleanDirectory(sc, targetUsageDir + "" + childDir );

    
    TransformTest.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(targetReconDir + "" + childDir)
        
    UsageDF.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(targetUsageDir + "" + childDir)
     
    //FinalRDD.saveAsTextFile(targetReconDir + "" + childDir );
    //UsageRDD.saveAsTextFile(targetUsageDir + "" + childDir );
        
    sc.stop()
    }
    
}