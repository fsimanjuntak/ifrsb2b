package com.ibm.id.isat.nonUsage.air

/*
 * Created by IBM
 * 27 May 2016
 * 
 * This script is used for selection area between adjustment and refill.
 */

import org.apache.spark.{SparkContext, SparkConf};
import java.util.Calendar;
import java.text.SimpleDateFormat;
import com.ibm.id.isat.utils.Common;


object AirSelection {
 
  def main(args: Array[String]) {

        if (args.length != 4) {
          println("Usage: [PRC_DT] [JOBID] [CONFIG] [INPUTDIR]")
          sys.exit(1);
        }
        
        val setSparkConf = new SparkConf();
        setSparkConf.set("spark.hadoop.validateOutputSpecs", "false");
        setSparkConf.set("mapred.output.compress", "false");
        
      
        val sc = new SparkContext(setSparkConf)
        sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
        
        val prcDt = args(0)
        val jobID = args(1)
        val configDir = args(2)
        val inputFile= args(3)
        
        
        val existDir= Common.isDirectoryExists(sc, inputFile);
        var textFileRDD = sc.textFile(inputFile).cache();  
        if ( textFileRDD.count() == 0 ){
           sys.exit(1);
        }

//      val configDir = "/user/apps/nonusage/air/reference/AIR.conf"
        val OUTPUT_FILE_ADJ = Common.getConfigDirectory(sc, configDir, "AIR.SPLIT.OUTPUT_SPLIT_ADJ").concat(prcDt).concat("_").concat(jobID)
        val OUTPUT_FILE_REF = Common.getConfigDirectory(sc, configDir, "AIR.SPLIT.OUTPUT_SPLIT_REFILL").concat(prcDt).concat("_").concat(jobID)
        val OUTPUT_FILE_REJ = Common.getConfigDirectory(sc, configDir, "AIR.SPLIT.OUTPUT_SPLIT_REJ").concat(prcDt).concat("_").concat(jobID)
        
        
        // This script is used for produce refill : delete "/t" , first and end column.
        val textAdjusment =  textFileRDD.filter { line => line.contains("ADJUSTMENTRECORDV2") }.map{row => getValidRecordArea(row,1,2,3)}
        val textRefill =  textFileRDD.filter{line => line.contains("REFILLRECORDV2")}.map{row => getValidRecordArea(row,1,2,3)} 
        val textRecordError =  textFileRDD.filter { line => line.contains("ERRORRECORDV2") }.map{row => getValidRecordArea(row,1,2,3)}
        
        // This script is used for creating file that based area file.
        Common.cleanDirectory(sc, OUTPUT_FILE_ADJ );
        textAdjusment.saveAsTextFile(OUTPUT_FILE_ADJ);
        
        Common.cleanDirectory(sc, OUTPUT_FILE_REF );
        textRefill.saveAsTextFile(OUTPUT_FILE_REF);

        Common.cleanDirectory(sc, OUTPUT_FILE_REJ );
        textRecordError.saveAsTextFile(OUTPUT_FILE_REJ);
        
        sc.stop();  
  }

      // replace "\tab" into "|"
      def getValidRecord(content: String): String = {
        return content.replaceAll("\t", "|");
      }
    
      
      // this script is used because there is anomaly delimiter in tupple 32 ( adjustment ) and tupple 56 ( refill )
      def getValidRecordArea(content: String, keyDelim01: Int, keyDelim02: Int, keyDelim03: Int): String = {
        try {
          return content.split("\t")(keyDelim01).concat(content.split("\t")(keyDelim02)).concat("|").concat(content.split("\t")(keyDelim03))
        } catch {
          case t: Throwable =>
            t.printStackTrace()
            return ""
        }
    
      }
    
      def goPrcDate(): String = {
        val today = Calendar.getInstance().getTime
        val curTimeFormat = new SimpleDateFormat("YYYYMMdd")
        val prcDt = curTimeFormat.format(today).toString()
        return prcDt;
      }
  
}