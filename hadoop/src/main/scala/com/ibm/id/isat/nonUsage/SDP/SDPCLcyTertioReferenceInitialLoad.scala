package com.ibm.id.isat.nonUsage.SDP

import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.SparkConf;
import org.apache.spark.sql//Context.implicits.*;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import com.ibm.id.isat.utils.ReferenceSchema
import com.ibm.id.isat.utils.Common
import java.util.Calendar
import java.text.SimpleDateFormat
import com.ibm.id.isat.utils.ReferenceDF

/*
 * 
 * This below script is only used for initial running. 
 * 
 */

object SDPCLcyTertioReferenceInitialLoad {
      def main(args: Array[String]) {
        
//        val sc = new SparkContext(new SparkConf())
          val sc = new SparkContext("local", "testing spark ", new SparkConf()); 
          sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")

          val sqlContext = new org.apache.spark.sql.SQLContext(sc)
          import sqlContext.implicits._
          import org.apache.log4j._
          val rootLogger = Logger.getRootLogger()
          rootLogger.setLevel(Level.ERROR)
          val log = LogManager.getRootLogger
          
           /*if (args.length != 5) {
            println("Usage: [PRC_DT] [JOBID] [CONFIG] [INPUT_INITIAL] [INPUT_FILE]")
            sys.exit(1);
          }
          
          // Set all the variables
          val PRC_DT=args(0)
          val JOB_ID=args(1)
          val CONF_DIR=args(2)
          val INPUT_INITIAL=args(3)
          val INPUT_FILE=args(4)
          val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
          val OUTPUT_TERTIO_REFERENCE = Common.getConfigDirectory(sc, CONF_DIR, "PPDEL.PPDEL_TRANSFORM_FILE.TERTIO_OUTPUT_REFERENCE_DIR").concat(CHILD_DIR);// gotten from result in split process.
          */
         
				  println("Start Processing SDP Lifecyle Tertio For Initial Running"); 
        	System.setProperty("hadoop.home.dir", "C:\\winutil\\")
          val PRC_DT="20161215"
          val JOB_ID="01"
          val CONF_DIR="C://testing"
          val INPUT_DIR="C:/works/Project/HADOOP/Tertio/reference/input/"
          val INPUT_FILE="C:/works/Project/HADOOP/Tertio/reference/input/"
          val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
          val OUTPUT_TERTIO_REFERENCE = "C:/works/Project/HADOOP/Tertio/reference/output/"
          
          /* val pattern = "delta_sim_" + this.goPrcDate(1) +"_000000";

          val FossOLDDF = ReferenceDF.getFossDF(sqlContext,pattern)
          FossOLDDF.registerTempTable("FossOLDDF")
          FossOLDDF.persist()
          FossOLDDF.count()
          FossOLDDF.show();*/
          
          /*****************************************************************************************************
          * 
          * STEP 01.Load Initial Data
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 01. LOAD INITIAL===================");
          
          val sdpLcyRefInitialRDD = sc.textFile(INPUT_DIR.concat("*"));
          sdpLcyRefInitialRDD.persist()
  
          val sdpLcyRefTertioInputInitialRDD = sdpLcyRefInitialRDD
          .map(line => line.split("\\|"))
          .map { p => Row( 
                 p(0), // ICCID
                 p(1), // IMSI
                 p(2), // MSISDN
                 p(3), // CARD_TYPE
                 p(4), // PROGRAM_CODE
                 p(5), // BRAND_CODE
                 p(6), // BRANCH_CODE
                 p(7), // ALLOC_NO
                 p(8), // DEALER_ID
                 p(9), // DISTRIBUTION_DATE
                 p(10) // EXPIRY_DATE
                 )};
                
          val sdpLcyRefInputInitialDF = sqlContext.createDataFrame(sdpLcyRefTertioInputInitialRDD, ReferenceSchema.fossSchema);
          sdpLcyRefInputInitialDF.registerTempTable("sdpLcyRefInputInitial");
          sqlContext.cacheTable("sdpLcyRefInputInitial");
          sdpLcyRefInputInitialDF.show()
          
          val sdpLcyRefDailyRDD = sc.textFile(INPUT_FILE);
          sdpLcyRefDailyRDD.persist()
          
           val sdpLcyRefTertioInputDailyRDD = sdpLcyRefDailyRDD
          .map(line => line.split("\\|"))
          .map { p => Row( 
                 p(0), // ICCID
                 p(1), // IMSI
                 p(2), // MSISDN
                 p(3), // CARD_TYPE
                 p(4), // PROGRAM_CODE
                 p(5), // BRAND_CODE
                 p(6), // BRANCH_CODE
                 p(7), // ALLOC_NO
                 p(8), // DEALER_ID
                 p(9), // DISTRIBUTION_DATE
                 p(10) // EXPIRY_DATE
                 )};
                
          val sdpLcyRefInputDailyDF = sqlContext.createDataFrame(sdpLcyRefTertioInputDailyRDD, ReferenceSchema.fossSchema);
          sdpLcyRefInputDailyDF.registerTempTable("sdpLcyRefInputDaily");
          sqlContext.cacheTable("sdpLcyRefInputDaily");
          sdpLcyRefInputDailyDF.show()
          
          
          /*****************************************************************************************************
          * 
          * STEP 02.Join Initial Data with Daily Data
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 02. JOIN WITH REFERENCE===================");
           val trfSDPLcyRefDF = sqlContext.sql(
              """
              SELECT
                 stg.ICCID,
                 stg.IMSI,
                 stg.MSISDN,
                 stg.CARD_TYPE,
                 stg.PROGRAM_CODE,
                 stg.BRAND_CODE,
                 stg.BRANCH_CODE,
                 stg.ALLOC_NO,
                 stg.DEALER_ID,
                 stg.DISTRIBUTION_DATE,
                 stg.EXPIRY_DATE
              FROM sdpLcyRefInputInitial stg
              left join sdpLcyRefInputDaily foss
                on stg.ICCID = foss.ICCID AND stg.IMSI = foss.IMSI 
              where (stg.MSISDN is not null OR stg.MSISDN <> '') AND (foss.MSISDN is null OR foss.MSISDN = '')
              UNION ALL
              SELECT
                 daily.ICCID,
                 daily.IMSI,
                 daily.MSISDN,
                 daily.CARD_TYPE,
                 daily.PROGRAM_CODE,
                 daily.BRAND_CODE,
                 daily.BRANCH_CODE,
                 daily.ALLOC_NO,
                 daily.DEALER_ID,
                 daily.DISTRIBUTION_DATE,
                 daily.EXPIRY_DATE
               FROM sdpLcyRefInputDaily daily
              """)
              
              
          Common.cleanDirectory(sc, OUTPUT_TERTIO_REFERENCE)
          trfSDPLcyRefDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_TERTIO_REFERENCE);
          trfSDPLcyRefDF.show()
          
      }
}