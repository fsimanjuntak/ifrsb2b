package com.ibm.id.isat.nonUsage.SDP

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
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

object SDPLcyTertioTransformV2 {
  
    def main(args: Array[String]) {
     
//      val sc = new SparkContext(new SparkConf())
        val sc = new SparkContext("local", "testing spark ", new SparkConf()); 
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
        
        import sqlContext.implicits._
        import org.apache.log4j._
        Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.INFO)
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
     
        
       /* if (args.length != 4) {
          println("Usage: [PRC_DT] [JOBID] [CONFIG] [INPUTDIR] ")
          sys.exit(1);
        }
        
        // Set all the variables
        val PRC_DT=args(0)
        val JOB_ID=args(1)
        val CONF_DIR=args(2)
        val INPUT_DIR=args(3)
        val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
        val OUTPUT_SDPLCYTERTIO = Common.getConfigDirectory(sc, CONF_DIR, "SDP.PPDEL_TRANSFORM_FILE.TERTIO_TRANSFORM_DIR").concat(CHILD_DIR);// gotten from result in split process.
        */
        
        println("Start Processing SDP Lifecycle"); 
        System.setProperty("hadoop.home.dir", "C:\\winutil\\")
        val PRC_DT="20161202"
        val JOB_ID="03"
        val CONF_DIR="C://testing"
        val INPUT_DIR="C:\\works\\Project\\HADOOP\\Tertio\\tertio_SIT\\data source reference dan SDP data\\SDP_data_sample_v2.txt"
        val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
        val OUTPUT_SDPLCYTERTIO = "C:/works/Project/HADOOP/Tertio/output/".concat(CHILD_DIR)

        
        /*****************************************************************************************************
        * 
        * STEP 01. Load SDP LifeCycle Data
        * 
        *****************************************************************************************************/
        log.warn("===============STEP 01. Load SDP LifeCycle Data===================");
        val sdpLcySrcRDD = sc.textFile(INPUT_DIR.concat("*"));
        val sdpLcyTertioInputRDD = sdpLcySrcRDD.filter (
            x => x.count (_ == '|') == 70 
            && x.split("\\|")(17) == "10001111" && x.split("\\|")(18) == "10001111"
            && x.split("\\|")(2).equalsIgnoreCase("SDP")
            )
        .map(line => line.split("\\|"))
        .map { p => Row( 
               if (p(5) != "")
                  "62".concat(p(5))
               else "",//MSISDN
               if (p(4) != "" && p(4).charAt(0).==('8'))
                  "62".concat(p(5))
               else if (p(4) != "" && p(4).charAt(0).!=('8'))
                  (p(5))
               else "", //MASTER_PHONE
               "", //PUK1,
               "" , //PUK2,
               "", //SUBSCRIBERID,
               "" , //ACCOUNTID
               p(19), //ACTIVATION_DATE
               "", //DELETE_DATE
               "", //DELETE_BY
               p(6), //AREA_CODES_SERVICE_CLASS_ID
               "", //SERVICE_CLASS_NAME
               p(7), //LANGUAGE_DESC,
               "1", //ORGANIZATIONID,
               "Indosat", //ORGANIZATIONNAME,
               p(12), //MOCExpired,
               p(14) //MTCExpired
               )};
              
          val sdpLcyTertioInputDF = sqlContext.createDataFrame(sdpLcyTertioInputRDD, model.SDPLcyTertio.SDPLcyTertioV2Schema);
          sdpLcyTertioInputDF.registerTempTable("STGLcyTertio");
          sdpLcyTertioInputDF.persist();
          sdpLcyTertioInputDF.show();

          
         /*****************************************************************************************************
          * 
          * STEP 02. This Process is used for removing duplicate data from SPLIT Process.
          * 
          *****************************************************************************************************/
          log.warn("=============== STEP 02. This Process is used for removing duplicate data from SPLIT Process. ===================");
          val sdpLcyFilterDateDF = sqlContext.sql(
            """
            Select
               tertio.MSISDN,
               tertio.MASTER_PHONE,
               tertio.PUK1,   
               tertio.PUK2,   
               tertio.SUBSCRIBERID,
               tertio.ACCOUNTID,
               tertio.ACTIVATION_DATE,
               tertio.DELETE_DATE,
               tertio.DELETE_BY,
               tertio.AREA_CODES_SERVICE_CLASS_ID,
               tertio.SERVICE_CLASS_NAME,
               tertio.LANGUAGE_DESC, 
               tertio.ORGANIZATIONID,
               tertio.ORGANIZATIONNAME,
               tertio.MOCExpired,
               tertio.MTCExpired        
            FROM STGLcyTertio tertio
            GROUP BY 
               tertio.MSISDN,
               tertio.MASTER_PHONE,
               tertio.PUK1,   
               tertio.PUK2,   
               tertio.SUBSCRIBERID,
               tertio.ACCOUNTID,
               tertio.ACTIVATION_DATE,
               tertio.DELETE_DATE,
               tertio.DELETE_BY,
               tertio.AREA_CODES_SERVICE_CLASS_ID,
               tertio.SERVICE_CLASS_NAME,
               tertio.LANGUAGE_DESC, 
               tertio.ORGANIZATIONID,
               tertio.ORGANIZATIONNAME,
               tertio.MOCExpired,
               tertio.MTCExpired
            """)
          sdpLcyFilterDateDF.registerTempTable("trfSDPLcyGroup");
          sdpLcyFilterDateDF.persist();
          sdpLcyFilterDateDF.show();
       
         Common.cleanDirectory(sc, OUTPUT_SDPLCYTERTIO)
         sdpLcyFilterDateDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_SDPLCYTERTIO);
          
    }
}