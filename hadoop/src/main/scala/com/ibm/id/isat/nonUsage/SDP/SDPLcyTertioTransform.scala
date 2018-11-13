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


object SDPLcyTertioTransform {
   
    def main(args: Array[String]) {
      
//      val sc = new SparkContext(new SparkConf())
        val sc = new SparkContext("local", "testing spark ", new SparkConf()); 
        sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
        
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._   
      
        import org.apache.log4j._
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.OFF)
        val log = LogManager.getRootLogger
        
        
    	/*	if (args.length != 5) {
          println("Usage: [PRC_DT] [JOBID] [CONFIG] [INPUTDIR]  [FOSSDIR]")
          sys.exit(1);
        }
        
        // Set all the variables
        val PRC_DT=args(0)
        val JOB_ID=args(1)
        val CONF_DIR=args(2)
        val INPUT_DIR=args(3)
        val FOSS_DIR=args(4)
        val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
        val OUTPUT_SDPLCYTERTIO = Common.getConfigDirectory(sc, CONF_DIR, "SDP.PPDEL_TRANSFORM_FILE.TERTIO_TRANSFORM_DIR").concat(CHILD_DIR);// gotten from result in split process.
        */
        
        println("Start Processing SDP Lifecycle"); 
        System.setProperty("hadoop.home.dir", "C:\\winutil\\")
        val PRC_DT="20161202"
        val JOB_ID="03"
        val CONF_DIR="C://testing"
        val INPUT_DIR="C:\\works\\Project\\HADOOP\\Tertio\\tertio_SIT\\data source reference dan SDP data\\SDP_data_sample_v2.txt"
        val FOSS_DIR="C:\\works\\Project\\HADOOP\\Tertio\\tertio_SIT\\data source reference dan SDP data\\delta_sim_20161222_000000_aa.txt"
        val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
        val OUTPUT_SDPLCYTERTIO = "C:/works/Project/HADOOP/Tertio/output/".concat(CHILD_DIR)
				
        
/*      val fossDF = ReferenceDF.getFossDF(sqlContext,null)
        fossDF.registerTempTable("Foss")
        fossDF.persist()
        fossDF.count()*/
        
                
        /*****************************************************************************************************
        * 
        * STEP 01.Load Reference Data
        * 
        *****************************************************************************************************/
        log.warn("===============STEP 01. LOAD REFERENCE===================");
        
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
               "",//SIM
               "",//IMSI
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
              
          val sdpLcyInputDF = sqlContext.createDataFrame(sdpLcyTertioInputRDD, model.SDPLcyTertio.SDPLcyTertioSchema);
          sdpLcyInputDF.registerTempTable("STGSDPLifecycle");
          sdpLcyInputDF.persist();
          sqlContext.cacheTable("STGSDPLifecycle");
          sdpLcyInputDF.show();
          
        
          /*****************************************************************************************************
          * 
          * STEP 02.Load Daily Reference Data
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 02. LOAD Daily REFERENCE===================");
          val sdpLcyRefDailyRDD = sc.textFile(FOSS_DIR.concat("*"));
          val sdpLcyRefTertioInputDailyRDD = sdpLcyRefDailyRDD
          .map(line => line.split("\\|"))
          .map { p => Row( 
                 p(0), // ICCID
                if (p(1) != "" && p(1).charAt(0).==('8'))
                  "62".concat(p(1))
                 else if (p(1) != "" && p(1).charAt(0).!=('8'))
                  (p(1))
                 else "", // MSISDN
                 p(2), // TOTAL_MSISDN
                 p(3), // IMSI
                 p(4), // ENTRY_DATE
                 p(5), // EXPIRY_DATE
                 p(6), // SERVICE_TYPE
                 p(7), // BUS_STATUS
                 p(8), // UPDATED_ON
                 p(9), // BRAND_CODE
                 p(10), // BRANCH_CODE
                 p(11), // NET_STATUS
                 p(12), // HLR_CODE
                 p(13), // CARD_SIZE
                 p(14), // DEALER_ID
                 p(15), // ENTERED_BY
                 p(16), // UPDATED_BY
                 p(17), // ARTWORK_CODE
                 p(18), // PROD_ORDER_NO
                 p(19), // PACK_ORDER_NO
                 p(20), // ALLOC_NO
                 p(21), // HLRAREA_CODE
                 p(22), // KI_A4KI
                 p(23), // DISTRIBUTION_DATE
                 p(24), // NOMINAL_VALUE
                 p(25), // ESN
                 p(26), // ADM1
                 p(27), // ACTIVE_DATE
                 p(28), // INACTIVE_DATE
                 p(29), // PROGRAM_CODE
                 p(30), // STOCK_ORDER_ID
                 p(31), // PRINT_FILE_DATE
                 p(32), // PRINT_FILE_USER
                 p(33), // INSPECT_OK_DATE
                 p(34), // INSPECT_OK_USER
                 p(35), // PACK_OK_DATE
                 p(36), // PACK_OK_USER
                 p(37), // REPERSO_DATE
                 p(38), // REPERSO_USER
                 p(39), // VERSION
                 p(40), // PIN1
                 p(41), // PIN2
                 p(42), // PUK1
                 p(43), // PUK2
                 p(44), // CARD_FLAG
                 p(45), // COMPOSITE_AREA_CODE
                 p(46), // IMSI_T
                 p(47), // SERVICE_CLASS
                 p(48), // LAC
                 p(49), // CI
                 p(50), // MSISDN_SEL_TYPE
                 p(51), // SVC_BRANDCODE
                 p(52), // PAID_VANITY
                 p(53), // CARD_TYPE
                 p(54) // ALGORITHM_VERSION
                 )};
                
          val sdpLcyRefDailyDF = sqlContext.createDataFrame(sdpLcyRefTertioInputDailyRDD, ReferenceSchema.fossSchema);
          sdpLcyRefDailyDF.registerTempTable("sdpLcyRefDaily");
          sdpLcyRefDailyDF.persist();
//        sqlContext.cacheTable("sdpLcyRefDaily");
          sdpLcyRefDailyDF.show()
          
          
          /*****************************************************************************************************
          * 
          * STEP 03.Load Daily Group Reference Data ( latest reference )
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 03. LOAD Daily REFERENCE===================");
          
          val trfSDPLcyGroupDF = sqlContext.sql(
            """
            Select
               max(refDaily.UPDATED_ON) UPDATED_ON, refDaily.MSISDN
            from sdpLcyRefDaily refDaily
            group by refDaily.MSISDN 
            """)
          trfSDPLcyGroupDF.registerTempTable("trfSDPLcyGroup");
          trfSDPLcyGroupDF.persist();
          trfSDPLcyGroupDF.show()
          
          
          /*****************************************************************************************************
          * 
          * STEP 03.Join Daily with result of group by daily
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 03.Join Daily with result of group by daily===================");
          
          val trfSDPLcyJoinGroupDF = sqlContext.sql(
            """
            Select
                 refDly.ICCID,              
                 refDly.MSISDN,             
                 refDly.TOTAL_MSISDN,       
                 refDly.IMSI,               
                 refDly.ENTRY_DATE,         
                 refDly.EXPIRY_DATE,        
                 refDly.SERVICE_TYPE,       
                 refDly.BUS_STATUS,         
                 refDly.UPDATED_ON,         
                 refDly.BRAND_CODE,         
                 refDly.BRANCH_CODE,        
                 refDly.NET_STATUS,         
                 refDly.HLR_CODE,           
                 refDly.CARD_SIZE,          
                 refDly.DEALER_ID,          
                 refDly.ENTERED_BY,         
                 refDly.UPDATED_BY,         
                 refDly.ARTWORK_CODE,       
                 refDly.PROD_ORDER_NO,      
                 refDly.PACK_ORDER_NO,      
                 refDly.ALLOC_NO,           
                 refDly.HLRAREA_CODE,       
                 refDly.KI_A4KI,            
                 refDly.DISTRIBUTION_DATE,  
                 refDly.NOMINAL_VALUE,      
                 refDly.ESN,                
                 refDly.ADM1,               
                 refDly.ACTIVE_DATE,        
                 refDly.INACTIVE_DATE,      
                 refDly.PROGRAM_CODE,       
                 refDly.STOCK_ORDER_ID,     
                 refDly.PRINT_FILE_DATE,    
                 refDly.PRINT_FILE_USER,    
                 refDly.INSPECT_OK_DATE,    
                 refDly.INSPECT_OK_USER,    
                 refDly.PACK_OK_DATE,       
                 refDly.PACK_OK_USER,       
                 refDly.REPERSO_DATE,       
                 refDly.REPERSO_USER,       
                 refDly.VERSION,            
                 refDly.PIN1,               
                 refDly.PIN2,                
                 refDly.PUK1,               
                 refDly.PUK2,               
                 refDly.CARD_FLAG,          
                 refDly.COMPOSITE_AREA_CODE,
                 refDly.IMSI_T,             
                 refDly.SERVICE_CLASS,      
                 refDly.LAC,                
                 refDly.CI,                 
                 refDly.MSISDN_SEL_TYPE,    
                 refDly.SVC_BRANDCODE,      
                 refDly.PAID_VANITY,        
                 refDly.CARD_TYPE,          
                 refDly.ALGORITHM_VERSION   
            from sdpLcyRefDaily refDly inner join 
            trfSDPLcyGroup grp
            on refDly.UPDATED_ON = grp.UPDATED_ON
            and refDly.MSISDN = grp.MSISDN 
            """)
          trfSDPLcyJoinGroupDF.registerTempTable("trfSDPLcyJoinGroup");
          trfSDPLcyJoinGroupDF.persist();
          trfSDPLcyJoinGroupDF.show()
//            --on from_unixtime(unix_timestamp(refDly.UPDATED_ON, "MM/dd/yyyy HH:mm:ss")) = from_unixtime(unix_timestamp(grp.UPDATED_ON, "MM/dd/yyyy HH:mm:ss"))
          
          /*****************************************************************************************************
          * 
          * STEP 05.Join STAGING with Daily Reference Data
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 05. LOAD Daily REFERENCE===================");
          val trfSDPLcyDF = sqlContext.sql(
            """
            Select
               stg.MSISDN,
               case when dly.ICCID is null then "" else dly.ICCID end as ICCID,
               case when dly.IMSI is null then "" else dly.IMSI end as IMSI,
               stg.MASTER_PHONE,
               stg.PUK1,
               stg.PUK2,
               stg.SUBSCRIBERID,
               stg.ACCOUNTID,
               stg.ACTIVATION_DATE,
               stg.DELETE_DATE,
               stg.DELETE_BY,
               stg.AREA_CODES_SERVICE_CLASS_ID,
               stg.SERVICE_CLASS_NAME,
               stg.LANGUAGE_DESC,
               stg.ORGANIZATIONID,
               stg.ORGANIZATIONNAME,
               stg.MOCExpired,
               stg.MTCExpired
            from STGSDPLifecycle stg
            inner join trfSDPLcyJoinGroup dly
              on stg.MSISDN = dly.MSISDN
            where (stg.MSISDN is not null or stg.MSISDN <> '')
            """)

//        trfSDPLcyDF.persist()   
        Common.cleanDirectory(sc, OUTPUT_SDPLCYTERTIO)
        trfSDPLcyDF.write.format("com.databricks.spark.csv").option("delimiter", ",").save(OUTPUT_SDPLCYTERTIO);
        trfSDPLcyDF.show();
         
        sc.stop();
    }
}