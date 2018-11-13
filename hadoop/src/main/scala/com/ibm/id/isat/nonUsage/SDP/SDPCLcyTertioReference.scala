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

object SDCLcyTertioReference {

    def main(args: Array[String]) {
    
          val sc = new SparkContext(new SparkConf())
//        val sc = new SparkContext("local", "testing spark ", new SparkConf()); 
          sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")

          val sqlContext = new org.apache.spark.sql.SQLContext(sc)
          import sqlContext.implicits._
          import org.apache.log4j._
          val rootLogger = Logger.getRootLogger()
          rootLogger.setLevel(Level.INFO)
          val log = LogManager.getRootLogger
          
          if (args.length != 5) {
            println("Usage: [PRC_DT] [JOBID] [CONFIG] [INPUT_INITIAL] [INPUT_FOSS]")
            sys.exit(1);
          }
          
          val PRC_DT=args(0)
          val JOB_ID=args(1)
          val CONF_DIR=args(2)
          val INPUT_INITIAL=args(3)
          val INPUT_FOSS=args(4)
          val CHILD_DIR=Common.getChildDirectory(PRC_DT, JOB_ID);
//        val OUTPUT_TERTIO_REFERENCE = Common.getConfigDirectory(sc, CONF_DIR, "SDP.PPDEL_TRANSFORM_FILE.TERTIO_OUTPUT_REFERENCE_DIR").concat(CHILD_DIR);// gotten from result in split process.
          val OUTPUT_TERTIO_REFERENCE = Common.getConfigDirectory(sc, CONF_DIR, "SDP.PPDEL_TRANSFORM_FILE.TERTIO_OUTPUT_REFERENCE_DIR");// gotten from result in split process.
         
				  
/*        println("Start Processing SDP Reference"); 
        	System.setProperty("hadoop.home.dir", "C:\\winutil\\")
          val PRC_DT="20161215"
          val JOB_ID="03"
          val CONF_DIR="C://testing"
          val INPUT_INITIAL="C:/works/Project/HADOOP/Tertio/tertio_SIT/tertio_daily/delta_sim_20161221_000000_aa.txt"
          val INPUT_FOSS="C:/works/Project/HADOOP/Tertio/tertio_SIT/tertio_daily/delta_sim_20161221_000000_bb.txt"
          val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
          val OUTPUT_TERTIO_REFERENCE = "C:/works/Project/HADOOP/Tertio/reference/output/".concat(CHILD_DIR)
*/          
          
          /*****************************************************************************************************
          * 
          * STEP 01.Load Initial Reference Data
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 01. LOAD Initial REFERENCE===================");
          val sdpLcyRefInitialRDD = sc.textFile(INPUT_INITIAL.concat("*"));
          val sdpLcyRefTertioInputInitialRDD = sdpLcyRefInitialRDD
          .filter ( x => x.count (_ == '|') == 54)
          .map(line => line.split("\\|"))
          .map { p => Row( 
                 p(0), // ICCID
                 p(1), // MSISDN
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
                
          val sdpLcyRefInitialDF = sqlContext.createDataFrame(sdpLcyRefTertioInputInitialRDD, ReferenceSchema.fossSchema);
          sdpLcyRefInitialDF.registerTempTable("sdpLcyRefInitial");
          sdpLcyRefInitialDF.persist();
          sdpLcyRefInitialDF.show()
          
          
        /*****************************************************************************************************
          * 
          * STEP 02.Load Daily Reference Data
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 02. LOAD Daily REFERENCE===================");
          
          val sdpLcyRefDailyRDD = sc.textFile(INPUT_FOSS.concat("*"));
          val sdpLcyRefTertioInputDailyRDD = sdpLcyRefDailyRDD
          .map(line => line.split("\\|"))
          .map { p => Row( 
                 p(0), // ICCID
                 p(1), // MSISDN
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
          sqlContext.cacheTable("sdpLcyRefDaily");
          sdpLcyRefDailyDF.show()
          
          
          /*****************************************************************************************************
          * 
          * STEP 03.Join Initial and Daily Reference Data ( Get Data that exists in old data but not exists in new data)
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 03.Join Initial and Daily Reference Data ( Get Data that exists in old data but not exists in new data)===================");

          val trfSDPLcyInitialfDF = sqlContext.sql(
              """
              SELECT
                 initial.ICCID,              
                 initial.MSISDN,             
                 initial.TOTAL_MSISDN,       
                 initial.IMSI,               
                 initial.ENTRY_DATE,         
                 initial.EXPIRY_DATE,        
                 initial.SERVICE_TYPE,       
                 initial.BUS_STATUS,         
                 initial.UPDATED_ON,         
                 initial.BRAND_CODE,         
                 initial.BRANCH_CODE,        
                 initial.NET_STATUS,         
                 initial.HLR_CODE,           
                 initial.CARD_SIZE,          
                 initial.DEALER_ID,          
                 initial.ENTERED_BY,         
                 initial.UPDATED_BY,         
                 initial.ARTWORK_CODE,       
                 initial.PROD_ORDER_NO,      
                 initial.PACK_ORDER_NO,      
                 initial.ALLOC_NO,           
                 initial.HLRAREA_CODE,       
                 initial.KI_A4KI,            
                 initial.DISTRIBUTION_DATE,  
                 initial.NOMINAL_VALUE,      
                 initial.ESN,                
                 initial.ADM1,               
                 initial.ACTIVE_DATE,        
                 initial.INACTIVE_DATE,      
                 initial.PROGRAM_CODE,       
                 initial.STOCK_ORDER_ID,     
                 initial.PRINT_FILE_DATE,    
                 initial.PRINT_FILE_USER,    
                 initial.INSPECT_OK_DATE,    
                 initial.INSPECT_OK_USER,    
                 initial.PACK_OK_DATE,       
                 initial.PACK_OK_USER,       
                 initial.REPERSO_DATE,       
                 initial.REPERSO_USER,       
                 initial.VERSION,            
                 initial.PIN1,               
                 initial.PIN2,                
                 initial.PUK1,               
                 initial.PUK2,               
                 initial.CARD_FLAG,          
                 initial.COMPOSITE_AREA_CODE,
                 initial.IMSI_T,             
                 initial.SERVICE_CLASS,      
                 initial.LAC,                
                 initial.CI,                 
                 initial.MSISDN_SEL_TYPE,    
                 initial.SVC_BRANDCODE,      
                 initial.PAID_VANITY,        
                 initial.CARD_TYPE,          
                 initial.ALGORITHM_VERSION   
              FROM sdpLcyRefInitial initial left join 
              sdpLcyRefDaily daily  on initial.ICCID = daily.ICCID AND initial.IMSI = daily.IMSI 
              where (initial.MSISDN is not null OR initial.MSISDN <> '')
              and ( daily.MSISDN is null OR  daily.MSISDN = '')
              """)
          trfSDPLcyInitialfDF.registerTempTable("trfSDPLcyLatestInitial");    
          trfSDPLcyInitialfDF.persist();
          trfSDPLcyInitialfDF.show();
          
          
          /*****************************************************************************************************
          * 
          * STEP 04.Get Daily Data based UPDATE ON
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 04.Get Daily Data based UPDATE ON===================");

          val trfSDPLcyLatestDailyDF = sqlContext.sql(
              """
               SELECT
                 daily.ICCID,              
                 daily.MSISDN,             
                 daily.TOTAL_MSISDN,       
                 daily.IMSI,               
                 daily.ENTRY_DATE,         
                 daily.EXPIRY_DATE,        
                 daily.SERVICE_TYPE,       
                 daily.BUS_STATUS,         
                 daily.UPDATED_ON,         
                 daily.BRAND_CODE,         
                 daily.BRANCH_CODE,        
                 daily.NET_STATUS,         
                 daily.HLR_CODE,           
                 daily.CARD_SIZE,          
                 daily.DEALER_ID,          
                 daily.ENTERED_BY,         
                 daily.UPDATED_BY,         
                 daily.ARTWORK_CODE,       
                 daily.PROD_ORDER_NO,      
                 daily.PACK_ORDER_NO,      
                 daily.ALLOC_NO,           
                 daily.HLRAREA_CODE,       
                 daily.KI_A4KI,            
                 daily.DISTRIBUTION_DATE,  
                 daily.NOMINAL_VALUE,      
                 daily.ESN,                
                 daily.ADM1,               
                 daily.ACTIVE_DATE,        
                 daily.INACTIVE_DATE,      
                 daily.PROGRAM_CODE,       
                 daily.STOCK_ORDER_ID,     
                 daily.PRINT_FILE_DATE,    
                 daily.PRINT_FILE_USER,    
                 daily.INSPECT_OK_DATE,    
                 daily.INSPECT_OK_USER,    
                 daily.PACK_OK_DATE,       
                 daily.PACK_OK_USER,       
                 daily.REPERSO_DATE,       
                 daily.REPERSO_USER,       
                 daily.VERSION,            
                 daily.PIN1,               
                 daily.PIN2,                
                 daily.PUK1,               
                 daily.PUK2,               
                 daily.CARD_FLAG,          
                 daily.COMPOSITE_AREA_CODE,
                 daily.IMSI_T,             
                 daily.SERVICE_CLASS,      
                 daily.LAC,                
                 daily.CI,                 
                 daily.MSISDN_SEL_TYPE,    
                 daily.SVC_BRANDCODE,      
                 daily.PAID_VANITY,        
                 daily.CARD_TYPE,          
                 daily.ALGORITHM_VERSION   
              FROM sdpLcyRefDaily daily
              """)
          trfSDPLcyLatestDailyDF.registerTempTable("trfSDPLcyLatestDaily");    
          trfSDPLcyLatestDailyDF.persist();
          trfSDPLcyLatestDailyDF.show();
          
          
          /*****************************************************************************************************
          * 
          * STEP 05.Union Initial and Daily Reference Data
          * 
          *****************************************************************************************************/
          log.warn("===============STEP 05.Union Initial and Daily Reference Data===================");
          
          val trfSDPLcyRefUnionDF = sqlContext.sql(
              """
              SELECT
                 ir.ICCID,              
                 ir.MSISDN,             
                 ir.TOTAL_MSISDN,       
                 ir.IMSI,               
                 ir.ENTRY_DATE,         
                 ir.EXPIRY_DATE,        
                 ir.SERVICE_TYPE,       
                 ir.BUS_STATUS,         
                 ir.UPDATED_ON,         
                 ir.BRAND_CODE,         
                 ir.BRANCH_CODE,        
                 ir.NET_STATUS,         
                 ir.HLR_CODE,           
                 ir.CARD_SIZE,          
                 ir.DEALER_ID,          
                 ir.ENTERED_BY,         
                 ir.UPDATED_BY,         
                 ir.ARTWORK_CODE,       
                 ir.PROD_ORDER_NO,      
                 ir.PACK_ORDER_NO,      
                 ir.ALLOC_NO,           
                 ir.HLRAREA_CODE,       
                 ir.KI_A4KI,            
                 ir.DISTRIBUTION_DATE,  
                 ir.NOMINAL_VALUE,      
                 ir.ESN,                
                 ir.ADM1,               
                 ir.ACTIVE_DATE,        
                 ir.INACTIVE_DATE,      
                 ir.PROGRAM_CODE,       
                 ir.STOCK_ORDER_ID,     
                 ir.PRINT_FILE_DATE,    
                 ir.PRINT_FILE_USER,    
                 ir.INSPECT_OK_DATE,    
                 ir.INSPECT_OK_USER,    
                 ir.PACK_OK_DATE,       
                 ir.PACK_OK_USER,       
                 ir.REPERSO_DATE,       
                 ir.REPERSO_USER,       
                 ir.VERSION,            
                 ir.PIN1,               
                 ir.PIN2,                
                 ir.PUK1,               
                 ir.PUK2,               
                 ir.CARD_FLAG,          
                 ir.COMPOSITE_AREA_CODE,
                 ir.IMSI_T,             
                 ir.SERVICE_CLASS,      
                 ir.LAC,                
                 ir.CI,                 
                 ir.MSISDN_SEL_TYPE,    
                 ir.SVC_BRANDCODE,      
                 ir.PAID_VANITY,        
                 ir.CARD_TYPE,          
                 ir.ALGORITHM_VERSION   
              FROM trfSDPLcyLatestInitial ir
              UNION ALL
              SELECT
                 dly.ICCID,              
                 dly.MSISDN,             
                 dly.TOTAL_MSISDN,       
                 dly.IMSI,               
                 dly.ENTRY_DATE,         
                 dly.EXPIRY_DATE,        
                 dly.SERVICE_TYPE,       
                 dly.BUS_STATUS,         
                 dly.UPDATED_ON,         
                 dly.BRAND_CODE,         
                 dly.BRANCH_CODE,        
                 dly.NET_STATUS,         
                 dly.HLR_CODE,           
                 dly.CARD_SIZE,          
                 dly.DEALER_ID,          
                 dly.ENTERED_BY,         
                 dly.UPDATED_BY,         
                 dly.ARTWORK_CODE,       
                 dly.PROD_ORDER_NO,      
                 dly.PACK_ORDER_NO,      
                 dly.ALLOC_NO,           
                 dly.HLRAREA_CODE,       
                 dly.KI_A4KI,            
                 dly.DISTRIBUTION_DATE,  
                 dly.NOMINAL_VALUE,      
                 dly.ESN,                
                 dly.ADM1,               
                 dly.ACTIVE_DATE,        
                 dly.INACTIVE_DATE,      
                 dly.PROGRAM_CODE,       
                 dly.STOCK_ORDER_ID,     
                 dly.PRINT_FILE_DATE,    
                 dly.PRINT_FILE_USER,    
                 dly.INSPECT_OK_DATE,    
                 dly.INSPECT_OK_USER,    
                 dly.PACK_OK_DATE,       
                 dly.PACK_OK_USER,       
                 dly.REPERSO_DATE,       
                 dly.REPERSO_USER,       
                 dly.VERSION,            
                 dly.PIN1,               
                 dly.PIN2,                
                 dly.PUK1,               
                 dly.PUK2,               
                 dly.CARD_FLAG,          
                 dly.COMPOSITE_AREA_CODE,
                 dly.IMSI_T,             
                 dly.SERVICE_CLASS,      
                 dly.LAC,                
                 dly.CI,                 
                 dly.MSISDN_SEL_TYPE,    
                 dly.SVC_BRANDCODE,      
                 dly.PAID_VANITY,        
                 dly.CARD_TYPE,          
                 dly.ALGORITHM_VERSION   
               FROM trfSDPLcyLatestDaily dly
              """)
          trfSDPLcyRefUnionDF.registerTempTable("sdpLcyRef");    
          trfSDPLcyRefUnionDF.persist();
          trfSDPLcyRefUnionDF.show();
          
          Common.cleanDirectory(sc, OUTPUT_TERTIO_REFERENCE)
          trfSDPLcyRefUnionDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_TERTIO_REFERENCE);
          sc.stop();
      }
    
      
      def goPrcDate(days : Int): String = {
          val curTimeFormat = new SimpleDateFormat("YYYYMMdd")
          var submittedDateConvert = Calendar.getInstance()
          submittedDateConvert.add(Calendar.DATE, -days)
          val prcDt = curTimeFormat.format(submittedDateConvert.getTime).toString()
          return prcDt;
      }
      
}
