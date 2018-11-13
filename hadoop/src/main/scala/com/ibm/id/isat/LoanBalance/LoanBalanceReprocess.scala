package com.ibm.id.isat.LoanBalance


import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import com.ibm.id.isat.nonUsage.air.model.subAirRefill;
import com.ibm.id.isat.utils.ReferenceSchema;
import org.apache.spark.sql.Row;
import com.ibm.id.isat.utils.Common;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import com.ibm.id.isat.utils.ReferenceDF;
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._



object LoanBalanceReprocess {
  
    def main(args: Array[String]) {
    
     //val sc = new SparkContext("local", "sub refill transformation spark ", new SparkConf());   //DEV     
     val sc = new SparkContext(new SparkConf())  //PRD
     
    
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
     
     import org.apache.log4j._
     Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.OFF)
     Logger.getLogger("org").setLevel(Level.ERROR)
     Logger.getLogger("akka").setLevel(Level.ERROR)
         
     val log = LogManager.getRootLogger
     
     // Import data frame library.
     import sqlContext.implicits._
   
      //PRD   
     if (args.length != 5) {
        println("Usage: [PRC_DT] [JOBID] [CONFIG] [WORKINGDIR1] [WORKINGDIR2]")
        sys.exit(1);
     }
     
       val prcDt = args(0)
       val jobID = args(1)
       val configDir = args(2)
       val inputFileRef = args(3)
       val inputFileRev = args(4)
     
     //val dummy1 = args(3)
       
     //val OUTPUT_CDR_INQ_DIR_REFFILL = Common.getConfigDirectory(sc, configDir, "AIR_LOAN.AIRLOAN_TRANSFORM.OUTPUT_CDR_INQ_DIR_AIRLOAN").concat(prcDt).concat("_").concat(jobID)  
     //val configDir = "/user/hdp-reload/spark/conf/AIR_LOAN.conf"
     val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, configDir, "AIR_LOAN.AIRLOAN_TRANSFORM.OUTPUT_AIRLOAN_REJ_BP_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REF_DIR = Common.getConfigDirectory(sc, configDir, "AIR_LOAN.AIRLOAN_TRANSFORM.OUTPUT_AIRLOAN_REJ_REF_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR_LOAN.AIRLOAN_TRANSFORM.OUTPUT_AIRLOAN_REJ_REV_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_DETAIL_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR_LOAN.AIRLOAN_TRANSFORM.OUTPUT_AIRLOAN_DETAIL_DIR")

     
       
     /*     
     //val prcDt =  "20160722"
     //val jobID = "10"
     //val configDir = "C:/testing"
     val inputFileRef = "C:/spark_development/Indosat/Loan_Balance/reprocess/reprocess_ref/rej_ref.REF"
     val inputFileRev = "C:/spark_development/Indosat/Loan_Balance/reprocess/reprocess_rev/rej_rev.REF"
     val OUTPUT_DETAIL_REV_DIR ="C:/spark_development/Indosat/Loan_Balance/reprocess/sor"
     val OUTPUT_REJ_REF_DIR = "C:/spark_development/Indosat/Loan_Balance/reprocess/rej_ref"
     val OUTPUT_REJ_REV_DIR = "C:/spark_development/Indosat/Loan_Balance/reprocess/rej_rev"
     //System.setProperty("hadoop.home.dir", "C:\\winutil\\")   
     */
     
     
/*    
        //val prcDt =  "20160722"
     //val jobID = "10"
     //val configDir = "C:/testing"
     val inputFileRef = "C:/spark_development/Indosat/Loan_Balance/reprocess/reprocess_ref/rej_ref.REF"
     val inputFileRev = "C:/spark_development/Indosat/Loan_Balance/reprocess/reprocess_rev/rej_rev.REF"     
     //System.setProperty("hadoop.home.dir", "C:\\winutil\\") 
     
     
     val prcDt = "20160722"
     val jobID = "10"
     val configDir = "C:/Data/WORK/Hadoop_Dev/Loan_Balance"
     val inputFile= "C:/Data/WORK/Hadoop_Dev/Loan_Balance/part-00000.txt"
     val OUTPUT_REJ_BP_DIR = "C:/Data/WORK/Hadoop_Dev/Loan_Balance/rej_bp"
     val OUTPUT_REJ_REF_DIR = "C:/Data/WORK/Hadoop_Dev/Loan_Balance/rej_ref"
     val OUTPUT_REJ_REV_DIR = "C:/Data/WORK/Hadoop_Dev/Loan_Balance/rej_rev"
     val OUTPUT_DETAIL_REV_DIR ="C:/Data/WORK/Hadoop_Dev/Loan_Balance/sor"
     val OUTPUT_CDR_INQ_DIR_REFFILL ="C:/Data/WORK/Hadoop_Dev/Loan_Balance/inquiry"
*/
     
    
     //trisna
     //val OUTPUT_STG_DIR = "C:/Data/WORK/Hadoop_Dev/Loan_Balance/stg"
     
     sqlContext.udf.register("getTupleInTuple", Common.getTupleInTuple _)
     
     sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
     val existDir= Common.isDirectoryExists(sc, inputFileRef);
     var subReprocessRejRefInptRDD = sc.textFile(inputFileRef);  
     
     val existDirRev= Common.isDirectoryExists(sc, inputFileRev);
     var subReprocessRejRevInptRDD = sc.textFile(inputFileRev);  
     
     
     /*****************************************************************************************************
     * 
     * STEP 01. Load Reference Data
     * 
     *****************************************************************************************************/
     
     log.warn("===============STEP 01. LOAD REFERENCE===================");  
      val geoAreaHlrDF = ReferenceDF.getGeoAreaHlrDF(sqlContext)
      geoAreaHlrDF.registerTempTable("GeoAreaHLR")
      geoAreaHlrDF.persist()
      geoAreaHlrDF.count()
     
     
      val svcClassDF = ReferenceDF.getServiceClassDF(sqlContext)
      svcClassDF.registerTempTable("ServiceClass")
      svcClassDF.persist()
      svcClassDF.count()
     
      val sdpOfferDF = ReferenceDF.getSDPOffer(sqlContext)
      sdpOfferDF.registerTempTable("SDPOffer")
      
      
      val intcctDF = ReferenceDF.getIntcctAllDF(sqlContext)   
      intcctDF.registerTempTable("Intcct")
      sqlContext.cacheTable("Intcct")
      intcctDF.count();
      
      
      val regionBranchDF = ReferenceDF.getRegionBranchDF(sqlContext)
      regionBranchDF.registerTempTable("RegBranch")
      regionBranchDF.persist()
      regionBranchDF.count();
      
      
      val revcodeDF = ReferenceDF.getRevenueCodeDF(sqlContext)
      revcodeDF.registerTempTable("RevenueCode")
      revcodeDF.persist()
      revcodeDF.count();
      
      val madaDF = ReferenceDF.getMaDaDF(sqlContext)
      madaDF.registerTempTable("Mada")
      madaDF.persist()
      madaDF.count();
      
      
      val svcClassOfferDF = ReferenceDF.getServiceClassOfferDF(sqlContext)
      svcClassOfferDF.registerTempTable("ServiceClassOffer")
      svcClassOfferDF.persist()
      svcClassOfferDF.count();
      
      
      val PostpaidSvcClassDF = ReferenceDF.getServiceClassPostpaidDF(sqlContext)   
      PostpaidSvcClassDF.registerTempTable("PostpaidSvcClass")
      PostpaidSvcClassDF.persist()
      PostpaidSvcClassDF.count()
      
      /*
       val reloadTypeDF = ReferenceDF.getRecordReloadTypeDF(sqlContext)   
      reloadTypeDF.registerTempTable("ReloadType")
      reloadTypeDF.persist()
      reloadTypeDF.count()
      
      val voucherTypeDF = ReferenceDF.getRecordVoucherTypeDF(sqlContext)
      voucherTypeDF.registerTempTable("VoucherType") 
      voucherTypeDF.persist()
      voucherTypeDF.count()
      
      val bankNameDF = ReferenceDF.getRecordBankNameDF(sqlContext)
      bankNameDF.registerTempTable("BankName")
      bankNameDF.persist()
      bankNameDF.count()
      
      val bankDetailDF = ReferenceDF.getRecordBankDetailDF(sqlContext)
      bankDetailDF.registerTempTable("BankDetail")
      bankDetailDF.persist()
      bankDetailDF.count()  
      
      */
      
      
        if (existDir != true ){
           var inputReprocessRejRef = sc.makeRDD(Seq(Row(
             null, //originNodeType
             null, //originHostName  
             null, //timeStamp
             null, //accountNumber             
             null, //refillProfileID             
             null, //currentServiceclass                                                    
             null, //transactionAmount                          
             null, //accountInformationAfterRefill
             null, //subscribernumber                         
             null, //realFilename             
             null, //originTimeStamp
             null, //jobID
             null, //recordID
             null, //prcDT
             null, //area
             null, //fileDT
             null //rejRsn                             
             )
           )
         )
         val stgReprocessRejRef = sqlContext.createDataFrame(inputReprocessRejRef, subAirRefill.stgReprocessAirLoanRejRefSchema);
         stgReprocessRejRef.registerTempTable("stg_air_loan_rej_ref");
         stgReprocessRejRef.persist();
         
          log.warn("===============NULL DATA===================");
        }else{ 
          

        //subReprocessRejRefInptRDD.collect foreach {case (a) => println (a)}
        
        var inputReprocessRejRef = subReprocessRejRefInptRDD.filter ( x => x.count (_ == '|') == 16).map(line => line.split("\\|")).map {p => Row(         
         p(0), //originNodeType
         p(1), //originHostName  
         p(2), //timeStamp
         p(3), //accountNumber             
         p(4), //refillProfileID             
         p(5), //currentServiceclass                                                    
         p(6), //transactionAmount                          
         p(7), //accountInformationAfterRefill
         p(8), //subscribernumber                         
         p(9), //realFilename             
         p(10), //originTimeStamp
         jobID,  //jobID
         p(12), //recordID
         goPrcDate(), //prcDT
         p(14), //area
         p(15), //fileDT
         p(16) //rejRsn             
        )
       }
         val stgReprocessRejRef = sqlContext.createDataFrame(inputReprocessRejRef, subAirRefill.stgReprocessAirLoanRejRefSchema);
         stgReprocessRejRef.registerTempTable("stg_air_rej_ref");
         stgReprocessRejRef.persist();
         println("SHOW SOURCE REJ REF")
         //stgReprocessRejRef.show();
         log.warn("===============ANY DATA===================");
      }
          
      
      
     
  
    
    sqlContext.udf.register("getTuple", Common.getTuple _)
    sqlContext.udf.register("toDouble", Common.toDouble _)
    
    //trisna,
    //stgAirRefillLoan.w  write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_STG_DIR);
    //trisna
    
    //stgAirRefLoan.persist()
    //stgAirRefLoan0.show();
    

     /*****************************************************************************************************
     * 
     *     
     * Step 02 : Air Refill Data Exclude Postpaid Service Class
     * 
     *****************************************************************************************************/
     log.warn("================== Step 02 : Reprocess Air Refill Data Exclude Postpaid Service Class. ================"+goToMinute());
     val ha1ReprocessAirRefLoan =  sqlContext.sql( 
          """
          SELECT
             a.originNodeType,
             a.originHostName,
             a.timeStamp, 
             a.accountNumber ,        
             a.accountNumber,                 
             a.refillProfileID,
             a.currentServiceclass,
             a.transactionAmount,                   
             a.accountInformationAfterRefill,                             
             a.subscribernumber,
             a.realFilename,   
             a.originTimeStamp,
             a.jobID,
             a.recordID,
             a.prcDT,
             a.area,
             a.fileDT,
             postSCCode             
          from
          (
             SELECT  postSC.postSCCode, 
                     loan.*
             FROM stg_air_rej_ref loan                                                
             left join PostpaidSvcClass postSC on loan.currentServiceclass = postSC.postSCCode 
          )  a                  
          """
         );
      ha1ReprocessAirRefLoan.registerTempTable("stg_Reprocess_air_reff_loan")      
      //ha1AirRefillData01.persist();
      println("SHOW A_HA (Excluding Postpaid)")
      //ha1ReprocessAirRefLoan.show();
      
     
  

     
     /*****************************************************************************************************
     * 
     *     
     * Step 03 : Air Refill Data
     * 
     *****************************************************************************************************/
     log.warn("================== Step 03 : Reprocess Air Refill Data. ================"+goToMinute());
     val ha1ReprocessAirRefillData01 =  sqlContext.sql( 
          """
          SELECT
             a.originNodeType,
             a.originHostName,
             substr(a.timeStamp,1,8) as tanggal, 
             a.accountNumber as msisdn,        
             a.timeStamp,  
             a.accountNumber,                 
             a.refillProfileID,
             --a.subscriberNumber,
             a.currentServiceclass,
             a.transactionAmount,                   
             a.accountInformationAfterRefill,  
             callclass, 
             DA_instr7,
             getTuple(substr(a.DA_instr7,instr(a.DA_instr7, "85"), 50),"*",0) DA_ID,           
             callclass,
             toDouble(getTuple(substr(a.DA_instr7,instr(a.DA_instr7, "85"), 50),"*",2)/-100) as revenue,                    
             a.subscribernumber,
             a.realFilename,   
             a.originTimeStamp,
             a.jobID,
             a.recordID,
             a.prcDT,
             a.area,
             a.fileDT           
      
          from
          (
             SELECT  getTuple(accountInformationAfterRefill,"#",7) as DA_instr7,
                    case when originNodeType = 'EXT' and originHostName not like 'ssp%' then originHostName + 'LoanBalc'
                         when originNodeType = 'EXT' and originHostName like 'ssp%' then 'SSPLoanBalc'
                         when originNodeType != 'EXT' then originNodeType + 'LoanBalc' 
                    end callclass,
                    loan.*
             FROM stg_Reprocess_air_reff_loan loan
                   --stg_air_reff_loan loan 
             WHERE postSCCode IS NULL and (getTupleinTuple(accountInformationAfterRefill,'#',7,'*',0) like '%85%' or  substr(refillProfileID,1,1) = '6') 
          )  a          
          """
         );                                        
                                                     
      ha1ReprocessAirRefillData01.registerTempTable("ha1ReprocessAirRefillData01")      
      //ha1AirRefillData01.persist();
      println("SHOW B_HA (Call Class)")
      //ha1ReprocessAirRefillData01.show();
      
      //stgAirRef.unpersist();  
      //svcClassDF.unpersist()
      //sdpOfferDF.unpersist()
      //intcctDF.unpersist();
      //regionBranchDF.unpersist();
      

 
    
    
      
     /*****************************************************************************************************
     * 
     *     
     * Step 04 : Reprocess Loan Balance Filter Lookup
     * 
     *****************************************************************************************************/
     log.warn("================== Step 04 : Reprocess Loan Balance - Filter Lookup. ================"+goToMinute());
     val ha1ReprocessAirRefillLoan01 =  sqlContext.sql( 
          """
          SELECT
             air.originNodeType,
             air.originHostName,
             air.timeStamp,
             air.tanggal, 
             air.tanggal event_dt,
             air.msisdn,  
             air.accountNumber,              
             case when  service_src='VOUCHER SMS'  then  'VoucherSMS'
                  when  service_src='LOAN BALANCE' then  concat(concat(air.callclass,'DA'),air.DA_ID) 
             end revenue_code,   
             case when  service_src='VOUCHER SMS'  then  toDouble(transactionAmount/100)
                  when  service_src='LOAN BALANCE' then  air.revenue 
             end total_amount,
             air.refillProfileID,             
             air.currentServiceclass,             
             air.transactionAmount,                   
             air.accountInformationAfterRefill, 
             air.subscribernumber,                    
             air.realFilename,   
             air.originTimeStamp,
             air.jobID,
             air.recordID,
             air.prcDT,
             air.area,
             air.fileDT,
             case
                  when svcclsofr.svcClassCode is not null then svcclsofr.svcClassName
                  else svccls.svcClassName
             end as svcClassName,
             case
                  when svcclsofr.svcClassCode is not null then svcclsofr.prmPkgCode
                  else svccls.prmPkgCode
             end as promo_package_code,
             case
                  when svcclsofr.svcClassCode is not null then svcclsofr.prmPkgName
                  else svccls.prmPkgName
             end as promo_package_name,
             case
                  when svcclsofr.svcClassCode is not null then svcclsofr.brndSCName
                  else svccls.brndSCName
             end as brand_name,    
             svcclsofr.offerId, 
             svcclsofr.areaName,               
             air.service_src,
             air.callclass ,
             --air.service_tp, 
             air.service_src,


             sdp.offerID
          from  (--LOAN BALANCE  
                 SELECT 'LOAN BALANCE' service_src  ,
                         --substr(DA_instr7, DA_loan,
                         loan.*  
                      
                 FROM ha1ReprocessAirRefillData01 loan where DA_instr7 like '%85*%'    --ha1AirRefillData01
                 UNION ALL
                 --VOUCHER SMS
                 SELECT 'VOUCHER SMS' service_src  ,
                         vou.* 
                 FROM ha1ReprocessAirRefillData01 vou where substr(refillProfileID,1,1) = '6'   --ha1AirRefillData01
                 )  air 
          left join SDPOffer sdp on air.msisdn = sdp.msisdn and tanggal between sdp.effDt and sdp.endDt 
          left join ServiceClassOffer svcclsofr on air.currentServiceclass = svcclsofr.svcClassCode 
            and (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and sdp.offerID = svcclsofr.offerId
          left join ServiceClass svccls on air.currentServiceclass = svccls.svcClassCode 
            and (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt )
          """
         );
      ha1ReprocessAirRefillLoan01.registerTempTable("ha1ReprocessAirRefillLoan01")      
      //ha1AirRefillLoan01.persist();
      println("SHOW C_HA (SDPOffer, ServiceClass)")
      //ha1ReprocessAirRefillLoan01.show();
   

      
      val ReprocessjoinIntcctDF = Common.getIntcct(sqlContext, ha1ReprocessAirRefillLoan01, intcctDF, "accountNumber", "event_dt", "prefix", "country", "city", "provider_id") 
      ReprocessjoinIntcctDF.registerTempTable("HA1ReprocessAirRefillJoinIntcctLoan")
      

    
    
     /*****************************************************************************************************
     * 
     *     
     * Step 05 : Reprocess Provide CDR Inquiry Baseline. -- ( Create 1 dedicated class for CDR Inquiry REF )
     * 
     *****************************************************************************************************/
     log.warn("==================  Step 05 : Reprocess Provide CDR Inquiry Baseline. ================"+goToMinute());
     val ha1ReprocessAirRefillJoinInquiryLoan = sqlContext.sql(
            """
            select
             air.originNodeType,
             air.originHostName,
             air.timeStamp,
             air.tanggal, 
             air.msisdn,  
             air.prefix,
             air.country,
             air.city,
             air.provider_id,
             air.accountNumber,                  
             air.revenue_code,   
             air.total_amount,
             air.refillProfileID,             
             air.currentServiceclass,             
             air.transactionAmount,                   
             air.accountInformationAfterRefill,  
             air.subscribernumber,     
             air.realFilename,                  
             air.realFilename FileName,   
             air.originTimeStamp,
             air.jobID,
             air.recordID,
             air.prcDT,
             air.area,
             air.fileDT,
             air.svcClassName,
             air.promo_package_code,
             air.promo_package_name,
             air.brand_name,    
             air.offerId, 
             air.areaName,                            
             air.service_src,
             air.callclass ,
             --air.service_tp, 
             air.service_src,
             regBranch.branch hlr_branch_nm, 
             regBranch.region hlr_region_nm,
             regBranch.city regBranchCity,
             
             air.svcClassName
                 
            from HA1ReprocessAirRefillJoinIntcctLoan air    --HA1AirRefillJoinIntcctLoan
            left join RegBranch regBranch on air.city = regBranch.city   
            """);
     
      ha1ReprocessAirRefillJoinInquiryLoan.registerTempTable("ha1ReprocessAirRefillJoinInquiryLoan")
      ha1ReprocessAirRefillJoinInquiryLoan.persist();
      println("SHOW D_HA (CDR Inquiry)")
      //ha1ReprocessAirRefillJoinInquiryLoan.show();
      

 
    
     /*****************************************************************************************************
     * 
     *     
     * Step 06 : Reprocess - Produce for Reject Reference (LOAD TO HIVE TABLE)
     * 
     *****************************************************************************************************/ 
     log.warn("================== Step 06 : Reprocess - Produce for Reject Reference (LOAD TO HIVE TABLE) ================"+goToMinute());
     val ReprocessRejectRef = sqlContext.sql( 
         """
            SELECT 
             air.originNodeType,
             air.originHostName,
             air.timeStamp,
             air.accountNumber,
             air.refillProfileID,  
             air.currentServiceclass,  
             air.transactionAmount,                   
             air.accountInformationAfterRefill,   
             air.subscribernumber, 
             air.realFilename, 
             air.originTimeStamp,
             air.jobID,
             air.recordID,
             air.prcDT,
             air.area,
             air.fileDT,
            case when prefix is null then 'Prefix Not Found'
              when svcClassName is null then 'Service Class Not Found'
              when regBranchCity is null then 'City Not Found'
              end as REJ_RSN              
                  
           FROM ha1ReprocessAirRefillJoinInquiryLoan air  --ha1AirRefillJoinInquiryLoan
           WHERE prefix = '' or prefix is null or svcClassName = '' or svcClassName is null or regBranchCity = '' or regBranchCity is null
         """
         );
     
       Common.cleanDirectory(sc, OUTPUT_REJ_REF_DIR );
       ReprocessRejectRef.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REF_DIR);
       println("SHOW REJ_REF")
       //ReprocessRejectRef.show();
  
     
    
   
     /*****************************************************************************************************
     * 
     *     
     * Step 07 : Reprocess - Join with Mada and Revenue Reference. ( Level 1 <-> Parent Level )
     * Join with used MA type
     * 
     *****************************************************************************************************/
     log.warn("================== Step 07 : Reprocess -  Join with Mada and Revenue Reference. ( Level 1 <-> Parent Level ) ================"+goToMinute());         
     val sorReprocessAirRefillLoan_01 =  sqlContext.sql( 
          """
            SELECT 
             air.msisdn as MSISDN,  
             air.prefix as PREFIX,
             air.city as CITY,
             air.provider_id as PROVIDER_ID,
             air.country as COUNTRY,
             air.currentServiceclass as SERVICECLASSID, 
             air.promo_package_code as PROMO_PACKAGE_CODE,
             air.promo_package_name as PROMO_PACKAGE_NAME,
             air.brand_name as BRAND_NAME,                 
             cast(sum(air.total_amount) as double) as TOTAL_AMOUNT,
             cast(count(*) as double) as TOTAL_HITS,
             air.revenue_code as REVENUE_CODE, 

             case when  service_src='VOUCHER SMS'  then  'VAS'
                  when  service_src='LOAN BALANCE' then  revcod.svcUsgTp 
             end  SERVICE_TYPE,
 
             revcod.lv4 as GL_CODE,  
             revcod.lv5 as GL_NAME, 
             air.FileName as REAL_FILENAME,   
             air.hlr_branch_nm as HLR_BRANCH_NM,
             air.hlr_region_nm as HLR_REGION_NM,
             
            case
          	  when mada.acc is null then cast (null as string)
          	  when revcod.svcUsgTp = 'VOICE' then mada.voiceRev
          	  when revcod.svcUsgTp = 'SMS' then mada.smsRev
          	  --when 'DATA' then mada.dataRev
          	  when revcod.svcUsgTp = 'VAS' then mada.vasRev
          	  when revcod.svcUsgTp = 'OTHER' then mada.othRev
              when revcod.svcUsgTp = 'DISCOUNT' then mada.discRev
              when revcod.svcUsgTp = 'DROP' then 'No'
              ELSE 'UNKNOWN'
             end as REVENUE_FLAG,

             air.originTimeStamp as ORIGIN_TIMESTAMP,

             cast (null as string) as MCCMNC,
             cast (null as string) as LAC,
             cast (null as string) as CI,
             cast (null as string) as LACI_CLUSTER_ID,
             cast (null as string) as LACI_CLUSTER_NM,
             cast (null as string) as LACI_REGION_ID,

             cast (null as string) as LACI_REGION_NM,
             cast (null as string) as LACI_AREA_ID,
             cast (null as string) as LACI_AREA_NM,
             cast (null as string) as LACI_SALESAREA_ID,
             cast (null as string) as LACI_SALESAREA_NM,

             
             cast (null as string) as MGR_SVCCLS_ID,
             

             air.offerId as OFFER_ID, 
             air.areaName as OFFER_AREA_NAME,

            air.recordID as RECORD_ID,
            air.prcDT as PRC_DT,
            air.fileDT  FILE_DT,
            air.area as SRC_TP,
            air.tanggal as TRANSACTION_DATE, 
            air.jobID as JOB_ID
            

          from
          (SELECT *                   
            FROM  ha1ReprocessAirRefillJoinInquiryLoan  --ha1AirRefillJoinInquiryLoan                   
            WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null
          )  air
          left join 
          (select  svcUsgTp, lv4, lv5, revSign,revCode,lv9 from RevenueCode) revcod 
          on revcod.revCode = concat ( air.revenue_code)
          left join 
          (select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from  Mada
          ) mada 
           on revcod.lv9 = mada.acc and air.promo_package_code = mada.grpCd and (tanggal between effDt and endDt )      
          group by 
             air.tanggal, 
             air.msisdn,  
             air.prefix,
             air.city,
             air.provider_id,
             air.country,
             air.currentServiceclass, 
             air.promo_package_code,
             air.promo_package_name,
             air.brand_name,  
             air.revenue_code, 
             case when  service_src='VOUCHER SMS'  then  'VAS'
                  when  service_src='LOAN BALANCE' then  revcod.svcUsgTp 
             end,
             revcod.lv4,  
             revcod.lv5, 
             air.FileName ,   
             air.hlr_branch_nm,
             air.hlr_region_nm,             
            case
          	  when mada.acc is null then cast (null as string)
          	  when revcod.svcUsgTp = 'VOICE' then mada.voiceRev
          	  when revcod.svcUsgTp = 'SMS' then mada.smsRev
          	  --when 'DATA' then mada.dataRev
          	  when revcod.svcUsgTp = 'VAS' then mada.vasRev
          	  when revcod.svcUsgTp = 'OTHER' then mada.othRev
              when revcod.svcUsgTp = 'DISCOUNT' then mada.discRev
              when revcod.svcUsgTp = 'DROP' then 'No'
              ELSE 'UNKNOWN'
             end,
             air.originTimeStamp,     
             air.offerId, 
             air.areaName,

             air.jobID,
             air.recordID,
             air.prcDT,
             air.area,
             air.fileDT

             --air.svcClassName                  
          """
         );
       
     sorReprocessAirRefillLoan_01.registerTempTable("sorReprocessAirRefillLoan_01")
     println("SHOW SOR HASIL REJ REF")
     //sorReprocessAirRefillLoan_01.show();
     
      //Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_REV_DIR, "/SRC_TP=AIR_REFILL_LOAN/*/JOB_ID=" + jobID)
      //sorReprocessAirRefillLoan.write.partitionBy("SRC_TP","TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_REV_DIR);
      

     
  
     /*****************************************************************************************************
     * 
     *     
     * Step 08 : Reprocess Reject Reference 2 ( Reject Mada )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 08 :  Reprocess Reject Reference 2 ( Reject Mada )  ================"+goToMinute());
       
         /*
       * @checked if file exists or not
       * 
       */
       
       if (existDirRev != true ) {
            var stgReprocessAirRevLoan =  sc.makeRDD(Seq(Row(
                 null, // msisdn
                 null, // prefix
                 null, // CITY
                 null, // PROVIDER_ID
                 null, // COUNTRY
                 null, // SERVICECLASSID
                 null, // PROMO_PACKAGE_CODE
                 null, // PROMO_PACKAGE_NAME
                 null, // BRAND_NAME
                 null, // TOTAL_AMOUNT
                 null, // TOTAL_HITS
                 null, // REVENUE_CODE
                 null, // SERVICE_TYPE
                 null, // GL_CODE
                 null, // GL_NAME
                 null, // REAL_FILENAME
                 null, // HLR_BRANCH_NM
                 null, // HLR_REGION_NM
                 null, // REVENUE_FLAG
                 null, // ORIGIN_TIMESTAMP
                 null, // OFFER_ID
                 null, // OFFER_AREA_NAME
                 null, // RECORD_ID
                 null, // PRC_DT
                 null, // FILE_DT
                 null, // AREA
                 null, // TRANSACTION_DATE
                 null, // JOB_ID
                 null  // REJ_RSN
               )
              )
           )
       val stgReprocessRevAirLoan = sqlContext.createDataFrame(stgReprocessAirRevLoan, subAirRefill.stgReprocessAirLoanRejRevSchema);
       stgReprocessRevAirLoan.registerTempTable("stg_reprocess_rev_air_loan")
        
         
       
        log.warn("===============NULL DATA===================");
       
       
        
      }else{
        
        subReprocessRejRevInptRDD.collect foreach {case (a) => println (a)}
        var stgReprocessARevirAdj = subReprocessRejRevInptRDD.map(line => line.split("\\|")).map {p => Row(             
             p(0), //msisdn
             p(1), //prefix
             p(2), //CITY,
             p(3), //PROVIDER_ID,
             p(4), //COUNTRY,
             p(5), // SERVICECLASSID,
             p(6), //PROMO_PACKAGE_CODE,
             p(7), //PROMO_PACKAGE_NAME,
             p(8), //BRAND_NAME, 
             p(9), //TOTAL_AMOUNT,
             p(10), //TOTAL_HITS,
             p(11), //REVENUE_CODE,
             p(12), //SERVICE_TYPE,
             p(13), //GL_CODE,
             p(14), //GL_NAME, 
             p(15), //REAL_FILENAME,
             p(16), //HLR_BRANCH_NM,
             p(17), //HLR_REGION_NM,
             p(18), //REVENUE_FLAG,
             p(19), //ORIGIN_TIMESTAMP,
             p(20), //OFFER_ID, 
             p(21), //OFFER_AREA_NAME,
             p(22), // RECORD_ID,
             goPrcDate(), //prcDT,
             p(24), //FILE_DT
             p(25), // area,
             p(26), // TRANSACTION_DATE,
             jobID, //jobID,
             p(28)  // REJ_RSN 
          )
       }
        
        val stgReprocessRevAirAdjustment = sqlContext.createDataFrame(stgReprocessARevirAdj, subAirRefill.stgReprocessAirLoanRejRevSchema);
        stgReprocessRevAirAdjustment.registerTempTable("stg_reprocess_rev_air_loan")
//        stgReprocessRevAirAdjustment.show();
         log.warn("===============ANY DATA===================");
    }
      
     
       
     /*****************************************************************************************************
     * 
     *     
     * Step 09 : Reprocess - Join with Mada and Revenue Reference. ( Level 1 <-> Parent Level )
     * Join with used MA type
     * 
     *****************************************************************************************************/
     log.warn("================== Step 09 : Reprocess -  Join with Mada and Revenue Reference. ( Level 1 <-> Parent Level ) ================"+goToMinute());
     val sorReprocessAirRefillLoan_02 =  sqlContext.sql( 
          """
            SELECT 
             air.msisdn as MSISDN,  
             air.prefix as PREFIX,
             air.regBranchCity as CITY,
             air.provider_id as PROVIDER_ID,
             air.country as COUNTRY,
             air.currentServiceclass as SERVICECLASSID, 
             air.promo_package_code as PROMO_PACKAGE_CODE,
             air.promo_package_name as PROMO_PACKAGE_NAME,
             air.brand_name as BRAND_NAME,                 
             cast(sum(air.total_amount) as double) as TOTAL_AMOUNT,
             cast(count(*) as double) as TOTAL_HITS,
             air.revenue_code as REVENUE_CODE, 

             --case when  service_src='VOUCHER SMS'  then  'VAS'
             --     when  service_src='LOAN BALANCE' then  revcod.svcUsgTp 
             --end  
             service_type as SERVICE_TYPE,
 
             revcod.lv4 as GL_CODE,  
             revcod.lv5 as GL_NAME, 
             air.realFilename as REAL_FILENAME,   
             air.hlr_branch_nm as HLR_BRANCH_NM,
             air.hlr_region_nm as HLR_REGION_NM,
             
            case
          	  when mada.acc is null then cast (null as string)
          	  when revcod.svcUsgTp = 'VOICE' then mada.voiceRev
          	  when revcod.svcUsgTp = 'SMS' then mada.smsRev
          	  --when 'DATA' then mada.dataRev
          	  when revcod.svcUsgTp = 'VAS' then mada.vasRev
          	  when revcod.svcUsgTp = 'OTHER' then mada.othRev
              when revcod.svcUsgTp = 'DISCOUNT' then mada.discRev
              when revcod.svcUsgTp = 'DROP' then 'No'
              ELSE 'UNKNOWN'
             end as REVENUE_FLAG,

             air.originTimeStamp as ORIGIN_TIMESTAMP,

             cast (null as string) as MCCMNC,
             cast (null as string) as LAC,
             cast (null as string) as CI,
             cast (null as string) as LACI_CLUSTER_ID,
             cast (null as string) as LACI_CLUSTER_NM,
             cast (null as string) as LACI_REGION_ID,

             cast (null as string) as LACI_REGION_NM,
             cast (null as string) as LACI_AREA_ID,
             cast (null as string) as LACI_AREA_NM,
             cast (null as string) as LACI_SALESAREA_ID,
             cast (null as string) as LACI_SALESAREA_NM,

             
             cast (null as string) as MGR_SVCCLS_ID,
             

             air.offerId as OFFER_ID, 
             air.areaName as OFFER_AREA_NAME,

            air.recordID as RECORD_ID,
            air.prcDT as PRC_DT,
            air.fileDT  FILE_DT,
            air.area as SRC_TP,
            air.tanggal as TRANSACTION_DATE, 
            air.jobID as JOB_ID
            

          from
          (SELECT *                                    
            FROM  stg_reprocess_rev_air_loan                     
            WHERE prefix <> '' and prefix is not null and --currentServiceclass <> '' and currentServiceclass is not null and 
                  regBranchCity <> '' and regBranchCity is not null
          )  air
          left join 
          (select  svcUsgTp, lv4, lv5, revSign,revCode,lv9 from RevenueCode) revcod 
          on revcod.revCode = concat ( air.revenue_code)
          left join 
          (select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from  Mada
          ) mada 
           on revcod.lv9 = mada.acc and air.promo_package_code = mada.grpCd and (tanggal between effDt and endDt )      
          group by 
             air.tanggal, 
             air.msisdn,  
             air.prefix,
             air.regBranchCity,
             air.provider_id,
             air.country,
             air.currentServiceclass, 
             air.promo_package_code,
             air.promo_package_name,
             air.brand_name,  
             air.revenue_code, 
             --case when  service_src='VOUCHER SMS'  then  'VAS'
             --     when  service_src='LOAN BALANCE' then  revcod.svcUsgTp 
             --end
              service_type,
             revcod.lv4,  
             revcod.lv5, 
             air.realFilename ,   
             air.hlr_branch_nm,
             air.hlr_region_nm,             
            case
          	  when mada.acc is null then cast (null as string)
          	  when revcod.svcUsgTp = 'VOICE' then mada.voiceRev
          	  when revcod.svcUsgTp = 'SMS' then mada.smsRev
          	  --when 'DATA' then mada.dataRev
          	  when revcod.svcUsgTp = 'VAS' then mada.vasRev
          	  when revcod.svcUsgTp = 'OTHER' then mada.othRev
              when revcod.svcUsgTp = 'DISCOUNT' then mada.discRev
              when revcod.svcUsgTp = 'DROP' then 'No'
              ELSE 'UNKNOWN'
             end,
             air.originTimeStamp,     
             air.offerId, 
             air.areaName,

             air.jobID,
             air.recordID,
             air.prcDT,
             air.area,
             air.fileDT

             --air.svcClassName                  
          """
         );
                                                     
     sorReprocessAirRefillLoan_02.registerTempTable("sorReprocessAirRefillLoan_02")
        

     /*****************************************************************************************************
     * 
     *     
     * Step 10 : Union ALL Source Table
     * 
     *****************************************************************************************************/
     println("SHOW SOR HASIL REJ MADA")
     //sorReprocessAirRefillLoan_02.show();
     println("SHOW SOR HASIL REJ REF")
     //sorReprocessAirRefillLoan_01.show();
     
     log.warn("==================Step 10 :  Union ALL Source Table ================"+goToMinute());
      val airLoanDetail = sqlContext.sql("""
          select * from sorReprocessAirRefillLoan_02
          UNION  
          select * from sorReprocessAirRefillLoan_01          
        """)
        
       airLoanDetail.registerTempTable("air_loan_detail")  
       println("SHOW SOR UNION")
       //airLoanDetail.show()
       
       
     /*****************************************************************************************************
     * 
     *     
     * Step 11 : Reprocess - Produce for Reject Rev 2 (MADA)
     * 
     *****************************************************************************************************/ 
     log.warn("================== Step 11 : Reprocess - Produce for Reject Rev (LOAD TO HIVE TABLE) ================"+goToMinute());  
      val rejReprocessRevAirRef_02=sqlContext.sql( 
        """
           SELECT 
             MSISDN,  
             PREFIX,
             CITY,
             PROVIDER_ID,
             COUNTRY,
             SERVICECLASSID, 
             PROMO_PACKAGE_CODE,
             PROMO_PACKAGE_NAME,
             BRAND_NAME,                 
             TOTAL_AMOUNT,
             TOTAL_HITS,
             REVENUE_CODE, 

             SERVICE_TYPE,
 
             GL_CODE,  
             GL_NAME, 
             REAL_FILENAME,   
             HLR_BRANCH_NM,
             HLR_REGION_NM,
             
             REVENUE_FLAG,
             ORIGIN_TIMESTAMP,
             
             OFFER_ID, 
             OFFER_AREA_NAME,

             RECORD_ID,
             PRC_DT,
             FILE_DT,
             SRC_TP,
             TRANSACTION_DATE, 
             JOB_ID,
              case 
                 when SERVICE_TYPE is null then 'Revenue Code is Not Found' 
                 when REVENUE_FLAG is null then 'Mada is Not Found'
                 else 'Reject Ref' end as REJ_RSN 
            from air_loan_detail ref
            where SERVICE_TYPE is NULL or SERVICE_TYPE = '' or
                  REVENUE_FLAG is NULL or REVENUE_FLAG = ''
        """
         );

     rejReprocessRevAirRef_02.registerTempTable("rejReprocessRevAirRef_02")
     println("SHOW REJ REF REVCODE MADA")
     //rejReprocessRevAirRef_02.show();

       Common.cleanDirectory(sc, OUTPUT_REJ_REV_DIR );
       rejReprocessRevAirRef_02.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REV_DIR);
     
     /*****************************************************************************************************
     * 
     *     
     * Step 12 : Reprocess - Final SOR Air Loan Balance. ( Level 1 <-> Parent Level )
     * Join with used MA type
     * 
     *****************************************************************************************************/
     log.warn("================== Step 12 : Reprocess -  Final SOR Air Loan Balance. ( Level 1 <-> Parent Level ) ================"+goToMinute());
     val sorFinReprocessAirRefillLoan =  sqlContext.sql( 
          """
            SELECT 
             MSISDN,  
             PREFIX,
             CITY,
             PROVIDER_ID,
             COUNTRY,
             SERVICECLASSID, 
             PROMO_PACKAGE_CODE,
             PROMO_PACKAGE_NAME,
             BRAND_NAME,                 
             TOTAL_AMOUNT,
             TOTAL_HITS TOTAL_HIT,
             REVENUE_CODE, 

             SERVICE_TYPE,
 
             GL_CODE,  
             GL_NAME, 
             REAL_FILENAME,   
             HLR_BRANCH_NM,
             HLR_REGION_NM,
             
             REVENUE_FLAG,

             ORIGIN_TIMESTAMP,

             cast (null as string) as MCCMNC,
             cast (null as string) as LAC,
             cast (null as string) as CI,
             cast (null as string) as LACI_CLUSTER_ID,
             cast (null as string) as LACI_CLUSTER_NM,
             cast (null as string) as LACI_REGION_ID,

             cast (null as string) as LACI_REGION_NM,
             cast (null as string) as LACI_AREA_ID,
             cast (null as string) as LACI_AREA_NM,
             cast (null as string) as LACI_SALESAREA_ID,
             cast (null as string) as LACI_SALESAREA_NM,

             
             cast (null as string) as MGR_SVCCLS_ID,
             

             OFFER_ID, 
             OFFER_AREA_NAME,

              RECORD_ID,
              PRC_DT,
              FILE_DT,
              SRC_TP,
              TRANSACTION_DATE, 
              JOB_ID
                 
            from air_loan_detail air
            where SERVICE_TYPE is not null and SERVICE_TYPE <> '' and 
                  REVENUE_FLAG is not null and REVENUE_FLAG <> ''
        """
         );
     
      sorFinReprocessAirRefillLoan.registerTempTable("sor_Fin_Reprocess_air_refillLoan")
      println("SHOW SOR FINAL ")
      //sorFinReprocessAirRefillLoan.show();
     
      Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_REV_DIR, "/SRC_TP=AIR_LOAN/*/JOB_ID=" + jobID)
      sorFinReprocessAirRefillLoan.write.partitionBy("SRC_TP","TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_REV_DIR);
      
     
    sc.stop();
 }
    
     def getTuppleRecord(content:String) : String = {
       var line : String = "";
       //val valueValid = Common.getTupleInTupleNonUsageRefill(content,"|",9,"#",7,"]",0,4)  
        val valueValid = Common.getTupleInTupleNonUsageRefill(content,"#",7,"]",0,4)  
        for (x <- valueValid){
          line =  line + x  
          ///println(x)
       }
       return line;
     }
     
      def getTuppleRecordCdrInquiry(content:String) : String = {
       var line : String = "";
       //val valueValid = Common.getTupleInTupleNonUsageRefill(content,"|",9,"#",7,"]",0,4)  
        val valueValid = getTupleInTupleNonUsageRefillCdrInquiry(content,"#",7,"]",0,4)  
        for (x <- valueValid){
          line =  line + x  
          ///println(x)
       }
       return line;
     }
     
     def goPrcDate() : String = {
         val today = Calendar.getInstance().getTime
         val curTimeFormat = new SimpleDateFormat("YYYYMMdd")
         val prcDt = curTimeFormat.format(today).toString()
         return prcDt;
     }
     
     def goToMinute() : String = {
         val today = Calendar.getInstance().getTime
         val curTimeFormat = new SimpleDateFormat("YYYYMMdd HH:MM:SS")
         val prcDt = curTimeFormat.format(today).toString()
         return prcDt;
     }
     
     
     def getInquiryRegular(content:String,flag:String) : String = {
       var line : String = ""; 
       println(content);
       if (!content.equalsIgnoreCase("")) {
         if (flag.equalsIgnoreCase("REGULAR")){
             line = Common.getTuple(content,"#",1) ;
         }else{
             line = content.split(']').map { x => x.split('*')(0).concat("#").concat((x.split('*')(5)))}.mkString("[")
         }
             line = "";
       }
       return line;
     }
     
     def getTupleInTupleNonUsageRefillCdrInquiry(str:String,delLV2:String,tupleLV2Int:Int,delLV3:String,tupleLV2Num01:Int,tupleLV2Num04:Int): Array[String] ={

       
       try{

  	    var LV2_ARR=str.split("[\\"+delLV2+"]");
    		if(LV2_ARR.length < tupleLV2Int){
    			return null;
    		}
    		var tupleLV2 =LV2_ARR(tupleLV2Int);
    		
    		var LV3_ARR=tupleLV2.split("\\"+delLV3+"");
    		var line:Array[String] = new Array[String](LV3_ARR.length);
  	  	var ch:Int=0;
     		while(ch < LV3_ARR.length){
      		  var tupple1 = Common.getTuple(LV3_ARR(ch),"*",tupleLV2Num01);
      		  if ( Common.getTuple(LV3_ARR(ch),"*",tupleLV2Num01).equals("")){
      		    tupple1="0";
      		  }
      		  var tupple3 = Common.getTuple(LV3_ARR(ch),"*",tupleLV2Num04);
      		  if ( Common.getTuple(LV3_ARR(ch),"*",tupleLV2Num04).equals("")){
      		    tupple3="0";
      		  }else{
      		    tupple3 = (Common.getTuple(LV3_ARR(ch),"*",tupleLV2Num04).toDouble/100).toString();
      		  }
      		  if (ch == LV3_ARR.length - 1){
      		    line (ch) = tupple1.concat("#").concat(tupple3)
      		  }else{
      		    line (ch) = tupple1.concat("#").concat(tupple3).concat("[")  
      		  }
      		  
      		  ch=ch+1;
      		}	
    
      		return line;
  	  }
	    catch {
	      case e: Exception => return null
	    }
	  }
     
     
}
