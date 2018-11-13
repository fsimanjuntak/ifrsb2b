package com.ibm.id.isat.nonUsage.air

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import com.ibm.id.isat.utils.ReferenceDF;
import org.apache.spark.sql.Row;
import com.ibm.id.isat.nonUsage.air.model.subAirRefill;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import com.ibm.id.isat.utils.Common;

object subAirRefillReprocess {
  

   def main(args: Array[String]) {
         
     
//       val sc = new SparkContext("local", "sub refill transformation spark ", new SparkConf());
       val sc = new SparkContext(new SparkConf())
       val sqlContext = new org.apache.spark.sql.SQLContext(sc)
       sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
       
       import org.apache.log4j._
       Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.OFF)
       Logger.getLogger("org").setLevel(Level.ERROR)
       Logger.getLogger("akka").setLevel(Level.ERROR)
       val log = LogManager.getRootLogger
         
       
       // Import data frame library.
       import sqlContext.implicits._
       
       
       if (args.length != 5) {
          //println("Adjustment: [job_id] [inputfile] [inputfile02] [rejectRefFile] [rejectAllFile] [sorResultFile]")
         println("Refill: [job_id] [prcDT] [CONFIG] [inputfile01] [inputfile02]  ")
          sys.exit(1);
       }
  
       val prcDt = args(0)
       val jobID = args(1)
       val configDir = args(2)
       val inputFileRef = args(3)
       val inputFileRev = args(4)
     
       //val configDir = "/user/apps/nonusage/air/reference/AIR.conf"
       val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_REFFILL_REJ_BP_DIR").concat(prcDt).concat("_").concat(jobID)
       val OUTPUT_REJ_REF_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_REFFILL_REJ_REF_DIR").concat(prcDt).concat("_").concat(jobID)
       val OUTPUT_REJ_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_REFFILL_REJ_REV_DIR").concat(prcDt).concat("_").concat(jobID)
       //val OUTPUT_CDR_INQ_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_CDR_INQ_DIR").concat(prcDt).concat("_").concat(jobID)
       val OUTPUT_DETAIL_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_DETAIL_DIR")

         
      /* val prcDt =  "20160722"
       val jobID = "10"
       val configDir = "C:/testing"
       val inputFileRef = "C:/testing/reprocess_ref/rej_ref.REF"
       val inputFileRev = "C:/testing/reprocess_rev/rej_rev.REF"
       val OUTPUT_REJ_REF_DIR = "C:/testing/reprocess/rej_ref"
       val OUTPUT_REJ_REV_DIR = "C:/testing/reprocess/rej_rev"
       val OUTPUT_DETAIL_REV_DIR ="C:/testing/reprocess/sor"
       System.setProperty("hadoop.home.dir", "C:\\winutil\\")  */
       
       val existDir= Common.isDirectoryExists(sc, inputFileRef);
       var subReprocessRejRefInptRDD = sc.textFile(inputFileRef);  
     
       val existDirRev= Common.isDirectoryExists(sc, inputFileRev);
       var subReprocessRejRevInptRDD = sc.textFile(inputFileRev);  
     
       /*****************************************************************************************************
       * 
       * Load Reference Data
       * 
       *****************************************************************************************************/
       log.warn("===============STEP 01. LOAD REFERENCE===================");
       
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
        svcClassOfferDF.count()
        
       /*
       * 
       * These below references is not used.
       * These below references is used for getting cdr inquiry data.
       * 
       */
      
        /*val reloadTypeDF = ReferenceDF.getRecordReloadTypeDF(sqlContext)   
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
        bankDetailDF.count()*/
  
         /*
         * @checked if file exists or not
         * 
         */
  
        /*if (existDir != true ){
             var inputReprocessRejRef = sc.makeRDD(Seq(Row(
               null, //origin node type
               null, //origin host name  
               null, //timestamp
               null, //accountNumber
               null, //refill Type
               null, //refill Profile
               null, //externalData1
               null, //transaction amount 
               null, //subscriber number
               null, //current service class
               null, //msisdn PAD
               null, //real filename
               null, //dedicated account
               null, //origin time stamp
               null, //accountFlagBefore
               null, //accountFlagAfter
               null, //jobID
               null, //recordID
               null, //prcDT
               null, //area
               null, //fileDT
               null, //activationDate
               null, //voucherBasedRefill
               null //rejRsn
               )
             )
           )
           val stgReprocessRejRef = sqlContext.createDataFrame(inputReprocessRejRef, subAirRefill.stgReprocessAirRejRefSchema);
           stgReprocessRejRef.registerTempTable("stg_air_rej_ref");
           stgReprocessRejRef.persist();
            log.warn("===============NULL DATA===================");
        }else{ 
          var inputReprocessRejRef = subReprocessRejRefInptRDD.filter ( x => x.count (_ == '|') == 22).map(line => line.split("\\|")).map {p => Row(
           p(0), //origin node type
           p(1), //origin host name  
           p(2), //timestamp
           p(3), //accountNumber
           p(4), //refill Type
           p(5), //refillProfileID
           p(4), //externalData1
           p(5), //transaction amount 
           p(6), //subscriber number
           p(7), //current service class
           p(8), //msisdn PAD
           p(9), //real filename
           p(10), //dedicated account
           p(11), //origin time stamp
           p(12), //accountFlagBefore
           p(13), //accountFlagAfter
           jobID, //jobID
           p(15), //recordID
           goPrcDate(), //prcDT
           p(17), //area
           p(18), //fileDT
           p(19), //activationDate
           p(20), //voucherBasedRefill
           p(21) //rejRsn
          )
         }
           val stgReprocessRejRef = sqlContext.createDataFrame(inputReprocessRejRef, subAirRefill.stgReprocessAirRejRefSchema);
           stgReprocessRejRef.registerTempTable("stg_air_rej_ref");
           stgReprocessRejRef.persist();
            log.warn("===============ANY DATA===================");
        }*/
          
        var inputReprocessRejRef = subReprocessRejRefInptRDD.filter ( x => x.count (_ == '|') == 22).map(line => line.split("\\|")).map {p => Row(
           p(0), //origin node type
           p(1), //origin host name  
           p(2), //timestamp
           p(3), //accountNumber
           p(4), //refill Type
           p(5), //refillProfileID
           p(4), //externalData1
           p(5), //transaction amount 
           p(6), //subscriber number
           p(7), //current service class
           p(8), //msisdn PAD
           p(9), //real filename
           p(10), //dedicated account
           p(11), //origin time stamp
           p(12), //accountFlagBefore
           p(13), //accountFlagAfter
           jobID, //jobID
           p(15), //recordID
           goPrcDate(), //prcDT
           p(17), //area
           p(18), //fileDT
           p(19), //activationDate
           p(20), //voucherBasedRefill
           p(21) //rejRsn
          )
         }
           val stgReprocessRejRef = sqlContext.createDataFrame(inputReprocessRejRef, subAirRefill.stgReprocessAirRejRefSchema);
           stgReprocessRejRef.registerTempTable("stg_air_rej_ref");
           stgReprocessRejRef.persist();
           log.warn("===============ANY DATA===================");
 
     
       /*****************************************************************************************************
       * 
       *     
       * Step 02 : Join Reprocess with Interconnect and Branch. ( Level 1 )
       * 
       *****************************************************************************************************/
       log.warn("================== Step 02 : Join with Interconnect and Branch. ( Level 1 ) ================"+goToMinute());
       val ha1AirRefill01 =  sqlContext.sql( 
            """
            SELECT 
            air.originNodeType,
              air.originNodeID,  
              air.timeStamp ,
              air.accountNumber,
              air.refillType,
              air.refillProfileID,
              air.externalData1,
              air.transactionAmount,
              air.subscriberNumber,
              air.currentServiceclass,
              air.msisdnPad,        
              air.realFilename,   
              air.originTimeStamp , 
              air.accountInformationBeforeRefill,
              air.accountInformationAfterRefill,
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
              svcclsofr.offerId, svcclsofr.areaName,
              substring(timeStamp,1,8) event_dt,
              activationDate,
              voucherBasedRefill
            from
            (
               SELECT * FROM stg_air_rej_ref a WHERE currentServiceclass IS NOT NULL OR accountNumber IS NOT NULL OR timeStamp IS NOT NULL OR originTimeStamp IS NOT NULL OR voucherBasedRefill = 'false'  
            )  air
            left join SDPOffer sdp on air.accountNumber = sdp.msisdn and (substring(air.timeStamp,1,8) between sdp.effDt and sdp.endDt )
            left join ServiceClassOffer svcclsofr on air.currentServiceclass = svcclsofr.svcClassCode 
              and (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and sdp.offerID = svcclsofr.offerId
            left join ServiceClass svccls on air.currentServiceclass = svccls.svcClassCode 
              and (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt )
            """
           );
          
         ha1AirRefill01.registerTempTable("ha1_reprocess_air_refill01")
         ha1AirRefill01.persist()
  //     stgReprocessRejRef.unpersist();  
         svcClassDF.unpersist()
         sdpOfferDF.unpersist()
         intcctDF.unpersist();
         regionBranchDF.unpersist();
       
        val joinIntcctDF = Common.getIntcct(sqlContext, ha1AirRefill01, intcctDF, "accountNumber", "event_dt", "prefix", "country", "city", "provider_id") 
        joinIntcctDF.registerTempTable("HA1AirRefillJoinIntcct")
 
        val ha1AirRefill = sqlContext.sql(
            """
            select
            air.originNodeType,
            air.originNodeID,  
            air.timeStamp ,
            air.accountNumber,
            air.refillType,
            air.refillProfileID,
            air.externalData1,
            air.transactionAmount,
            air.subscriberNumber,
            air.currentServiceclass,
            air.msisdnPad,        
            air.realFilename,   
            air.originTimeStamp , 
            air.accountInformationBeforeRefill,
            air.accountInformationAfterRefill,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT,
            air.prefix,
            air.country,
            air.city,
            air.provider_id,
            air.svcClassName ,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            branch hlr_branch_nm, 
            region hlr_region_nm,
            regBranch.city regBranchCity,
            air.activationDate,
            air.offerId, 
            air.areaName,
            air.voucherBasedRefill
            from HA1AirRefillJoinIntcct air
            left join  RegBranch regBranch on air.city = regBranch.city
            """);
      ha1AirRefill.registerTempTable("ha1_air_refill")
      ha1AirRefill.persist();
      
      
      
    /*****************************************************************************************************
     * 
     *     
     * Step 03 : Produce for Reject Reference.
     * 
     *****************************************************************************************************/
     log.warn("================== Step 03 : Produce for Reject Reference. ================"+goToMinute());
     
     val rejectRef = sqlContext.sql( 
         """
            SELECT 
            air.originNodeType ORIGINNODETYPE,
            air.originNodeID ORIGINNODEID, 
            air.timeStamp TIMESTAMP,
            air.accountNumber ACCOUNTNUMBER,
            air.refillType REFILLTYPE,
            air.refillProfileID REFILLPROFILEID,
            air.externalData1 EXTERNALDATA1,
            air.transactionAmount TRANSACTIONAMOUNT,
            air.subscriberNumber SUBSCRIBERNUMBER,
            air.currentServiceclass CURRENTSERVICECLASS,
            air.msisdnPad MSISDNPAD,        
            air.realFilename REALFILENAME,   
            air.originTimeStamp ORIGINTIMESTAMP, 
            air.accountInformationBeforeRefill ACCOUNTINFORMATIONBEFOREREFILL,
            air.accountInformationAfterRefill ACCOUNTINFORMATIONAFTERREFILL,
            air.jobID JOBID,
            air.recordID RECORDID,
            air.prcDT PRCDT,
            air.area AREA,
            air.fileDT FILEDT,
            air.activationDate ACTIVATIONDATE,
            air.voucherBasedRefill VOUCHERBASEDREFILL,
            case when prefix is null then 'Prefix Not Found'
              when svcClassName is null then 'Service Class Not Found'
              when regBranchCity is null then 'City Found'
              end as REJ_RSN
           FROM ha1_air_refill air WHERE prefix = '' or prefix is null or svcClassName = '' or svcClassName is null or regBranchCity = '' or regBranchCity is null
         """
         );
     
      Common.cleanDirectory(sc, OUTPUT_REJ_REF_DIR );
      rejectRef.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REF_DIR);  
         
         
         
     /*****************************************************************************************************
     * 
     *     
     * Step 04 :  Step 04 : Join with MADA reference ( Parent Level )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 04 : Join with MADA reference  ================"+goToMinute());
      
     val sorSubAirReprocessParent =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            null refillType,
            null refillProfileID,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( transactionAmount * (-1) ) / 100
              else
              transactionAmount / 100 
            end as total_amount,
            count(*) as total_hits,
            concat ( externalData1 , 'MA') revenue_code,
            revcod.svcUsgTp as service_type, 
            revcod.lv4 as gl_code,  
            revcod.lv5 as gl_name, 
            air.realFilename as real_filename,
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
            end as revenueflag,
            air.originTimeStamp as origin_timestamp,
            null as mccmnc,
            null as lac,
            null as ci,
            null as laci_cluster_id,
            null as laci_cluster_nm,
            null as laci_region_id,
            null as laci_region_nm,
            null as laci_area_id,
            null as laci_area_nm,
            null as laci_salesarea_id,
            null as laci_salesarea_nm,
            null as mgr_svccls_id,
            air.offerId offerId,
            air.areaName areaName,
            --null as pname,
            --null as load_job_id,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          from
          (
            SELECT * FROM  ha1_air_refill WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null 
          )  air
          left join 
          (
            select  svcUsgTp, lv4, lv5, revSign,revCode,lv9 from RevenueCode
          ) revcod on revcod.revCode = concat ( air.externalData1 , 'MA')
          left join 
          (
            select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from  Mada
          ) mada 
           on revcod.lv9 = mada.acc and air.promo_package_code = mada.grpCd and (substring(air.timeStamp,1,8) between effDt and endDt )
          group by 
            substring(air.timeStamp,1,8) ,
            air.accountNumber,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( transactionAmount * (-1) ) / 100
              else
              transactionAmount / 100 
            end,
            concat ( externalData1 , 'MA') ,
            revcod.svcUsgTp , 
            revcod.lv4 ,  
            revcod.lv5 , 
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
            end ,
            air.originTimeStamp ,
            air.offerId,
            air.areaName,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          """
         );
     sorSubAirReprocessParent.registerTempTable("ha1_sor_air_parent_mada")
    
       
     
      /*****************************************************************************************************
       * 
       *     
       * Step 05 : Transpose for Level 2. ( SOR LEVEL : Before and After Refill)
       * 
       *****************************************************************************************************/
       log.warn("==================Step 05 : Transpose for Level 2. ( SOR LEVEL : Before and After Refill) ================"+goToMinute());
           
       val transposeBeforeRefillRDD = ha1AirRefill.where("prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null and accountInformationBeforeRefill is not null and accountInformationBeforeRefill <> '' ")
            .map { row => row(0) + "|" + row(1) + "|" +row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" +  row(9) + "|" +  row(10) + "|" +  
              row(11) + "|" + row(12)  + "|" + row(13) + "|" + row(14) + "|" + row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
              row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27) + "|" + row(28) + "|" + row(29) + "|" + row(30) +  "|" + row(31) + "|" + row(32) + "|" + row(33) +
              "~" + getTuppleRecord(row(13).toString())      
              }
          //transposeBeforeRefillRDD.collect foreach {case (a) => println (a)}; 
          //+ "|" +  row(13) + "|" + row(14) 
           
          val resultBeforeRefillTransposeRDD = transposeBeforeRefillRDD.map(line => line.split("\\~")).map(field => (field(0), field(1)))
          .flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
          .map { replaced => replaced.replace("@", "|") };
          //resultBeforeRefillTransposeRDD.collect foreach {case (a) => println (a)}; 
          
          val ha1SubBeforeReffillInptDF = resultBeforeRefillTransposeRDD.filter ( x => x.count (_ == '|') == 35).filter(line => ! line.split("\\|")(34).equals("xxx") &&  ! line.split("\\|")(35).equals("xxx") ).map(line => line.split("\\|")).map { p => Row( 
             p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), 
             p(23),p(24),p(25), p(26), p(27),p(28),p(29), p(30),p(31), p(32), p(33), p(34), p(35)           
             )
           }
           
          val ha1SubBeforeRef = sqlContext.createDataFrame(ha1SubBeforeReffillInptDF, subAirRefill.ha1SubAirRefSchema)
          ha1SubBeforeRef.registerTempTable("ha1_sub_air_before_ref")
          

          val transposeAfterRefillRDD = ha1AirRefill.where("prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null and accountInformationBeforeRefill is not null and accountInformationBeforeRefill <> '' ")
            .map { row => row(0) + "|" + row(1) + "|" + row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" +  row(9) + "|" +  row(10) + "|" + 
              row(11) + "|" + row(12)  + "|" + row(13) + "|" + row(14) + "|" +  row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
              row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27)  + "|" + row(28) + "|" + row(29) + "|" + row(30) +  "|" + row(31) + "|" + row(32) + "|" + row(33) +     
              "~" + getTuppleRecord(row(14).toString())      
          }
          //transposeAfterRefillRDD.collect foreach {case (a) => println (a)};
          //  "|" +  row(13) + "|" + row(14) +
          
          val resultAfterRefillTransposeRDD = transposeAfterRefillRDD.map(line => line.split("\\~")).map(field => (field(0), field(1)))
          .flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
          .map { replaced => replaced.replace("@", "|") };
          //resultAfterRefillTransposeRDD.collect foreach {case (a) => println (a)}; 
          
          val ha1SubAfterReffillInptDF = resultAfterRefillTransposeRDD.filter ( x => x.count (_ == '|') == 35).filter(line => ! line.split("\\|")(34).equals("xxx") &&  ! line.split("\\|")(35).equals("xxx") ).map(line => line.split("\\|")).map { p => Row( 
             p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), 
             p(23),p(24),p(25), p(26), p(27),p(28),p(29), p(30),p(31), p(32), p(33) 
             )
           }
          val ha1SubAfterRef = sqlContext.createDataFrame(ha1SubAfterReffillInptDF, subAirRefill.ha1SubAirRefSchema)
          ha1SubAfterRef.registerTempTable("ha1_sub_air_after_ref");
          
      
     /*****************************************************************************************************
     * 
     *     
     * Clean Memory Cache
     * 
     *****************************************************************************************************/ 
     ha1AirRefill.unpersist();     
     ha1AirRefill01.unpersist();
          
          
     /*****************************************************************************************************
     * 
     *     
     * Step 06 : Union Parent with Sub Air
     * 
     *****************************************************************************************************/
    log.warn("================== Step 06 : Union Parent with Sub Air ================"+goToMinute());
    
    val sorAirRefillUnion = sqlContext.sql("""
        select * from ha1_sub_air_before_ref
        UNION ALL
        select * from ha1_sub_air_after_ref
      """)
    sorAirRefillUnion.registerTempTable("ha1_sor_air_refill_union")
    
    
       
     /*****************************************************************************************************
     * 
     *     
     * Step 07a : Produce for Level 2 ( Join With Mada and Reference) 
     * Get DA values
     * 
     *****************************************************************************************************/    
    log.warn("================== Step 07a : Produce for Level 2 ( Join With Mada and Reference)  ================"+goToMinute());
    val sorSubAirReprocessChildTemp =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) timeStamp,
            air.accountNumber,
            air.refillType,
            air.refillProfileID,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( tupple5 * (-1) ) / 100
              else
              tupple5 / 100 
            end as total_amount,
            concat(externalData1,concat('DA' , tupple1)) revenue_code,
            revcod.svcUsgTp as service_type, 
            revcod.lv4 as gl_code,  
            revcod.lv5 as gl_name,  
            air.realFilename as real_filename,
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
            end as revenueflag,
            air.originTimeStamp as origin_timestamp,
            null as mccmnc,
            null as lac,
            null as ci,
            null as laci_cluster_id,
            null as laci_cluster_nm,
            null as laci_region_id,
            null as laci_region_nm,
            null as laci_area_id,
            null as laci_area_nm,
            null as laci_salesarea_id,
            null as laci_salesarea_nm,
            null as mgr_svccls_id,
            air.offerId offerId,
            air.areaName areaName,
            --null as pname,
            --null as load_job_id,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          from
          (
            SELECT * FROM  ha1_sor_air_refill_union WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null 
          )  air
          left join 
          (
            select  svcUsgTp, lv4, lv5, revSign,revCode,lv9 from RevenueCode
          ) revcod on revcod.revCode = concat(externalData1,concat('DA' , tupple1))
          left join 
          (
            select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from  Mada
          ) mada 
           on revcod.lv9 = mada.acc and air.promo_package_code = mada.grpCd and (substring(air.timeStamp,1,8) between effDt and endDt )
         
          """
         );
     sorSubAirReprocessChildTemp.registerTempTable("ha1_sor_air_child_temp")
       
    
    /*****************************************************************************************************
     * 
     *     
     * Step 07b : Produce for Level 2 ( Join With Mada and Reference) 
     * Get DA values
     * 
     *****************************************************************************************************/    
    log.warn("================== Step 07b : Produce for Level 2 ( Join With Mada and Reference)  ================"+goToMinute()); 

    val sorSubAirReprocessChild01 =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            air.refillType,
            air.refillProfileID,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            sum(total_amount) total_amount,
            count(*) as total_hits,
            revenue_code,
            service_type, 
            gl_name,  
            gl_code, 
            air.real_filename,
            air.hlr_branch_nm,
            air.hlr_region_nm,
            air.revenueflag,
            air.origin_timestamp,
            mccmnc,
            lac,
            ci,
            laci_cluster_id,
            laci_cluster_nm,
            laci_region_id,
            laci_region_nm,
            laci_area_id,
            laci_area_nm,
            laci_salesarea_id,
            laci_salesarea_nm,
            mgr_svccls_id,
            air.offerId,
            air.areaName,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          FROM  ha1_sor_air_child_temp air
          group by 
            substring(air.timeStamp,1,8),
            air.accountNumber,
            air.refillType,
            air.refillProfileID,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            revenue_code,
            service_type, 
            gl_name,  
            gl_code, 
            air.real_filename,
            air.hlr_branch_nm,
            air.hlr_region_nm,
            air.revenueflag,
            air.origin_timestamp,
            mccmnc,
            lac,
            ci,
            laci_cluster_id,
            laci_cluster_nm,
            laci_region_id,
            laci_region_nm,
            laci_area_id,
            laci_area_nm,
            laci_salesarea_id,
            laci_salesarea_nm,
            mgr_svccls_id,
            air.offerId,
            air.areaName,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          """
         );
     sorSubAirReprocessChild01.registerTempTable("ha1_sor_air_child01")  
    
       
          
     /*****************************************************************************************************
     * 
     *     
     * Process for Reject Revenue
     * 
     *****************************************************************************************************/   
       
        
        
      /*
       * @checked if file exists or not
       * 
       */
    /*   if (existDirRev != true ){
         var inputReprocessRejRev = sc.makeRDD(Seq(Row(
             null, //TRANSACTION_DATE
             null, //msisdn  
             null, //prefix
             null, //city
             null, //provider id
             null, //country 
             null, //svcClassName
             null, //promo_package_code
             null, //promo_package_name
             null, //brand_name
             null, //total_amount
             null, //total_hits
             null, //revenue_code
             null, //service_type
             null, //gl_code
             null, //gl_name
             null, //real_filename
             null, //hlr_branch_nm
             null, //hlr_region_nm
             null, //revenueflag
             null, //origin_timestamp
             null, //offerId
             null, //areaName
             null, //jobID
             null, //recordID
             null, //prcDT
             null, //area
             null //fileDT
             )
           )
       )
         val stgReprocessRejRev = sqlContext.createDataFrame(inputReprocessRejRev, subAirRefill.stgReprocessAirRejRevSchema);
         stgReprocessRejRev.registerTempTable("stg_repcrocess_rej_rev")
//         stgReprocessRejRev.show();
          log.warn("===============NULL DATA===================");
       }else{
         var inputReprocessRejRev = subReprocessRejRevInptRDD.map(line => line.split("\\|")).map {p => Row(
             p(0), //TRANSACTION_DATE
             p(1), //msisdn  
             p(2), //prefix
             p(3), //city
             p(4), //provider id
             p(5), //country 
             p(6), //svcClassName
             p(7), //promo_package_code
             p(8), //promo_package_name
             p(9), //brand_name
             p(10), //total_amount
             p(11), //total_hits
             p(12), //revenue_code
             p(13), //service_type
             p(14), //gl_code
             p(15), //gl_name
             p(16), //real_filename
             p(17), //hlr_branch_nm
             p(18), //hlr_region_nm
             p(19), //revenueflag
             p(31), //origin_timestamp
             p(33), //offerId
             p(34), //areaName
             jobID, //jobID
             p(36), //recordID
             goPrcDate(), //prcDT
             p(38), //area
             p(39) //fileDT
          )
         }
         
         val stgReprocessRejRev = sqlContext.createDataFrame(inputReprocessRejRev, subAirRefill.stgReprocessAirRejRevSchema);
         stgReprocessRejRev.registerTempTable("stg_repcrocess_rej_rev")
//       stgReprocessRejRev.show();
        log.warn("===============ANY DATA===================");
       }*/
     
       var inputReprocessRejRev = subReprocessRejRevInptRDD.map(line => line.split("\\|")).map {p => Row(
             p(0), //TRANSACTION_DATE
             p(1), //msisdn  
             p(2), //prefix
             p(3), //city
             p(4), //provider id
             p(5), //country 
             p(6), //svcClassName
             p(7), //promo_package_code
             p(8), //promo_package_name
             p(9), //brand_name
             p(10), //total_amount
             p(11), //total_hits
             p(12), //revenue_code
             p(13), //service_type
             p(14), //gl_code
             p(15), //gl_name
             p(16), //real_filename
             p(17), //hlr_branch_nm
             p(18), //hlr_region_nm
             p(19), //revenueflag
             p(31), //origin_timestamp
             p(33), //offerId
             p(34), //areaName
             jobID, //jobID
             p(36), //recordID
             goPrcDate(), //prcDT
             p(38), //area
             p(39) //fileDT
          )
         }
         
         val stgReprocessRejRev = sqlContext.createDataFrame(inputReprocessRejRev, subAirRefill.stgReprocessAirRejRevSchema);
         stgReprocessRejRev.registerTempTable("stg_repcrocess_rej_rev")
//       stgReprocessRejRev.show();
        log.warn("===============ANY DATA===================");
       
     /*****************************************************************************************************
     * 
     *     
     * Step 08 : Produce for Level 2 ( SOR LEVEL )
     * 
     *****************************************************************************************************/       
    log.warn("==================Step 08 : Produce for Level 2 ( SOR LEVEL ) ================"+goToMinute());
      
    val sorSubRejRev =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            null refillType,
            null refillProfileID,
            air.prefix,
            air.regBranchCity city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            air.total_amount,
            count(*) as total_hits,
            air.revenue_code revenue_code,
            revcod.svcUsgTp as service_type, 
            revcod.lv4 as gl_name,  
            revcod.lv5 as gl_code, 
            air.realFilename as real_filename,
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
            end as revenueflag,
            air.originTimeStamp as origin_timestamp,
            null as mccmnc,
            null as lac,
            null as ci,
            null as laci_cluster_id,
            null as laci_cluster_nm,
            null as laci_region_id,
            null as laci_region_nm,
            null as laci_area_id,
            null as laci_area_nm,
            null as laci_salesarea_id,
            null as laci_salesarea_nm,
            null as mgr_svccls_id,
            air.offerId ,
            air.areaName ,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          from
          (
            SELECT * FROM  stg_repcrocess_rej_rev 
            WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null 
          )  air
          left join 
          (
            select  svcUsgTp, lv4, lv5, revSign,revCode,lv9 from RevenueCode
          ) revcod on revcod.revCode = air.revenue_code
          left join 
          (
            select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from  Mada
          ) mada 
           on revcod.lv9 = mada.acc and air.promo_package_code = mada.grpCd and (substring(air.timeStamp,1,8) between effDt and endDt )
          group by 
            substring(air.timeStamp,1,8) ,
            air.accountNumber,
            air.prefix,
            air.regBranchCity,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            air.total_amount,
            air.revenue_code ,
            revcod.svcUsgTp , 
            revcod.lv4 ,  
            revcod.lv5 , 
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
            end ,
            air.originTimeStamp ,
            air.offerId,
            air.areaName,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          """
         );
       sorSubRejRev.registerTempTable("ha1_sor_air_child02")
       
       
       
       
      /*****************************************************************************************************
     * 
     *     
     * Step 09 : Union ALL Source Table
     * 
     *****************************************************************************************************/
     log.warn("==================Step 09 :  Union ALL Source Table ================"+goToMinute());
      val airRefillDetail = sqlContext.sql("""
          select * from ha1_sor_air_parent_mada
         UNION
          select * from ha1_sor_air_child02
         UNION
          select * from ha1_sor_air_child01 
        """)
        
       airRefillDetail.registerTempTable("air_refill_detail")  
       
       
       
     /*****************************************************************************************************
     * 
     *     
     * Step 10 : Produce for Level Final ( Reject SOR LEVEL )
     * 
     *****************************************************************************************************/
     log.warn("==================Step 10 : Produce for Level Final ( Reject SOR LEVEL ) ================"+goToMinute());
     
     /*val rejRevAirRef=sqlContext.sql( 
        """
            SELECT sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUEFLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT,
            case 
                 when service_type is null then 'Revenue Code is Not Found' 
                 when trim(revenueflag) = 'UNKNOWN' then 'Mada is Not Found'
                 else 'Reject Ref' end as rej_rsn
          FROM ha1_sor_air_parent_mada sub where service_type is null and revenueflag ='UNKNOWN'
          UNION 
            SELECT sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUEFLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT,
            case 
                  when service_type is null then 'Revenue Code is Not Found' 
                  when trim(revenueflag) = 'UNKNOWN' then 'Mada is Not Found'
                  else 'Reject Ref' end as rej_rsn 
          FROM ha1_sor_air_child02 sub where service_type is null and revenueflag ='UNKNOWN'
          UNION  
            SELECT sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUEFLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT,
            case 
                     when service_type is null then 'Revenue Code is Not Found' 
                     when trim(revenueflag) = 'UNKNOWN' then 'Mada is Not Found'
                     else 'Reject Ref' end as rej_rsn 
          FROM ha1_sor_air_child01 sub where service_type is null and revenueflag ='UNKNOWN'
        """
         );
      */
      
      val rejRevAirRef=sqlContext.sql( 
        """
           SELECT sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUE_FLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT,
            case 
                     when service_type is null then 'Revenue Code is Not Found' 
                     when revenueflag is null then 'Mada is Not Found'
                     else 'Reject Ref' end as REJ_RSN 
          FROM air_refill_detail sub where service_type is null and revenueflag  is null 
        """
         );
      
       Common.cleanDirectory(sc, OUTPUT_REJ_REV_DIR );
       rejRevAirRef.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REV_DIR);
//       rejRevAirRef.show();
       
     
     
     /*****************************************************************************************************
     * 
     *     
     * Step 11 : Produce for Level Final ( SOR LEVEL )
     * 
     *****************************************************************************************************/   
     log.warn("==================Step 11 : Produce for Level Final ( SOR LEVEL ) ================"+goToMinute());
     
     
     /*val sorFinalAirRef=sqlContext.sql( 
        """
            SELECT sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUEFLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT
          FROM ha1_sor_air_parent_mada sub where service_type is not  null and revenueflag <> 'UNKNOWN'
          UNION 
            SELECT sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUEFLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT
         FROM ha1_sor_air_child02 sub where service_type is not null and revenueflag  <> 'UNKNOWN'
         UNION 
            SELECT  sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUEFLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT
           from ha1_sor_air_child01 sub where service_type is not null and revenueflag  <> 'UNKNOWN'
        """
         );
      
     */
     val sorFinalAirRef=sqlContext.sql( 
        """
          SELECT sub.transaction_date TRANSACTION_DATE,
            sub.msisdn MSISDN,
            sub.prefix PREFIX,
            sub.city CITY,
            sub.provider_id PROVIDER_ID,
            sub.country COUNTRY,
            sub.svcClassName SERVICECLASSID,
            sub.promo_package_code PROMO_PACKAGE_CODE,
            sub.promo_package_name PROMO_PACKAGE_NAME,
            sub.brand_name BRAND_NAME,
            cast(sub.total_amount as double) TOTAL_AMOUNT,
            cast(sub.total_hits as double) TOTAL_HIT,
            sub.revenue_code REVENUE_CODE,
            sub.service_type SERVICE_TYPE,
            sub.gl_code GL_CODE,            
            sub.gl_name GL_NAME,
            sub.real_filename REAL_FILENAME,
            sub.hlr_branch_nm HLR_BRANCH_NM,
            sub.hlr_region_nm HLR_REGION_NM,
            sub.revenueflag REVENUE_FLAG,
            cast (null as string) MCCMNC,
            cast (null as string) LAC,
            cast (null as string) CI,
            cast (null as string) LACI_CLUSTER_ID,
            cast (null as string) LACI_CLUSTER_NM,
            cast (null as string) LACI_REGION_ID,
            cast (null as string) LACI_REGION_NM,
            cast (null as string) LACI_AREA_ID,
            cast (null as string) LACI_AREA_NM,
            cast (null as string) LACI_SALESAREA_ID,
            cast (null as string) LACI_SALESAREA_NM,
            sub.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            sub.offerId OFFER_ID,
            sub.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            sub.jobID JOB_ID,
            sub.recordID RECORD_ID,
            sub.prcDT PRC_DT,
            sub.area SRC_TP,
            sub.fileDT FILE_DT
           from air_refill_detail sub where service_type is not null and revenueflag is not null
        """
         );
      
     
     Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_REV_DIR, "/SRC_TP=AIR_REFILL/*/JOB_ID=" + jobID);
     sorFinalAirRef.write.partitionBy("SRC_TP","TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_REV_DIR);
//     sorFinalAirRef.show();
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
}