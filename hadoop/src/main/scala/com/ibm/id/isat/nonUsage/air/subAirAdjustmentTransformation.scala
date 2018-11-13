package com.ibm.id.isat.nonUsage.air

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import com.ibm.id.isat.nonUsage.air.model.subAirAdjustment;
import com.ibm.id.isat.utils.ReferenceSchema;
import org.apache.spark.sql.Row;
import com.ibm.id.isat.utils.Common;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import com.ibm.id.isat.utils.ReferenceDF;

/*
 * 
 * This script is used for handling Parent and Child of Air Adjustment
 * 
 */

object subAirAdjustmentTransformation {
  
    def main(args: Array[String]) {

//   val sc = new SparkContext("local", "sub adj transformation spark ", new SparkConf());  
     val sc = new SparkContext(new SparkConf())
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
     
     // Import data frame library.
     import sqlContext.implicits._
     import org.apache.log4j._
     Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.OFF)
     Logger.getLogger("org").setLevel(Level.ERROR)
     Logger.getLogger("akka").setLevel(Level.ERROR)
         
     val log = LogManager.getRootLogger
     
     if (args.length != 4) {
        println("Usage: [PRC_DT] [JOBID] [CONFIG] [INPUTDIR]")
        sys.exit(1);
      }
     
      val prcDt = args(0)
      val jobID = args(1)
      val configDir = args(2)
      val inputFile= args(3)
     
//   val configDir = "/user/apps/nonusage/air/reference/AIR.conf"
     val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_ADJ_REJ_BP_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REF_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_ADJ_REJ_REF_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_ADJ_REJ_REV_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_CDR_INQ_DIR_ADJ = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_CDR_INQ_DIR_ADJ").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_DETAIL_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_DETAIL_DIR")
     
    /* val prcDt = "20160722"
     val jobID = "10"
     val configDir = "C:/testing"
     val inputFile= "C:/testing/AIR_2016.ADJ"
     val OUTPUT_REJ_BP_DIR = "C:/testing/rej_bp"
     val OUTPUT_REJ_REF_DIR = "C:/testing/rej_ref"
     val OUTPUT_REJ_REV_DIR = "C:/testing/rej_rev"
     val OUTPUT_DETAIL_REV_DIR ="C:/testing/sor"
     val OUTPUT_CDR_INQ_DIR_ADJ="C:/testing/inquiry"
     System.setProperty("hadoop.home.dir", "C:\\winutil\\")*/
     
     
     val existDir= Common.isDirectoryExists(sc, inputFile);
     var subAirInptRDD = sc.textFile(inputFile);  
     if ( subAirInptRDD.count() == 0){
       sys.exit(1);
     }
     
     
     
    /*****************************************************************************************************
     * 
     * STEP 01.Load Reference Data
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
  //  intcctDF.show();
      
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
    
      val stgAirAdj = subAirInptRDD.filter ( x => x.count (_ == '|') == 33).map(line => line.split("\\|")).map {p => Row(
         p(0), //originNodeType
         p(1), //originHostName
         p(8), //timeStamp
         "62".concat(p(14)), //accountNumber
         "62".concat(p(17)), //subscriberNumber
         p(9), //current service class
         p(12),//transactionAmount
         p(23), //externalData1
         p(32), //realFilename
         p(5),  //originTimeStamp
         p(30), //dedicated_account ( used_for_level_2 )
         "62".concat(p(14)).padTo(8, ".").mkString(""),// msisdn_pad
         p(20), //accountFlagBefore
         p(21), //accountFlagAfter
         jobID, // job_id
         Common.getKeyCDRInq("62".concat(p(14)),  p(8).toString().substring(0, 14)), //rcrd_id
         goPrcDate(), // prc_dt
         "AIR_ADJ", // area
         p(33) // file DT
         )
      }
       
     val stgAirAdjustment = sqlContext.createDataFrame(stgAirAdj, subAirAdjustment.stgAirAdjSchema);
     stgAirAdjustment.registerTempTable("stg_air_adjustment")
     stgAirAdjustment.persist();
//     stgAirAdjustment.show();
     
     
     
     /*****************************************************************************************************
     * 
     * STEP 02. Load Reject BP - CREATE FOR REJECT BP (LOAD TO HIVE TABLE)
     * 
     *****************************************************************************************************/
     log.warn("==================STEP 02. CREATE FOR REJECT BP (LOAD TO HIVE TABLE) ================"+goToMinute());
     
     val rejectBP = sqlContext.sql( 
         """
            SELECT a.originNodeType,
            a.originNodeID,
            a.timeStamp,
            a.accountNumber,
            case when trim(a.subscriberNumber) = '62' then a.accountNumber else a.subscriberNumber  end as subscriberNumber,
            a.currentServiceclass,
            a.transactionAmount,
            a.externalData1,
            a.realFilename,
            a.originTimeStamp,
            a.dedicatedAccount,
            a.msisdnPad,
            a.accountFlagBefore,
            a.accountFlagAfter,
            a.jobID,
            a.recordID,
            a.prcDT,
            a.area,
            a.fileDT,
            case 
              when timeStamp = '' then 'Null Adjustment Time Stamp'
              when accountNumber = '' then 'Null Account Number'
              when originTimeStamp = '' then 'Null Origin Time Stamp'
              when currentServiceclass = '' then 'Null Curent Service Class'
              when externalData1 not like '2668%' then 'External Data NOT like 2668'
            else
              'Invalid Adjustment Record Type'
           end as rejRsn
            FROM stg_air_adjustment  a 
            WHERE externalData1 NOT LIKE '2668%' OR currentServiceclass IS NULL OR accountNumber IS NULL OR timeStamp IS NULL OR originTimeStamp IS NULL
          """
         );
     
     Common.cleanDirectory(sc, OUTPUT_REJ_BP_DIR ); 
     rejectBP.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_BP_DIR);
//     rejectBP.show();
     
     
     
     
     /*****************************************************************************************************
     * 
     *     
     * Step 03 : Join with Interconnect and Branch. ( Level 1 )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 03 : Join with Interconnect and Branch. ( Level 1 ) ================"+goToMinute());
     val ha1AirAdj01 =  sqlContext.sql( 
          """
          SELECT
            air.originNodeType,
            air.originNodeID,
            air.timeStamp ,
            air.accountNumber ,
            air.externalData1,
            air.transactionAmount,
            case when trim(air.subscriberNumber) = '62' then air.accountNumber else air.subscriberNumber end as subscriberNumber,
            air.currentServiceclass,
            air.msisdnPad,        
            air.realFilename,   
            air.dedicatedAccount,
            air.originTimeStamp , 
            air.accountFlagBefore,
            air.accountFlagAfter,
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
            substring(air.timeStamp,1,8) event_dt
          from
          (
            SELECT * FROM stg_air_adjustment where externalData1 LIKE '2668%'  
            and currentServiceclass IS NOT NULL and accountNumber IS NOT NULL and timeStamp IS NOT NULL and originTimeStamp IS NOT NULL
          )  air
           left join SDPOffer sdp on air.accountNumber = sdp.msisdn and (substring(air.timeStamp,1,8) between sdp.effDt and sdp.endDt )
          left join ServiceClassOffer svcclsofr on air.currentServiceclass = svcclsofr.svcClassCode 
            and (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and sdp.offerID = svcclsofr.offerId
          left join ServiceClass svccls on air.currentServiceclass = svccls.svcClassCode 
            and (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt )

          """
        );
        ha1AirAdj01.registerTempTable("ha1_air_adj01")
        ha1AirAdj01.persist();
//        ha1AirAdj01.show();
               
        val joinIntcctDF = Common.getIntcct(sqlContext, ha1AirAdj01, intcctDF, "accountNumber", "event_dt", "prefix", "country", "city", "provider_id") 
        joinIntcctDF.registerTempTable("HA1AirAdjJoinIntcct");
//        joinIntcctDF.show();
        
         val ha1AirAdj = sqlContext.sql(
            """
            select
            air.originNodeType,
            air.originNodeID,
            air.timeStamp ,
            air.accountNumber ,
            air.externalData1,
            air.transactionAmount,
            air.subscriberNumber,
            air.currentServiceclass,
            air.msisdnPad,        
            air.realFilename,   
            air.dedicatedAccount,
            air.originTimeStamp , 
            air.accountFlagBefore,
            air.accountFlagAfter,
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
            air.offerId, 
            air.areaName
            from HA1AirAdjJoinIntcct air
            left join  RegBranch regBranch on air.city = regBranch.city
            """);
      ha1AirAdj.registerTempTable("ha1_air_adj")
      ha1AirAdj.persist();
//      ha1AirAdj.show();
       
      stgAirAdjustment.unpersist();
      svcClassDF.unpersist()
      sdpOfferDF.unpersist()
      intcctDF.unpersist();
      regionBranchDF.unpersist();
      
      
      
       /*****************************************************************************************************
       * 
       *     
       * Step 04 : Produce for Reject Reference
       * 
       *****************************************************************************************************/
       log.warn("================== Step 04 : Produce for Reject Reference  ================"+goToMinute());
       
       val rejectRef = sqlContext.sql( 
         """
            SELECT
            air.originNodeType ORIGINNODETYPE,
            air.originNodeID ORIGINNODEID, 
            air.timeStamp TIMESTAMP,
            air.accountNumber ACCOUNTNUMBER,
            air.externalData1 EXTERNALDATA1,
            air.transactionAmount TRANSACTIONAMOUNT,
            air.subscriberNumber SUBSCRIBERNUMBER,
            air.currentServiceclass CURRENTSERVICECLASS,
            air.msisdnPad MSISDNPAD,        
            air.realFilename REALFILENAME,   
            air.dedicatedAccount DEDICATEDACCOUNT,
            air.originTimeStamp ORIGINTIMESTAMP, 
            air.accountFlagBefore ACCOUNTFLAGBEFORE,
            air.accountFlagAfter ACCOUNTFLAGAFTER,
            air.jobID JOBID,
            air.recordID RECORDID,
            air.prcDT PRCDT,
            air.area AREA,
            air.fileDT FILEDT,
            case when prefix is null then 'Prefix Not Found'
              when svcClassName is null then 'Service Class Not Found'
              when regBranchCity is null then 'City Not Found'
              end as REJ_RSN
           FROM ha1_air_adj air 
           WHERE prefix = '' or prefix is null or svcClassName = '' or svcClassName is null or regBranchCity = '' or regBranchCity is null
         """
         );

       Common.cleanDirectory(sc, OUTPUT_REJ_REF_DIR ); 
       rejectRef.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REF_DIR);
//     rejectRef.show();
     
     
       
    /*****************************************************************************************************
     * 
     *     
     * Step 05 : Join with Mada and Revenue - MA
     * 
     *****************************************************************************************************/
     log.warn("================== Step 05 : Join with Mada and Revenue - MA ================"+goToMinute());
       
         val sorAirAdj =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.currentServiceclass svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( transactionAmount * (-1) ) / 100
              else
              transactionAmount / 100 
            end as total_amount,
            count(*) as total_hits,
            concat ( air.externalData1 , 'MA') revenue_code,
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
            SELECT * FROM  ha1_air_adj WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null 
          )  air
          left join 
          (
            select  svcUsgTp, lv4, lv5, revSign,revCode,lv9 from RevenueCode
          ) revcod on revcod.revCode = concat ( air.externalData1 , 'MA')
          left join 
          (
            select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from Mada
          ) mada 
           on revcod.lv9 = mada.acc and air.promo_package_code = mada.grpCd and (substring(air.timeStamp,1,8) between effDt and endDt )
          group by 
            substring(air.timeStamp,1,8) ,
            air.accountNumber,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.currentServiceclass,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( transactionAmount * (-1) ) / 100
              else
              transactionAmount / 100 
            end,
            concat ( air.externalData1 , 'MA') ,
            revcod.svcUsgTp , 
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
         
       sorAirAdj.registerTempTable("sor_air_adj");
//       sorAirAdj.show();
       
       
       
    /*****************************************************************************************************
 		* 
 		*     
		* Step 06 : Transpose dedicated Account
 		* 
	  *****************************************************************************************************/
    log.warn("================== Step 06 : Transpose dedicated Account  ================"+goToMinute());
       
    val transposeRDD = ha1AirAdj.where("(prefix <> '' and prefix is not null) and (svcClassName <> '' and svcClassName is not null) and (regBranchCity <> '' and regBranchCity is not null) and (dedicatedAccount is not null and dedicatedAccount <> '') ")
      .map { row => row(0) + "|" + row(1) + "|" + row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" + row(9) + "|" + 
        row(11) + "|" + row(12) + "|" +  row(13) + "|" + row(14) + "|" + row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
        row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27) + "|" + row(28) + "|" + row(30) + "|" + row(31) +
        "~" + row(10)
        }
//     transposeRDD.collect foreach {case (a) => println (a)};
      
    val resultTransposeRDD = transposeRDD.map(line => line.split("\\~")).map(field => (field(0), field(1))).flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
     .map { replaced => replaced.replace("#", "|") };
     
//   initiate into sub air adjustment class and create data frame.
//     val ha1SubAdjInptDF = resultTransposeRDD.map(line => line.split("\\|")).filter(line => ! line(30).isEmpty() && ! line(32).isEmpty()).map { p => Row( 
//       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), p(23),p(24),p(25),p(26),p(27),
//       p(28),p(29),
//       p(30),p(32)
//       )
//     }
//     

     val ha1SubAdjInptDF = resultTransposeRDD.map(line => line.split("\\|")).map { p => Row( 
       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), p(23),p(24),p(25),p(26),p(27),
       p(28),p(29),
       p(30),p(32)
       )
     }
     
      
      val ha1SubAirAdj = sqlContext.createDataFrame(ha1SubAdjInptDF, subAirAdjustment.ha1SubAirAdjSchema)
      ha1SubAirAdj.registerTempTable("ha1_sub_air_adj")
//    ha1SubAirAdj.show();
    
    
    /*****************************************************************************************************
     * 
     *     
     * Step 07 : Join with Mada and Revenue - DA
     * 
     *****************************************************************************************************/
     log.warn("================== Step 07 :  Join with Mada and Revenue - DA  ================"+goToMinute());   
      val sorSubAirAdj =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.currentServiceclass svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( tupple3 * (-1) ) / 100
              else tupple3 / 100 
            end as total_amount,
            count(*) as total_hits,
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
            SELECT * FROM  ha1_sub_air_adj WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null --and regBranchCity <> '' and regBranchCity is not null 
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
          group by 
            substring(air.timeStamp,1,8) ,
            air.accountNumber,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.currentServiceclass,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( tupple3 * (-1) ) / 100
              else tupple3 / 100 
            end,
            concat(externalData1,concat('DA' , tupple1)) ,
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
            end,
            air.originTimeStamp ,
            air.offerId ,
            air.areaName ,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          """
         );
      
     sorSubAirAdj.registerTempTable("sor_sub_air_adj")
//     sorSubAirAdj.show();
       
       
     /*****************************************************************************************************
     * 
     *     
     * Step 08 : Union ALL Source Table
     * 
     *****************************************************************************************************/
     log.warn("==================Step 08 :  Union ALL Source Table ================"+goToMinute());
      val airAdjDetail = sqlContext.sql("""
          select * from sor_air_adj
          UNION  
          select * from sor_sub_air_adj
        """)
        
       airAdjDetail.registerTempTable("air_adj_detail")  
//       airAdjDetail.show();
     
     
     
    /*****************************************************************************************************
     * 
     *     
     *  Step 09 : Produce for Reject Ref 2 ( Revenue )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 09 :  Produce for Reject Ref 2 ( Revenue )  ================"+goToMinute());
     /* val rejRevAirAdj=sqlContext.sql( 
        """
            SELECT adj.transaction_date TRANSACTION_DATE,
            adj.msisdn MSISDN,
            adj.prefix PREFIX,
            adj.city CITY,
            adj.provider_id PROVIDER_ID,
            adj.country COUNTRY,
            adj.svcClassName SERVICECLASSID,
            adj.promo_package_code PROMO_PACKAGE_CODE,
            adj.promo_package_name PROMO_PACKAGE_NAME,
            adj.brand_name BRAND_NAME,
            cast(adj.total_amount as double) TOTAL_AMOUNT,
            cast(adj.total_hits as double) TOTAL_HIT,
            adj.revenue_code REVENUE_CODE,
            adj.service_type SERVICE_TYPE,
            adj.gl_code GL_CODE,
            adj.gl_name GL_NAME,
            adj.real_filename REAL_FILENAME,
            adj.hlr_branch_nm HLR_BRANCH_NM,
            adj.hlr_region_nm HLR_REGION_NM,
            adj.revenueflag REVENUEFLAG,
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
            adj.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            adj.offerId OFFER_ID,
            adj.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            adj.jobID JOB_ID,
            adj.recordID RECORD_ID,
            adj.prcDT PRC_DT,
            adj.area SRC_TP,
            adj.fileDT FILE_DT,
            case 
                     when service_type is null then 'Revenue Code is Not Found' 
                     when trim(revenueflag) = 'UNKNOWN' then 'Mada is Not Found'
                     else 'Reject Ref' end as rej_rsn 
          from sor_air_adj adj where service_type is null and revenueflag ='UNKNOWN'
          UNION  
            SELECT adj.transaction_date TRANSACTION_DATE,
            adj.msisdn MSISDN,
            adj.prefix PREFIX,
            adj.city CITY,
            adj.provider_id PROVIDER_ID,
            adj.country COUNTRY,
            adj.svcClassName SERVICECLASSID,
            adj.promo_package_code PROMO_PACKAGE_CODE,
            adj.promo_package_name PROMO_PACKAGE_NAME,
            adj.brand_name BRAND_NAME,
            cast(adj.total_amount as double) TOTAL_AMOUNT,
            cast(adj.total_hits as double) TOTAL_HIT,
            adj.revenue_code REVENUE_CODE,
            adj.service_type SERVICE_TYPE,
            adj.gl_code GL_CODE,
            adj.gl_name GL_NAME,
            adj.real_filename REAL_FILENAME,
            adj.hlr_branch_nm HLR_BRANCH_NM,
            adj.hlr_region_nm HLR_REGION_NM,
            adj.revenueflag REVENUEFLAG,
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
            adj.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            adj.offerId OFFER_ID,
            adj.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            adj.jobID JOB_ID,
            adj.recordID RECORD_ID,
            adj.prcDT PRC_DT,
            adj.area SRC_TP,
            adj.fileDT FILE_DT,
            case 
               when service_type is null then 'Revenue Code is Not Found' 
               when trim(revenueflag) = 'UNKNOWN' then 'Mada is Not Found'
               else 'Reject Ref' 
            end as rej_rsn 
        from sor_sub_air_adj adj 
        where service_type is null and revenueflag ='UNKNOWN'
            """
         );
      
     */
      val rejRevAirAdj=sqlContext.sql( 
        """
        SELECT adj.transaction_date TRANSACTION_DATE,
            adj.msisdn MSISDN,
            adj.prefix PREFIX,
            adj.city CITY,
            adj.provider_id PROVIDER_ID,
            adj.country COUNTRY,
            adj.svcClassName SERVICECLASSID,
            adj.promo_package_code PROMO_PACKAGE_CODE,
            adj.promo_package_name PROMO_PACKAGE_NAME,
            adj.brand_name BRAND_NAME,
            cast(adj.total_amount as double) TOTAL_AMOUNT,
            cast(adj.total_hits as double) TOTAL_HIT,
            adj.revenue_code REVENUE_CODE,
            adj.service_type SERVICE_TYPE,
            adj.gl_code GL_CODE,
            adj.gl_name GL_NAME,
            adj.real_filename REAL_FILENAME,
            adj.hlr_branch_nm HLR_BRANCH_NM,
            adj.hlr_region_nm HLR_REGION_NM,
            adj.revenueflag REVENUE_FLAG,
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
            adj.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            adj.offerId OFFER_ID,
            adj.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            adj.jobID JOB_ID,
            adj.recordID RECORD_ID,
            adj.prcDT PRC_DT,
            adj.area SRC_TP,
            adj.fileDT FILE_DT,
            case 
               when service_type is null then 'Revenue Code is Not Found' 
               when revenueflag is null then 'Mada is Not Found'
               else 'Reject Ref' 
            end as REJ_RSN 
        from air_adj_detail adj 
        where service_type is null and revenueflag is null
            """
         );
      
     
      Common.cleanDirectory(sc,OUTPUT_REJ_REV_DIR);
      rejRevAirAdj.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REV_DIR);
//      rejRevAirAdj.show();
    
    
     
    /*****************************************************************************************************
     * 
     *     
     *  Step 10 : Produce for SOR Detail  
     * 
     *****************************************************************************************************/
     log.warn("================== Step 10 : Produce for SOR Detail  ================"+goToMinute());
     
    /* val sorFinalAirAdj=sqlContext.sql( 
        """
        SELECT adj.transaction_date TRANSACTION_DATE,
            adj.msisdn MSISDN,
            adj.prefix PREFIX,
            adj.city CITY,
            adj.provider_id PROVIDER_ID,
            adj.country COUNTRY,
            adj.svcClassName SERVICECLASSID,
            adj.promo_package_code PROMO_PACKAGE_CODE,
            adj.promo_package_name PROMO_PACKAGE_NAME,
            adj.brand_name BRAND_NAME,
            cast(adj.total_amount as double) TOTAL_AMOUNT,
            cast(adj.total_hits as double) TOTAL_HIT,
            adj.revenue_code REVENUE_CODE,
            adj.service_type SERVICE_TYPE,
            adj.gl_code GL_CODE,
            adj.gl_name GL_NAME,
            adj.real_filename REAL_FILENAME,
            adj.hlr_branch_nm HLR_BRANCH_NM,
            adj.hlr_region_nm HLR_REGION_NM,
            adj.revenueflag REVENUEFLAG,
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
            adj.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            adj.offerId OFFER_ID,
            adj.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            adj.jobID JOB_ID,
            adj.recordID RECORD_ID,
            adj.prcDT PRC_DT,
            adj.area SRC_TP,
            adj.fileDT FILE_DT
         from sor_air_adj adj where service_type is not  null and revenueflag <> 'UNKNOWN'
         UNION  
            SELECT adj.transaction_date TRANSACTION_DATE,
            adj.msisdn MSISDN,
            adj.prefix PREFIX,
            adj.city CITY,
            adj.provider_id PROVIDER_ID,
            adj.country COUNTRY,
            adj.svcClassName SERVICECLASSID,
            adj.promo_package_code PROMO_PACKAGE_CODE,
            adj.promo_package_name PROMO_PACKAGE_NAME,
            adj.brand_name BRAND_NAME,
            cast(adj.total_amount as double) TOTAL_AMOUNT,
            cast(adj.total_hits as double) TOTAL_HIT,
            adj.revenue_code REVENUE_CODE,
            adj.service_type SERVICE_TYPE,
            adj.gl_code GL_CODE,
            adj.gl_name GL_NAME,
            adj.real_filename REAL_FILENAME,
            adj.hlr_branch_nm HLR_BRANCH_NM,
            adj.hlr_region_nm HLR_REGION_NM,
            adj.revenueflag REVENUEFLAG,
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
            adj.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            adj.offerId OFFER_ID,
            adj.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            adj.jobID JOB_ID,
            adj.recordID RECORD_ID,
            adj.prcDT PRC_DT,
            adj.area SRC_TP,
            adj.fileDT FILE_DT
         from sor_sub_air_adj adj 
         where service_type is not null and revenueflag  <> 'UNKNOWN'
        """
         );*/
      
       val sorFinalAirAdj=sqlContext.sql( 
        """
        SELECT adj.transaction_date TRANSACTION_DATE,
            adj.msisdn MSISDN,
            adj.prefix PREFIX,
            adj.city CITY,
            adj.provider_id PROVIDER_ID,
            adj.country COUNTRY,
            adj.svcClassName SERVICECLASSID,
            adj.promo_package_code PROMO_PACKAGE_CODE,
            adj.promo_package_name PROMO_PACKAGE_NAME,
            adj.brand_name BRAND_NAME,
            cast(adj.total_amount as double) TOTAL_AMOUNT,
            cast(adj.total_hits as double) TOTAL_HIT,
            adj.revenue_code REVENUE_CODE,
            adj.service_type SERVICE_TYPE,
            adj.gl_code GL_CODE,
            adj.gl_name GL_NAME,
            adj.real_filename REAL_FILENAME,
            adj.hlr_branch_nm HLR_BRANCH_NM,
            adj.hlr_region_nm HLR_REGION_NM,
            adj.revenueflag REVENUE_FLAG,
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
            adj.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            adj.offerId OFFER_ID,
            adj.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            adj.jobID JOB_ID,
            adj.recordID RECORD_ID,
            adj.prcDT PRC_DT,
            adj.area SRC_TP,
            adj.fileDT FILE_DT
         from air_adj_detail adj 
         where service_type is not null and revenueflag is not null
        """
         );
      
         Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_REV_DIR, "/SRC_TP=AIR_ADJ/*/JOB_ID=" + jobID)
         sorFinalAirAdj.write.partitionBy("SRC_TP","TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_REV_DIR);
//         sorFinalAirAdj.show();
     
     
     
    /*****************************************************************************************************
     * 
     *     
     *  Step 11 : Load Data for CDR Inquiry  ( Create 1 dedicated class for CDR Inquiry ADJ )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 11 : Produce for CDR Inquiry ( Cdr Inquiry )  ================"+goToMinute());
     
     val airAdjInqDF = sqlContext.sql(
           """SELECT recordID as KEY,
                accountNumber MSISDN, 
                timeStamp TRANSACTIONTIMESTAMP,
                case
                  when substr(currentServiceclass,1,1) in ('4','7') then 'POSTPAID'
                  else 'PREPAID'
                end as SUBSCRIBERTYPE,
                (transactionAmount/100) as AMOUNT,
                "" DA_DETAIL,
                currentServiceclass as CURRENTSERVICECLASS,
                "" ACTIVATIONDATE,
                currentServiceclass as SERVICECLASSID,
                svcClassName as SERVICECLASSNAME,
                promo_package_name as PROMOPACKAGENAME,
                brand_name as BRANDNAME ,            
                "" NEWSERVICECLASS ,  
                "" NEWSERVICECLASSNAME ,  
                "" NEWPROMOPACKAGENAME ,  
                "" NEWBRANDNAME,
                originNodeType ORIGINNODETYPE,
                originNodeID ORIGINHOSTNAME,
                externalData1 as EXTERNALDATA1,
                "" PROGRAMNAME,
                "" PROGRAMOWNER,   
                "" PROGRAMCATEGORY,
                "" BANKNAME,
                "" BANKDETAIL,
                "" MAINACCOUNTBALANCEBEFORE,
                "" MAINACCOUNTBALANCEAFTER,
                "" LAC,
                "" CELLID,
                'Discount' as TRANSACTIONTYPE,
                realFilename FILENAMEINPUT,
                "" VOURCHER_TYPE,
                "" RELOAD_TYPE,
                jobID JOBID,
                "" CDR_TP       
               from ha1_air_adj          
           """)

         Common.cleanDirectory(sc,OUTPUT_CDR_INQ_DIR_ADJ); 
         airAdjInqDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_CDR_INQ_DIR_ADJ);
//       airAdjInqDF.show();
      
       sc.stop();
    
  }
    
    /*
     * @checked empty record
     *     
     */
     def isCheckRecord(content:String , key:Int) : String = {
       if (!content.split("\\|")(key).isEmpty()) {
          return  content.split("\\|")(key)
       }
       return null;
     }
     
     /*
     * @checked contains key value
     *     
     */
     def isAnyRecord(content:String , key:Int , value:String) : String = {
       if (content.split("\\|")(key).contains(value)) {
          return  content.split("\\|")(key)
       }else if (!content.split("\\|")(key).isEmpty()) {
         return  content.split("\\|")(key)
       }
       return null;
     }
     
     def goToDate() : String = {
         val today = Calendar.getInstance().getTime
         val curTimeFormat = new SimpleDateFormat("YYYYMMdd")
         val prcDt = curTimeFormat.format(today).toString()
         return prcDt;
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