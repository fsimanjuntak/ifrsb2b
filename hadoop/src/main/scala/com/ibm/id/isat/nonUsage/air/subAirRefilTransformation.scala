package com.ibm.id.isat.nonUsage.air

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



object subAirRefilTransformation {
  
    def main(args: Array[String]) {
     
//   val sc = new SparkContext("local", "sub refill transformation spark ", new SparkConf());  
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
     
     if (args.length != 4) {
        println("Usage: [PRC_DT] [JOBID] [CONFIG] [INPUTDIR]")
        sys.exit(1);
     }
     
     val prcDt = args(0)
     val jobID = args(1)
     val configDir = args(2)
     val inputFile= args(3)
      
     
     
//   val configDir = "/user/apps/nonusage/air/reference/AIR.conf"
     val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_REFFILL_REJ_BP_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REF_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_REFFILL_REJ_REF_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_REFFILL_REJ_REV_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_CDR_INQ_DIR_REFFILL = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_CDR_INQ_DIR_REFFILL").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_DETAIL_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_DETAIL_DIR")
     
     
/*   val prcDt = "20160722"
     val jobID = "10"
     val configDir = "C:/testing"
     val inputFile= "C:/testing/AIR_2016.REF"
     val OUTPUT_REJ_BP_DIR = "C:/testing/rej_bp"
     val OUTPUT_REJ_REF_DIR = "C:/testing/rej_ref"
     val OUTPUT_REJ_REV_DIR = "C:/testing/rej_rev"
     val OUTPUT_DETAIL_REV_DIR ="C:/testing/sor"
     val OUTPUT_CDR_INQ_DIR_REFFILL ="C:/testing/inquiry"
     System.setProperty("hadoop.home.dir", "C:\\winutil\\")  */  

     
     val existDir= Common.isDirectoryExists(sc, inputFile);
     var subAirInptRDD = sc.textFile(inputFile);  
     if ( subAirInptRDD.count() == 0){
       sys.exit(1);
     }

     
     
     /*****************************************************************************************************
     * 
     * STEP 01. Load Reference Data
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
      svcClassOfferDF.count();
      
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
      
      
      log.warn("==================STEP 02. SPLIT INPUT FILE ================"+goToMinute());         
      val stgAirRefill = subAirInptRDD.filter ( x => x.count (_ == '|') == 57).map(line => line.split("\\|")).map {p => Row(
         p(0), //originNodeType
         p(1), //originHostName
         p(8), //timeStamp
         if (p(22) != null) "62".concat(p(22)) else "",  //accountNumber
         p(17), //refillType
         p(18), //refillProfileID
         if (p(24) != null) "62".concat(p(24)) else "",//subscriberNumber
         p(9), //current service class
         p(13),//transactionAmount
         p(40), //externalData1
         p(56), //realFilename
         p(5),  //originTimeStamp
         p(26), //account information before refill
         p(27), //account information after refill
         p(10), //voucher based refill
         p(35), //activation date
         "62".concat(p(22)).padTo(8, ".").mkString(""),// msisdn_pad
         jobID, // job_id
         Common.getKeyCDRInq("62".concat(p(22)),  p(8).toString().substring(0, 14)), //rcrd_id
         goPrcDate(), // prc_dt
         "AIR_REFILL", // area
         p(57) // file DT
        )
    }
   
    val stgAirRef = sqlContext.createDataFrame(stgAirRefill, subAirRefill.stgAirReffSchema);
    stgAirRef.registerTempTable("stg_air_reff")
    stgAirRef.persist()
//    stgAirRef.show();
    
    
          
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
                   a.refillType,
                   a.refillProfileID,
                   a.subscriberNumber,
                   a.currentServiceclass,
                   a.transactionAmount,
                   a.externalData1,
                   a.realFilename,
                   a.originTimeStamp,
                   a.accountInformationBeforeRefill,
                   a.accountInformationAfterRefill,
                   a.voucherBasedRefill,
                   a.activationDate,
                   a.msisdnPad,
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
                      when voucherBasedRefill <> 'false' then 'Not Valid Voucher Based'
                   else
                      'Invalid Record Type'
                   end as rejRsn
            FROM stg_air_reff a 
            WHERE currentServiceclass IS NULL OR accountNumber IS NULL OR timeStamp IS NULL OR originTimeStamp IS NULL OR voucherBasedRefill <> 'false'
         """
         );
     
     Common.cleanDirectory(sc, OUTPUT_REJ_BP_DIR );
    rejectBP.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_BP_DIR);
//    rejectBP.show()
     
    
     
     /*****************************************************************************************************
     * 
     *     
     * Step 03 : Join with Interconnect and Branch
     * 
     *****************************************************************************************************/
     log.warn("================== Step 03 : Join with Interconnect and Branch. ================"+goToMinute());
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
             SELECT * FROM stg_air_reff a   
          )  air
          left join SDPOffer sdp on air.accountNumber = sdp.msisdn and (substring(air.timeStamp,1,8) between sdp.effDt and sdp.endDt )
          left join ServiceClassOffer svcclsofr on air.currentServiceclass = svcclsofr.svcClassCode 
            and (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and sdp.offerID = svcclsofr.offerId
          left join ServiceClass svccls on air.currentServiceclass = svccls.svcClassCode 
            and (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt )
          """
         );
      ha1AirRefill01.registerTempTable("ha1_air_refill01")
      ha1AirRefill01.persist();
      stgAirRef.unpersist();  
      svcClassDF.unpersist()
      sdpOfferDF.unpersist()
      intcctDF.unpersist();
      regionBranchDF.unpersist();
      
      val joinIntcctDF = Common.getIntcct(sqlContext, ha1AirRefill01, intcctDF, "accountNumber", "event_dt", "prefix", "country", "city", "provider_id") 
      joinIntcctDF.registerTempTable("HA1AirRefillJoinIntcct")
      
      
        
     /*****************************************************************************************************
     * 
     *     
     * Step 04 : Provide CDR Inquiry Baseline. -- ( Create 1 dedicated class for CDR Inquiry REF )
     * 
     *****************************************************************************************************/
     log.warn("==================  Step 04 : Provide CDR Inquiry Baseline. ================"+goToMinute());
     val ha1AirRefillJoinInquiry = sqlContext.sql(
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
            cast(null as string) hlr_branch_nm, 
            cast(null as string) hlr_region_nm,
            cast(null as string) regBranchCity,
            air.activationDate,
            air.offerId, 
            air.areaName,
            air.voucherBasedRefill
            from HA1AirRefillJoinIntcct air
            """);
      ha1AirRefillJoinInquiry.registerTempTable("ha1_air_refill_inquiry")
      ha1AirRefillJoinInquiry.persist();
 
      
      
     /*****************************************************************************************************
     * 
     *     
     * Step 05 : Provide Main Process (AIR) Baseline.
     * 
     *****************************************************************************************************/
     log.warn("==================  Step 05 : Provide Main Process (AIR) Baseline. ================"+goToMinute());
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
            voucherBasedRefill
            from (
                select * from HA1AirRefillJoinIntcct 
                WHERE currentServiceclass IS NOT NULL and accountNumber IS NOT NULL and timeStamp IS NOT NULL 
                and originTimeStamp IS NOT NULL and lower(voucherBasedRefill) like '%false%'
            ) air 
            left join  RegBranch regBranch on air.city = regBranch.city
            """);
      ha1AirRefill.registerTempTable("ha1_air_refill")
      ha1AirRefill.persist();

      
      
    
    /*****************************************************************************************************
     * 
     *     
     * Step 06 : Produce for Reject Reference (LOAD TO HIVE TABLE)
     * 
     *****************************************************************************************************/ 
     log.warn("================== Step 06 : Produce for Reject Reference (LOAD TO HIVE TABLE) ================"+goToMinute());
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
            air.accountInformationBeforeRefill,
            air.accountInformationAfterRefill,
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
     * Step 07 : Join with Mada and Revenue Reference. ( Level 1 <-> Parent Level )
     * Join with used MA type
     * 
     *****************************************************************************************************/
     log.warn("================== Step 07 : Join with Mada and Revenue Reference. ( Level 1 <-> Parent Level ) ================"+goToMinute());
     val sorAirRefill =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            air.refillType,
            air.refillProfileID,
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
            air.refillType,
            air.refillProfileID,
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
            concat ( air.externalData1 , 'MA'),
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
       
     sorAirRefill.registerTempTable("sor_air_refill")
//     sorAirRefill.show();
        
     
     
     
     /*****************************************************************************************************
     * 
     *     
     * Step 08 : Transpose for Level 2. ( SOR LEVEL : Before and After Refill)
     * 
     *****************************************************************************************************/
     log.warn("==================Step 08 : Transpose for Level 2. ( SOR LEVEL : Before and After Refill) ================"+goToMinute());
     val transposeBeforeRefillRDD = ha1AirRefill.where("prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null and accountInformationBeforeRefill is not null and accountInformationBeforeRefill <> '' ")
      .map { row => row(0) + "|" + row(1) + "|" +row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" +  row(9) + "|" +  row(10) + "|" +  
        row(11) + "|" + row(12)  + "|" + row(13) + "|" + row(14) + "|" + row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
        row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27) + "|" + row(28) + "|" + row(29) + "|" + row(30) +  "|" + row(31) + "|" + row(32) + "|" + row(33) +    
        "~" + getTuppleRecord(row(13).toString())      
        }
//    transposeBeforeRefillRDD.collect foreach {case (a) => println (a)}; 

     
    val resultBeforeRefillTransposeRDD = transposeBeforeRefillRDD.map(line => line.split("\\~")).map(field => (field(0), field(1)))
    .flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
    .map { replaced => replaced.replace("@", "|") };
    
    val ha1SubBeforeReffillInptDF = resultBeforeRefillTransposeRDD.filter ( x => x.count (_ == '|') == 35).filter(line => ! line.split("\\|")(34).equals("xxx") &&  ! line.split("\\|")(35).equals("xxx") ).map(line => line.split("\\|")).map { p => Row( 
       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), 
       p(23),p(24),p(25), p(26), p(27),p(28),p(29), p(30),p(31), p(32), p(33),p(34), p(35)  
       )
     }
     
    val ha1SubBeforeRef = sqlContext.createDataFrame(ha1SubBeforeReffillInptDF, subAirRefill.ha1SubAirRefSchema)
    ha1SubBeforeRef.registerTempTable("ha1_sub_air_before_ref")
    
    
//    log.warn("==================Step 08.b : Transpose for Level 2 ( For After Refill) ================");
    val transposeAfterRefillRDD = ha1AirRefill.where("prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null and accountInformationBeforeRefill is not null and accountInformationBeforeRefill <> '' ")
      .map { row => row(0) + "|" + row(1) + "|" + row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" +  row(9) + "|" +  row(10) + "|" + 
        row(11) + "|" + row(12)  + "|" + row(13) + "|" + row(14) + "|" +  row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
        row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27)  + "|" + row(28) + "|" + row(29) + "|" + row(30) +  "|" + row(31) + "|" + row(32) + "|" + row(33) +     
        "~" + getTuppleRecord(row(14).toString())      
    }
//    transposeAfterRefillRDD.collect foreach {case (a) => println (a)};
    
    
    val resultAfterRefillTransposeRDD = transposeAfterRefillRDD.map(line => line.split("\\~")).map(field => (field(0), field(1)))
    .flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
    .map { replaced => replaced.replace("@", "|") };
    
    
    val ha1SubAfterReffillInptDF = resultAfterRefillTransposeRDD.filter ( x => x.count (_ == '|') == 35).filter(line => ! line.split("\\|")(34).equals("xxx") &&  ! line.split("\\|")(35).equals("xxx") ).map(line => line.split("\\|")).map { p => Row( 
       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), 
       p(23),p(24),p(25), p(26), p(27),p(28),p(29), p(30),p(31), p(32), p(33),p(34), p(35) 
       )
     }
    val ha1SubAfterRef = sqlContext.createDataFrame(ha1SubAfterReffillInptDF, subAirRefill.ha1SubAirRefSchema)
    ha1SubAfterRef.registerTempTable("ha1_sub_air_after_ref")
//    ha1SubAfterRef.show();


    
    
    /* *//*****************************************************************************************************
     * 
     *     
     * Step 09 : Transpose for Level 2 for CDR Inquiry. ( SOR LEVEL : Before and After Refill)
     * Revised in 12 August 2016
     * Not Used Again.
     * Line 648 - 742
     * 
     *****************************************************************************************************//*
    log.warn("==================Step 09 : Transpose for Level 2 for CDR Inquiry. ( SOR LEVEL : Before and After Refill) ================");
    val transposeBeforeRefillCdrInquiryRDD = ha1AirRefillJoinInquiry.map { row => row(0) + "|" + row(1) + "|" +row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" +  row(9) + "|" +  row(10) + "|" +  
        row(11) + "|" + row(12)  + "|" + row(13) + "|" + row(14) + "|" + row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
        row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27) + "|" + row(28) + "|" + row(29) + "|" + row(30)  +  "|" + row(31) +  "|" + row(32) + "|" + row(33) +       
        "~" + getTuppleRecordCdrInquiry(row(13).toString())      
        }
    //transposeBeforeRefillCdrInquiryRDD.collect foreach {case (a) => println (a)}; 
    
     
    log.warn("==================Step 09.b : Replaced CDR Inquiry Before Refill ================"); 
    val resultBeforeRefillTransposeCdrInquiryRDD = transposeBeforeRefillCdrInquiryRDD.map(line => line.split("\\~")).map(field => (field(0), field(1)))
    .flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
    .map { replaced => replaced.replace("@", "|") };
    resultBeforeRefillTransposeCdrInquiryRDD.collect foreach {case (a) => println (a)}; 
    
    
    log.warn("==================Step 09.c : Create Schema for before refill ================");
    val ha1SubBeforeReffillCdrInquiryInptDF = resultBeforeRefillTransposeCdrInquiryRDD.filter ( x => x.count (_ == '|') == 36).map(line => line.split("\\|")).map { p => Row( 
       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), 
       p(23),p(24),p(25), p(26), p(27),p(28),p(29), p(30),p(31), p(32), p(33), p(34), p(35),p(36)
       )
     }
     
    val ha1SubBeforeCdrInquiryRef = sqlContext.createDataFrame(ha1SubBeforeReffillCdrInquiryInptDF, subAirRefill.ha1SubAirBeforeRefInquirySchema)
    ha1SubBeforeCdrInquiryRef.registerTempTable("ha1_sub_air_cdr_inquiry_before_ref")
    ha1SubBeforeCdrInquiryRef.show();
    
    
    log.warn("==================Step 09.d : Transpose for Level 2 ( For After Refill) - CDR Inqyuiry================");
    val transposeAfterRefillCdrInquiryRDD = ha1SubBeforeCdrInquiryRef.map { row => row(0) + "|" + row(1) + "|" + row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" +  row(9) + "|" +  row(10) + "|" + 
        row(11) + "|" + row(12)  + "|" + row(13) + "|" + row(14) + "|" +  row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
        row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27)  + "|" + row(28) + "|" + row(29) + "|" + row(30) +  "|" + row(31) +  "|" + row(32) + "|" + row(33) +  "|" + row(34) + "|" + row(35) + "|" + row(36) +
        "~" + getTuppleRecordCdrInquiry(row(14).toString())      
    }
   // transposeAfterRefillCdrInquiryRDD.collect foreach {case (a) => println (a)};

    
   log.warn("==================Step 09.e : Replaced ( For After Refill) - CDR Inqyuiry================");  
   val resultAfterRefillTransposeCdrInquiryRDD = transposeAfterRefillCdrInquiryRDD.map(line => line.split("\\~")).map(field => (field(0), field(1)))
    .flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
    .map { replaced => replaced.replace("@", "|") };
   // resultAfterRefillTransposeCdrInquiryRDD.collect foreach {case (a) => println (a)}; 
    
    
   log.warn("==================Step 09.f : Create Schema for after refill ================");
    val ha1SubAfterReffillCdrInquiryInptDF = resultAfterRefillTransposeCdrInquiryRDD.filter ( x => x.count (_ == '|') == 39).map(line => line.split("\\|")).map { p => Row( 
       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), 
       p(23),p(24),p(25), p(26), p(27),p(28),p(29), p(30),p(31), p(32), p(33),p(34), p(35),p(36),p(37), p(38),p(39)
       )
     }
    val ha1SubAfterCdrInquiryRef = sqlContext.createDataFrame(ha1SubAfterReffillCdrInquiryInptDF, subAirRefill.ha1SubAirAfterRefInquirySchema)
    ha1SubAfterCdrInquiryRef.registerTempTable("ha1_sor_air_cdr_inquiry_refill_union")
    ha1SubAfterCdrInquiryRef.show();*/
    
    
    
    
     /*****************************************************************************************************
     * 
     *     
     * Step 10 : Union Parent with Sub Air
     * 
     *****************************************************************************************************/
     log.warn("==================Step 10 : Union Parent with Sub Air ================"+goToMinute());
      val sorAirRefillUnion = sqlContext.sql("""
          select * from ha1_sub_air_before_ref
          UNION ALL 
          select * from ha1_sub_air_after_ref
        """)
        
       sorAirRefillUnion.registerTempTable("ha1_sor_air_refill_union")
      
     
       
     
     /*****************************************************************************************************
     * 
     *     
     * Step 11 : Produce for Level 2 for child to get DA ( Join With Mada and Reference) 
     * Get DA values
     * 
     *****************************************************************************************************/    
     log.warn("================== Step 11 : Produce for Level 2 for child to get DA ( Join With Mada and Reference)   ================"+goToMinute());
      val sorSubAirRefill01 =  sqlContext.sql( 
            """
              SELECT substring(air.timeStamp,1,8) timeStamp,
              air.accountNumber,
              air.refillType,
              air.refillProfileID,
              air.prefix,
              air.city,
              air.provider_id,
              air.country,
              air.currentServiceclass svcClassName,
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
         sorSubAirRefill01.registerTempTable("ha1_sor_sub_air_rev")
       
       
       
     /*****************************************************************************************************
     * 
     *     
     * Grouping result from step 11 
     * 
     * 
     *****************************************************************************************************/
      val sorSubAirRefill02 =  sqlContext.sql( 
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
          FROM  ha1_sor_sub_air_rev air
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
       sorSubAirRefill02.registerTempTable("sor_sub_air_ref_final")  
       
       
            
      /*****************************************************************************************************
     * 
     *     
     * Step 12 : Union ALL Source Table
     * 
     *****************************************************************************************************/
     log.warn("==================Step 12 :  Union ALL Source Table ================"+goToMinute());
      val airRefillDetail = sqlContext.sql("""
          select * from sor_air_refill
          UNION  
          select * from sor_sub_air_ref_final
        """)
        
       airRefillDetail.registerTempTable("air_refill_detail")  
       
         
       
     /*****************************************************************************************************
     * 
     *     
     * Produce for Reject Ref 2 ( Revenue )
     * 
     *****************************************************************************************************/ 
     log.warn("==================Step 13 : Produce for Reject Ref 2 ( Revenue ) ================"+goToMinute());  
     /*val rejRevAirRef=sqlContext.sql( 
        """
            SELECT ref.transaction_date TRANSACTION_DATE,
            ref.msisdn MSISDN,
            ref.prefix PREFIX,
            ref.city CITY,
            ref.provider_id PROVIDER_ID,
            ref.country COUNTRY,
            ref.svcClassName SERVICECLASSID,
            ref.promo_package_code PROMO_PACKAGE_CODE,
            ref.promo_package_name PROMO_PACKAGE_NAME,
            ref.brand_name BRAND_NAME,
            cast(ref.total_amount as double) TOTAL_AMOUNT,
            cast(ref.total_hits as double) TOTAL_HIT,
            ref.revenue_code REVENUE_CODE,
            ref.service_type SERVICE_TYPE,
            ref.gl_code GL_CODE,
            ref.gl_name GL_NAME,
            ref.real_filename REAL_FILENAME,
            ref.hlr_branch_nm HLR_BRANCH_NM,
            ref.hlr_region_nm HLR_REGION_NM,
            ref.revenueflag REVENUEFLAG,
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
            ref.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            ref.offerId OFFER_ID,
            ref.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            ref.jobID JOB_ID,
            ref.recordID RECORD_ID,
            ref.prcDT PRC_DT,
            ref.area SRC_TP,
            ref.fileDT FILE_DT,
            case 
                     when service_type is null then 'Revenue Code is Not Found' 
                     when trim(revenueflag) = 'UNKNOWN' then 'Mada is Not Found'
                     else 'Reject Ref'
            end as rej_rsn 
          FROM sor_air_refill ref where service_type is null and revenueflag ='UNKNOWN'
         UNION ALL
            SELECT ref.transaction_date TRANSACTION_DATE,
            ref.msisdn MSISDN,
            ref.prefix PREFIX,
            ref.city CITY,
            ref.provider_id PROVIDER_ID,
            ref.country COUNTRY,
            ref.svcClassName SERVICECLASSID,
            ref.promo_package_code PROMO_PACKAGE_CODE,
            ref.promo_package_name PROMO_PACKAGE_NAME,
            ref.brand_name BRAND_NAME,
            cast(ref.total_amount as double) TOTAL_AMOUNT,
            cast(ref.total_hits as double) TOTAL_HIT,
            ref.revenue_code REVENUE_CODE,
            ref.service_type SERVICE_TYPE,
            ref.gl_code GL_CODE,
            ref.gl_name GL_NAME,
            ref.real_filename REAL_FILENAME,
            ref.hlr_branch_nm HLR_BRANCH_NM,
            ref.hlr_region_nm HLR_REGION_NM,
            ref.revenueflag REVENUEFLAG,
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
            ref.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            ref.offerId OFFER_ID,
            ref.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            ref.jobID JOB_ID,
            ref.recordID RECORD_ID,
            ref.prcDT PRC_DT,
            ref.area SRC_TP,
            ref.fileDT FILE_DT,
            case 
               when service_type is null then 'Revenue Code is Not Found' 
               when trim(revenueflag) = 'UNKNOWN' then 'Mada is Not Found'
               else 'Reject Ref' end as rej_rsn 
            from sor_sub_air_ref_final ref
            where service_type is null and revenueflag ='UNKNOWN'
        """
         );*/
      
      
     val rejRevAirRef=sqlContext.sql( 
        """
           SELECT ref.transaction_date TRANSACTION_DATE,
            ref.msisdn MSISDN,
            ref.prefix PREFIX,
            ref.city CITY,
            ref.provider_id PROVIDER_ID,
            ref.country COUNTRY,
            ref.svcClassName SERVICECLASSID,
            ref.promo_package_code PROMO_PACKAGE_CODE,
            ref.promo_package_name PROMO_PACKAGE_NAME,
            ref.brand_name BRAND_NAME,
            cast(ref.total_amount as double) TOTAL_AMOUNT,
            cast(ref.total_hits as double) TOTAL_HIT,
            ref.revenue_code REVENUE_CODE,
            ref.service_type SERVICE_TYPE,
            ref.gl_code GL_CODE,
            ref.gl_name GL_NAME,
            ref.real_filename REAL_FILENAME,
            ref.hlr_branch_nm HLR_BRANCH_NM,
            ref.hlr_region_nm HLR_REGION_NM,
            ref.revenueflag REVENUE_FLAG,
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
            ref.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            ref.offerId OFFER_ID,
            ref.areaName OFFER_AREA_NAME,
            ref.jobID JOB_ID,
            ref.recordID RECORD_ID,
            ref.prcDT PRC_DT,
            ref.area SRC_TP,
            ref.fileDT FILE_DT,
            case 
               when service_type is null then 'Revenue Code is Not Found' 
               when revenueflag is null then 'Mada is Not Found'
               else 'Reject Ref' end as REJ_RSN 
            from air_refill_detail ref
            where service_type is null and revenueflag is null
        """
         );

     Common.cleanDirectory(sc, OUTPUT_REJ_REV_DIR );
     rejRevAirRef.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REV_DIR);
//     rejRevAirRef.show();
       
     
     
     /*****************************************************************************************************
     * 
     *     
     *Step 14 : Produce for SOR Detail
     * 
     *****************************************************************************************************/ 
     log.warn("==================Step 14 : Produce for SOR Detail ================"+goToMinute());
    
    /* val sorFinalAirRef=sqlContext.sql( 
        """
            SELECT ref.transaction_date TRANSACTION_DATE,
            ref.msisdn MSISDN,
            ref.prefix PREFIX,
            ref.city CITY,
            ref.provider_id PROVIDER_ID,
            ref.country COUNTRY,
            ref.svcClassName SERVICECLASSID,
            ref.promo_package_code PROMO_PACKAGE_CODE,
            ref.promo_package_name PROMO_PACKAGE_NAME,
            ref.brand_name BRAND_NAME,
            cast(ref.total_amount as double) TOTAL_AMOUNT,
            cast(ref.total_hits as double) TOTAL_HIT,
            ref.revenue_code REVENUE_CODE,
            ref.service_type SERVICE_TYPE,
            ref.gl_code GL_CODE,
            ref.gl_name GL_NAME,
            ref.real_filename REAL_FILENAME,
            ref.hlr_branch_nm HLR_BRANCH_NM,
            ref.hlr_region_nm HLR_REGION_NM,
            ref.revenueflag REVENUEFLAG,
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
            ref.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            ref.offerId OFFER_ID,
            ref.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            ref.jobID JOB_ID,
            ref.recordID RECORD_ID,
            ref.prcDT PRC_DT,
            ref.area SRC_TP,
            ref.fileDT FILE_DT
          from sor_air_refill ref where service_type is not  null and revenueflag <> 'UNKNOWN'
          UNION ALL 
            SELECT  ref.transaction_date TRANSACTION_DATE,
            ref.msisdn MSISDN,
            ref.prefix PREFIX,
            ref.city CITY,
            ref.provider_id PROVIDER_ID,
            ref.country COUNTRY,
            ref.svcClassName SERVICECLASSID,
            ref.promo_package_code PROMO_PACKAGE_CODE,
            ref.promo_package_name PROMO_PACKAGE_NAME,
            ref.brand_name BRAND_NAME,
            cast(ref.total_amount as double) TOTAL_AMOUNT,
            cast(ref.total_hits as double) TOTAL_HIT,
            ref.revenue_code REVENUE_CODE,
            ref.service_type SERVICE_TYPE,
            ref.gl_code GL_CODE,
            ref.gl_name GL_NAME,
            ref.real_filename REAL_FILENAME,
            ref.hlr_branch_nm HLR_BRANCH_NM,
            ref.hlr_region_nm HLR_REGION_NM,
            ref.revenueflag REVENUEFLAG,
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
            ref.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            ref.offerId OFFER_ID,
            ref.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            ref.jobID JOB_ID,
            ref.recordID RECORD_ID,
            ref.prcDT PRC_DT,
            ref.area SRC_TP,
            ref.fileDT FILE_DT
           from sor_sub_air_ref_final ref
           where service_type is not null and revenueflag  <> 'UNKNOWN'
        """
         );
         
        */
     
        val sorFinalAirRef=sqlContext.sql( 
        """
          SELECT  ref.transaction_date TRANSACTION_DATE,
            ref.msisdn MSISDN,
            ref.prefix PREFIX,
            ref.city CITY,
            ref.provider_id PROVIDER_ID,
            ref.country COUNTRY,
            ref.svcClassName SERVICECLASSID,
            ref.promo_package_code PROMO_PACKAGE_CODE,
            ref.promo_package_name PROMO_PACKAGE_NAME,
            ref.brand_name BRAND_NAME,
            cast(ref.total_amount as double) TOTAL_AMOUNT,
            cast(ref.total_hits as double) TOTAL_HIT,
            ref.revenue_code REVENUE_CODE,
            ref.service_type SERVICE_TYPE,
            ref.gl_code GL_CODE,
            ref.gl_name GL_NAME,
            ref.real_filename REAL_FILENAME,
            ref.hlr_branch_nm HLR_BRANCH_NM,
            ref.hlr_region_nm HLR_REGION_NM,
            ref.revenueflag REVENUE_FLAG,
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
            ref.origin_timestamp ORIGIN_TIMESTAMP,
            cast (null as string) MGR_SVCCLS_ID,
            ref.offerId OFFER_ID,
            ref.areaName OFFER_AREA_NAME,
            --cast (null as string) pname,
            --cast (null as string) load_job_id,
            ref.jobID JOB_ID,
            ref.recordID RECORD_ID,
            ref.prcDT PRC_DT,
            ref.area SRC_TP,
            ref.fileDT FILE_DT
           from air_refill_detail ref
           where service_type is not null and revenueflag is not null
        """
         );
         
              
       Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_REV_DIR, "/SRC_TP=AIR_REFILL/*/JOB_ID=" + jobID)
       sorFinalAirRef.write.partitionBy("SRC_TP","TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_REV_DIR);
//       sorFinalAirRef.show();
       
       
     /*****************************************************************************************************
     * 
     *     
     * Step 14 : Produce for CDR Inquiry and Join with Bank and Reload Type  ( Create 1 dedicated class for CDR Inquiry REF )
     * 
     *****************************************************************************************************/  
     log.warn("================== Step 14 : Produce for CDR Inquiry and Join with Bank and Reload Type ================"+goToMinute());
     val airCdrInquiryJoin = sqlContext.sql(
           """
             SELECT recordID as KEY,
             accountNumber MSISDN, 
             originTimeStamp TRANSACTIONTIMESTAMP,
             case
                  when substr(currentServiceclass,1,1) in ('4','7') then 'POSTPAID'
                  else 'PREPAID'
             end as SUBSCRIBERTYPE,
             (transactionAmount/100) as AMOUNT ,
             "" as DA_DETAIL,
             currentServiceclass as CURRENTSERVICECLASS,
             activationDate ACTIVATIONDATE,
             currentServiceclass as SERVICECLASSID,
             svcClassName as SERVICECLASSNAME,
             promo_package_name as PROMOPACKAGENAME,
             brand_name as BRANDNAME ,            
             "" as NEWSERVICECLASS ,  
             "" as NEWSERVICECLASSNAME ,  
             "" as NEWPROMOPACKAGENAME ,  
             "" as NEWBRANDNAME,
             air.originNodeType ORIGINNODETYPE,
             air.originNodeID ORIGINHOSTNAME,
             externalData1 as EXTERNALDATA1,
             "" as PROGRAMNAME,
             "" as PROGRAMOWNER,   
             "" as PROGRAMCATEGORY,
             case 
              when ref.column4 is null then ""
              else ref.column4
             end as BANKNAME,
             case 
              when ref_detail.column2 is null then ""
              else ref_detail.column2
             end as BANKDETAIL,
             accountInformationBeforeRefill MAINACCOUNTBALANCEBEFORE,
             accountInformationAfterRefill MAINACCOUNTBALANCEAFTER,
             "" as LAC,
             "" as CELLID,
             'Refill' as TRANSACTIONTYPE,
             realFilename FILENAMEINPUT,
             case reload_Type.reloadType 
                when 'IVDB' then 'REGULAR'
                when 'IGATE' then 'REGULAR'
             else
              voucher_Type.column9 
             end as VOUCHER_TYPE,
             case 
              	when air.originNodeID = 'ssppadpsvr1' then 'Transfer Pulsa'				      
                when reload_Type.reloadType = 'IVDB' then 'VOUCHER PHYSIC'
                when reload_Type.reloadType = 'IGATE' then 'PARTNER'
                when reload_Type.reloadType = 'SEV' then 'ELECTRIC'
        		end as RELOAD_TYPE,
            jobID  JOBID
            from 
               ( select * from ha1_air_refill_inquiry ) air  
               left join BankName ref on substr(trim(air.externalData1),7,3) = trim(ref.column1) 
               left join BankDetail ref_detail on substr(trim(air.externalData1),10,2) = trim(ref_detail.column1)
               left join VoucherType voucher_Type on air.refillType = voucher_Type.column2 and air.refillProfileID = voucher_Type.column3
               left join ReloadType reload_Type on air.originNodeType = reload_Type.originNodeType and air.originNodeID = reload_Type.originHostName
           """) 
  
       airCdrInquiryJoin.registerTempTable("sor_air_cdr_inquiry_refill01")
     
       val airCdrInquiryJoinRegular = airCdrInquiryJoin.where("VOUCHER_TYPE='REGULAR' or RELOAD_TYPE in ('Transfer Pulsa') ").map {
       row => Row(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10),row(11),row(12),row(13),row(14),row(15),row(16),row(17), row(18),
             row(19),row(20),row(21),row(22),row(23),row(24).toString().split("#")(1),row(25).toString().split("#")(1),row(26),row(27), row(28),
             row(29),row(30),row(31),row(32))
       }
     
       val ha1CdrInquiryJoinRegular = sqlContext.createDataFrame(airCdrInquiryJoinRegular, subAirRefill.ha1CdrInquirySchema)
       ha1CdrInquiryJoinRegular.registerTempTable("ha1_cdr_inquiry_Regular")
      
      val airCdrInquiryJoinNonRegular = airCdrInquiryJoin.where("VOUCHER_TYPE in ('SMS','DATA','GPRS')").map {
       row => Row( row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10),row(11),row(12),row(13),row(14),row(15),row(16),row(17), row(18),
             row(19),row(20),row(21),row(22),row(23),getTuppleRecordCdrInquiry(row(24).toString()),getTuppleRecordCdrInquiry(row(25).toString()), 
             row(26),row(27),row(28),row(29),row(30),row(31),row(32))
       }

       val ha1CdrInquiryJoinNonRegular = sqlContext.createDataFrame(airCdrInquiryJoinNonRegular, subAirRefill.ha1CdrInquirySchema)
       ha1CdrInquiryJoinNonRegular.registerTempTable("ha1_cdr_inquiry_NonRegular")
       
       
     /*****************************************************************************************************
     * 
     *     
     * Step 15 : Detail Cdr Inquiry -- ( Create 1 dedicated class for CDR Inquiry REF )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 15 : Detail Cdr Inquiry ================"+goToMinute());
     val airRefillInqCdrInquiryFinal = sqlContext.sql(
           """
             SELECT KEY,
                MSISDN, 
                TRANSACTIONTIMESTAMP,
                SUBSCRIBERTYPE,
                AMOUNT,
                DA_DETAIL,
                CURRENTSERVICECLASS,
                ACTIVATIONDATE,
                SERVICECLASSID,
                SERVICECLASSNAME,
                PROMOPACKAGENAME,
                BRANDNAME ,            
                NEWSERVICECLASS ,  
                NEWSERVICECLASSNAME ,  
                NEWPROMOPACKAGENAME ,  
                NEWBRANDNAME,
                ORIGINNODETYPE,
                ORIGINHOSTNAME,
                EXTERNALDATA1,
                PROGRAMNAME,
                PROGRAMOWNER,   
                PROGRAMCATEGORY,
                BANKNAME,
                BANKDETAIL,
                cast ((MAINACCOUNTBALANCEBEFORE/100) as string) MAINACCOUNTBALANCEBEFORE,
                cast ((MAINACCOUNTBALANCEAFTER/100)  as string) MAINACCOUNTBALANCEAFTER,
                LAC,
                CELLID,
                TRANSACTIONTYPE,
                FILENAMEINPUT,
                CASE 
                    WHEN VOUCHER_TYPE IS NULL THEN ""
                    ELSE VOUCHER_TYPE
                END AS VOUCHER_TYPE,
                RELOAD_TYPE,
                JOBID,
                "" CDR_TP 
             FROM ha1_cdr_inquiry_Regular 
             UNION
                SELECT KEY,
                MSISDN, 
                TRANSACTIONTIMESTAMP,
                SUBSCRIBERTYPE,
                AMOUNT,
                DA_DETAIL,
                CURRENTSERVICECLASS,
                ACTIVATIONDATE,
                SERVICECLASSID,
                SERVICECLASSNAME,
                PROMOPACKAGENAME,
                BRANDNAME ,            
                NEWSERVICECLASS ,  
                NEWSERVICECLASSNAME ,  
                NEWPROMOPACKAGENAME ,  
                NEWBRANDNAME,
                ORIGINNODETYPE,
                ORIGINHOSTNAME,
                EXTERNALDATA1,
                PROGRAMNAME,
                PROGRAMOWNER,   
                PROGRAMCATEGORY,
                BANKNAME,
                BANKDETAIL,
                MAINACCOUNTBALANCEBEFORE,
                MAINACCOUNTBALANCEAFTER,
                LAC,
                CELLID,
                TRANSACTIONTYPE,
                FILENAMEINPUT,
                CASE 
                    WHEN VOUCHER_TYPE IS NULL THEN ""
                    ELSE VOUCHER_TYPE
                END AS VOUCHER_TYPE,
                RELOAD_TYPE,
                JOBID,
                case   
                  when (MAINACCOUNTBALANCEAFTER like '1#%' or MAINACCOUNTBALANCEAFTER like '[1#%') then 'C'
                  when (MAINACCOUNTBALANCEAFTER like '35#%' or MAINACCOUNTBALANCEAFTER like '[35#%') then 'P'
                  else ""
                end CDR_TP  
             FROM ha1_cdr_inquiry_NonRegular 
            """) 
  
       Common.cleanDirectory(sc, OUTPUT_CDR_INQ_DIR_REFFILL );
       airRefillInqCdrInquiryFinal.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_CDR_INQ_DIR_REFFILL);
    
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