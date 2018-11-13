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
 * This script is used for handling Adjustment CDR Inquiry
 * 
 */

object subAirAdjInquiry {
  
    def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

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
    val inputFile = args(3)
    val OUTPUT_CDR_INQ_DIR_ADJ = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_CDR_INQ_DIR_ADJ").concat(prcDt).concat("_").concat(jobID)

    val existDir = Common.isDirectoryExists(sc, inputFile);
    var subAirInptRDDInq = sc.textFile(inputFile);
    if (subAirInptRDDInq.count() == 0) {
      sys.exit(1);
    }

    /**
     * ***************************************************************************************************
     *
     * STEP 01.Load Reference Data
     *
     * ***************************************************************************************************
     */
    log.warn("===============STEP 01. LOAD REFERENCE===================");

    val svcClassDFInq = ReferenceDF.getServiceClassDF(sqlContext)
    svcClassDFInq.registerTempTable("ServiceClassInq")
    svcClassDFInq.persist()
    svcClassDFInq.count()

    val sdpOfferDFInq = ReferenceDF.getSDPOffer(sqlContext)
    sdpOfferDFInq.registerTempTable("SDPOfferInq")

    val intcctDFInq = ReferenceDF.getIntcctAllDF(sqlContext)
    intcctDFInq.registerTempTable("IntcctInq")
    sqlContext.cacheTable("IntcctInq")
    intcctDFInq.count();
    //  intcctDF.show();

    val regionBranchDFInq = ReferenceDF.getRegionBranchDF(sqlContext)
    regionBranchDFInq.registerTempTable("RegBranchInq")
    regionBranchDFInq.persist()
    regionBranchDFInq.count();

    val revcodeDFInq = ReferenceDF.getRevenueCodeDF(sqlContext)
    revcodeDFInq.registerTempTable("RevenueCodeInq")
    revcodeDFInq.persist()
    revcodeDFInq.count();

    val madaDFInq = ReferenceDF.getMaDaDF(sqlContext)
    madaDFInq.registerTempTable("MadaInq")
    madaDFInq.persist()
    madaDFInq.count();

    val svcClassOfferDFInq = ReferenceDF.getServiceClassOfferDF(sqlContext)
    svcClassOfferDFInq.registerTempTable("ServiceClassOfferInq")
    svcClassOfferDFInq.persist()
    svcClassOfferDFInq.count()

    val stgAirAdjInq = subAirInptRDDInq.filter(x => x.count(_ == '|') == 33).map(line => line.split("\\|")).map { p =>
      Row(
        p(0), //originNodeType
        p(1), //originHostName
        p(8), //timeStamp
        "62".concat(p(14)), //accountNumber
        "62".concat(p(17)), //subscriberNumber
        p(9), //current service class
        p(12), //transactionAmount
        p(23), //externalData1
        p(32), //realFilename
        p(5), //originTimeStamp
        p(30), //dedicated_account ( used_for_level_2 )
        "62".concat(p(14)).padTo(8, ".").mkString(""), // msisdn_pad
        p(20), //accountFlagBefore
        p(21), //accountFlagAfter
        jobID, // job_id
        Common.getKeyCDRInq("62".concat(p(14)), p(8).toString().substring(0, 14)), //rcrd_id
        goPrcDate(), // prc_dt
        "AIR_ADJ", // area
        p(33) // file DT
        )
    }

    val stgAirAdjustmentInq = sqlContext.createDataFrame(stgAirAdjInq, subAirAdjustment.stgAirAdjSchema);
    stgAirAdjustmentInq.registerTempTable("stg_air_adjustmentInq")
    stgAirAdjustmentInq.persist();
    
    

    /**
     * ***************************************************************************************************
     *
     *
     * Step 01 : Join with Interconnect and Branch. ( Level 1 )
     *
     * ***************************************************************************************************
     */
    log.warn("================== Step 03 : Join with Interconnect and Branch. ( Level 1 ) ================" + goToMinute());
    val ha1AirAdj01Inq = sqlContext.sql(
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
            SELECT * FROM stg_air_adjustmentInq where externalData1 LIKE '2668%'  
            and currentServiceclass IS NOT NULL and accountNumber IS NOT NULL and timeStamp IS NOT NULL and originTimeStamp IS NOT NULL
          )  air
           left join SDPOfferInq sdp on air.accountNumber = sdp.msisdn and (substring(air.timeStamp,1,8) between sdp.effDt and sdp.endDt )
          left join ServiceClassOfferInq svcclsofr on air.currentServiceclass = svcclsofr.svcClassCode 
            and (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and sdp.offerID = svcclsofr.offerId
          left join ServiceClassInq svccls on air.currentServiceclass = svccls.svcClassCode 
            and (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt )

          """);
    ha1AirAdj01Inq.registerTempTable("ha1_air_adj01Inq")
    ha1AirAdj01Inq.persist();

    val joinIntcctDFInq = Common.getIntcct(sqlContext, ha1AirAdj01Inq, intcctDFInq, "accountNumber", "event_dt", "prefix", "country", "city", "provider_id")
    joinIntcctDFInq.registerTempTable("HA1AirAdjJoinIntcctInq")

    val ha1AirAdjInq = sqlContext.sql(
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
            from HA1AirAdjJoinIntcctInq air
            left join  RegBranchInq regBranch on air.city = regBranch.city
            """);
    ha1AirAdjInq.registerTempTable("ha1_air_adjInq")
    ha1AirAdjInq.persist();
    //      ha1AirAdj.show();

    stgAirAdjustmentInq.unpersist();
    svcClassDFInq.unpersist()
    sdpOfferDFInq.unpersist()
    intcctDFInq.unpersist();
    regionBranchDFInq.unpersist();

    /**
     * ***************************************************************************************************
     *
     *
     *  Step 02 : Load Data for CDR Inquiry
     *
     * ***************************************************************************************************
     */
    log.warn("================== Step 02 : Produce for CDR Inquiry ( Cdr Inquiry )  ================" + goToMinute());

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
               from ha1_air_adjInq          
           """)

    Common.cleanDirectory(sc, OUTPUT_CDR_INQ_DIR_ADJ);
    airAdjInqDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_CDR_INQ_DIR_ADJ);
    
      
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