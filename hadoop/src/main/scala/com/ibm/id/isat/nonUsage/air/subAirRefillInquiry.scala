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



object subAirRefillInquiry  {
  
    def main(args: Array[String]) {
      
     val sc = new SparkContext(new SparkConf())
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     
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
      
     val OUTPUT_CDR_INQ_DIR_REFFILL = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_CDR_INQ_DIR_REFFILL").concat(prcDt).concat("_").concat(jobID)
     
     val existDir= Common.isDirectoryExists(sc, inputFile);
     var subAirInptRDDInq = sc.textFile(inputFile);  
     if ( subAirInptRDDInq.count() == 0){
       sys.exit(1);
     }

     
     
     /*****************************************************************************************************
     * 
     * STEP 01. Load Reference Data
     * 
     *****************************************************************************************************/

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
      svcClassOfferDFInq.count();
      
       val reloadTypeDFInq = ReferenceDF.getRecordReloadTypeDF(sqlContext)   
      reloadTypeDFInq.registerTempTable("ReloadTypeInq")
      reloadTypeDFInq.persist()
      reloadTypeDFInq.count()
      
      val voucherTypeDFInq = ReferenceDF.getRecordVoucherTypeDF(sqlContext)
      voucherTypeDFInq.registerTempTable("VoucherTypeInq") 
      voucherTypeDFInq.persist()
      voucherTypeDFInq.count()
      
      val bankNameDFInq = ReferenceDF.getRecordBankNameDF(sqlContext)
      bankNameDFInq.registerTempTable("BankNameInq")
      bankNameDFInq.persist()
      bankNameDFInq.count()
      
      val bankDetailDFInq = ReferenceDF.getRecordBankDetailDF(sqlContext)
      bankDetailDFInq.registerTempTable("BankDetailInq")
      bankDetailDFInq.persist()
      bankDetailDFInq.count()  
      
      
      log.warn("==================STEP 02. SPLIT INPUT FILE ================"+goToMinute());         
      val stgAirRefillInq = subAirInptRDDInq.filter ( x => x.count (_ == '|') == 57).map(line => line.split("\\|")).map {p => Row(
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
   
    val stgAirRefInq = sqlContext.createDataFrame(stgAirRefillInq, subAirRefill.stgAirReffSchema);
    stgAirRefInq.registerTempTable("stg_air_reffInq")
    stgAirRefInq.persist()
      
        /*****************************************************************************************************
     * 
     *     
     * Step 03 : Join with Interconnect and Branch
     * 
     *****************************************************************************************************/
     log.warn("================== Step 03 : Join with Interconnect and Branch. ================"+goToMinute());
     val ha1AirRefill01Inq =  sqlContext.sql( 
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
             SELECT * FROM stg_air_reffInq a   
          )  air
          left join SDPOfferInq sdp on air.accountNumber = sdp.msisdn and (substring(air.timeStamp,1,8) between sdp.effDt and sdp.endDt )
          left join ServiceClassOfferInq svcclsofr on air.currentServiceclass = svcclsofr.svcClassCode 
            and (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and sdp.offerID = svcclsofr.offerId
          left join ServiceClassInq svccls on air.currentServiceclass = svccls.svcClassCode 
            and (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt )
          """
         );
      ha1AirRefill01Inq.registerTempTable("ha1_air_refill01Inq")
      ha1AirRefill01Inq.persist();
      stgAirRefInq.unpersist();  
      svcClassDFInq.unpersist()
      sdpOfferDFInq.unpersist()
      intcctDFInq.unpersist();
      regionBranchDFInq.unpersist();
      
      val joinIntcctDFInq = Common.getIntcct(sqlContext, ha1AirRefill01Inq, intcctDFInq, "accountNumber", "event_dt", "prefix", "country", "city", "provider_id") 
      joinIntcctDFInq.registerTempTable("HA1AirRefillJoinIntcctInq")
      
              
     /*****************************************************************************************************
     * 
     *     
     * Step 04 : Provide CDR Inquiry Baseline.
     * 
     *****************************************************************************************************/
     log.warn("==================  Step 04 : Provide CDR Inquiry Baseline. ================"+goToMinute());
      val ha1AirRefillJoinInquiryInq = sqlContext.sql(
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
            from HA1AirRefillJoinIntcctInq air
            """);
      ha1AirRefillJoinInquiryInq.registerTempTable("ha1_air_refill_inquiryInq")
      ha1AirRefillJoinInquiryInq.persist();
      
      
    /*****************************************************************************************************
     * 
     *     
     * Step 05 : Produce for CDR Inquiry and Join with Bank and Reload Type
     * 
     *****************************************************************************************************/  
     log.warn("================== Step 05 : Produce for CDR Inquiry and Join with Bank and Reload Type ================"+goToMinute());
     val airCdrInquiryJoinInq = sqlContext.sql(
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
               ( select * from ha1_air_refill_inquiryInq ) air  
               left join BankNameInq ref on substr(trim(air.externalData1),7,3) = trim(ref.column1) 
               left join BankDetailInq ref_detail on substr(trim(air.externalData1),10,2) = trim(ref_detail.column1)
               left join VoucherTypeInq voucher_Type on air.refillType = voucher_Type.column2 and air.refillProfileID = voucher_Type.column3
               left join ReloadTypeInq reload_Type on air.originNodeType = reload_Type.originNodeType and air.originNodeID = reload_Type.originHostName
           """) 
  
          airCdrInquiryJoinInq.registerTempTable("sor_air_cdr_inquiry_refill01Inq")

          
       val airCdrInquiryJoinRegularInq = airCdrInquiryJoinInq.where("VOUCHER_TYPE='REGULAR' or RELOAD_TYPE in ('Transfer Pulsa') ").map {
       row => Row(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10),row(11),row(12),row(13),row(14),row(15),row(16),row(17), row(18),
             row(19),row(20),row(21),row(22),row(23),row(24).toString().split("#")(1),row(25).toString().split("#")(1),row(26),row(27), row(28),
             row(29),row(30),row(31),row(32))
       }
     
       val ha1CdrInquiryJoinRegularInq = sqlContext.createDataFrame(airCdrInquiryJoinRegularInq, subAirRefill.ha1CdrInquirySchema)
       ha1CdrInquiryJoinRegularInq.registerTempTable("ha1_cdr_inquiry_RegularInq")
       
      
      val airCdrInquiryJoinNonRegularInq = airCdrInquiryJoinInq.where("VOUCHER_TYPE in ('SMS','DATA','GPRS')").map {
       row => Row( row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10),row(11),row(12),row(13),row(14),row(15),row(16),row(17), row(18),
             row(19),row(20),row(21),row(22),row(23),getTuppleRecordCdrInquiry(row(24).toString()),getTuppleRecordCdrInquiry(row(25).toString()), 
             row(26),row(27),row(28),row(29),row(30),row(31),row(32))
       }
       
       val ha1CdrInquiryJoinNonRegularInq = sqlContext.createDataFrame(airCdrInquiryJoinNonRegularInq, subAirRefill.ha1CdrInquirySchema)
       ha1CdrInquiryJoinNonRegularInq.registerTempTable("ha1_cdr_inquiry_NonRegularInq")
       
       
     /*****************************************************************************************************
     * 
     *     
     * Step 06 : Detail Cdr Inquiry
     * 
     *****************************************************************************************************/
     log.warn("================== Step 06 : Detail Cdr Inquiry ================"+goToMinute());
     val airRefillInqCdrInquiryFinalInq = sqlContext.sql(
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
             FROM ha1_cdr_inquiry_RegularInq 
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
             FROM ha1_cdr_inquiry_NonRegularInq 
            """) 
  
       Common.cleanDirectory(sc, OUTPUT_CDR_INQ_DIR_REFFILL );
       airRefillInqCdrInquiryFinalInq.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_CDR_INQ_DIR_REFFILL);
    
  }

  def getTuppleRecord(content: String): String = {
    var line: String = "";
    //val valueValid = Common.getTupleInTupleNonUsageRefill(content,"|",9,"#",7,"]",0,4)  
    val valueValid = Common.getTupleInTupleNonUsageRefill(content, "#", 7, "]", 0, 4)
    for (x <- valueValid) {
      line = line + x
      ///println(x)
    }
    return line;
  }

  def getTuppleRecordCdrInquiry(content: String): String = {
    var line: String = "";
    //val valueValid = Common.getTupleInTupleNonUsageRefill(content,"|",9,"#",7,"]",0,4)  
    val valueValid = getTupleInTupleNonUsageRefillCdrInquiry(content, "#", 7, "]", 0, 4)
    for (x <- valueValid) {
      line = line + x
      ///println(x)
    }
    return line;
  }

  def goPrcDate(): String = {
    val today = Calendar.getInstance().getTime
    val curTimeFormat = new SimpleDateFormat("YYYYMMdd")
    val prcDt = curTimeFormat.format(today).toString()
    return prcDt;
  }

  def goToMinute(): String = {
    val today = Calendar.getInstance().getTime
    val curTimeFormat = new SimpleDateFormat("YYYYMMdd HH:MM:SS")
    val prcDt = curTimeFormat.format(today).toString()
    return prcDt;
  }

  def getInquiryRegular(content: String, flag: String): String = {
    var line: String = "";
    println(content);
    if (!content.equalsIgnoreCase("")) {
      if (flag.equalsIgnoreCase("REGULAR")) {
        line = Common.getTuple(content, "#", 1);
      } else {
        line = content.split(']').map { x => x.split('*')(0).concat("#").concat((x.split('*')(5))) }.mkString("[")
      }
      line = "";
    }
    return line;
  }

  def getTupleInTupleNonUsageRefillCdrInquiry(str: String, delLV2: String, tupleLV2Int: Int, delLV3: String, tupleLV2Num01: Int, tupleLV2Num04: Int): Array[String] = {

    try {

      var LV2_ARR = str.split("[\\" + delLV2 + "]");
      if (LV2_ARR.length < tupleLV2Int) {
        return null;
      }
      var tupleLV2 = LV2_ARR(tupleLV2Int);

      var LV3_ARR = tupleLV2.split("\\" + delLV3 + "");
      var line: Array[String] = new Array[String](LV3_ARR.length);
      var ch: Int = 0;
      while (ch < LV3_ARR.length) {
        var tupple1 = Common.getTuple(LV3_ARR(ch), "*", tupleLV2Num01);
        if (Common.getTuple(LV3_ARR(ch), "*", tupleLV2Num01).equals("")) {
          tupple1 = "0";
        }
        var tupple3 = Common.getTuple(LV3_ARR(ch), "*", tupleLV2Num04);
        if (Common.getTuple(LV3_ARR(ch), "*", tupleLV2Num04).equals("")) {
          tupple3 = "0";
        } else {
          tupple3 = (Common.getTuple(LV3_ARR(ch), "*", tupleLV2Num04).toDouble / 100).toString();
        }
        if (ch == LV3_ARR.length - 1) {
          line(ch) = tupple1.concat("#").concat(tupple3)
        } else {
          line(ch) = tupple1.concat("#").concat(tupple3).concat("[")
        }

        ch = ch + 1;
      }

      return line;
    } catch {
      case e: Exception => return null
    }
  }
}