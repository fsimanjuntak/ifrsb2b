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
    

/**
 * This object is used to produce revenue from SDP Lifecycle
 * Insert into CDR Inquiry
 * Insert into SDP Lifecycle detail
 * Insert into SDP Lifecycle summary
 * @author Meliani Efelina
 */
    
object SDPLcyCDRInq {
  

    // define sql Context
    //val sc = new SparkContext("local", "testing spark ", new SparkConf());  
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
    import sqlContext.implicits._
    
    
    
    def main(args: Array[String]) {
    
        println("Start Processing SDP Lifecycle");  
        //System.setProperty("hadoop.home.dir", "C:\\winutil\\") 
        import org.apache.log4j.{Level, Logger}
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)
        
        // Get all reference Data
        
        // Service Class  
        val svcClassDF = ReferenceDF.getServiceClassDF(sqlContext)
        svcClassDF.registerTempTable("ServiceClass")
        svcClassDF.persist()
        svcClassDF.count()
        
         //Interconnect Network and Short Code
        val intcctDF = ReferenceDF.getIntcctAllDF(sqlContext)   
        intcctDF.registerTempTable("Intcct")
        sqlContext.cacheTable("Intcct")
        
        // Region Branch   
        val regionBranchDF = ReferenceDF.getRegionBranchDF(sqlContext)
        regionBranchDF.registerTempTable("RegBranch")
        regionBranchDF.persist()
        regionBranchDF.count()
                
        // Revenue Code
        val revcodeDF = ReferenceDF.getRevenueCodeDF(sqlContext)
        revcodeDF.registerTempTable("RevenueCode")
        revcodeDF.persist()
        revcodeDF.count()
        
        // MADA
        val madaDF = ReferenceDF.getMaDaDF(sqlContext)
        madaDF.registerTempTable("Mada")
        madaDF.persist()
        madaDF.count()
        
        
        // Service Class Offer 
        val svcClassOfferDF = ReferenceDF.getServiceClassOfferDF(sqlContext)
        svcClassOfferDF.registerTempTable("ServiceClassOffer")
        svcClassOfferDF.persist()
        svcClassOfferDF.count()
        
        // Offer
        val sdpOffrDF = ReferenceDF.getSDPOffer(sqlContext)
        sdpOffrDF.registerTempTable("SDPOffer")
        
        
        // Set all the variables
        val today = Calendar.getInstance().getTime;
        val curTimeFormat = new SimpleDateFormat("YYYYMMdd");
        val PRC_DT=args(0)
        val JOB_ID=args(1)
        val CONF_DIR=args(2)
        val INPUT_DIR=args(3)
        val CHILD_DIR = Common.getChildDirectory(PRC_DT, JOB_ID);
        val SOURCE = INPUT_DIR
        
        
        // List of Source file SDP Lifecycle

        val existDir= Common.isDirectoryExists(sc, SOURCE)
        val sdpLcySrcRDD = sc.textFile(SOURCE.concat("*"));

        if (existDir == false)
           sys.exit(1);
        
        if (sdpLcySrcRDD.count() == 0)
           sys.exit(1);
      

        
        // List of output SDP Lifecycle
        val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_LCY_TRANSFORM.OUTPUT_SDP_REJ_BP_DIR").concat(CHILD_DIR);
        val OUTPUT_REJ_REF1_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_LCY_TRANSFORM.OUTPUT_SDP_REJ_REF1_DIR").concat(CHILD_DIR);
        val OUTPUT_REJ_REF2_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_LCY_TRANSFORM.OUTPUT_SDP_REJ_REF2_DIR").concat(CHILD_DIR);
        val OUTPUT_CDR_INQ_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_LCY_TRANSFORM.OUTPUT_SDP_CDR_INQ_DIR").concat(CHILD_DIR);
        val OUTPUT_DETAIL_REV_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_LCY_TRANSFORM.OUTPUT_SDP_DETAIL_DIR")
        
        
        
        println("============= Input Data ================");  
        /*
         * variable sdpLcyInputRDD
         * This variable contains some columns that will be used in the next process of SDP Lifecycle
         */
        val sdpLcyInputRDD = sdpLcySrcRDD.filter ( x => x.count (_ == '|') == 70).map(line => line.split("\\|")).map { p => Row(  
               p(2), //oriNodeTp
               p(3), //timeStamp
               if (p(5) != "") 
                  "62".concat(p(5))
               else "", //subscriberNumber
               p(6),//svcClsId
               p(9),//initAmt
                Common.toDouble(p(16)),//cldAccVal
               p(17),//acFlagsBef
               p(18),//acFlagsAf
               p(19),//actDt
               p(69),//fileNm
               p(34),//oriNodeId
               p(36),//origTimeStmp
               "", //msisdnPad
               p(50), //subLcy
               //"MA".concat("###").concat(p(16)).concat(p(50).split(']').map { x => "DA".concat(x.split('#')(0)).concat("###").concat(x.split('#')(2))}.mkString("]")), //daDtl
               //p(50).filter ( y =>  y.count (_ == '[') >= 1 ).split('[').map { x => "DA".concat(x.split('#')(0)).concat("###").concat(x.split('#')(2))}.mkString("["), //daDtl
               if (p(50).count (_ == '[') >= 1 ) 
                 "MA".concat("#").concat(p(56)).concat("#").concat(p(57)).concat("#").concat(p(16)).concat("[").concat(p(50).split('[').map { x => "DA".concat(x.split('#')(0)).concat("###").concat(x.split('#')(2))}.mkString("["))
               else if (p(50).count (_ == '[') == 0 && p(50).count (_ == '#') >= 1) 
                 "MA".concat("#").concat(p(56)).concat("#").concat(p(57)).concat("#").concat(p(16)).concat("[").concat("DA").concat(p(50).split('#')(0)).concat("###").concat(p(50).split('#')(2))
               else if (p(50).count (_ == '#') == 0 && p(17) == "00000000" && p(18) == "10000000")
                   "MA".concat("#").concat(p(56)).concat("#").concat(p(57)).concat("#").concat(Common.toDouble(p(57)).-(Common.toDouble(p(56))).toString())//daDtl
               else 
                 "MA".concat("#").concat(p(56)).concat("#").concat(p(57)).concat("#").concat(p(16)),//daDtl
               JOB_ID, //jobId
               Common.getKeyCDRInq("62".concat(p(5)), (p(3).substring(0,14))), //rcrd_id
               PRC_DT,//prcDt
               "SDP_LCY",//srcTp
               p(56),//balBfr
               p(57),//balAft
               p(70)//fileDt
               )};
        val sdpLcyInputDF = sqlContext.createDataFrame(sdpLcyInputRDD, model.SDPLcy.SDPLcySchema);
        sdpLcyInputDF.registerTempTable("STGSDPLifecycle");
        sdpLcyInputDF.show()
        
        
        println("============= Transformation  1 ================");  
        // Join record with reference service class, interconnect, and region and branch
        val ha1SDPLcyDF = sqlContext.sql(
            """
            SELECT sdp.oriNodeTp, 
               sdp.timeStamp, 
               sdp.subsNum,
               sdp.svcClsId,
               sdp.initAmt,
               sdp.cldAccVal,
               sdp.acFlagsBef,
               sdp.acFlagsAf,
               sdp.actDt,
               sdp.fileNm,
               sdp.oriNodeId,
               sdp.origTimeStmp,
               sdp.msisdnPad,
               sdp.subLcy,
               sdp.daDtl,
               sdp.jobId, 
               sdp.rcrd_id, 
               sdp.prcDt, 
               sdp.srcTp, 
               sdp.balBfr, 
               sdp.balAft, 
               sdp.fileDt,
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
               offerId, areaName,
               case 
                  when svcclsofr.svcClassCode is not null then svcclsofr.brndSCName
                  else svccls.brndSCName
               end as brand_name,
               offerId, areaName,
               substring(timeStamp,1,8) event_dt
            from
               (SELECT * FROM STGSDPLifecycle 
               where timeStamp <> '' and subsNum <> '' and origTimeStmp <> '' and svcClsId <> ''
                )  sdp
               left join SDPOffer offr on sdp.subsNum = offr.msisdn and substring(timeStamp,1,8) >= offr.effDt and substring(timeStamp,1,8) <= offr.endDt           
               left join ServiceClassOffer svcclsofr on sdp.svcClsId = svcclsofr.svcClassCode and 
               (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and offr.offerID = svcclsofr.offerId
               left join ServiceClass svccls on sdp.svcClsId = svccls.svcClassCode and 
               (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt)  and 
               (substring(timeStamp,1,8) between brndSCEffDt and brndSCEndDt)
             """)
        ha1SDPLcyDF.registerTempTable("HA1SDPLifecycle")
        ha1SDPLcyDF.persist()
        sdpLcyInputDF.unpersist()
    		
        
        val joinIntcctDF = Common.getIntcct(sqlContext, ha1SDPLcyDF, intcctDF, "subsNum", "event_dt", "prefix", "country", "city", "provider_id") 
        joinIntcctDF.registerTempTable("HA1SDPLifecycleIntcct")
        
        
        val joinRegBranchDF = sqlContext.sql(
            """
            select 
               oriNodeTp,  timeStamp, subsNum, svcClsId, initAmt,
               cldAccVal, acFlagsBef, acFlagsAf, actDt, fileNm,
               oriNodeId, origTimeStmp, msisdnPad, subLcy, daDtl,
               jobId, rcrd_id,  prcDt,  srcTp,  balBfr,  balAft, 
               fileDt, prefix,  a.city, provider_id, country,
               svcClassName, promo_package_code, promo_package_name,
               brand_name, branch hlr_branch_nm, region hlr_region_nm,
               offerId offer_id, areaName areaname,
               regBranch.city regBranchCity
               from HA1SDPLifecycleIntcct a
               left join  RegBranch regBranch on a.city = regBranch.city
            """);
        joinRegBranchDF.registerTempTable("HA1SDPLifecycleRegBranch")
        joinRegBranchDF.show()
        
        println("============= CDR  Inquiry ================");  
        val sdpLcyInqDF = sqlContext.sql(
            """
            SELECT rcrd_id as KEY, subsNum MSISDN, timeStamp TRANSACTIONTIMESTAMP, 
               case 
                 when substr(svcClsId,1,1) in ('4','7') then 'POSTPAID'
                 else 'PREPAID' 
               end as SUBSCRIBERTYPE,
               cldAccVal as AMOUNT,
               daDtl as DA_DETAIL,
               svcClsId as CURRENTSERVICECLASS,
               actDt as ACTIVATIONDATE,
               svcClsId as SERVICECLASSID,
               svcClassName as SERVICECLASSNAME,
               promo_package_name as PROMOPACKAGENAME,
               brand_name as BRANDNAME ,             
               '' as NEWSERVICECLASS ,   
               '' as NEWSERVICECLASSNAME ,   
               '' as NEWPROMOPACKAGENAME ,   
               '' as NEWBRANDNAME, 
               oriNodeTp ORIGINNODETYPE,
               oriNodeId ORIGINHOSTNAME,
               '' as EXTERNALDATA1,
               '' as PROGRAMNAME	,
               '' as PROGRAMOWNER,	
               '' as PROGRAMCATEGORY,
               '' as BANKNAME,
               '' as BANKDETAIL,
               balBfr as MAINACCOUNTBALANCEBEFORE,
               balAft as MAINACCOUNTBALANCEAFTER,
               '' as LAC,
               '' as CELLID,               
               case when ((acFlagsBef = 10001110 and acFlagsAf = 10001111) or (acFlagsBef = 10001111 and acFlagsAf = 00000000))
                   then "Clear Credit(LCY)"
                when (acFlagsBef = 00000000 and acFlagsAf = 10000000)
                   then "Preloaded"
                else '' end
                as  TRANSACTIONTYPE,
               fileNm FILENAMEINPUT,
               '' as VOURCHER_TYPE,
               '' as RELOAD_TYPE,
               jobId as JOBID,
               case   
                when daDtl like '%DA1#%' then 'C'
                when daDtl like '%DA35#%' then 'P'
                else ''
              end CDR_TP        
            from HA1SDPLifecycle           
            """)       
        Common.cleanDirectory(sc, OUTPUT_CDR_INQ_DIR +"/" + CHILD_DIR)
        sdpLcyInqDF.filter(" TRANSACTIONTYPE is not null").show()
        sdpLcyInqDF.filter(" TRANSACTIONTYPE is not null").write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "|").save(OUTPUT_CDR_INQ_DIR);
        
    
    }
}