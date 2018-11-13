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
    
object SDPLcyTransform {
  

    // define sql Context
    //val sc = new SparkContext("local", "testing spark ", new SparkConf());  
    val sc = new SparkContext(new SparkConf())
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
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
        sqlContext.cacheTable("STGSDPLifecycle");
        
        
        println("============= Reject BP ================");  
        /*
         *  Reject BP SDP Lifecycle
         *  Filter every record that has null Lifecycle timestamp, Subscriber Number, Origin Timestamp, or Null Service Class Id
         */     
        val rejBPSDPLcyDF = sqlContext.sql(
            """
            Select t.*, 
               case when timeStamp = '' then 'Null Lifecycle Time Stamp'
                 when subsNum = '' then 'Null Subscriber Number'
                 when origTimeStmp = '' then 'Null Origin Time Stamp'
                 when svcClsId = '' then 'Null Service Class Id'
                 else 'Subscriber status not expired or deleted'
               end as rejRsn
            from STGSDPLifecycle t
               where 
               timeStamp = '' or subsNum = '' or origTimeStmp = '' or svcClsId = ''
               or (acFlagsBef='10001110' and acFlagsAf <> '10001111') or
               (acFlagsBef<>'10001110' and acFlagsAf = '10001111') or
               (acFlagsBef = 10001111 and acFlagsAf <> 00000000) or 
               (acFlagsBef <> 10001111 and acFlagsAf = 00000000) or
               (acFlagsBef<>'10001110' and acFlagsAf <> '10001111' and acFlagsBef <> 10001111 and acFlagsAf <> 00000000 ) 
            """)
        Common.cleanDirectory(sc, OUTPUT_REJ_BP_DIR +"/" + CHILD_DIR)
        rejBPSDPLcyDF.write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "|").save(OUTPUT_REJ_BP_DIR);
               
         
        
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
               jobId as JOBID ,
               case   
                when daDtl like '%DA1#%' then 'C'
                when daDtl like '%DA35#%' then 'P'
                else ''
              end CDR_TP                   
            from HA1SDPLifecycle           
            where event_dt > 20161005
            """)       
            
        Common.cleanDirectory(sc, OUTPUT_CDR_INQ_DIR +"/" + CHILD_DIR)
        sdpLcyInqDF.filter(" TRANSACTIONTYPE is not null").write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "|").save(OUTPUT_CDR_INQ_DIR);
        
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
               where  ((acFlagsBef = 10001110 and acFlagsAf = 10001111) or (acFlagsBef = 10001111 and acFlagsAf = 00000000))
            """);
        joinRegBranchDF.registerTempTable("HA1SDPLifecycleRegBranch")
        joinRegBranchDF.persist()
        
        println("============= Rej  Ref 1 ================");  
        /*
         *  Reject REF 1 SDP Lifecycle
         *  Filter every record that has invalid prefix, service class, or city
         */     
        val rejRefSDPLcy1 = sqlContext.sql(
            """
            SELECT 
               sdp.oriNodeTp ORINODETP,
               sdp.timeStamp TIMESTAMP, 
               sdp.subsNum SUBSNUM,
               sdp.svcClsId SVCCLSID,
               sdp.initAmt INITAMT,
               sdp.cldAccVal CLDACCVAL,
               sdp.acFlagsBef ACFLAGSBEF,
               sdp.acFlagsAf ACFLAGSAF,
               sdp.actDt ACTDT,
               sdp.fileNm FILENM,
               sdp.oriNodeId ORINODEID,
               sdp.origTimeStmp ORIGTIMESTMP,
               sdp.msisdnPad MSISDNPAD,
               sdp.subLcy SUBLCY,
               sdp.daDtl DADTL,
               sdp.jobId JOBID, 
               sdp.rcrd_id RCRD_ID, 
               sdp.prcDt PRCDT, 
               sdp.srcTp SRCTP, 
               sdp.balBfr BALBFR, 
               sdp.balAft BALAFT,
               sdp.fileDt FILEDT,
               case 
                  when prefix is null then 'Prefix Not Found'
                  when svcClassName is null then 'Service Class Not Found'
                  when regBranchCity is null then 'City Not Found'
               end as REJRSN
            from HA1SDPLifecycleRegBranch sdp
               where prefix = '' or prefix is null or svcClassName = '' or svcClassName is null or regBranchCity = '' or regBranchCity is null
            """)
             
        Common.cleanDirectory(sc, OUTPUT_REJ_REF1_DIR +"/" + CHILD_DIR)
        rejRefSDPLcy1.write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "|").save(OUTPUT_REJ_REF1_DIR);
            
               
            
        println("============= Transpose ================");  
        /*
         * timeStamp 1, subsNum 2, svcClsId 3, origTimeStmp 11, fileNm 9,  prefix 22,  city 23,
         * prvdr_id 24, country 25, promo_package_code 27, promo_package_nm 28, brand_nm 29,  hlr_branch_nm 30
         * hlr_region_nm 31, jobId 15, rcrd_id 16, prcDt 17, srcTp 18, filedt 21, sdpLcySub 13
         * 
         */
        val transposeRDD = joinRegBranchDF.where("""
            (prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and 
            regBranchCity <> '' and regBranchCity is not null) and subLcy <> ''
            """).map { 
               row => row(1) + "|" + row(2) + "|" + row(3) + "|" + row(11) + "|" + row(9) +
               "|" + row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(27) + "|" + row(28) + "|" + row(29) + "|" + row(30) +
               "|" + row(31) + "|" + row(32) + "|" + row(33) +
               "|" + row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(21) +
                "~" + row(13) }
             //transposeRDD.collect foreach {case (a) => println (a)}; 
           transposeRDD.persist() 
             
        
        val resultTransposeRDD = transposeRDD.map(line => line.split("\\~")).map(field => (field(0), field(1))).
               flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
         			 .map { replaced => replaced.replace("#", "|") };
               //resultTransposeRDD.collect foreach {case (a) => println (a)};    
     resultTransposeRDD.persist()
     
     
        // define columns for sub air lifecycle class and create data frame.
        val sdpLcySubInptRDD = resultTransposeRDD.filter ( x => x.count (_ == '|') == 35).map(line => line.split("\\|")).map { p => Row( 
        			 p(0), //timeStamp
        			 p(1), //subsNum
       				 p(2), //svcClsId
               p(3), //origTimeStmp
               p(4), //fileNm
               p(5), //prefix
               p(6), //city
               p(7), //provider_id
               p(8), //country
               p(9), //promo_package_code
               p(10), //promo_package_nm
               p(11), //brand_nm
               p(12), //hlr_branch_nm
               p(13), //hlr_region_nm
               p(14), //offer_id
               p(15), //areaname
               p(16), //jobId
               p(17), //rcrd_id
               p(18), //prcDt
               p(19), //srcTp
               p(20), //filedt
               p(21), //da_id
               Common.toDouble(p(23)) //cldAccVal
       			)}  
        val sdpLcySubInptDF = sqlContext.createDataFrame(sdpLcySubInptRDD, model.SDPLcy.SDPLcySubSchema)
    		sdpLcySubInptDF.registerTempTable("HA1SDPLifecycleSub")
    		sdpLcySubInptDF.persist()

    
        println("============= Union ================");  
        val unionSDPLcyDF = sqlContext.sql(
        		"""
        		select substring(timeStamp,1,8) as TRANSACTION_DATE, subsNum, prefix, city, provider_id, country,
        			 svcClsId, promo_package_code, promo_package_name, brand_name,
        			 sdpLcy.cldAccVal as total_amount,
               "OUTPAYMENTMA" as revenue_code,
               fileNm real_filename,
               hlr_branch_nm,
               hlr_region_nm,
               offer_id,
               areaname,
               cast (null as string)  mgr_svccls_id,
               origTimeStmp,
               jobId,
               rcrd_id,
               prcDt,
               srcTp,
              fileDt
            from HA1SDPLifecycleRegBranch sdpLcy 
              where
            	prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and 
              regBranchCity <> '' and regBranchCity is not null 
            UNION ALL
            select substring(timeStamp,1,8) as TRANSACTION_DATE, subsNum, prefix, city, provider_id, country,
               svcClsId, promo_package_code, promo_package_name, brand_name, 
               SDPSub.cldAccVal as total_amount ,
               concat('OUTPAYMENTDA',da_id) as revenue_code,
               fileNm real_filename,
               hlr_branch_nm,
               hlr_region_nm,
               offer_id,
               areaname,
               cast (null as string) mgr_svccls_id,
               origTimeStmp,
               jobId,
               rcrd_id,
               prcDt,
               srcTp,
              fileDt
            from HA1SDPLifecycleSub SDPSub
            """)
        unionSDPLcyDF.registerTempTable("SDPLcyUnion");
        unionSDPLcyDF.persist()
           
           
    
        println("============= Final Transform ================");  
    
        /*
         * This transformation will join the data to MADA and Revcode reference
         * and reformat the data to revenue non usage table details
         */
    		val finalTransformDF = sqlContext.sql(
    				"""
    				select TRANSACTION_DATE, subsNum MSISDN, prefix PREFIX, city CITY, provider_id PROVIDER_ID, 
                   country COUNTRY, svcClsId SERVICECLASSID, promo_package_code PROMO_PACKAGE_CODE, 
                   promo_package_name PROMO_PACKAGE_NAME, brand_name BRAND_NAME,
                   sum(case when  revcod.revSign = '-' then CAST((-1*nvl(total_amount,0)) AS DOUBLE)
                    else CAST(nvl(total_amount,0) AS DOUBLE) end) TOTAL_AMOUNT, 
                   CAST(count(*) AS DOUBLE) TOTAL_HIT,
                   revenue_code REVENUE_CODE, revcod.svcUsgTp as SERVICE_TYPE, 
                   lv4 as GL_CODE, lv5 as GL_NAME, real_filename REAL_FILENAME, hlr_branch_nm HLR_BRANCH_NM,
                   hlr_region_nm HLR_REGION_NM, 
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
                   origTimeStmp ORIGIN_TIMESTAMP, 
                   cast (null as string)  as MCCMNC,
                   cast (null as string)  as LAC,
                   cast (null as string)  as CI,
                   cast (null as string)  as LACI_CLUSTER_ID,
                   cast (null as string)  as LACI_CLUSTER_NM,
                   cast (null as string)  as LACI_REGION_ID,
                   cast (null as string)  as LACI_REGION_NM,
                   cast (null as string)  as LACI_AREA_ID,
                   cast (null as string)  as LACI_AREA_NM,
                   cast (null as string)  as LACI_SALESAREA_ID,
                   cast (null as string)  as LACI_SALESAREA_NM,
                   mgr_svccls_id MGR_SVCCLS_ID,
                   cast (offer_id as string)  as OFFER_ID,
                   cast (areaname as string)  as OFFER_AREA_NAME,
                   jobId  JOB_ID,
                   rcrd_id RECORD_ID,
                   prcDt PRC_DT,
                   srcTp SRC_TP,
                   fileDt FILE_DT
      		  from SDPLcyUnion sdp
      		  left join RevenueCode revcod on upper(sdp.revenue_code) = upper(revcod.revCode)
      		  left join ( select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from  Mada) mada 
       		  on revcod.lv9 = mada.acc and promo_package_code =  mada.grpCd and TRANSACTION_DATE between mada.effDt and mada.endDt
      		  GROUP BY 
            TRANSACTION_DATE, 
            subsNum, prefix , city , provider_id , 
                   country , svcClsId , promo_package_code , 
                   promo_package_name , brand_name ,  
                    revenue_code , revcod.svcUsgTp , 
                   lv4 , lv5 , real_filename , hlr_branch_nm ,
                   hlr_region_nm , 
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
                   origTimeStmp , 
                   mgr_svccls_id ,
                   cast (offer_id as string) ,
                   cast (areaname as string),
                   jobId  ,
                   rcrd_id ,
                   prcDt ,
                   srcTp ,
                   fileDt 
      		  """);
        finalTransformDF.registerTempTable("SDPLcyUnionTransform");
        finalTransformDF.persist()

    			     
        
    		println("============= Reject Reference 2 ================");  
    		/*
     		 * Reject REF 2 SDP Lifecycle
     		 * Filter every record that has invalid revenue code or mada
     		 */    
    	  val rejRefTransformDF = sqlContext.sql(
    			  """
        	  select TRANSACTION_DATE, MSISDN, PREFIX, CITY, PROVIDER_ID, COUNTRY, SERVICECLASSID, 
                   PROMO_PACKAGE_CODE, PROMO_PACKAGE_NAME, BRAND_NAME, TOTAL_AMOUNT, REVENUE_CODE, 
                   REAL_FILENAME, HLR_BRANCH_NM, HLR_REGION_NM, OFFER_ID,
                   OFFER_AREA_NAME, MGR_SVCCLS_ID, ORIGIN_TIMESTAMP,
        	  	     JOB_ID, RECORD_ID, PRC_DT, SRC_TP, FILE_DT,
                   case 
        	 	   	     when SERVICE_TYPE is null then 'Revenue Code is Not Found'
        	 	         when REVENUE_FLAG is null then 'Mada is Not Found'
            	     end as REJRSN
        	         from SDPLcyUnionTransform
        		       where SERVICE_TYPE is null or REVENUE_FLAG is null
    				""");
    		Common.cleanDirectory(sc, OUTPUT_REJ_REF2_DIR +"/" + CHILD_DIR);
    		rejRefTransformDF.write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "|").save(OUTPUT_REJ_REF2_DIR);
    	   Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_REV_DIR, "/SRC_TP=SDP_LCY/*/JOB_ID=" + JOB_ID)
         finalTransformDF.where(" SERVICE_TYPE is not null and REVENUE_FLAG is not null").write.partitionBy("SRC_TP","TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_REV_DIR);

    }
}