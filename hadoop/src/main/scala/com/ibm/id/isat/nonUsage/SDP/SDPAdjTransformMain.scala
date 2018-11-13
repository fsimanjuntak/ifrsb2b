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
 * This object is used to produce revenue non usage from SDP Adjusment
 * Insert into CDR Inquiry
 * Insert into SDP Adjustment detail
 * Insert into SDP Adjustment summary
 * @author Meliani Efelina
 */

object SDPAdjTransformMain  {

    // Servic Context Configuration
    val sc = new SparkContext(new SparkConf())  
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

      
    def main(args: Array[String]) {
    
        println("Start Processing SDP Adjustment");  

        // Do not show all the logs except ERROR
        import org.apache.log4j.{Level, Logger}
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)
        
        /*
         *  Get all reference Data
         */
        
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
        
        
        
        // List of Source file SDP Adjustment
        //val sdpAdjSrcRDD = sc.textFile("C:/Users/IBM_ADMIN/Documents/MY STUFF/WORK/HADOOP JAVA/Hadoop Indosat/Sourcedata/SDP_b.ADJ");
        
        val existDir= Common.isDirectoryExists(sc, SOURCE)
        val sdpAdjSrcRDD = sc.textFile(SOURCE.concat("*"));

      
        println("============= Input Data ================");  
        /*
         * variable sdpAdjnputRDD
         * This variable contains some columns that will be used in the next process of SDP Adjustment
         */
        val OUTPUT_REJ_BP_DIR      = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_REJ_BP_DIR").concat(CHILD_DIR);
        val OUTPUT_REJ_REF1_DIR    = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_REJ_REF1_DIR").concat(CHILD_DIR);
        val OUTPUT_REJ_REF2_DIR    = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_REJ_REF2_DIR").concat(CHILD_DIR);
        val OUTPUT_CDR_INQ_DIR     = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_CDR_INQ_DIR").concat(CHILD_DIR);
        val OUTPUT_DETAIL_REV_DIR  = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_DETAIL_DIR");
        
    
        
         /*
         * variable sdpAdjInputRDD
         * This variable contains some columns that will be used in the next process of SDP Adjustment
         */
        val sdpAdjInputRDD = sdpAdjSrcRDD.filter ( x => x.count (_ == '|') == 96).map(line => line.split("\\|")).map { p => Row(  
               p(0), //adjRcrdTp
               if (p(4) != "") 
                 "62".concat(p(4))
               else "", //subsNum
               p(5),//adjTimeStamp
               p(6), //adjAction
               p(7), //balBef
               p(8), //balAf
               p(9), //adjAmt
               p(10), //svcClsId
               p(14),//originNodeType
               p(15),//originNodeId
               p(17),//origTrnscTimeStamp
               p(18),//trscTp
               p(19),//trscCd
               p(31), //newServiceClass
               p(36), //actDt
               p(59), //ddcAmt
               p(95), //fileNm
               "", //msisdnPad
               p(72), //subAdj
               if (p(0) == "1" && p(72) == "") 
                 "MA".concat("#").concat(p(7)).concat("#").concat(p(8)).concat("#").concat(p(9))
               else if (p(0) == "1" && p(72) != "") 
                 "MA".concat("#").concat(p(7)).concat("#").concat(p(8)).concat("#").concat(p(9))+("[").concat(p(72).split('[').map { x => "DA".concat(x.split('#')(0)).concat("#").concat(x.split('#')(2)).concat("#").concat(x.split('#')(3)).concat("#").concat(x.split('#')(4))}.mkString("["))
               else if (p(0) == "9" && p(72) != "") 
                 p(72).split('[').map { x => "DA".concat(x.split('#')(0)).concat("###").concat(x.split('#')(9))}.mkString("[")
               else "", //daDtl
               JOB_ID, //jobId
               Common.getRecordID("62".concat(p(4)), p(5).substring(0,14)), //rcrd_id
               PRC_DT,//prcDt
               "SDP_ADJ",//srcTp
               p(96)//fileDt
               )};
        val sdpAdjInputDF = sqlContext.createDataFrame(sdpAdjInputRDD, model.SDPAdj.SDPAdjSchema);
        sdpAdjInputDF.registerTempTable("STGSDPAdjustment");
        sdpAdjInputDF.persist()
        
         
        println("============= Transformation 1 ================");  
        /*
         * First Lookup Reference in SDP Adjustment
         * Joining with Service Class, Interconnect, RegionCityBranch, 
         * Null Adjustment Time Stamp, Null Subscriber Number, Null Origin Time Stamp,
         * Null New Service Class, and Invalid Adjustment Record Type
         */
        val ha1SDPAdjDF = sqlContext.sql(
            """
            SELECT adjRcrdTp, 
               sdp.subsNum,
               sdp.adjTimeStamp,
               sdp.adjAction,
               sdp.balBef,
               sdp.balAf,
               sdp.adjAmt,
               sdp.svcClsId,
               sdp.originNodeType,
               sdp.originNodeId,
               sdp.origTrnscTimeStamp,
               sdp.trscTp,
               sdp.trscCd,
               sdp.newServiceClass,
               sdp.ddcAmt,
               sdp.fileNm,
               sdp.msisdnPad,
               sdp.subAdj,
               sdp.daDtl,
               sdp.actDt,
               jobId, 
               rcrd_id, 
               prcDt, 
               srcTp, 
               fileDt,
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
               case 
                  when svcclsofrnew.svcClassCode is not null then svcclsofrnew.svcClassName
                  else svcclsnew.svcClassName 
               end as newServiceClassName,
               case 
                  when svcclsofrnew.svcClassCode is not null then svcclsofrnew.prmPkgName
                  else svcclsnew.prmPkgName
               end as newPromoPackageName,
               case 
                  when svcclsofrnew.svcClassCode is not null then svcclsofrnew.brndSCName
                  else svcclsnew.brndSCName
               end as newBrandName,
               svcclsofr.offerId, svcclsofr.areaName,
               substring(adjTimeStamp,1,8) event_dt
            from
               (SELECT * FROM STGSDPAdjustment where adjTimeStamp <> '' and subsNum <> '' and adjTimeStamp <> ''
                 and 
                ((adjRcrdTp = '6' and newServiceClass <> '') OR 
                    adjRcrdTp = '9' or (adjRcrdTp = '1' and trscTp = '0000000001' and trscCd = '0000000001')) 
                   )  sdp
               left join SDPOffer offr on sdp.subsNum = offr.msisdn and substring(adjTimeStamp,1,8) >= offr.effDt and substring(adjTimeStamp,1,8) <= offr.endDt           
               left join ServiceClassOffer svcclsofr on sdp.svcClsId = svcclsofr.svcClassCode and 
               (substring(adjTimeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and offr.offerID = svcclsofr.offerId
               left join ServiceClass svccls on sdp.svcClsId = svccls.svcClassCode and 
               (substring(adjTimeStamp,1,8) between svccls.svcClassEffDt and svccls.svcClassEndDt ) and 
               (substring(adjTimeStamp,1,8) between svccls.brndSCEffDt and svccls.brndSCEndDt ) 
               
               left join ServiceClassOffer svcclsofrnew on sdp.newServiceClass = svcclsofrnew.svcClassCode and 
               (substring(adjTimeStamp,1,8) between svcclsofrnew.effDt and svcclsofrnew.endDt ) and offr.offerID = svcclsofrnew.offerId
               left join ServiceClass svcclsnew on sdp.newServiceClass = svcclsnew.svcClassCode and 
               (substring(adjTimeStamp,1,8) between svcclsnew.svcClassEffDt and svcclsnew.svcClassEndDt ) and 
               (substring(adjTimeStamp,1,8) between svcclsnew.brndSCEffDt and svcclsnew.brndSCEndDt ) 
             """);
          ha1SDPAdjDF.registerTempTable("HA1SDPAdjustment");
          ha1SDPAdjDF.persist()
         
         
        // Join with Interconnect Reference
        val joinIntcctDF = Common.getIntcct(sqlContext, ha1SDPAdjDF, intcctDF, "subsNum", "event_dt", "prefix", "country", "city", "provider_id");
        joinIntcctDF.registerTempTable("HA1SDPAdjIntcct");
        joinIntcctDF.persist()     
        
        
        // Join with Region Branch Reference
        val joinRegBranchDF = sqlContext.sql(
            """
            select adjRcrdTp, subsNum, adjTimeStamp, adjAction, 
            balBef, balAf, adjAmt, svcClsId, originNodeType,
               originNodeId, origTrnscTimeStamp, trscTp, trscCd, newServiceClass, ddcAmt,
               fileNm, msisdnPad, subAdj, daDtl, actDt, jobId, rcrd_id, prcDt, srcTp, 
               fileDt, svcClassName, promo_package_code, promo_package_name, brand_name,
               branch hlr_branch_nm, region hlr_region_nm,
               offerId offer_id, areaName areaname,
               newServiceClass, newServiceClassName, newPromoPackageName, newBrandName, 
               prefix,  a.city, provider_id, country, 
               regBranch.city regBranchCity
               from HA1SDPAdjIntcct a
               left join  RegBranch regBranch on a.city = regBranch.city
            """);
         joinRegBranchDF.registerTempTable("HA1SDPAdjRegBranch");
         joinRegBranchDF.persist()
        joinRegBranchDF.show()
        
         
        println("============= Reject Reference 1 ================");  
        /*
         * Filtering the data from first join that 
         * should be rejected because reference is not found
         * Reason of reject:
         * Prefix Not Found, Service Class Not Found, City Not Found
         */  
        val rejRefSDPAdj1 = sqlContext.sql(
            """
            SELECT adjRcrdTp, 
               sdp.subsNum,
               sdp.adjTimeStamp,
               sdp.adjAction,
               sdp.balBef,
               sdp.balAf,
               sdp.adjAmt,
               sdp.svcClsId,
               sdp.originNodeType,
               sdp.originNodeId,
               sdp.origTrnscTimeStamp,
               sdp.trscTp,
               sdp.trscCd,
               sdp.newServiceClass,
               sdp.ddcAmt,
               sdp.fileNm,
               sdp.msisdnPad,
               sdp.subAdj,
               sdp.daDtl,
               jobId, 
               rcrd_id, 
               prcDt, 
               srcTp, 
               fileDt,
               case 
                 when prefix is null then 'Prefix Not Found'
                 when svcClassName is null then 'Service Class Not Found'
                 when regBranchCity is null then 'City Not Found'
                 when (adjRcrdTp = '6' and newServiceClassName is null) then 'New Service Class Not Found'
               else null
               end as rejRsn
            from HA1SDPAdjRegBranch sdp 
               where prefix = '' or prefix is null or 
               svcClassName = '' or svcClassName is null or 
               regBranchCity = '' or regBranchCity is null or 
              (adjRcrdTp = '6' and (newServiceClassName is null or newServiceClassName ='')) 
            """);
         rejRefSDPAdj1.show()
               
        /*
         * Transpose the record to get level 2 of SDP Adjustment data
         * adjRcrdTp 0, subsNum 1, adjTimeStamp 2, svcClsId 7, origTrnscTimeStamp 10, fileNm 15,  prefix 37,  city 38,
         * prvdr_id 39, country 40, promo_package_code 26, promo_package_nm 27, brand_nm 28,  hlr_branch_nm 29
         * hlr_region_nm 30, offer id 30,offerareaname 32,jobId 20, rcrd_id 21, prcDt 22, srcTp 23, filedt 24, subAdj 17
         */
        val transposeRDD = joinRegBranchDF.where(
            """
            (prefix <> '' and prefix is not null) and (svcClassName <> '' and svcClassName is not null)
            and (regBranchCity <> '' and regBranchCity is not null and adjRcrdTp in (1,9)) and (subAdj is not null and subAdj <> '')
            """).map { 
               row => row(0) + "|" + row(1) + "|" + row(2) + "|" + row(7) + "|" + row(10) + "|" + row(15) + "|" + row(37) +
               "|" + row(38) + "|" + row(39) + "|" + row(40) + "|" + row(26) + "|" + row(27) + "|" + row(28) + "|" + row(29) + "|" + 
               row(30) + "|" + row(31) + "|" + row(32) + "|" + row(20) + "|" + row(21) + "|" + row(22) + "|" + row(23) + "|" + row(24) +
               "~" + row(17) };
               //transposeRDD.collect foreach {case (a) => println (a)};
               transposeRDD.persist()
       
        val resultTransposeRDD = transposeRDD.filter ( x => x.count (_ == '#') >= 10).map(line => line.split("\\~")).map(field => (field(0), field(1))).
               flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
               .map { replaced => replaced.replace("#", "|") };
               //resultTransposeRDD.collect foreach {case (a) => println (a)};
               resultTransposeRDD.persist()
    
     
         /*
          * Mapping the level2 data that have been transposed into the format that will be the
          * same as the main adjustment data
          */
         val sdpAdjSubInptRDD = resultTransposeRDD.map(line => line.split("\\|")).map { p => Row( 
               p(0), //adjRcrdTp
               p(1), //subsNum
               p(2), //adjTimeStamp
               p(3), //svcClsId
               p(4), //origTrnscTimeStamp
               p(5), //fileNm
               p(6), //prefix
               p(7), //city
               p(8), //provider_id
               p(9), //country
               p(10), //promo_package_code
               p(11), //promo_package_nm
               p(12), //brand_nm
               p(13), //hlr_branch_nm
               p(14), //hlr_region_nm
               p(15), //offer_id
               p(16), //areaname
               p(17), //jobId
               p(18), //rcrd_id
               p(19), //prcDt
               p(20), //srcTp
               p(21), //filedt
               p(22), //da_id
               p(26), //adjAmt
               p(27), //actn
               p(31) //clearedAccountValue
            )};
         val sdpAdjSubInptDF = sqlContext.createDataFrame(sdpAdjSubInptRDD, model.SDPAdj.SDPAdjSubSchema);
         sdpAdjSubInptDF.registerTempTable("HA1SDPAdjustmentSub");
         sdpAdjSubInptDF.persist()
         
         
         println("============= Union Data ================");  
         /*
          * Combine level1 and level2 of SDP adjustment data
          * and do the transformation to get the revenue code and revenue amount
          */
         val unionSDPAdjDF = sqlContext.sql(
            """
            select substring(adjTimeStamp,1,8) as TRANSACTION_DATE, subsNum, prefix, city, provider_id, country,
               svcClsId, promo_package_code, promo_package_name, brand_name,
               case adjRcrdTp
                 when '1' then case adjAction when 1 then adjAmt when 2 then ((-1)*adjAmt) end
                 when '6' then ddcAmt
                 else 0 
               end as total_amount,
               case  
                 when (adjRcrdTp = '1' and adjAmt is not null and adjAmt <> '') then 'CCAdjustMA' 
                 when adjRcrdTp = '6'  then 'SCChangeMA' 
               end as revenue_code,
               fileNm real_filename,
               hlr_branch_nm,
               hlr_region_nm,
               offer_id,
               areaname,
               newServiceClass mgr_svccls_id,
               origTrnscTimeStamp,
               jobId,
               rcrd_id,
               prcDt,
               srcTp,
              fileDt
            from HA1SDPAdjRegBranch 
               where 
                (prefix <> '' and prefix is not null) and 
                (svcClassName <> '' and svcClassName is not null) and 
                (regBranchCity <> '' and regBranchCity is not null) and
                (adjRcrdTp in (1,9) or (adjRcrdTp = '6' and newServiceClassName is not null and newServiceClassName <> ''))
            UNION ALL
               select substring(adjTimeStamp,1,8) as TRANSACTION_DATE, subsNum, prefix, city, provider_id, country,
               svcClsId, promo_package_code, promo_package_name, brand_name, 
               case adjRcrdTp
                 when '1' then case action when 1 then adjAmt when 2 then ((-1)*adjAmt) end
                 when '9' then clearedAccountValue
                 else 0 
               end as total_amount ,
               case  adjRcrdTp
                 when '1' then concat('CCAdjustDA',da_id)  
                 when '9' then concat('OUTPAYMENTDA',da_id) 
                 else null 
               end as revenue_code,
               fileNm real_filename,
               hlr_branch_nm,
               hlr_region_nm,
               offer_id,
               areaname,
               null mgr_svccls_id,
               origTrnscTimeStamp,
               jobId,
               rcrd_id,
               prcDt,
               srcTp,
              fileDt
            from HA1SDPAdjustmentSub
            """);
         unionSDPAdjDF.registerTempTable("SDPAdjUnion");
         unionSDPAdjDF.persist()
         
        
        
         println("============= Final Transform ================");  
         /*
          * Map the data to detail non revenue table and divine whether the record 
          * is revenue or not by joining the record with the reference MADA and Revenue Code
          */
         val finalTransformDF = sqlContext.sql(
            """SELECT TRANSACTION_DATE, subsNum MSISDN, prefix PREFIX, city CITY, provider_id PROVIDER_ID, 
                   country COUNTRY, svcClsId SERVICECLASSID, promo_package_code PROMO_PACKAGE_CODE, 
                   promo_package_name PROMO_PACKAGE_NAME, brand_name BRAND_NAME, 
                   sum(
                   case when  revcod.revSign = '-' then (-1)*nvl(total_amount,0)
                   else nvl(total_amount,0) end) TOTAL_AMOUNT, 
                   CAST(count(*) AS DOUBLE) TOTAL_HIT, revenue_code REVENUE_CODE, revcod.svcUsgTp as SERVICE_TYPE,
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
                   origTrnscTimeStamp ORIGIN_TIMESTAMP,
                   mgr_svccls_id MGR_SVCCLS_ID,
                   cast (offer_id as string)  as OFFER_ID,
                   cast (areaname as string)  as OFFER_AREA_NAME,
                   jobId JOB_ID,
                   rcrd_id RECORD_ID,
                   prcDt PRC_DT,
                   srcTp SRC_TP,
                   fileDt FILE_DT
               from SDPAdjUnion sdp
               left join RevenueCode revcod 
                   on upper(sdp.revenue_code) = upper(revcod.revCode)
               left join ( select voiceRev, smsRev, vasRev, othRev, discRev, acc, grpCd, effDt, endDt from  Mada) mada 
                   on revcod.lv9 = mada.acc and promo_package_code =  mada.grpCd and TRANSACTION_DATE between mada.effDt and mada.endDt
               GROUP BY 
                   TRANSACTION_DATE,  subsNum, prefix , city , provider_id , 
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
                   origTrnscTimeStamp , 
                   mgr_svccls_id ,
                   cast (offer_id as string) ,
                   cast (areaname as string),
                   jobId,  rcrd_id, prcDt, srcTp, fileDt 
              """);
         finalTransformDF.registerTempTable("SDPAdjUnionTransform");
         finalTransformDF.persist()
         finalTransformDF.show()
         
         println("============= Reject Reference 2 ================");  
         /*
          * Filter the data that has no id in reference MADA and Revenue Code
          */
         val rejRefTransform = sqlContext.sql(
            """
            select TRANSACTION_DATE, MSISDN, PREFIX, CITY, PROVIDER_ID, COUNTRY, SERVICECLASSID, 
                   PROMO_PACKAGE_CODE, PROMO_PACKAGE_NAME, BRAND_NAME, TOTAL_AMOUNT, TOTAL_HIT, REVENUE_CODE, 
                   REAL_FILENAME, HLR_BRANCH_NM, HLR_REGION_NM, OFFER_ID,
                   OFFER_AREA_NAME, MGR_SVCCLS_ID, ORIGIN_TIMESTAMP,
        	  	     JOB_ID, RECORD_ID, PRC_DT, SRC_TP,  FILE_DT,
                   case 
        	 	   	     when SERVICE_TYPE is null then 'Revenue Code is Not Found'
        	 	         when REVENUE_FLAG is null then 'Mada is Not Found'
            	     end as REJRSN
        	         from SDPAdjUnionTransform
        		      -- where SERVICE_TYPE is null or REVENUE_FLAG is null
            """);
         rejRefTransform.persist()
         rejRefTransform.show()
    }
}
