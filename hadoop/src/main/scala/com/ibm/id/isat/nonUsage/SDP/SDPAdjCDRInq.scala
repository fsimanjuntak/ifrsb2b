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

object SDPAdjCDRInq  {


    val sc = new SparkContext(new SparkConf())  
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

      
    def main(args: Array[String]) {
    
        println("Start Processing SDP Adjustment CDR Inq");  

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

        if (existDir == false)
           sys.exit(1);
        
        if (sdpAdjSrcRDD.count() == 0)
           sys.exit(1);
        
      

        println("============= Input Data ================");  
        /*
         * variable sdpAdjnputRDD
         * This variable contains some columns that will be used in the next process of SDP Adjustment
         */
        val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_REJ_BP_DIR").concat(CHILD_DIR);
        val OUTPUT_REJ_REF1_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_REJ_REF1_DIR").concat(CHILD_DIR);
        val OUTPUT_REJ_REF2_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_REJ_REF2_DIR").concat(CHILD_DIR);
        val OUTPUT_CDR_INQ_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_CDR_INQ_DIR").concat(CHILD_DIR);
        val OUTPUT_DETAIL_REV_DIR = Common.getConfigDirectory(sc, CONF_DIR, "SDP.SDP_ADJ_TRANSFORM.OUTPUT_SDP_DETAIL_DIR");
        
    
        
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
        sdpAdjSrcRDD.unpersist();
        
               
       
         
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
               
               left join SDPOffer offrnew on sdp.subsNum = offrnew.msisdn and substring(adjTimeStamp,1,8) >= offrnew.effDt and substring(adjTimeStamp,1,8) <= offrnew.endDt           
               left join ServiceClassOffer svcclsofrnew on sdp.newServiceClass = svcclsofrnew.svcClassCode and 
               (substring(adjTimeStamp,1,8) between svcclsofrnew.effDt and svcclsofr.endDt ) and offrnew.offerID = svcclsofrnew.offerId
               left join ServiceClass svcclsnew on sdp.newServiceClass = svcclsnew.svcClassCode and 
               (substring(adjTimeStamp,1,8) between svcclsnew.svcClassEffDt and svcclsnew.svcClassEndDt ) and 
               (substring(adjTimeStamp,1,8) between svcclsnew.brndSCEffDt and svcclsnew.brndSCEndDt ) 
             """);
        ha1SDPAdjDF.registerTempTable("HA1SDPAdjustment");
        sdpAdjInputDF.unpersist();
         
         
          /*
          * Mapping the level2 data that have been transposed into the format that will be the
          * same as the main adjustment data
          * 
          * 
          *        
          */

         
         
         
         println("============= CDR Inquiry ================"); 
         /*
          * Processing CDR Inquiry of SDP Adjustment
          * by using data from first join
          */
         val sdpAdjInqDF = sqlContext.sql(
            """
            SELECT a.rcrd_id as KEY, subsNum MSISDN, adjTimeStamp TRANSACTIONTIMESTAMP, 
               case 
                 when substr(svcClsId,1,1) in ('4','7') then 'POSTPAID'
                 else 'PREPAID' 
               end as SUBSCRIBERTYPE,
               ddcAmt as AMOUNT,
                a.daDtl DA_DETAIL,
               svcClsId as CURRENTSERVICECLASS,
               actDt as ACTIVATIONDATE,
               svcClsId as SERVICECLASSID,
               nvl(svcClassName,"") as SERVICECLASSNAME,
               nvl(promo_package_name,"") as PROMOPACKAGENAME,
               nvl(brand_name,"") as BRANDNAME ,             
               nvl(newServiceClass,"") as NEWSERVICECLASS ,   
               nvl(newServiceClassName,"") as NEWSERVICECLASSNAME ,   
               nvl(newPromoPackageName,"") as NEWPROMOPACKAGENAME ,   
               nvl(newBrandName,"") as NEWBRANDNAME, 
               originNodeType ORIGINNODETYPE,
               originNodeId ORIGINHOSTNAME,
               "" as EXTERNALDATA1,
               "" as PROGRAMNAME	,
               "" as PROGRAMOWNER,	
               "" as PROGRAMCATEGORY,
               "" as BANKNAME,
               "" as BANKDETAIL,
               balBef as MAINACCOUNTBALANCEBEFORE,
               balAf as MAINACCOUNTBALANCEAFTER,
               "" as LAC,
               "" as CELLID,
               case 
                 when adjRcrdTp = '1' then 'Adjustment'
                 when adjRcrdTp = '6' then 'Service Change'
                 when adjRcrdTp = '9' then 'Clear Credit(ADJ)'
               end as  TRANSACTIONTYPE,
               fileNm FILENAMEINPUT,
               "" as VOURCHER_TYPE,
               "" as RELOAD_TYPE,
               jobId as JOBID ,
               case   
                when daDtl like '%DA1#%' then 'C'
                when daDtl like '%DA35#%' then 'P'
                else ''
              end CDR_TP            
            from HA1SDPAdjustment    a
            where event_dt > 20161005
            """);
         sdpAdjInqDF.persist()
         Common.cleanDirectory(sc, OUTPUT_CDR_INQ_DIR)
         
         sdpAdjInqDF.write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "|").save(OUTPUT_CDR_INQ_DIR);
               
    
     
    }
}