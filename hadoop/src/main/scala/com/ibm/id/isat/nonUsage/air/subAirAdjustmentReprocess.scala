package com.ibm.id.isat.nonUsage.air

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import com.ibm.id.isat.nonUsage.air.model.subAirAdjustment;
//import com.ibm.id.isat.UT.PublishSchema;
import com.ibm.id.isat.utils.ReferenceSchema;
import org.apache.spark.sql.Row;
//import com.ibm.id.isat.UT.PublishSchema
import com.ibm.id.isat.utils.Common
import java.util.Calendar
import java.text.SimpleDateFormat
import com.ibm.id.isat.utils.ReferenceDF

object subAirAdjustmentReprocess {
  
    def main(args: Array[String]) {
             

//   val sc = new SparkContext("local", "sub adj transformation spark ", new SparkConf());
     val sc = new SparkContext(new SparkConf())
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")

//   Import data frame library.
     import sqlContext.implicits._
     import org.apache.log4j._
     Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.INFO)
     Logger.getLogger("org").setLevel(Level.INFO)
     Logger.getLogger("akka").setLevel(Level.INFO)
     val log = LogManager.getRootLogger
      
     if (args.length != 6) {
       println("Adjustment: [job_id] [prcDT] [CONFIG] [inputfile01] [inputfile02] [flag] ")
        sys.exit(1);
     }

     val prcDt = args(0)
     val jobID = args(1)
     val configDir = args(2)
     val inputFileRef = args(3)
     val inputFileRev = args(4)
     val flag = args(5)
     
       
//   val configDir = "/user/apps/nonusage/air/reference/AIR.conf"
     val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_ADJ_REJ_BP_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REF_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_ADJ_REJ_REF_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_REJ_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_ADJ_REJ_REV_DIR").concat(prcDt).concat("_").concat(jobID)
//   val OUTPUT_CDR_INQ_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_CDR_INQ_DIR").concat(prcDt).concat("_").concat(jobID)
     val OUTPUT_DETAIL_REV_DIR = Common.getConfigDirectory(sc, configDir, "AIR.AIR_TRANSFORM.OUTPUT_AIR_DETAIL_DIR")
		
     
    /*val prcDt =  "20160722"
     val jobID = "10"
     val configDir = "C:/testing"
     val inputFileRef = "C:/testing/reprocess_ref/rej_ref.ADJ"
     val inputFileRev = "C:/testing/reprocess_rev/rej_rev.ADJ"
     val OUTPUT_REJ_REF_DIR = "C:/testing/reprocess/rej_ref"
     val OUTPUT_REJ_REV_DIR = "C:/testing/reprocess/rej_rev"
     val OUTPUT_DETAIL_REV_DIR ="C:/testing/reprocess/sor"
     System.setProperty("hadoop.home.dir", "C:\\winutil\\")  */
     
     val existDir= Common.isDirectoryExists(sc, inputFileRef);
     if (flag == true || flag == "true" ){
         existDir == true;
     }
     var subReprocessAirInptRDD = sc.textFile(inputFileRef);  
     
     val existDirRev= Common.isDirectoryExists(sc, inputFileRev);
     if (flag == true || flag == "true" ){
       existDirRev == true;
     }
     var subReprocessRevAirInptRDD = sc.textFile(inputFileRev);  
     
     
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
   
      /*
       * @checked if file exists or not
       * 
       */
       /* if ( existDir != true ){
          var stgReprocessAirAdj = sc.makeRDD(Seq(Row(
               null, //origin node type
               null, //origin host name  
               null, //timestamp
               null, //accountNumber
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
               null //rejRsn
               )
           )
       )
       val stgReprocessAirAdjustment = sqlContext.createDataFrame(stgReprocessAirAdj, subAirAdjustment.stgReprocessAirRejRefSchema);
       stgReprocessAirAdjustment.registerTempTable("stg_reprocess_air_adjustment")
       stgReprocessAirAdjustment.persist();
//       stgReprocessAirAdjustment.show();
        log.warn("===============NULL DATA===================");
      }
      else{       
      var stgReprocessAirAdj = subReprocessAirInptRDD.filter ( x => x.count (_ == '|') == 19).map(line => line.split("\\|")).map {p => Row(
         p(0), //origin node type
         p(1), //origin host name  
         p(2), //timestamp
         p(3), //accountNumber
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
         jobID, // job_id
         p(15), //recordID
         goPrcDate(), //prcDT
         p(17), //area
         p(18), //fileDT
         p(19) //rejRsn
        )
       }
       val stgReprocessAirAdjustment = sqlContext.createDataFrame(stgReprocessAirAdj, subAirAdjustment.stgReprocessAirRejRefSchema);
       stgReprocessAirAdjustment.registerTempTable("stg_reprocess_air_adjustment")
       stgReprocessAirAdjustment.persist()
//       stgReprocessAirAdjustment.show();
        log.warn("===============ANY DATA===================");
      }*/
 
      var stgReprocessAirAdj = subReprocessAirInptRDD.filter ( x => x.count (_ == '|') == 19).map(line => line.split("\\|")).map {p => Row(
         p(0), //origin node type
         p(1), //origin host name  
         p(2), //timestamp
         p(3), //accountNumber
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
         jobID, // job_id
         p(15), //recordID
         goPrcDate(), //prcDT
         p(17), //area
         p(18), //fileDT
         p(19) //rejRsn
        )
       }
       val stgReprocessAirAdjustment = sqlContext.createDataFrame(stgReprocessAirAdj, subAirAdjustment.stgReprocessAirRejRefSchema);
       stgReprocessAirAdjustment.registerTempTable("stg_reprocess_air_adjustment")
       stgReprocessAirAdjustment.persist()
       log.warn("===============ANY DATA===================");
     
     
     /*****************************************************************************************************
     * 
     *     
     * Step 02 : Join Reprocess with Interconnect and Branch. ( Level 1 )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 02 : Join with Interconnect and Branch. ( Level 1 ) ================"+goToMinute());
     
      val ha1ReprocessAirAdj01 =  sqlContext.sql( 
          """
          SELECT
            air.originNodeType,
            air.originNodeID,
            air.timeStamp,
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
            substring(timeStamp,1,8) event_dt
          from
          (
            SELECT * FROM stg_reprocess_air_adjustment 
          )  air
          left join SDPOffer sdp on air.accountNumber = sdp.msisdn and (substring(air.timeStamp,1,8) between sdp.effDt and sdp.endDt )
          left join ServiceClassOffer svcclsofr on air.currentServiceclass = svcclsofr.svcClassCode 
            and (substring(timeStamp,1,8) between svcclsofr.effDt and svcclsofr.endDt ) and sdp.offerID = svcclsofr.offerId
          left join ServiceClass svccls on air.currentServiceclass = svccls.svcClassCode 
            and (substring(timeStamp,1,8) between svcClassEffDt and svcClassEndDt )

          """
         );
        ha1ReprocessAirAdj01.registerTempTable("ha1_reprocess_air_adj01")
        ha1ReprocessAirAdj01.persist();
        svcClassDF.unpersist()
        sdpOfferDF.unpersist()
        intcctDF.unpersist();
        regionBranchDF.unpersist();
        
        val joinIntcctDF = Common.getIntcct(sqlContext, ha1ReprocessAirAdj01, intcctDF, "accountNumber", "event_dt", "prefix", "country", "city", "provider_id") 
        joinIntcctDF.registerTempTable("HA1AirAdjJoinIntcct01")
        
         val ha1ReprocessAirAdj = sqlContext.sql(
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
            from HA1AirAdjJoinIntcct01 air
            left join RegBranch regBranch on air.city = regBranch.city
            """);
     
        ha1ReprocessAirAdj.registerTempTable("ha1_reprocess_air_adj")
        ha1ReprocessAirAdj.persist();
        
        
          
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
              when regBranchCity is null then 'City Found'
              end as REJ_RSN
           FROM ha1_reprocess_air_adj air 
           WHERE prefix = '' or prefix is null or svcClassName = '' or svcClassName is null or regBranchCity = '' or regBranchCity is null
         """
         );
       
       Common.cleanDirectory(sc, OUTPUT_REJ_REF_DIR );
       rejectRef.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REF_DIR);
//       rejectRef.show()
    
       
       
    /*****************************************************************************************************
     * 
     *     
     *  Step 04 : Join with MADA reference (MA)
     * 
     *****************************************************************************************************/
     log.warn("================== Step 04 : Join with MADA reference (MA) ================"+goToMinute());
       
       
     val ha1AdjReprocess01 =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
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
            SELECT * FROM  ha1_reprocess_air_adj WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null 
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
            air.svcClassName,
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
            air.originTimeStamp,
            air.offerId,
            air.areaName,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          """
         );
     
       ha1AdjReprocess01.registerTempTable("ha1_adj_reprocess01");
//     ha1AdjReprocess01.show();
       

       
     /*****************************************************************************************************
     * 
     *     
     * Step 05 : Produce for Level 2 ( SOR LEVEL ) 
     * 
     *****************************************************************************************************/
     log.warn("================== Step 05 : Produce for Level 2 ( SOR LEVEL )  ================"+goToMinute());
     
     val transposeRDD = ha1ReprocessAirAdj.where("prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and regBranchCity <> '' and regBranchCity is not null and dedicatedAccount is not null and dedicatedAccount <> '' ")
      .map { row => row(0) + "|" + row(1) + "|" + row(2) + "|" + row(3) + "|" + row(4) + "|" + row(5) + "|" + row(6) + "|" + row(7) + "|" + row(8) + "|" + row(9) + "|" + 
        row(11) + "|" + row(12) + "|" +  row(13) + "|" + row(14) + "|" + row(15) + "|" + row(16) + "|" + row(17) + "|" + row(18) + "|" + row(19) + "|" + row(20) + "|" + row(21) + "|" +
        row(22) + "|" + row(23) + "|" + row(24) + "|" + row(25) + "|" + row(26) + "|" + row(27) + "|" + row(28)  + "|" + row(30) + "|" + row(31) +
        "~" + row(10)
        }
      transposeRDD.collect foreach {case (a) => println (a)};
      
    val resultTransposeRDD = transposeRDD.map(line => line.split("\\~")).map(field => (field(0), field(1))).flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)}
     .map { replaced => replaced.replace("#", "|") };
      resultTransposeRDD.collect foreach {case (a) => println (a)};
     
    /*val ha1SubAdjRepInptDF = resultTransposeRDD.map(line => line.split("\\|")).map { p => Row( 
       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), p(23),p(24),p(25),p(26),p(27),
       p(28), //tupple 1 
       p(30) // tupple 3
       )
     }*/
    
     val ha1SubAdjRepInptDF = resultTransposeRDD.map(line => line.split("\\|")).filter(line => ! line(30).isEmpty() && ! line(32).isEmpty()).map { p => Row( 
       p(0), p(1), p(2),p(3),p(4),p(5),p(6),p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14), p(15), p(16),p(17),p(18),p(19),p(20),p(21), p(22), p(23),p(24),p(25),p(26),p(27),
       p(28),p(29),
       p(30),p(32)
       )
     }
     
      val ha1SubAirAdjRep = sqlContext.createDataFrame(ha1SubAdjRepInptDF, subAirAdjustment.ha1SubAirAdjSchema)
      ha1SubAirAdjRep.registerTempTable("ha1_sub_air_adj_reprocess01")
//    ha1SubAirAdjRep.show()
      
      
      
     /*****************************************************************************************************
     * 
     *     
     * Clean Memory Cache
     * 
     *****************************************************************************************************/ 
     ha1ReprocessAirAdj.unpersist();     
     ha1ReprocessAirAdj01.unpersist();
          
    

     /*****************************************************************************************************
     * 
     *     
     * Step 05 : Join with MADA reference (DA)
     * 
     *****************************************************************************************************/
     log.warn("==================  Step 05 : Join with MADA reference (DA) ================"+goToMinute());   
     
     val ha1AdjReprocess02 =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( tupple3 * (-1) ) / 100
              else
              tupple3 / 100 
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
            SELECT * FROM  ha1_sub_air_adj_reprocess01 WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null --and regBranchCity <> '' and regBranchCity is not null 
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
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( tupple3 * (-1) ) / 100
              else
              tupple3 / 100 
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
      
         ha1AdjReprocess02.registerTempTable("ha1_adj_reprocess02")
//       ha1AdjReprocess02.show();

       
       
       
     /*****************************************************************************************************
     * 
     *     
     * Step 06 : Reprocess Reject Reference 2 ( Reject Mada )
     * 
     *****************************************************************************************************/
     log.warn("================== Step 06 :  Reprocess Reject Reference 2 ( Reject Mada )  ================"+goToMinute());
       
         /*
       * @checked if file exists or not
       * 
       */
     /* 
       if (existDirRev != true ) {
            var stgReprocessARevirAdj =  sc.makeRDD(Seq(Row(
                 null, //timestamp
                 null, //account number  
                 null, //prefix
                 null, //city
                 null, //provider id
                 null, //country 
                 null, //service class id
                 null, //promo package code
                 null, //promo package name
                 null, //brand name
                 null, //total_amount
                 null, //total_hits
                 null, //revenue_code
                 null, //service_type
                 null, //gl_name
                 null, //gl_code
                 null, //real_filename
                 null, //hlr_branch_nm
                 null, //hlr_region_nm
                 null, //revenueflag
                 null, // mccmnc,
                 null, // lac,
                 null, // ci,
                 null, // laci_cluster_id,
                 null, // laci_cluster_nm,
                 null, // laci_region_id,
                 null, // laci_region_nm,
                 null, // laci_area_id,
                 null, // laci_area_nm,
                 null, // laci_salesarea_id,
                 null, // laci_salesarea_nm,
                 null, //origin_timestamp
                 null, // mgr_svccls_id,
                 null, // offer_id,
                 null, // offer_area_name,
                 null, //jobID,
                 null, //recordID,
                 null, //prcDT,
                 null, //area,
                 null //fileDT
               )
              )
           )
       val stgReprocessRevAirAdjustment = sqlContext.createDataFrame(stgReprocessARevirAdj, subAirAdjustment.stgReprocessAirRejRevSchema);
       stgReprocessRevAirAdjustment.registerTempTable("stg_rev_air_adjustment")
        log.warn("===============NULL DATA===================");
      }else{
        var stgReprocessARevirAdj = subReprocessRevAirInptRDD.map(line => line.split("\\|")).map {p => Row(
             p(0), //timestamp
             p(1), //account number  
             p(2), //prefix
             p(3), //city
             p(4), //provider id
             p(5), //country 
             p(6), //service class id
             p(7), //promo package code
             p(8), //promo package name
             p(9), //brand name
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
             p(20), // mccmnc,
             p(21), // lac,
             p(22), // ci,
             p(23), // laci_cluster_id,
             p(24), // laci_cluster_nm,
             p(25), // laci_region_id,
             p(26), // laci_region_nm,
             p(27), // laci_area_id,
             p(28), // laci_area_nm,
             p(29), // laci_salesarea_id,
             p(30), // laci_salesarea_nm,
             p(31), //origin_timestamp
             p(32), // mgr_svccls_id,
             p(33), // offer_id,
             p(34), // offer_area_name,
             jobID, //jobID,
             p(36), //recordID,
             goPrcDate(), //prcDT,
             p(38), //area,
             p(39) //fileDT
          )
       }
        
        val stgReprocessRevAirAdjustment = sqlContext.createDataFrame(stgReprocessARevirAdj, subAirAdjustment.stgReprocessAirRejRevSchema);
        stgReprocessRevAirAdjustment.registerTempTable("stg_rev_air_adjustment")
//        stgReprocessRevAirAdjustment.show();
         log.warn("===============ANY DATA===================");
    }*/
     
    var stgReprocessARevirAdj = subReprocessRevAirInptRDD.map(line => line.split("\\|")).map {p => Row(
             p(0), //timestamp
             p(1), //account number  
             p(2), //prefix
             p(3), //city
             p(4), //provider id
             p(5), //country 
             p(6), //service class id
             p(7), //promo package code
             p(8), //promo package name
             p(9), //brand name
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
             p(20), // mccmnc,
             p(21), // lac,
             p(22), // ci,
             p(23), // laci_cluster_id,
             p(24), // laci_cluster_nm,
             p(25), // laci_region_id,
             p(26), // laci_region_nm,
             p(27), // laci_area_id,
             p(28), // laci_area_nm,
             p(29), // laci_salesarea_id,
             p(30), // laci_salesarea_nm,
             p(31), //origin_timestamp
             p(32), // mgr_svccls_id,
             p(33), // offer_id,
             p(34), // offer_area_name,
             jobID, //jobID,
             p(36), //recordID,
             goPrcDate(), //prcDT,
             p(38), //area,
             p(39) //fileDT
          )
       }
        
        val stgReprocessRevAirAdjustment = sqlContext.createDataFrame(stgReprocessARevirAdj, subAirAdjustment.stgReprocessAirRejRevSchema);
        stgReprocessRevAirAdjustment.registerTempTable("stg_rev_air_adjustment")
//        stgReprocessRevAirAdjustment.show();
         log.warn("===============ANY DATA===================");
       
       
    /*****************************************************************************************************
     * 
     *     
     * Step 07 : Join Step 06 with Reference Revenue and Mada (DA)
     * 
     *****************************************************************************************************/
     log.warn("================== Step 07 : Join Step 06 with Reference Revenue and Mada (DA)  ================"+goToMinute());
         
     val ha1AdjReprocess03 =  sqlContext.sql( 
          """
            SELECT substring(air.timeStamp,1,8) as transaction_date ,
            air.accountNumber as msisdn,
            air.prefix,
            air.city,
            air.provider_id,
            air.country,
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( total_amount * (-1) ) / 100
              else
              total_amount / 100 
            end as total_amount,
            count(*) as total_hits,
            --concat ( air.externalData1 , 'MA') revenue_code,
            revenue_code,
            revcod.svcUsgTp as service_type, 
            revcod.lv4 as gl_code,  
            revcod.lv5 as gl_name, 
            real_filename,
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
            SELECT * FROM  stg_rev_air_adjustment WHERE prefix <> '' and prefix is not null and svcClassName <> '' and svcClassName is not null and city <> '' and city is not null 
          )  air
          left join 
          (
            select  svcUsgTp, lv4, lv5, revSign,revCode,lv9 from RevenueCode
          ) revcod on revcod.revCode = revenue_code
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
            air.svcClassName,
            air.promo_package_code,
            air.promo_package_name,
            air.brand_name,
            case revcod.revSign 
              when '-' then ( total_amount * (-1) ) / 100
              else
              total_amount / 100 
            end,
            --concat ( air.externalData1 , 'MA') ,
            revenue_code,
            revcod.svcUsgTp , 
            revcod.lv4 ,  
            revcod.lv5 , 
            real_filename ,
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
            air.originTimeStamp,
            air.offerId,
            air.areaName,
            air.jobID,
            air.recordID,
            air.prcDT,
            air.area,
            air.fileDT
          """
         );
        
       ha1AdjReprocess03.registerTempTable("ha1_adj_reprocess03")
//      sorReprocessAirAdj.show();
      

       
      /*****************************************************************************************************
     * 
     *     
     * Step 08 : Union ALL Source Table
     * 
     *****************************************************************************************************/
     log.warn("==================Step 08 :  Union ALL Source Table ================"+goToMinute());
      val airAdjDetail = sqlContext.sql("""
          select * from ha1_adj_reprocess03
          UNION  
          select * from ha1_adj_reprocess02
          UNION
          select * from ha1_adj_reprocess01
        """)
        
       airAdjDetail.registerTempTable("air_adj_detail")  
       
       
      
      
     /*****************************************************************************************************
     * 
     *     
     * Step 09 : Produce for Reject Reference 2 ( MADA ) 
     * 
     *****************************************************************************************************/
     log.warn("================== Step 09 : Produce for Reject Reference 2 ( MADA )  ================"+goToMinute());
     /*val rejRevAirAdj=sqlContext.sql( 
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
           FROM ha1_adj_reprocess03 adj where service_type is null and revenueflag ='UNKNOWN'
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
                     else 'Reject Ref' end as rej_rsn 
          FROM ha1_adj_reprocess02 adj where service_type is null and revenueflag ='UNKNOWN'
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
                     else 'Reject Ref' end as rej_rsn
             from ha1_adj_reprocess01 adj where service_type is null and revenueflag ='UNKNOWN'
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
                     else 'Reject Ref' end as REJ_RSN 
             from air_adj_detail adj where service_type is null and revenueflag is null
        """
         );
       
     Common.cleanDirectory(sc, OUTPUT_REJ_REV_DIR );
     rejRevAirAdj.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REV_DIR);
//     rejRevAirAdj.show();
       
     
     
     
     /*****************************************************************************************************
     * 
     *     
     * Step 10 : Produce for SOR Detail
     * 
     *****************************************************************************************************/
     log.warn("================== Step 10 : Produce for SOR Detail  ================"+goToMinute());
     
     /*val sorFinalAirAdj=sqlContext.sql( 
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
           FROM ha1_adj_reprocess03 adj where service_type is not null and revenueflag <> 'UNKNOWN'
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
          FROM ha1_adj_reprocess02 adj where service_type is not  null and revenueflag <> 'UNKNOWN'
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
           from ha1_adj_reprocess01 adj where service_type is not  null and revenueflag <> 'UNKNOWN'
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
           from air_adj_detail adj where service_type is not  null and revenueflag is not null
        """
         );       
       Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_REV_DIR, "/SRC_TP=AIR_ADJ/*/JOB_ID=" + jobID)
       sorFinalAirAdj.write.partitionBy("SRC_TP","TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_REV_DIR);
//       sorFinalAirAdj.show();
      
      sc.stop;
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