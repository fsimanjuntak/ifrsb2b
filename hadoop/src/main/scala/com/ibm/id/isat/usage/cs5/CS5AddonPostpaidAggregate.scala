package com.ibm.id.isat.usage.cs5

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.ibm.id.isat.utils._
import org.apache.spark.storage.StorageLevel



 /*****************************************************************************************************
 * 
 * CS5 Transformation for Postpaid
 * Input: CCN CS5 Conversion
 * Output: CS5 CDR Inquiry, CS5 Transformation, CS5 Reject Reference, CS5 Reject BP
 * 
 * @author anico@id.ibm.com
 * @author IBM Indonesia for Indosat Ooredoo
 * June 2016
 * 
 *****************************************************************************************************/   
object CS5AddonPostpaidAggregate {
  def main(args: Array[String]): Unit = {
    
    /*****************************************************************************************************
     * 
     * Parameter check and setting
     * 
    ******************************************************************************************************/
  
    var prcDt = ""
    var jobID = ""
    var configDir = ""
    var transaction_date = ""

    try {
      prcDt = args(0)
      jobID = args(1)
      configDir = args(2)
      transaction_date = args(3)
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        println("Please provide correct parameter. Application exiting...")
        return
    }
   
    

    
    /**
     * Initiate Spark Context
     */
//    val sc = new SparkContext("local[*]", "testing spark ", new SparkConf());
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    
    /**
     * Target directory assignments
     */
    val pathCS5OrangeOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_ADDON_ORANGE_REPORT_POSTPAID_DIR")
    val pathCCNPostpaidAddon = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_POSTPAID_ADDON")
    val ngsspLocation = Common.getConfigDirectory(sc, configDir, "USAGE.NGSSP.OUTPUT_NGSSP_SOR_DIR")
    
    
    
    /*****************************************************************************************************
     * 
     * Load references
     * DATAFRAME action start here
     * 
     *****************************************************************************************************/
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    val refGLSvcTypeDF = ReferenceDF.getPostpaidGLtoServTypeDF(sqlContext)
    refGLSvcTypeDF.registerTempTable("refGLSvcType")
    refGLSvcTypeDF.persist(storageLevel) 
    refGLSvcTypeDF.count()
    
    /*
     * Reference postpaid GL Service Type (for VAS) -- without Revenue ID
     */
    val refGLSvcTypeVASDF = ReferenceDF.getPostpaidGLtoServTypeVASDF(sqlContext)
    refGLSvcTypeVASDF.registerTempTable("refGLSvcTypeVAS")
    refGLSvcTypeVASDF.persist(storageLevel) 
    refGLSvcTypeVASDF.count()
    
    /*
     * Reference rev code (for VAS) -- need to have distinct rev_code
     */
    val refRevCodeDF = ReferenceDF.getRevenueCodeDF(sqlContext)
    refRevCodeDF.registerTempTable("refRevCode")
    refRevCodeDF.persist(storageLevel) 
    refRevCodeDF.count()
    
    /*
     * Reference rev code VAS (join REF_REVCODE and REF_GLSVCTP_VAS)
     */
    val refGLSvcTypeVASCompleteDF = sqlContext.sql(
    """select '' LegacyId, revCode RevenueId, lv4 Level4, lv5 Level5, SvcType, SvcTypeDA1, SvcTypeNDA1 
       from refRevCode a
       join refGLSvcTypeVAS b 
       on a.lv4=b.Level4"""
    )
    refGLSvcTypeVASCompleteDF.registerTempTable("refGLSvcTypeVASComplete")
    refGLSvcTypeVASCompleteDF.persist(storageLevel)
    refGLSvcTypeVASCompleteDF.count()
    
    /*
     * Reference rev code complete (join REF_REVCODE and REF_GLSVCTP)
     */
    val refGLSvcTypeCompleteDF = broadcast( sqlContext.sql(
    """select LegacyId, RevenueId, Level4, Level5, SvcType, SvcTypeDA1, SvcTypeNDA1 from refGLSvcType 
     union all
     select LegacyId, RevenueId, Level4, Level5, SvcType, SvcTypeDA1, SvcTypeNDA1 from refGLSvcTypeVASComplete"""
    ) )
    refGLSvcTypeCompleteDF.registerTempTable("refGLSvcTypeComplete")
    refGLSvcTypeCompleteDF.persist(storageLevel)
    refGLSvcTypeCompleteDF.count()
    
    /*
     * Other references (non-DataFrame)
     */
    //val postRef = new CS5Reference.postEventType("/user/apps/reference/ref_postpaid_eventtype_revenue/REF_POSTPAID_EVENTTYPE_REVENUE_*" , sc)
    val postRef = ReferenceDF.getPostEventTypeRef(sc)
    val intcctRef = ReferenceDF.getIntcctAllRef(sc)
    val postCorpUsageTypeRef = ReferenceDF.getPostCorpProductID(sc)
    
    /*
     * Register UDFs
     */
    sqlContext.udf.register("getPostAxis1", CS5Select.getPostAxis1 _)
    sqlContext.udf.register("getPostAxis2", CS5Select.getPostAxis2 _)
    sqlContext.udf.register("getPostAxis3", CS5Select.getPostAxis3 _)
    sqlContext.udf.register("getPostAxis4", CS5Select.getPostAxis4 _)
    sqlContext.udf.register("getPostAxis5", CS5Select.getPostAxis5 _)
    
    sqlContext.udf.register("getEventTypeRevCode", postRef.getEventTypeRevCode _)
    
    //TODO: load Postpaid Addon Output
    val CCSNCS5PostPaidAddonDF = sqlContext.read
      .option("basePath", pathCCNPostpaidAddon)
      .parquet(pathCCNPostpaidAddon + "/TRANSACTION_DATE=" + transaction_date)
    
    
    /*
     * Convert back to RDD
     * Separating the records as (key, value) for transpose
     * Only records NOT REJECT REFERENCE
     */
    //TODO: Include SSPTransId in transpose
    val formatRDD = CCSNCS5PostPaidAddonDF
        .filter("RevenueCode <> '' and GLCode <> ''")
        .rdd.filter{r => r.size == 148 && r.getString(35) != null}.map( r => 
                           ( r.getString(0 + 35)
        .concat("|").concat( r.getString(1 + 35 ) )
        .concat("|").concat( r.getString(2 + 35 ) )
        .concat("|").concat( r.getString(3 + 35 ) )
        .concat("|").concat( r.getString(4 + 35 ) )
        .concat("|").concat( r.getString(5 + 35 ) )
        .concat("|").concat( r.getString(6 + 35 ) ) 
        .concat("|").concat( r.getString(7 + 35 ) )
        .concat("|").concat( r.getString(8 + 35 ) )
        .concat("|").concat( r.getString(9 + 35 ) )
        .concat("|").concat( r.getDouble(10 + 35 ).toString() )
        .concat("|").concat( r.getString(11 + 35 ) )
        .concat("|").concat( r.getString(12 + 35 ) )
        .concat("|").concat( r.getString(13 + 35 ) )
        .concat("|").concat( r.getString(14 + 35 ) )
        .concat("|").concat( r.getString(15 + 35 ) )
        .concat("|").concat( r.getString(16 + 35 ) )
        .concat("|").concat( r.getString(17 + 35 ) )
        .concat("|").concat( r.getString(18 + 35 ) )
        .concat("|").concat( r.getDouble(19 + 35 ).toString() )
        .concat("|").concat( r.getDouble(20 + 35 ).toString() )
        .concat("|").concat( r.getDouble(21 + 35 ).toString() )
        .concat("|").concat( r.getString(22 + 35 ) )
        .concat("|").concat( r.getString(23 + 35 ) )
        .concat("|").concat( r.getString(24 + 35 ) )
        .concat("|").concat( r.getString(25 + 35 ) )
        .concat("|").concat( r.getString(26 + 35 ) )
        .concat("|").concat( r.getString(27 + 35 ) )
        .concat("|").concat( r.getString(28 + 35 ) )
        .concat("|").concat( r.getString(29 + 35 ) )
        .concat("|").concat( r.getString(30 + 35 ) )
        .concat("|").concat( r.getString(31 + 35 ) )
        .concat("|").concat( r.getString(32 + 35 ) )
        .concat("|").concat( r.getString(33 + 35 ) )
        .concat("|").concat( r.getString(34 + 35 ) )
        .concat("|").concat( r.getString(35 + 35 ) )
        .concat("|").concat( r.getString(36 + 35 ) )
        .concat("|").concat( r.getString(37 + 35 ) )
        .concat("|").concat( r.getString(38 + 35 ) )
        .concat("|").concat( r.getString(39 + 35 ) )
        .concat("|").concat( r.getString(40 + 35 ) )
        .concat("|").concat( r.getString(41 + 35 ) )
        .concat("|").concat( r.getString(42 + 35 ) )
        .concat("|").concat( r.getString(43 + 35 ) )
        .concat("|").concat( r.getString(44 + 35 ) )
        .concat("|").concat( r.getString(45 + 35 ) )
        .concat("|").concat( r.getString(46 + 35 ) )
        .concat("|").concat( r.getString(47 + 35 ) )
        .concat("|").concat( r.getString(48 + 35 ) )
        .concat("|").concat( r.getString(49 + 35 ) )
        .concat("|").concat( r.getString(50 + 35 ) )
        .concat("|").concat( r.getString(51 + 35 ) )
        .concat("|").concat( r.getString(52 + 35 ) )
        .concat("|").concat( r.getString(53 + 35 ) )
        .concat("|").concat( r.getString(54 + 35 ) )
        .concat("|").concat( r.getString(55 + 35 ) )
        .concat("|").concat( r.getString(56 + 35 ) )
        .concat("|").concat( r.getString(57 + 35 ) )
        .concat("|").concat( r.getString(58 + 35 ) )
        .concat("|").concat( r.getString(59 + 35 ) )
        .concat("|").concat( r.getString(60 + 35 ) )
        .concat("|").concat( r.getString(61 + 35 ) )
        .concat("|").concat( r.getString(62 + 35 ) )
        .concat("|").concat( r.getString(63 + 35 ) )
        .concat("|").concat( r.getString(64 + 35 ) )
        .concat("|").concat( r.getString(65 + 35 ) )
        .concat("|").concat( r.getString(66 + 35 ) )
        .concat("|").concat( r.getString(67 + 35 ) )
        .concat("|").concat( r.getString(68 + 35 ) )
        .concat("|").concat( r.getString(69 + 35 ) )
        .concat("|").concat( r.getString(70 + 35 ) )
        .concat("|").concat( r.getString(71 + 35 ) )
        .concat("|").concat( r.getString(72 + 35 ) )
        .concat("|").concat( r.getString(73 + 35 ) )
        .concat("|").concat( r.getString(74 + 35 ) )
        .concat("|").concat( r.getString(75 + 35 ) )
        .concat("|").concat( r.getString(76 + 35 ) )
        .concat("|").concat( r.getString(77 + 35 ) )
        .concat("|").concat( r.getString(78 + 35 ) )
        .concat("|").concat( r.getString(79 + 35 ) )
        .concat("|").concat( r.getString(80 + 35 ) )
        .concat("|").concat( r.getString(81 + 35 ) )
        .concat("|").concat( r.getString(82 + 35 ) )
        .concat("|").concat( r.getString(83 + 35 ) )
        .concat("|").concat( r.getString(84 + 35 ) )
        .concat("|").concat( r.getString(85 + 35 ) )
        .concat("|").concat( r.getString(86 + 35 ) )
        .concat("|").concat( r.getString(87 + 35 ) )
        .concat("|").concat( r.getString(88 + 35 ) )
        .concat("|").concat( r.getString(89 + 35 ) )
        .concat("|").concat( r.getString(90 + 35 ) )
        .concat("|").concat( r.getString(91 + 35 ) )
        .concat("|").concat( r.getString(92 + 35 ) )
        .concat("|").concat( r.getString(93 + 35 ) )
        .concat("|").concat( r.getString(94 + 35 ) )
        .concat("|").concat( r.getString(95 + 35 ) )
        .concat("|").concat( r.getString(96 + 35 ) )
        .concat("|").concat( r.getString(97 + 35 ) )
        .concat("|").concat( r.getString(98 + 35 ) )
        .concat("|").concat( r.getString(99 + 35 ) )
        .concat("|").concat( r.getString(100 + 35 ) )
        .concat("|").concat( r.getString(101 + 35 ) )
        .concat("|").concat( r.getString(102 + 35 ) )
        .concat("|").concat( r.getString(103 + 35 ) )
        .concat("|").concat( r.getString(104 + 35 ) )
        .concat("|").concat( r.getString(105 + 35 ) )
        .concat("|").concat( r.getString(106 + 35 ) )
        .concat("|").concat( r.getString(107 + 35 ) )
        .concat("|").concat( r.getString(108 + 35 ) )
        .concat("|").concat( r.getString(109 + 35 ) ),
         r.getString(110 + 35 ) )
      )
    
     /* 
     * Transpose DA column into records 
     * Sample input : a|b|c|RevCodeBase[DA1~moneyamt~unitamt~uom^DA2~moneyamt~unitamt~uom
     * Sample output : a|b|c|RevCodeBase|DA1|moneyamt|unitamt|uom & a|b|c|RevCodeBase|DA2|moneyamt|unitamt|uom
     */
    val cs5TransposeRDD = formatRDD.flatMapValues(word => word.split("\\^")).map{case (k,v) => k.concat("|").concat( Common.getTuple(v, "~", 1)+"|"+Common.getTuple(v, "~", 2)+"|"+Common.getTuple(v, "~", 3)+"|"+Common.getTuple(v, "~", 0) ) };
    
     /*
     * Create new dataframe from CS5 transpose result
     */    
    //TODO: Include SSPTransID
    val cs5TransposeRow = cs5TransposeRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
        p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10).toDouble, p(11), p(12),p(13), p(14), p(15), p(16),
        p(17), p(18), p(19).toDouble, p(20).toDouble, p(21).toDouble, p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29),
        p(30), p(31), p(32), p(33), p(34), p(35), p(36), p(37), p(38), p(39), p(40), p(41), p(42), 
        p(43), p(44), p(45), p(46), p(47), p(48), p(49), p(50), p(51), p(52), p(53), p(54), p(55), 
        p(56), p(57), p(58), p(59), p(60), p(61), p(62), p(63), p(64), p(65), p(66), p(67), p(68),
        p(69), p(70), p(71), p(72), p(73), p(74), p(75), p(76), p(77), p(78), p(79), p(80), p(81),
        p(82), p(83), p(84), p(85), p(86), p(87), p(88), p(89), p(90), p(91), p(92), p(93), p(94),
        p(95), p(96), p(97), p(98), p(99), p(100), p(101), p(102), p(103), p(104), p(105), p(106), p(107), p(108), p(109),
        p(110).toDouble, p(111), p(112), p(113)
        ))
    //TODO: change schema to use CS5AddonPostTransfopose Schema with SSPTransId
    val cs5TransposeDF = sqlContext.createDataFrame(cs5TransposeRow, CS5Schema.CS5PostAddonTranspose).as("ADDON")
    cs5TransposeDF.persist(storageLevel)
   // cs5TransposeDF.registerTempTable("cs5Transpose")
    cs5TransposeDF.registerTempTable("ADDON")
    cs5TransposeDF.select("RevenueCode").show(1)
    
    cs5TransposeDF.select("SSPTransId").show(false)
    
    //TODO: Do addon join ngssp here
    
    val ngsspDF = sqlContext.read.parquet(ngsspLocation + "/TRANSACTIONDATE=" + transaction_date).as("NGSSP")
    ngsspDF.registerTempTable("NGSSP")
    
    //val ngsspCS5JoinDF = cs5TransposeDF.join(ngsspCDRDF.withColumnRenamed("CGI", "NGSSPCGI"), col("ADDON.SSPTransId") === col("NGSSP.ORDER_ID"), "left_outer")
    val ngsspCS5JoinDF = sqlContext.sql("""
      select 
        ADDON.MSISDN,
        ADDON.TransactionDate,
        ADDON.ServiceIdent,
        ADDON.ServiceClass,
        ADDON.ServiceScenario,
        ADDON.RoamingPosition,
        ADDON.FAF,
        ADDON.DedicatedAccountRow,
        ADDON.AccountValueDeducted,
        ADDON.TreeDefinedFields,
        ADDON.AccumulatedCost,
        ADDON.BonusAdjustment,
        ADDON.RecordNumber,
        ADDON.ChargingContextId,
        ADDON.FileName,
        ADDON.OrigCCRTriggerTime,
        substr(
          regexp_replace(
            CASE WHEN nvl(NGSSP.CDR_CGI, '') <> '' THEN NGSSP.CDR_CGI
            WHEN nvl(NGSSP.TDR_CGI, '') <> '' THEN NGSSP.TDR_CGI
            ELSE ''
            END, '[^a-z0-9]', ''
          ), 7, 4
        ) LAC,
        substr(
          regexp_replace(
            CASE WHEN nvl(NGSSP.CDR_CGI, '') <> '' THEN NGSSP.CDR_CGI
            WHEN nvl(NGSSP.TDR_CGI, '') <> '' THEN NGSSP.TDR_CGI
            ELSE ''
            END, '[^a-z0-9]', ''
          ), 11, 4
        ) CI,
        ADDON.APN,
        ADDON.UsageDuration,
        ADDON.UsageVolume,
        ADDON.EventDuration,
        ADDON.RatingGroup,
        ADDON.ContentType,
        ADDON.RATType,
        ADDON.IMEI,
        ADDON.OrigCCREventTime,
        ADDON.IMSI,
        ADDON.GGSNAddress,
        ADDON.SGSNAddress,
        ADDON.MSCAddress,
        ADDON.CorrelationId,
        ADDON.ChargingId,
        ADDON.OriginRealm,
        ADDON.OriginHost,
        ADDON.OtherPartyNum,
        ADDON.Costband,
        ADDON.Calltype,
        ADDON.CalltypeName,
        ADDON.Location,
        ADDON.Discount,
        ADDON.RatTypeName,
        ADDON.EventId,
        ADDON.CCRTriggerTime,
        ADDON.CCREventTime,
        ADDON.DedicatedAccountValuesRow,
        ADDON.VasServiceId,
        ADDON.UsedOfferSC,
        ADDON.OfferAttributeAreaname,
        ADDON.CCRCCAAccountValueBefore,
        ADDON.CCRCCAAccountValueAfter,
        ADDON.CCNNode,
        ADDON.CommunityId1,
        ADDON.CommunityId2,
        ADDON.CommunityId3,
        ADDON.AccountGroup,
        ADDON.ServiceOfferId,
        ADDON.SpOfrId,
        ADDON.SpConsume,
        ADDON.Accumulator,
        CASE WHEN nvl(NGSSP.CDR_CGI, '') <> ''
          THEN concat_ws('', substr(CDR_CGI, 2, 1), substr(CDR_CGI, 1, 1), substr(CDR_CGI, 4, 1), substr(CDR_CGI, 6, 1), substr(CDR_CGI, 5, 1))
        WHEN nvl(NGSSP.TDR_CGI, '') <> ''
          THEN concat_ws('', substr(TDR_CGI, 2, 1), substr(TDR_CGI, 1, 1), substr(TDR_CGI, 4, 1), substr(TDR_CGI, 6, 1), substr(TDR_CGI, 5, 1))
        ELSE ''
        END MCCMNC,
        ADDON.ECI,
        ADDON.ServiceUsageType,
        ADDON.JobId,
        ADDON.RecordId,
        ADDON.PrcDt,
        ADDON.SourceType,
        ADDON.FileDate,
        ADDON.SubsLocNo,
        ADDON.SSPTransId,
        ADDON.APartyNumber,
        ADDON.NormCCRTriggerTime,
        '' RsvFld1,
        '' RsvFld2,
        '' RsvFld3,
        ADDON.RsvFld4,
        ADDON.RsvFld5,
        ADDON.RsvFld6,
        ADDON.RsvFld7,
        ADDON.RsvFld8,
        ADDON.RsvFld9,
        ADDON.RsvFld10,
        ADDON.APrefix,
        ADDON.ServiceCountryName,
        ADDON.ServiceProviderId,
        ADDON.ServiceCityName,
        ADDON.BPrefix,
        ADDON.DestinationCountryName,
        ADDON.DestinationProviderId,
        ADDON.DestinationCityName,
        ADDON.hlrBranchNm,
        ADDON.hlrRegionNm,
        ADDON.postSCName,
        ADDON.CostbandName,
        ADDON.LocationName,
        ADDON.laciCluster,
        ADDON.laciSalesArea,
        ADDON.laciArea,
        ADDON.laciMicroCluster,
        ADDON.siteTech,
        ADDON.laciRatType,
        ADDON.PAxis1,
        ADDON.PAxis2,
        ADDON.PAxis3,
        ADDON.PAxis4,
        ADDON.PAxis5,
        ADDON.RevenueCode,
        ADDON.GLCode,
        ADDON.GLName,
        ADDON.ServiceType,
        ADDON.ServiceTypeDA1,
        ADDON.ServiceTypeNDA1,
        ADDON.CorpUsgType,
        ADDON.UsageAmount,
        ADDON.DAUnitAmount,
        ADDON.DAUnitUOM,
        ADDON.Account,
        CASE WHEN nvl(NGSSP.CDR_ORIGIN, '') <> ''
          THEN NGSSP.CDR_ORIGIN
        WHEN nvl(NGSSP.ORIGIN, '') <> ''
          THEN NGSSP.ORIGIN
        ELSE ''
        END Origin
      from NGSSP
      right join ADDON on NGSSP.TDR_TRANSACTIONID = ADDON.SSPTransId 
        and NGSSP.TDR_MSISDN = ADDON.MSISDN
        and NGSSP.TDR_CGI rlike '^[0-9a-z]{14}$'
    """)
    
    ngsspCS5JoinDF.registerTempTable("cs5TransposeNGSSP")
    ngsspCS5JoinDF.select("RevenueCode").show(1)
    
     /*****************************************************************************************************
     * 
     * CS5 TRANSFORMATION OUTPUT
     * 
     *****************************************************************************************************/
    //TODO: Include Origin from join result
    val cs5PostOutput = sqlContext.sql("""
        select
        MSISDN SERVED_MSISDN,
        TransactionDate TRANSACTION_DATE,
        ServiceIdent SERVICE_IDENTIFIER,
        ServiceClass CCR_CCA_SERVICE_CLASS_ID,
        ServiceScenario CCR_SERVICE_SCENARIO,
        RoamingPosition CCR_ROAMING_POSITION,
        FAF CCR_CCA_FAF_ID,
        DedicatedAccountRow CCR_CCA_DEDICATED_ACCOUNTS,
        AccountValueDeducted CCR_CCA_ACCOUNT_VALUE_DEDUCTED,
        TreeDefinedFields CCR_TREE_DEFINED_FIELDS,
        AccumulatedCost ACCUMULATEDCOST_AMOUNT,
        BonusAdjustment BONUS_ADJ_VALUE_CHANGE,
        RecordNumber RECORD_NUMBER,
        ChargingContextId CHARGING_CONTEXT_ID,
        FileName REAL_FILENAME,
        OrigCCRTriggerTime ORIGINAL_TRIGGER_TIME,
        MCCMNC,
        CASE regexp_replace(LAC, '[0-9]', '')
          WHEN '' THEN LAC
        ELSE ''
        END LAC,
        CASE regexp_extract(CI, '[0-9]', '')
          WHEN '' THEN CI
        ELSE ''
        END CI,
        APN,
        UsageDuration DURATION,
        UsageVolume VOLUME,
        EventDuration EVENT_DUR,
        RatingGroup RATING_GROUP,
        ContentType CONTENT_TYPE,
        RATType   RAT_TYPE,
        IMEI,
        OrigCCREventTime EVENT_TIME,
        IMSI,
        GGSNAddress GGSN_IPADDR,
        SGSNAddress SGSN_IPADDR,
        MSCAddress MSC_IPADDDR,
        CorrelationId CALL_REFFERENCE,
        ChargingId CHARGING_ID,
        OriginRealm ORIGIN_REALM,
        OriginHost ORIGIN_HOST,
        APrefix PREFIX,
        ServiceCityName CITY,
        ServiceProviderId PROVIDERID,
        ServiceCountryName COUNTRY,
        OtherPartyNum OTHER_PARTY_NUMBER,
        BPrefix B_PREFIX,
        DestinationCityName DEST_CITY,
        DestinationProviderId DEST_PROVID,
        DestinationCountryName DEST_COUNTRY,
        hlrRegionNm REGION,
        hlrBranchNm BRANCH,
        Costband COSTBAND,
        CostbandName COSTBAND_NAME,
        Calltype CALLTYPE,
        CalltypeName CALLTYPE_NAME,
        Location LOCATION,
        LocationName LOCATION_NAME,
        Discount DISCOUNT,
        RatTypeName RATTYPE_NAME,
        EventId EVENT_ID,
        postSCName POSTPAID_SERVICE_NAME,
        PAxis1 P_AXIS_1,
        PAxis2 P_AXIS_2,
        PAxis3 P_AXIS_3,
        PAxis4 P_AXIS_4,
        PAxis5 P_AXIS_5,
        RevenueCode REVENUE_CODE,
        GLCode GL_CODE,
        GLName GL_NAME,
        ServiceType SERVICE_TYPE,
        ServiceTypeDA1 SERVICE_TYPE_DA1,
        ServiceTypeNDA1 SERVICE_TYPE_NDA1,
        case when Account='MA' or Account='DA1' then 'REGULER'
          else Account
          end ACCOUNT_ID,
        UsageAmount CHARGE_AMOUNT,
        case when Account='MA' or Account='DA1' then ServiceTypeDA1
          else ServiceTypeNDA1
          end SERVICE_USAGE_TYPE,
        case when Account='MA' or Account='DA1' then 'Yes'
          else 'No'
          end REVENUE_FLAG,
        ECI,
        '' CLUSTER_ID,
        laciCluster CLUSTER_NM,
        '' LACI_REGION_ID,
        '' LACI_REGION_NM,
        '' LACI_AREA_ID,
        laciArea LACI_AREA_NM,
        '' LACI_SALESAREA_ID,
        laciSalesArea LACI_SALESAREA_NM,
        siteTech SITE_TECH,
        laciRatType SITE_OPERATOR,
        1 HITS,
        '' USG_MGR_SVCCLS_ID,
        UsedOfferSC USG_OFFER_ID,
        case when OfferAttributeAreaname='' then ''
          else 'AREANAME'
          end USG_OFFER_ATTR_KEY,
        OfferAttributeAreaname USG_OFFER_ATTR_VALUE,
        '' USG_OFFER_AREA_NAME,
        DAUnitAmount DA_UNIT_AMOUNT ,
        DAUnitUOM DA_UNIT_TYPE,
        JobId JOB_ID,
        RecordId RCRD_ID,
        PrcDt PRC_DT,
        SourceType SRC_TP,
        FileDate FILEDATE,
        case when CorpUsgType='P' then 'Personal'
          when CorpUsgType='C' then 'Corporate'
          else ''
        end USAGE_TYPE,
        NVL(Origin,'') ORIGIN
        from cs5TransposeNGSSP
        where 
        RevenueCode <> "" 
        and GLCode <> ""
        """)
        cs5PostOutput.persist(storageLevel)
        cs5PostOutput.registerTempTable("cs5PostOutput")
    cs5PostOutput.select("REVENUE_CODE").show(1)
        
     /*****************************************************************************************************
     * 
     * CS5 TRANSFORMATION ORANGE REPORT OUTPUT
     * 
     *****************************************************************************************************/
    // TODO: Include Origin, Origin Realm, Origin host, Microcluster
        val cs5PostOrangeOutput = sqlContext.sql("""
          select 
          SERVED_MSISDN	,
          TRANSACTION_DATE CCR_TRIGGER_TIME	,
          REVENUE_FLAG	,
          SERVICE_IDENTIFIER	,
          CCR_CCA_SERVICE_CLASS_ID	,
          CCR_SERVICE_SCENARIO	,
          CCR_ROAMING_POSITION	,
          CCR_CCA_FAF_ID	,
          CCR_CCA_DEDICATED_ACCOUNTS	,
          CCR_CCA_ACCOUNT_VALUE_DEDUCTED	,
          CCR_TREE_DEFINED_FIELDS	,
          ACCUMULATEDCOST_AMOUNT	,
          BONUS_ADJ_VALUE_CHANGE	,
          RECORD_NUMBER	,
          CHARGING_CONTEXT_ID	,
          REAL_FILENAME	,
          ORIGINAL_TRIGGER_TIME	,
          MCCMNC	,
          LAC	,
          CI	,
          APN	,
          DURATION	,
          VOLUME	,
          EVENT_DUR	,
          RATING_GROUP	,
          CONTENT_TYPE	,
          RAT_TYPE	,
          IMEI	,
          EVENT_TIME	,
          IMSI	,
          GGSN_IPADDR	,
          SGSN_IPADDR	,
          MSC_IPADDDR	,
          CALL_REFFERENCE	,
          CHARGING_ID	,
          ORIGIN_REALM	,
          ORIGIN_HOST	,
          PREFIX	,
          CITY	,
          PROVIDERID	,
          OTHER_PARTY_NUMBER	,
          B_PREFIX	,
          DEST_CITY	,
          DEST_PROVID	,
          REGION	,
          BRANCH	,
          COSTBAND	,
          COSTBAND_NAME	,
          CALLTYPE	,
          CALLTYPE_NAME	,
          LOCATION	,
          LOCATION_NAME	,
          DISCOUNT	,
          RATTYPE_NAME	,
          EVENT_ID	,
          POSTPAID_SERVICE_NAME	,
          P_AXIS_1	,
          P_AXIS_2	,
          P_AXIS_3	,
          P_AXIS_4	,
          P_AXIS_5	,
          REVENUE_CODE	,
          GL_CODE	,
          GL_NAME	,
          SERVICE_TYPE	,
          SERVICE_TYPE_DA1	,
          SERVICE_TYPE_NDA1	,
          ACCOUNT_ID	,
          CHARGE_AMOUNT	,
          SERVICE_USAGE_TYPE	,
          PRC_DT LOAD_DATE	,
          HITS	,
          USG_MGR_SVCCLS_ID	,
          USG_OFFER_ID	,
          USG_OFFER_ATTR_KEY	,
          USG_OFFER_ATTR_VALUE	,
          USG_OFFER_AREA_NAME	,
          JOB_ID,
          RCRD_ID RECORD_ID,
          PRC_DT,
          SRC_TP,
          FILEDATE FILE_DATE,
          USAGE_TYPE,
          ORIGIN,
          CLUSTER_ID MICROCLUSTER_ID,
          CLUSTER_NM MICROCLUSTER_NAME
          from cs5PostOutput
        """)
        

     /*****************************************************************************************************
     * 
     * WRITE OUTPUT
     * 
     *****************************************************************************************************/   
     
     /*
      * Disable metadata parquet
      */
     sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")   
        
     
      /*
       * Output Orange output
       */
     // TODO: change output to orange addon directory
      Common.cleanDirectoryWithPattern(sc, pathCS5OrangeOutput, "/*_" + jobID)
      cs5PostOrangeOutput.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(pathCS5OrangeOutput + "/" + prcDt + "_" + jobID)  
    

    sc.stop();
    
  }
  
}