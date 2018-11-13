package com.ibm.id.isat.usage.cs5

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat



 /*****************************************************************************************************
 * 
 * CS5 Transformation for Usage Revenue
 * Input: CCN CS5 Conversion
 * Output: CS5 CDR Inquiry, CS5 Transformation, CS5 Reject Reference, CS5 Reject BP
 * 
 * @author IBM Indonesia for Indosat Ooredoo
 * 
 * June 2016
 * 
 *****************************************************************************************************/   
object CS5AddonAggregate {
  def main(args: Array[String]): Unit = {
    
    /*****************************************************************************************************
     * 
     * Parameter check and setting
     * 
     *****************************************************************************************************/
    
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
    
    /*
     * Initiate Spark Context
     */
//    val sc = new SparkContext("local", "testing spark ", new SparkConf());
    val sc = new SparkContext(new SparkConf());
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")   //Disable metadata parquet
    
    /*
     * Log level initialization
     */
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO) //Level.ERROR
    
    /*
     * Target directory assignments
     */
    val pathAddonAggGreenReportOutput = Common.getConfigDirectory(sc, configDir, "USAGE.GREEN_REPORT_PREPAID.OUTPUT_SPARK_ADDON_GREEN_REPORT_PREPAID")
    val pathAddonTdwSmyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_ADDON_TDW_SUMMARY_DIR")
    val pathAddonIPCNOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_ADDON_IPCN_DIR")
    val ngsspLocation = Common.getConfigDirectory(sc, configDir, "USAGE.NGSSP.OUTPUT_NGSSP_SOR_DIR")
    
    
    /*****************************************************************************************************
     * 
     * Load references
     * DATAFRAME action start here
     * 
     *****************************************************************************************************/
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    sqlContext.udf.register("hexToInt", Common.hexToInt _)
    
    /*
     * Reference rev code
     */
    val refRevCodeDF = ReferenceDF.getRevenueCodeDF(sqlContext)
    refRevCodeDF.registerTempTable("refRevCode")
    
    /*
     * Reference MADA
     */
    val refMADADF = ReferenceDF.getMaDaDF(sqlContext)
    refMADADF.registerTempTable("refMADA")

    
    // Load the addon in input for the transaction date
    val cs5AddonIPCNDF = sqlContext.read
      .option("basePath", pathAddonIPCNOutput)
      .parquet(pathAddonIPCNOutput + "/TRANSACTION_DATE=" + transaction_date)
    
    
    /*
     * Convert back to RDD
     * Separating the records as (key, value) for transpose
     */
    // the+ 35 are based ON offset caused by previous IPCN fields
    val formatRDD = cs5AddonIPCNDF.rdd.filter{r => (r.size == 141 || r.size == 143) && r.getString(37) != null}.map( r => 
      r.size match {
        case 141 => ( r.getString(106 + 33)
        .concat("|").concat( "00" )
        .concat("|").concat( r.getString(2 + 33) )
        .concat("|").concat( r.getString(3 + 33) )
        .concat("|").concat( r.getString(4 + 33) )
        .concat("|").concat( r.getString(5 + 33) )
        .concat("|").concat( r.getString(6 + 33) ) 
        .concat("|").concat( r.getString(7 + 33) )
        .concat("|").concat( r.getString(8 + 33) )
        .concat("|").concat( r.getString(9 + 33) )
        .concat("|").concat( r.getString(10 + 33) )
        .concat("|").concat( r.getString(11 + 33) )
        .concat("|").concat( r.getString(12 + 33) )
        .concat("|").concat( r.getString(13 + 33) )
        .concat("|").concat( r.getString(14 + 33) )
        .concat("|").concat( r.getString(15 + 33) )
        .concat("|").concat( r.getString(16 + 33) )
        .concat("|").concat( r.getString(17 + 33) )
        .concat("|").concat( r.getString(18 + 33) )
        .concat("|").concat( r.getString(19 + 33) )
        .concat("|").concat( r.getString(20 + 33) )
        .concat("|").concat( r.getString(21 + 33) )
        .concat("|").concat( r.getString(22 + 33) )
        .concat("|").concat( r.getString(23 + 33) )
        .concat("|").concat( r.getString(24 + 33) )
        .concat("|").concat( r.getString(25 + 33) )
        .concat("|").concat( r.getString(26 + 33) )
        .concat("|").concat( r.getString(27 + 33) )
        .concat("|").concat( r.getString(28 + 33) )
        .concat("|").concat( r.getString(29 + 33) )
        .concat("|").concat( r.getString(30 + 33) )
        .concat("|").concat( r.getString(31 + 33) )
        .concat("|").concat( r.getString(32 + 33) )
        .concat("|").concat( r.getString(33 + 33) )
        .concat("|").concat( r.getString(34 + 33) )
        .concat("|").concat( r.getString(35 + 33) )
        .concat("|").concat( r.getString(36 + 33) )
        .concat("|").concat( r.getString(37 + 33) )
        .concat("|").concat( r.getString(38 + 33) )
        .concat("|").concat( r.getString(39 + 33) )
        .concat("|").concat( r.getDouble(40 + 33).toString() )
        .concat("|").concat( r.getInt(41 + 33).toString() )
        .concat("|").concat( r.getDouble(42 + 33).toString() )
        .concat("|").concat( r.getDouble(43 + 33).toString() )
        .concat("|").concat( r.getString(44 + 33) )
        .concat("|").concat( r.getString(45 + 33) )
        .concat("|").concat( r.getString(46 + 33) )
        .concat("|").concat( r.getString(47 + 33) )
        .concat("|").concat( r.getString(48 + 33) )
        .concat("|").concat( r.getString(49 + 33) )
        .concat("|").concat( r.getString(50 + 33) )
        .concat("|").concat( r.getString(51 + 33) )
        .concat("|").concat( r.getString(52 + 33) )
        .concat("|").concat( r.getString(53 + 33) )
        .concat("|").concat( r.getString(54 + 33) )
        .concat("|").concat( r.getString(55 + 33) )
        .concat("|").concat( r.getString(56 + 33) )
        .concat("|").concat( r.getString(57 + 33) )
        .concat("|").concat( r.getString(58 + 33) )
        .concat("|").concat( r.getString(59 + 33) )
        .concat("|").concat( r.getString(60 + 33) )
        .concat("|").concat( r.getString(61 + 33) )
        .concat("|").concat( r.getString(62 + 33) )
        .concat("|").concat( r.getString(63 + 33) )
        .concat("|").concat( r.getString(64 + 33) )
        .concat("|").concat( r.getString(65 + 33) )
        .concat("|").concat( r.getString(66 + 33) )
        .concat("|").concat( r.getString(67 + 33) )
        .concat("|").concat( r.getString(68 + 33) )
        .concat("|").concat( r.getString(69 + 33) )
        .concat("|").concat( r.getString(70 + 33) )
        .concat("|").concat( r.getString(71 + 33) )
        .concat("|").concat( r.getString(72 + 33) )
        .concat("|").concat( r.getString(73 + 33) )
        .concat("|").concat( r.getString(74 + 33) )
        .concat("|").concat( r.getString(75 + 33) )
        .concat("|").concat( r.getString(76 + 33) )
        .concat("|").concat( r.getString(77 + 33) )
        .concat("|").concat( r.getString(78 + 33) )
        .concat("|").concat( r.getString(79 + 33) )
        .concat("|").concat( r.getString(80 + 33) )
        .concat("|").concat( r.getString(81 + 33) )
        .concat("|").concat( r.getString(82 + 33) )
        .concat("|").concat( r.getString(83 + 33) )
        .concat("|").concat( r.getString(84 + 33) )
        .concat("|").concat( r.getString(85 + 33) )
        .concat("|").concat( r.getString(86 + 33) )
        .concat("|").concat( r.getString(87 + 33) )
        .concat("|").concat( r.getString(88 + 33) )
        .concat("|").concat( r.getString(89 + 33) )
        .concat("|").concat( r.getString(90 + 33) )
        .concat("|").concat( r.getString(91 + 33) )
        .concat("|").concat( r.getString(92 + 33) )
        .concat("|").concat( r.getString(93 + 33) )
        .concat("|").concat( r.getString(94 + 33) )
        .concat("|").concat( r.getString(95 + 33) )
        .concat("|").concat( r.getString(96 + 33) )
        .concat("|").concat( r.getString(97 + 33) )
        .concat("|").concat( r.getString(98 + 33) )
        .concat("|").concat( r.getString(99 + 33) )
        .concat("|").concat( r.getString(100 + 33) )
        .concat("|").concat( r.getString(101 + 33) )
        .concat("|").concat( r.getString(102 + 33) )
        .concat("|").concat( r.getString(103 + 33) )
        .concat("|").concat( r.getString(104 + 33) ) ,
         r.getString(105 + 33) )
        case _ => ( r.getString(0+ 35)
        .concat("|").concat( r.getString(1+ 35) )
        .concat("|").concat( r.getString(2+ 35) )
        .concat("|").concat( r.getString(3+ 35) )
        .concat("|").concat( r.getString(4+ 35) )
        .concat("|").concat( r.getString(5+ 35) )
        .concat("|").concat( r.getString(6+ 35) ) 
        .concat("|").concat( r.getString(7+ 35) )
        .concat("|").concat( r.getString(8+ 35) )
        .concat("|").concat( r.getString(9+ 35) )
        .concat("|").concat( r.getString(10+ 35) )
        .concat("|").concat( r.getString(11+ 35) )
        .concat("|").concat( r.getString(12+ 35) )
        .concat("|").concat( r.getString(13+ 35) )
        .concat("|").concat( r.getString(14+ 35) )
        .concat("|").concat( r.getString(15+ 35) )
        .concat("|").concat( r.getString(16+ 35) )
        .concat("|").concat( r.getString(17+ 35) )
        .concat("|").concat( r.getString(18+ 35) )
        .concat("|").concat( r.getString(19+ 35) )
        .concat("|").concat( r.getString(20+ 35) )
        .concat("|").concat( r.getString(21+ 35) )
        .concat("|").concat( r.getString(22+ 35) )
        .concat("|").concat( r.getString(23+ 35) )
        .concat("|").concat( r.getString(24+ 35) )
        .concat("|").concat( r.getString(25+ 35) )
        .concat("|").concat( r.getString(26+ 35) )
        .concat("|").concat( r.getString(27+ 35) )
        .concat("|").concat( r.getString(28+ 35) )
        .concat("|").concat( r.getString(29+ 35) )
        .concat("|").concat( r.getString(30+ 35) )
        .concat("|").concat( r.getString(31+ 35) )
        .concat("|").concat( r.getString(32+ 35) )
        .concat("|").concat( r.getString(33+ 35) )
        .concat("|").concat( r.getString(34+ 35) )
        .concat("|").concat( r.getString(35+ 35) )
        .concat("|").concat( r.getString(36+ 35) )
        .concat("|").concat( r.getString(37+ 35) )
        .concat("|").concat( r.getString(38+ 35) )
        .concat("|").concat( r.getString(39+ 35) )
        .concat("|").concat( r.getDouble(40+ 35).toString() )
        .concat("|").concat( r.getInt(41+ 35).toString() )
        .concat("|").concat( r.getDouble(42+ 35).toString() )
        .concat("|").concat( r.getDouble(43+ 35).toString() )
        .concat("|").concat( r.getString(44+ 35) )
        .concat("|").concat( r.getString(45+ 35) )
        .concat("|").concat( r.getString(46+ 35) )
        .concat("|").concat( r.getString(47+ 35) )
        .concat("|").concat( r.getString(48+ 35) )
        .concat("|").concat( r.getString(49+ 35) )
        .concat("|").concat( r.getString(50+ 35) )
        .concat("|").concat( r.getString(51+ 35) )
        .concat("|").concat( r.getString(52+ 35) )
        .concat("|").concat( r.getString(53+ 35) )
        .concat("|").concat( r.getString(54+ 35) )
        .concat("|").concat( r.getString(55+ 35) )
        .concat("|").concat( r.getString(56+ 35) )
        .concat("|").concat( r.getString(57+ 35) )
        .concat("|").concat( r.getString(58+ 35) )
        .concat("|").concat( r.getString(59+ 35) )
        .concat("|").concat( r.getString(60+ 35) )
        .concat("|").concat( r.getString(61+ 35) )
        .concat("|").concat( r.getString(62+ 35) )
        .concat("|").concat( r.getString(63+ 35) )
        .concat("|").concat( r.getString(64+ 35) )
        .concat("|").concat( r.getString(65+ 35) )
        .concat("|").concat( r.getString(66+ 35) )
        .concat("|").concat( r.getString(67+ 35) )
        .concat("|").concat( r.getString(68+ 35) )
        .concat("|").concat( r.getString(69+ 35) )
        .concat("|").concat( r.getString(70+ 35) )
        .concat("|").concat( r.getString(71+ 35) )
        .concat("|").concat( r.getString(72+ 35) )
        .concat("|").concat( r.getString(73+ 35) )
        .concat("|").concat( r.getString(74+ 35) )
        .concat("|").concat( r.getString(75+ 35) )
        .concat("|").concat( r.getString(76+ 35) )
        .concat("|").concat( r.getString(77+ 35) )
        .concat("|").concat( r.getString(78+ 35) )
        .concat("|").concat( r.getString(79+ 35) )
        .concat("|").concat( r.getString(80+ 35) )
        .concat("|").concat( r.getString(81+ 35) )
        .concat("|").concat( r.getString(82+ 35) )
        .concat("|").concat( r.getString(83+ 35) )
        .concat("|").concat( r.getString(84+ 35) )
        .concat("|").concat( r.getString(85+ 35) )
        .concat("|").concat( r.getString(86+ 35) )
        .concat("|").concat( r.getString(87+ 35) )
        .concat("|").concat( r.getString(88+ 35) )
        .concat("|").concat( r.getString(89+ 35) )
        .concat("|").concat( r.getString(90+ 35) )
        .concat("|").concat( r.getString(91+ 35) )
        .concat("|").concat( r.getString(92+ 35) )
        .concat("|").concat( r.getString(93+ 35) )
        .concat("|").concat( r.getString(94+ 35) )
        .concat("|").concat( r.getString(95+ 35) )
        .concat("|").concat( r.getString(96+ 35) )
        .concat("|").concat( r.getString(97+ 35) )
        .concat("|").concat( r.getString(98+ 35) )
        .concat("|").concat( r.getString(99+ 35) )
        .concat("|").concat( r.getString(100+ 35) )
        .concat("|").concat( r.getString(101+ 35) )
        .concat("|").concat( r.getString(102+ 35) )
        .concat("|").concat( r.getString(103+ 35) )
        .concat("|").concat( r.getString(104+ 35) ) ,
         r.getString(105+ 35) )
      }
                           
      )
    val cs5TransposeRDD = formatRDD.flatMapValues(word => word.split("\\^")).map{case (k,v) => k.concat("|").concat( Common.getTuple(v, "~", 2)+"|"+Common.getTuple(v, "~", 3)+"|"+Common.getTuple(v, "~", 4)+"|"+Common.getTuple(v, "~", 0)+"|"+Common.getTuple(v, "~", 1) ) };
    
    
     /*
     * Create new dataframe FROM CS5 transpose result
     */
    // Include SSP_TRANSACTION_ID field
    val cs5TransposeRow = cs5TransposeRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
        p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12),p(13), p(14), p(15), p(16),
        p(17), p(18), p(19), p(20), p(21), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29),
        p(30), p(31), p(32), p(33), p(34), p(35), p(36), p(37), p(38), p(39), p(40).toDouble, p(41).toInt, p(42).toDouble, 
        p(43).toDouble, p(44), p(45), p(46), p(47), p(48), p(49), p(50), p(51), p(52), p(53), p(54), p(55), 
        p(56), p(57), p(58), p(59), p(60), p(61), p(62), p(63), p(64), p(65), p(66), p(67), p(68),
        p(69), p(70), p(71), p(72), p(73), p(74), p(75), p(76), p(77), p(78), p(79), p(80), p(81),
        p(82), p(83), p(84), p(85), p(86), p(87), p(88), p(89), p(90), p(91), p(92), p(93), 
        p(94), p(95), p(96), p(97), p(98), p(99), p(100), p(101), p(102), p(103), p(104),  
        p(105).toDouble, p(106), p(107), p(108), p(109)
        ))
    // Create new transpose schema with SSP_TRANSACTION_ID
    val cs5TransposeDF = sqlContext
      .createDataFrame(cs5TransposeRow, CS5Schema.CS5Transpose)
      .as("ADDON")
    cs5TransposeDF.persist(storageLevel)
    cs5TransposeDF.registerTempTable("ADDON")
    
    val ngsspDF = sqlContext.read
      .option("basePath", ngsspLocation)
      .parquet(ngsspLocation + "/TRANSACTIONDATE=" + transaction_date).as("NGSSP")
    ngsspDF.registerTempTable("NGSSP")
    
    val ngsspCS5JoinDF = sqlContext.sql("""
      select 
        ADDON.TransactionDate,
        ADDON.TransactionHour,
        ADDON.MSISDN,
        ADDON.CCNNode,
        ADDON.APartyNumber,
        ADDON.BPartyNumber,
        ADDON.ServiceClass,
        ADDON.CallClassCat,
        ADDON.MSCAddress,
        ADDON.OriginRealm,
        ADDON.OriginHost,
        CASE WHEN nvl(NGSSP.CDR_CGI, '') <> ''
          THEN concat_ws('', substr(CDR_CGI, 2, 1), substr(CDR_CGI, 1, 1), substr(CDR_CGI, 4, 1), substr(CDR_CGI, 6, 1), substr(CDR_CGI, 5, 1))
        WHEN nvl(NGSSP.TDR_CGI, '') <> ''
          THEN concat_ws('', substr(TDR_CGI, 2, 1), substr(TDR_CGI, 1, 1), substr(TDR_CGI, 4, 1), substr(TDR_CGI, 6, 1), substr(TDR_CGI, 5, 1))
        ELSE ''
        END MCCMNC,
        substr(
          regexp_replace(
            CASE WHEN nvl(ADDON.CGI, '') <> '' THEN ADDON.CGI
            WHEN nvl(NGSSP.CDR_CGI, '') <> '' THEN NGSSP.CDR_CGI
            WHEN nvl(NGSSP.TDR_CGI, '') <> '' THEN NGSSP.TDR_CGI
            ELSE ''
            END, '[^a-z0-9]', ''
          ), 7, 4
        ) LAC,
        substr(
          regexp_replace(
            CASE WHEN nvl(ADDON.CGI, '') <> '' THEN ADDON.CGI
            WHEN nvl(NGSSP.CDR_CGI, '') <> '' THEN NGSSP.CDR_CGI
            WHEN nvl(NGSSP.TDR_CGI, '') <> '' THEN NGSSP.TDR_CGI
            ELSE ''
            END, '[^a-z0-9]', ''
          ), 11, 4
        ) CI,
        ADDON.ECI,
        ADDON.IMSI,
        ADDON.APN,
        ADDON.ServiceScenario,
        ADDON.RoamingPosition,
        ADDON.FAF,
        ADDON.FAFNumber,
        ADDON.RatingGroup,
        ADDON.ContentType,
        ADDON.IMEI,
        ADDON.GGSNAddress,
        ADDON.SGSNAddress,
        ADDON.CorrelationId,
        ADDON.ChargingId,
        ADDON.RATType,
        ADDON.ServiceOfferId,
        ADDON.PaymentCategory,
        ADDON.DedicatedAccountRow,
        ADDON.DedicatedAccountValuesRow,
        ADDON.AccountGroup,
        ADDON.FAFName,
        ADDON.AccountValueDeducted,
        ADDON.Accumulator,
        ADDON.CommunityId1,
        ADDON.CommunityId2,
        ADDON.CommunityId3,
        ADDON.AccumulatedCost,
        ADDON.Hit,
        ADDON.UsageVolume,
        ADDON.UsageDuration,
        ADDON.IntcctAPartyNo,
        ADDON.IntcctBPartyNo,
        ADDON.SpOfrId,
        ADDON.SpConsume,
        ADDON.VasServiceId,
        ADDON.TrafficCase,
        ADDON.UsedOfferSC,
        ADDON.OfferAttributeAreaname,
        ADDON.CCRCCAAccountValueBefore,
        ADDON.CCRCCAAccountValueAfter,
        ADDON.ServiceUsageType,
        ADDON.CCRTriggerTime,
        ADDON.CCREventTime,
        ADDON.JobId,
        ADDON.RecordId,
        ADDON.PrcDt,
        ADDON.SourceType,
        ADDON.FileName,
        ADDON.FileDate,
        ADDON.MicroclusterId,
        ADDON.NormCCRTriggerTime,
        ADDON.SSPTransId,
        ADDON.SubsLocNo,
        regexp_replace(
          CASE WHEN nvl(ADDON.CGI, '') <> '' THEN ADDON.CGI
          WHEN nvl(NGSSP.CDR_CGI, '') <> '' THEN NGSSP.CDR_CGI
          WHEN nvl(NGSSP.TDR_CGI, '') <> '' THEN NGSSP.TDR_CGI
          ELSE ''
          END, '[^a-z0-9]', ''
        ) CGI,
        ADDON.RsvFld2,
        ADDON.RsvFld3,
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
        ADDON.callClassBase,
        ADDON.callClassRuleId,
        ADDON.callClassTariff,
        ADDON.hlrBranchNm,
        ADDON.hlrRegionNm,
        ADDON.prmPkgCode,
        ADDON.prmPkgName,
        ADDON.brndSCName,
        ADDON.mgrSvcClassId,
        ADDON.offerAttrName,
        ADDON.offerAttrValue,
        ADDON.areaName,
        ADDON.microclusterName,
        ADDON.laciCluster,
        ADDON.laciSalesArea,
        ADDON.laciArea,
        ADDON.laciMicroCluster,
        ADDON.siteTech,
        ADDON.laciRatType,
        ADDON.revenueCodeBase,
        ADDON.UsageAmount,
        ADDON.DAUnitAmount,
        ADDON.DAUnitUOM,
        ADDON.TransposeId,
        ADDON.Account,
        ADDON.SSPTransId SSP_TRANSACTION_ID,
        CASE WHEN nvl(NGSSP.CDR_ORIGIN, '') <> ''
          THEN NGSSP.CDR_ORIGIN
        WHEN nvl(NGSSP.ORIGIN, '') <> ''
          THEN NGSSP.ORIGIN
        ELSE ''
        END ORIGIN,
        ADDON.LAC ADDONLAC,
        ADDON.CI ADDONCI,
        ADDON.CGI ADDONCGI,
        NGSSP.TDR_CGI TDRCGI,
        NGSSP.CDR_CGI CDRCGI
      from NGSSP
      right join ADDON on NGSSP.TDR_TRANSACTIONID = ADDON.SSPTransId 
        and NGSSP.TDR_MSISDN = ADDON.MSISDN
        and NGSSP.TDR_CGI rlike '^[0-9a-z]{14}$'
    """)
      //cs5TransposeDF.join(ngsspCDRDF.withColumnRenamed("CGI", "NGSSPCGI"), col("ADDON.SSPTransId") === col("NGSSP.ORDER_ID"), "left_outer")
    //ngsspCS5JoinDF.show(false)
    ngsspCS5JoinDF.persist(storageLevel)
    ngsspCS5JoinDF.registerTempTable("cs5TransposeNGSSP")
    
    /*
     * Register UDFs
     */
    sqlContext.udf.register("genRevenueCode", CS5Select.genRevenueCode _)
    sqlContext.udf.register("genAccountForMADA", CS5Select.genAccountForMADA _)
    sqlContext.udf.register("genSvcUsgTypeFromRevCode", CS5Select.genSvcUsgTypeFromRevCode _)
    sqlContext.udf.register("genSvcUsgDirFromRevCode", CS5Select.genSvcUsgDirFromRevCode _)
    sqlContext.udf.register("genSvcUsgDestFromRevCode", CS5Select.genSvcUsgDestFromRevCode _)
    
    
    /*
     * Reference lookup 4:
     * 1. Revenue Code 
     * 2. MADA
     */
    // Include ORIGIN Field FROM Join
    val cs5LookupMADADF = sqlContext.sql(
        """SELECT 
              a.*,
              genRevenueCode(a.revenueCodeBase,a.Account,a.ServiceUsageType) revenueCode,
              NVL(b.lv1,'') revCodeLv1, NVL(b.lv2,'') revCodeLv2, NVL(b.lv3,'') revCodeLv3, NVL(b.lv4,'') revCodeLv4,
              NVL(b.lv5,'') revCodeLv5, NVL(b.lv6,'') revCodeLv6, NVL(b.lv7,'') revCodeLv7, NVL(b.lv8,'') revCodeLv8,
              NVL(b.lv9,'') revCodeLv9, NVL(b.svcUsgTp,'') revCodeUsgTp, NVL(b.dirTp,'') revCodeDirTp,
              NVL(b.distTp,'') revCodeDistTp,
              NVL(c.acc, '') MADAAcc,
              genSvcUsgTypeFromRevCode( b.svcUsgTp , b.lv9, b.lv5 ) finalServiceUsageType,
              genSvcUsgDirFromRevCode( b.svcUsgTp, b.lv3 ) SvcUsgDirection,
              genSvcUsgDestFromRevCode( b.svcUsgTp, b.lv2 ) SvcUsgDestination,
              CASE 
                 WHEN b.svcUsgTp='VOICE' then NVL(c.voiceTrf, '')
                 WHEN b.svcUsgTp='SMS' then NVL(c.smsTrf, '') 
                 WHEN b.svcUsgTp='DATA' then NVL(c.dataVolTrf, '')
                 ELSE ''
              END TrafficFlag,
              CASE WHEN b.svcUsgTp='VOICE' then NVL(c.voiceRev, '')
                 WHEN b.svcUsgTp='SMS' then NVL(c.smsRev, '') 
                 WHEN b.svcUsgTp='DATA' then NVL(c.dataVolRev, '')
                 WHEN b.svcUsgTp='VAS' then NVL(c.vasRev, '')
                 WHEN b.svcUsgTp='OTHER' then NVL(c.othRev, '')
                 WHEN b.svcUsgTp='DISCOUNT' then NVL(c.discRev, '')
                 WHEN b.svcUsgTp='DROP' then 'No'
                 ELSE 'UNKNOWN'
              END RevenueFlag
           FROM 
              cs5TransposeNGSSP a            
              LEFT JOIN refRevCode b ON genRevenueCode(a.revenueCodeBase,a.Account,a.ServiceUsageType)=b.revCode
              LEFT JOIN refMADA c ON a.prmPkgCode=c.grpCd and genAccountForMADA(a.Account)=c.acc and a.TransactionDate>=c.effDt and a.TransactionDate<c.endDt"""
        )
    cs5LookupMADADF.persist(storageLevel)
    cs5LookupMADADF.registerTempTable("cs5LookupMADA")
    
     /*****************************************************************************************************
     * 
     * CS5 TRANSFORMATION OUTPUT
     * 
     *****************************************************************************************************/
    // Add Origin
    val cs5Output = sqlContext.sql("""
        SELECT
          TransactionDate TRANSACTION_DATE ,
          TransactionHour TRANSACTION_HOUR,
          APartyNumber A_PARTY_NUMBER,
          APrefix A_PREFIX,
          ServiceCityName SERVICE_CITY_NAME,
          ServiceProviderId SERVICE_PROVIDER_ID,
          ServiceCountryName SERVICE_COUNTRY_NM,
          hlrBranchNm HLR_BRANCH_NM,
          hlrRegionNm HLR_REGION_NM,
          BPartyNumber B_PARTY_NUMBER,
          BPrefix B_PREFIX,
          DestinationCityName DESTINATION_CITY_NAME,
          DestinationProviderId DESTINATION_PROVIDER_ID,
          DestinationCountryName DESTINATION_COUNTRY_NM,
          ServiceClass SERVICE_CLASS_ID,
          prmPkgCode PROMO_PACKAGE_CODE,
          prmPkgName PROMO_PACKAGE_NAME,
          brndSCName BRAND_NAME,
          CallClassCat CALL_CLASS_CAT,
          callClassBase CALL_CLASS_BASE,
          callClassRuleId CALL_CLASS_RULEID,
          callClassTariff CALL_CLASS_TARIF,
          MSCAddress MSC_ADDRESS,
          OriginRealm ORIGINAL_REALM,
          OriginHost ORIGINAL_HOST,
          MCCMNC MCC_MNC,
          hexToInt(LAC) LAC,
          hexToInt(CI) CI,
          MicroclusterId LACI_CLUSTER_ID,
          laciCluster LACI_CLUSTER_NM,
          '' LACI_REGION_ID,
          '' LACI_REGION_NM,
          '' LACI_AREA_ID,
          laciArea LACI_AREA_NM,
          '' LACI_SALESAREA_ID,
          laciSalesArea LACI_SALESAREA_NM,
          IMSI,
          APN,
          ServiceScenario SERVICE_SCENARIO,
          RoamingPosition ROAMING_POSITION,
          FAF,
          FAFNumber FAF_NUMBER,
          RatingGroup RATING_GROUP,
          ContentType CONTENT_TYPE,
          IMEI,
          GGSNAddress GGSN_ADDRESS,
          SGSNAddress SGSN_ADDRESS,
          CorrelationId CALL_REFERENCE,
          ChargingId CHARGING_ID,
          RATType RAT_TYPE,
          ServiceOfferId SERVICE_OFFER_ID,
          PaymentCategory PAYMENT_CATEGORY,
          Account ACCOUNT_ID,
          AccountGroup ACCOUNT_GROUP_ID,
          FAFName FAF_NAME,
          '' TRAFFIC_CASE,
          TrafficCase TRAFFIC_CASE_NAME,
          revenueCode REVENUE_CODE,
          revCodeDirTp DIRECTION_TYPE,
          revCodeDistTp DISTANCE_TYPE,
          revCodeUsgTp SERVICE_TYPE,
          finalServiceUsageType SERVICE_USG_TYPE,
          revCodeLv1 REVENUE_CODE_L1,
          revCodeLv2 REVENUE_CODE_L2,
          revCodeLv3 REVENUE_CODE_L3,
          revCodeLv4 REVENUE_CODE_L4,
          revCodeLv5 REVENUE_CODE_L5,
          revCodeLv6 REVENUE_CODE_L6,
          revCodeLv7 REVENUE_CODE_L7,
          revCodeLv8 REVENUE_CODE_L8,
          revCodeLv9 REVENUE_CODE_L9,
          SvcUsgDirection SVC_USG_DIRECTION,
          SvcUsgDestination SVC_USG_DESTINATION,
          TrafficFlag TRAFFIC_FLAG,
          RevenueFlag REVENUE_FLAG,
          FileName REAL_FILENAME,
          CASE 
             WHEN TransposeId='1' then UsageVolume 
             ELSE 0 
          END USAGE_VOLUME,
          UsageAmount USAGE_AMOUNT,
          CASE 
             WHEN TransposeId='1' then UsageDuration
             ELSE 0
          END USAGE_DURATION,
          CASE 
             WHEN TransposeId='1' then Hit 
             ELSE 0
          END HIT,
          Accumulator ACCUMULATOR,
          CommunityId1 COMMUNITY_ID_1,
          CommunityId2 COMMUNITY_ID_2,
          CommunityId3 COMMUNITY_ID_3,
          AccumulatedCost ACCUMULATED_COST,
          '' RECORD_TYPE,
          CCRTriggerTime TRIGGER_TIME,
          CCREventTime EVENT_TIME,
          '' RECORD_ID_NUMBER,
          CCRCCAAccountValueBefore CCR_CCA_ACCOUNT_VALUE_BEFORE,
          CCRCCAAccountValueAfter CCR_CCA_ACCOUNT_VALUE_AFTER,
          ECI,
          '' UNIQUE_KEY,
          siteTech SITE_TECH,
          laciRatType SITE_OPERATOR,
          mgrSvcClassId MGR_SVCCLSS_ID,
          UsedOfferSC OFFER_ID,
          offerAttrName OFFER_ATTR_KEY,
          offerAttrValue OFFER_ATTR_VALUE,
          areaName OFFER_AREA_NAME,
          DAUnitAmount DA_UNIT_AMOUNT,
          DAUnitUOM DA_UNIT_TYPE,
          JobId JOB_ID,
          RecordId RCRD_ID,
          PrcDt PRC_DT,
          SourceType SRC_TP,
          FileDate FILEDATE,
          microclusterName MICROCLUSTER_NAME,
          NVL(ORIGIN,'') ORIGIN,
          SSPTransId,
          MSISDN
       FROM 
          cs5LookupMADA
       WHERE 
          prmPkgCode <> ""
          and revCodeUsgTp <> ""
          and MADAAcc <> ""
        """)
     cs5Output.persist(storageLevel)
     cs5Output.registerTempTable("cs5Output")
     sqlContext.sql("select SSPTransId, MSISDN, JOB_ID, LAC, CI from cs5Output")
     .write.mode("overwrite").parquet("/tmp/sample")
       

     /*****************************************************************************************************
     * 
     * AGGREGATION
     * 
     *****************************************************************************************************/
     
      val cs5GreenReportAggOutput = CS5Aggregate.genGreenReportSummaryAddon(sqlContext)
      cs5GreenReportAggOutput.registerTempTable("cs5GreenReportAggOutput") 
      
      val cs5TdwSmyAggOutput = CS5Aggregate.genPrepaidTdwSummaryAddon(sqlContext)
     
     /*****************************************************************************************************
     * 
     * WRITE OUTPUT
     * 
     *****************************************************************************************************/
     /*
       * Output Aggregation Green Report
       */
      Common.cleanDirectoryWithPattern(sc, pathAddonAggGreenReportOutput, "/process_id=*_" + jobID + "_CS5")
      cs5GreenReportAggOutput.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save( pathAddonAggGreenReportOutput + "/process_id=" + prcDt + "_" + jobID + "_CS5")
      /*
       * Output Aggregation TDW Summary
       */
      Common.cleanDirectoryWithPattern(sc, pathAddonTdwSmyOutput, "/*_" + jobID)
      cs5TdwSmyAggOutput.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        
        .save( pathAddonTdwSmyOutput + "/" + prcDt + "_" + jobID)
     
        
    sc.stop();
  }
}