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
 * @author anico@id.ibm.com
 * @author IBM Indonesia for Indosat Ooredoo
 * 
 * June 2016
 * 
 *****************************************************************************************************/   
object CS5PrepaidTransform {
  def main(args: Array[String]): Unit = {
    
    /*****************************************************************************************************
     * 
     * Parameter check and setting
     * 
     *****************************************************************************************************/
    
    var prcDt = ""
    var jobID = ""
    var reprocessFlag = ""
    var configDir = ""
    var inputDir = ""
      
    try {
      prcDt = args(0)
      jobID = args(1)
      reprocessFlag = args(2)
      configDir = args(3)
      inputDir = args(4)
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    /*
     * Initiate Spark Context
     */
    //val sc = new SparkContext("local", "testing spark ", new SparkConf());
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
     * Get SYSDATE -90 for Filtering Reject Permanent
     */
    val dateFormat = new SimpleDateFormat("yyyyMMdd");
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -90);
    val minDate = dateFormat.format(cal.getTime())   
    
    /*
     * Assign child dir
     */
    val childDir = "/" + prcDt + "_" + jobID
    
    /*
     * Target directory assignments
     */
    val pathCS5TransformOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_TRANSFORM_DIR")
    val pathRejBP = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_REJ_BP_DIR") + childDir
    val pathRejRef = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_REJ_REF_DIR") + childDir
    val pathCS5CDRInqOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_CDR_INQ_DIR") + childDir
    val pathAggHourlyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_HOURLY_PREPAID.OUTPUT_SPARK_AGG_HOURLY_PREPAID")
    val pathAggDailyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_DAILY_PREPAID.OUTPUT_SPARK_AGG_DAILY_PREPAID")
    val pathAggGreenReportOutput = Common.getConfigDirectory(sc, configDir, "USAGE.GREEN_REPORT_PREPAID.OUTPUT_SPARK_GREEN_REPORT_PREPAID")
    val pathTdwSmyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_TDW_SUMMARY_DIR") + childDir
    val pathRejRefPerm =  Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_REJ_REF_PERM_DIR") + childDir
    val pathAddonIPCNOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_ADDON_IPCN_DIR")
    val pathMCNoCatSMSOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_MC_NOCAT_SMS_DIR")
    val pathMCNoCatVoiceOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_PREPAID_TRANSFORM.OUTPUT_CS5_PREPAID_MC_NOCAT_VOICE_DIR")
    
    //val path_ref = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/Reference";
    val path_ref = "/user/apps/CS5/reference";
    val pathRefServiceClassOfr = Common.getConfigDirectory(sc, configDir, "USAGE.REFERENCE.REF_SERVICE_CLASS_OFR")
    
    /* OLD DIRECTORY 
    val pathOutput = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/output"
    //val pathOutput = "/user/apps/CS5/output"
    val pathRejBP = pathOutput+"/CS5RejectBP"
    val pathRejRef = pathOutput+"/CS5RejectRef"
    val pathCS5TransformOutput = pathOutput+"/CS5TransformOutput"
    val pathCS5CDRInqOutput = pathOutput+"/CS5CDRInqOutput"
    */
        
    /*
     * Get source file
     */
    if ( reprocessFlag=="REPCS" ) sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val cs5InputRDD = sc.textFile(inputDir);
    

    
    /*****************************************************************************************************
     * 
     * Transformation CS5 and Reject BP
     * Reject BP rules :  null subscriber id or null service class or null trigger time
     * 
     *****************************************************************************************************/
    
    /*
     * Get transformed result, either from:
     * 1) conversion result for main process, or
     * 2) rejected data for reprocess
     */
    val cs5Transform1RDD = {
      if ( reprocessFlag!= "REPCS" ) { 
          //CREATE NEW TRANSFORMATION FROM CONVERSION RESULT
          
          //1. Cache the filtered data
          val cs5InputFilteredRDD = cs5InputRDD
            .filter(line => CS5Select.cs5ConvertedRowIsValid(line) )
            //.filter(line => CS5Select.cs5RecordsIsNotGarbage(line) )
            .filter(line => CS5Select.getPaymentCategory(line)=="PREPAID" )
          //--20161001--cs5InputFilteredRDD.cache()
          
          //2. Transform the rest
          cs5InputFilteredRDD
            //.map(line => CS5Select.genCS5TransformedRow(  Common.getTuple(line, "\t", 1).concat("|").concat(Common.getTuple(line, "\t", 2)).concat("|").concat(Common.getTuple(line, "\t", 3)), prcDt, jobID ) )
            .flatMap(line => CS5Select.genCS5TransformedRowList(  Common.getTuple(line, "\t", 1).concat("|").concat(Common.getTuple(line, "\t", 2)).concat("|").concat(Common.getTuple(line, "\t", 3))  , "PREPAID" , prcDt, jobID  ) )
          
      } else { 
          //GET THE REJECTED DATA
          
          //1. Get data not reject permanent
          val cs5InputFilteredRDD = cs5InputRDD.filter(line => Common.getTuple(line, "|", 0)>=minDate)
          
          //2. Transform the rest
          cs5InputFilteredRDD.map(line => CS5Select.genCS5RejectedRow(line, prcDt, jobID))
      }
    }
    
    //cs5InputRDD.persist(storageLevel)
    
    /*
     * Reject BP 
     * Only for main process
     */
    val rejBpRDD = {
       if ( reprocessFlag!= "REPCS" ) cs5InputRDD.filter(line => !CS5Select.cs5ConvertedRowIsValid(line) )
       else null
    }
    
    /*
     * Reject REF PERM 
     * Only for rej process
     */
    val rejPermRDD = { 
      if ( reprocessFlag== "REPCS" ) {
        cs5InputRDD.filter(line => Common.getTuple(line, "|", 0)<minDate)
      }
      else null
    }
    
    
    /*
     * DEBUG ONLY: print
     */
    //cs5Transform1RDD.collect foreach {case (a) => println (a)}
    
    
    /*****************************************************************************************************
     * 
     * Load references
     * DATAFRAME action start here
     * 
     *****************************************************************************************************/
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    /*
     * Reference Service Class
     */
//    val refServiceClass = sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter",",")
//    .schema(ReferenceSchema.svcClassSchema)
//    .load(path_ref+"/ref_service_class_no_-99.csv") 
    val refServiceClass = ReferenceDF.getServiceClassDF(sqlContext)
    refServiceClass.registerTempTable("refServiceClass")
    refServiceClass.persist()
    refServiceClass.count()
    
    //refServiceClass.show()
    
    /*
     * Reference Service Class Offer
     */
//    val refServiceClassOfr = sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter",",")
//    .schema(ReferenceSchema.svcClassOfrSchema)
//    .load(path_ref+"/ref_service_class_ofr_20160606.csv") 
    val refServiceClassOfr = ReferenceDF.getServiceClassOfferDF2(sqlContext, pathRefServiceClassOfr)
    refServiceClassOfr.registerTempTable("refServiceClassOfr")
    refServiceClassOfr.persist()
    refServiceClassOfr.count()
    //refServiceClassOfr.show()
    
    /*
     * Join reference service class old and offer
     */
    val refServiceClassComplete = broadcast( sqlContext.sql(
        """select svcClassCode, '' svcClassCodeOld, '' svcClassName, '' offerId, '' offerAttrName, '' offerAttrValue, '' areaName, prmPkgCode,prmPkgName,brndSCName, svcClassEffDt, svcClassEndDt, brndSCEffDt, brndSCEndDt from refServiceClass 
         union all
         select svcClassCode, svcClassCodeOld, svcClassName, offerId, offerAttrName, offerAttrValue, areaName, prmPkgCode, prmPkgName, brndSCName, effDt svcClassEffDt, endDt svcClassEndDt, effDt brndSCEffDt, endDt brndSCEndDt from refServiceClassOfr"""
        ) )
    refServiceClassComplete.registerTempTable("refServiceClassComplete")
    refServiceClassComplete.persist()
    refServiceClassComplete.count()
    //refServiceClassComplete.show()
    
    /*
     * Reference Interconnect
     */  
//     val intcctDF =  broadcast(sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.intcctAllSchema)
//    .load(path_ref+"/ref_intcct_length_all.txt"))
    val intcctDF = ReferenceDF.getIntcctAllDF(sqlContext)

    
    
    /*
     * Reference Call Class
     */
//    val refCallClass = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter",",")
//    .schema(ReferenceSchema.callClassSchema)
//    .load(path_ref+"/ref_call_class_id.csv") )
    val refCallClass = ReferenceDF.getCallClassDF(sqlContext)
    refCallClass.registerTempTable("refCallClass")
    refCallClass.persist()
    refCallClass.count()
    //refCallClass.show()
        
    /*
     * Reference branch region
     */
//    val refRegionBranch = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter",",")
//    .schema(ReferenceSchema.regionBranchSchema)
//    .load(path_ref+"/ref_region_branch.csv") )
    val refRegionBranch = ReferenceDF.getRegionBranchDF(sqlContext)
    refRegionBranch.registerTempTable("refRegionBranch")
    refRegionBranch.persist()
    refRegionBranch.count()
    //refRegionBranch.show()
    
    /*
     * Reference lac ci hierarchy
     */
//    val refLacCiHierarchy = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.lacCiHierarchySchema)
//    .load(path_ref+"/REF_LAC_CI_HIERARCHY_20160523.txt") )
    val refLacCiHierarchy = ReferenceDF.getLACCIHierarchy(sqlContext)
    refLacCiHierarchy.registerTempTable("refLacCiHierarchy")
    refLacCiHierarchy.persist()
    refLacCiHierarchy.count()
    //refLacCiHierarchy.show()
    
    /*
     * Reference rev code
     */
    val refRevCodeDF = ReferenceDF.getRevenueCodeDF(sqlContext)
    refRevCodeDF.registerTempTable("refRevCode")
    refRevCodeDF.persist()
    refRevCodeDF.count()
    //refRevCodeDF.show()
    
    /*
     * Reference MADA
     */
//    val refMADADF = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.madaSchema)
//    .load(path_ref+"/REF_MADA_20160524.txt") )
    val refMADADF = ReferenceDF.getMaDaDF(sqlContext)
    refMADADF.registerTempTable("refMADA")
    refMADADF.persist()
    refMADADF.count()
    //refMADADF.show()
    
    /*
     * Reference microcluster
     */
//    val refMicroclusterDF = sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter",",")
//    .schema(ReferenceSchema.microclusterSchema)
//    .load(path_ref+"/LDM_map_microcluster_20160804.csv") 
    val refMicroclusterDF = ReferenceDF.getMicroClusterDF(sqlContext)
    refMicroclusterDF.registerTempTable("refMicrocluster")
    refMicroclusterDF.persist() 
    refMicroclusterDF.count()
    
    val refDistinctMicroclusterDF = sqlContext.sql(
        """select distinct microclusterId, microclusterName from refMicrocluster"""
    )
    refDistinctMicroclusterDF.registerTempTable("refDistinctMicrocluster")
    refDistinctMicroclusterDF.persist() 
    refDistinctMicroclusterDF.count()
    //refDistinctMicroclusterDF.show()

    /*
     * Reference MCC MNC
     */
    val refMCCMNCDF = ReferenceDF.getMCCMNCDF(sqlContext)
    refMCCMNCDF.registerTempTable("refMCCMNC")
    refMCCMNCDF.persist() 
    refMCCMNCDF.count()

    
    /*
     * Other references (non-DataFrame)
     */
    //val intcctRef = new Common.interconnectRef("C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/Reference/ref_intcct_length_all.txt" , sc)
    val intcctRef = ReferenceDF.getIntcctAllRef(sc)
    
    /*****************************************************************************************************
     * 
     * Join references using DataFrames
     * 
     * 
     *****************************************************************************************************/
        
    /*
     * Create new dataframe from CS5 Transform 1 result
     */    
    val cs5Transform1DF = sqlContext.createDataFrame(cs5Transform1RDD, CS5Schema.CS5Schema)
    cs5Transform1DF.registerTempTable("CS5Transform1")
    //cs5Transform1DF.show()

   
    /*
     * Reference lookup 1:
     * 1. Interconnect A 
     * 2. Interconnect B
     */
    
    /*
     * Old Interconnect (Multiple joins)
     */
//    val joinAintcctDF = Common.getIntcctWithSID(sqlContext, cs5Transform1DF, intcctDF, "APartyNumber", "TransactionDate", "VasServiceId", "APrefix", "ServiceCountryName", "ServiceCityName", "ServiceProviderId")
//    joinAintcctDF.persist(storageLevel)
//    val joinBintcctDF = Common.getIntcct(sqlContext, joinAintcctDF, intcctDF, "BPartyNumber", "TransactionDate", "BPrefix", "DestinationCountryName", "DestinationCityName", "DestinationProviderId")
//    joinBintcctDF.registerTempTable("cs5Lookup2")
//    joinBintcctDF.persist(storageLevel)
//    joinBintcctDF.show()
    
    /*
     * Old Interconnect (Double Loop)
     */
//    val joinAintcctDF = Common.getIntcctWithSID2(sqlContext, cs5Transform1DF, intcctRef, "APartyNumber", "TransactionDate", "VasServiceId", "APrefix", "ServiceCountryName", "ServiceCityName", "ServiceProviderId")
//    val joinBintcctDF = Common.getIntcctWithSID2(sqlContext, joinAintcctDF, intcctRef, "BPartyNumber", "TransactionDate", "VasServiceId", "BPrefix", "DestinationCountryName", "DestinationCityName", "DestinationProviderId")

    /*
     * New Interconnect (Single Loop)
     */
    val joinBintcctDF = Common.getIntcctWithSID4(
        sqlContext, cs5Transform1DF, intcctRef, "APartyNumber", 
        "BPartyNumber", "TransactionDate", "VasServiceId", "APrefix", 
        "ServiceCountryName", "ServiceCityName", "ServiceProviderId", 
        "BPrefix", "DestinationCountryName", "DestinationCityName", 
        "DestinationProviderId", "ServiceScenario")
    
    joinBintcctDF.registerTempTable("cs5Lookup2")
    //joinBintcctDF.persist(storageLevel)
    //joinBintcctDF.show()

      
    /*
     * Reference lookup 2: 
     * 1. Call Class
     * 2. Service Class 
     * 3. Branch/region
     * 4. Microcluster
     */
    val cs5Lookup3DF = sqlContext.sql(
        """select 
              a.TransactionDate, a.TransactionHour, a.MSISDN, a.CCNNode, a.APartyNumber, a.BPartyNumber, a.ServiceClass, a.CallClassCat, a.MSCAddress, a.OriginRealm, a.OriginHost, a.MCCMNC, a.LAC, a.CI, a.ECI, a.IMSI, a.APN, a.ServiceScenario, a.RoamingPosition, a.FAF, a.FAFNumber, a.RatingGroup, a.ContentType, a.IMEI, a.GGSNAddress, a.SGSNAddress, a.CorrelationId, a.ChargingId, 
              case when a.ServiceUsageType='SMS' or a.ServiceUsageType='VOICE' then 
                case f.serviceType when '3G' then 1
                when '2G' then 2
                when 'Super WiFi' then 3
                when '1G' then 4
                when 'HSPA' then 5
                when '4G' then 6
                when 'Unknown' then 8 
                else a.RATType end 
              else a.RATType 
              end RATType, 
              a.ServiceOfferId, a.PaymentCategory, a.DedicatedAccountRow, a.DedicatedAccountValuesRow, a.AccountGroup, a.FAFName, a.AccountValueDeducted, a.Accumulator, a.CommunityId1, a.CommunityId2, a.CommunityId3, a.AccumulatedCost, a.Hit, a.UsageVolume, a.UsageDuration, a.IntcctAPartyNo, a.IntcctBPartyNo, a.SpOfrId, a.SpConsume, a.VasServiceId, a.TrafficCase, a.UsedOfferSC, a.OfferAttributeAreaname, a.CCRCCAAccountValueBefore, a.CCRCCAAccountValueAfter, a.ServiceUsageType, a.CCRTriggerTime, a.CCREventTime, a.JobId, a.RecordId, a.PrcDt, a.SourceType, a.FileName, a.FileDate, a.MicroclusterId, a.NormCCRTriggerTime, a.SSPTransId, a.SubsLocNo, a.CGI, nvl(d.customerSegment, '') RsvFld2, a.RsvFld3, a.RsvFld4, a.RsvFld5, a.RsvFld6, a.RsvFld7, a.RsvFld8, a.RsvFld9, a.RsvFld10, a.APrefix, a.ServiceCountryName, a.ServiceProviderId, a.ServiceCityName, a.BPrefix, a.DestinationCountryName, a.DestinationProviderId, a.DestinationCityName, 
              nvl(b.baseNm,'') callClassBase, nvl(b.ruleId,'') callClassRuleId, nvl(b.tariffZone,'') callClassTariff, nvl(c.branch,'') hlrBranchNm, nvl(c.region,'') hlrRegionNm,
           nvl(s.prmPkgCode,d.prmPkgCode,'') prmPkgCode, 
           nvl(s.prmPkgName,d.prmPkgName,'') prmPkgName, 
           nvl(s.brndSCName,d.brndSCName,'') brndSCName, 
           nvl(d.svcClassCodeOld,'') mgrSvcClassId,
           case when (a.OfferAttributeAreaname='') then ''          
               else nvl(d.offerAttrName,'AREANAME')   end offerAttrName, 
           nvl(d.offerAttrValue,a.OfferAttributeAreaname,'') offerAttrValue, 
           nvl(d.areaName,'') areaName,
           nvl(e.microclusterName,'') microclusterName
           from cs5Lookup2 a 
           left join refCallClass b on a.CallClassCat = b.callClassId 
           left join refRegionBranch c on a.DestinationCityName = c.city
           left join refServiceClass s
               on a.ServiceClass=s.svcClassCode  
               and a.TransactionDate>=s.svcClassEffDt and a.TransactionDate<s.svcClassEndDt and a.TransactionDate>=s.brndSCEffDt and a.TransactionDate<s.brndSCEndDt
           left join refServiceClassOfr d 
               on a.ServiceClass=d.svcClassCode  
               and a.OfferAttributeAreaname=d.offerAttrValue
               and a.TransactionDate>=d.effDt  and a.TransactionDate<d.endDt
           left join refDistinctMicrocluster e on a.MicroclusterId=e.microclusterId
           left join refMicrocluster f on a.CGI=f.cgi and a.MicroclusterId=f.microclusterId
        """
        )
    cs5Lookup3DF.registerTempTable("cs5Lookup3")
    //cs5Lookup3DF.persist(storageLevel)
    //cs5Lookup3DF.show(80)
    
    /*
     * Reference lookup 3:
     * 1. Join LAC CI hierarchy
     * 
     * Transformation:
     * 1. Define revenue code base (without Account ID [MA/DA] )
     * 2. Create row of account for transpose : MA~MAvalue^DA1~DA1value^DA2~DA2value ...
     * 3. For REPROCESS, no need to add MA to the account since it has been transposed before
     */
    sqlContext.udf.register("getTotalDAFromDedicatedAccountValuesRow", CS5Select.getTotalDAFromDedicatedAccountValuesRow _)
    sqlContext.udf.register("getAccountRowForTranspose", CS5Select.getAccountRowForTranspose _)
    sqlContext.udf.register("getAccountRowNoTranspose", CS5Select.getAccountRowNoTranspose _)
    val cs5Lookup6DF = {
      if (reprocessFlag!="REPCS")
          sqlContext.sql(
            """select a.*, nvl(b.clusterName,'') laciCluster, nvl(b.salesArea,'') laciSalesArea, nvl(b.area,'') laciArea, nvl(b.microCluster,'') laciMicroCluster, nvl(b.siteTech,'') siteTech, nvl(b.ratType,'') laciRatType,
              case when a.callClassRuleId = "1" then concat(a.callClassBase,a.TrafficCase,a.FAFName,a.callClassTariff)
              when a.callClassRuleId = "2" then concat(a.callClassBase)
              when a.callClassRuleId = "3" then concat(a.callClassBase,callClassTariff)
              when a.ServiceUsageType = "VAS" then concat(a.VasServiceId)
              else callClassBase
              end revenueCodeBase,
              getAccountRowForTranspose( DedicatedAccountValuesRow, AccountValueDeducted ) AccountRow 
              from cs5Lookup3 a left join refLacCiHierarchy b on a.LAC=b.lacDec and a.CI=b.ciDec"""
            )
       else 
         sqlContext.sql(
            """select a.*, nvl(b.clusterName,'') laciCluster, nvl(b.salesArea,'') laciSalesArea, nvl(b.area,'') laciArea, nvl(b.microCluster,'') laciMicroCluster, nvl(b.siteTech,'') siteTech, nvl(b.ratType,'') laciRatType,
              case when a.callClassRuleId = "1" then concat(a.callClassBase,a.TrafficCase,a.FAFName,a.callClassTariff)
              when a.callClassRuleId = "2" then concat(a.callClassBase)
              when a.callClassRuleId = "3" then concat(a.callClassBase,callClassTariff)
              when a.ServiceUsageType = "VAS" then concat(a.VasServiceId)
              else callClassBase
              end revenueCodeBase,
              getAccountRowNoTranspose( DedicatedAccountValuesRow, AccountValueDeducted ) AccountRow
              from cs5Lookup3 a left join refLacCiHierarchy b on a.LAC=b.lacDec and a.CI=b.ciDec"""
            )
    }    
    cs5Lookup6DF.persist(storageLevel)
    cs5Lookup6DF.registerTempTable("cs5Lookup6")
    //cs5Lookup6DF.show()
    
    
    /*****************************************************************************************************
     * 
     * CDR INQUIRY OUTPUT
     * 
     *****************************************************************************************************/
    
    val cs5CDRInquiry = CS5CDRInquiry.genPrepaidOutput(sqlContext)
    
    /*****************************************************************************************************
     * 
     * OTHER OUTPUT
     * 
     *****************************************************************************************************/
    
    val cs5AddonIPCNDF = CS5FilteredOutput.genAddonOutput(sqlContext)
    
    val cs5MCNoCatSMSDF = CS5FilteredOutput.genMCNoCategorySMS(sqlContext)
    
    val cs5MCNoCatVoiceDF = CS5FilteredOutput.genMCNoCategoryVoice(sqlContext)
    
     /*****************************************************************************************************
     * 
     * TRANSFORMATION PART 2
     * 
     *****************************************************************************************************/    
    
    /*
     * Convert back to RDD
     * Separating the records as (key, value) for transpose
     */
    val formatRDD = cs5Lookup6DF.rdd.map( r => 
                           ( r.getString(0)
        .concat("|").concat( r.getString(1) )
        .concat("|").concat( r.getString(2) )
        .concat("|").concat( r.getString(3) )
        .concat("|").concat( r.getString(4) )
        .concat("|").concat( r.getString(5) )
        .concat("|").concat( r.getString(6) ) 
        .concat("|").concat( r.getString(7) )
        .concat("|").concat( r.getString(8) )
        .concat("|").concat( r.getString(9) )
        .concat("|").concat( r.getString(10) )
        .concat("|").concat( r.getString(11) )
        .concat("|").concat( r.getString(12) )
        .concat("|").concat( r.getString(13) )
        .concat("|").concat( r.getString(14) )
        .concat("|").concat( r.getString(15) )
        .concat("|").concat( r.getString(16) )
        .concat("|").concat( r.getString(17) )
        .concat("|").concat( r.getString(18) )
        .concat("|").concat( r.getString(19) )
        .concat("|").concat( r.getString(20) )
        .concat("|").concat( r.getString(21) )
        .concat("|").concat( r.getString(22) )
        .concat("|").concat( r.getString(23) )
        .concat("|").concat( r.getString(24) )
        .concat("|").concat( r.getString(25) )
        .concat("|").concat( r.getString(26) )
        .concat("|").concat( r.getString(27) )
        .concat("|").concat( r.getString(28) )
        .concat("|").concat( r.getString(29) )
        .concat("|").concat( r.getString(30) )
        .concat("|").concat( r.getString(31) )
        .concat("|").concat( r.getString(32) )
        .concat("|").concat( r.getString(33) )
        .concat("|").concat( r.getString(34) )
        .concat("|").concat( r.getString(35) )
        .concat("|").concat( r.getString(36) )
        .concat("|").concat( r.getString(37) )
        .concat("|").concat( r.getString(38) )
        .concat("|").concat( r.getString(39) )
        .concat("|").concat( r.getDouble(40).toString() )
        .concat("|").concat( r.getInt(41).toString() )
        .concat("|").concat( r.getDouble(42).toString() )
        .concat("|").concat( r.getDouble(43).toString() )
        .concat("|").concat( r.getString(44) )
        .concat("|").concat( r.getString(45) )
        .concat("|").concat( r.getString(46) )
        .concat("|").concat( r.getString(47) )
        .concat("|").concat( r.getString(48) )
        .concat("|").concat( r.getString(49) )
        .concat("|").concat( r.getString(50) )
        .concat("|").concat( r.getString(51) )
        .concat("|").concat( r.getString(52) )
        .concat("|").concat( r.getString(53) )
        .concat("|").concat( r.getString(54) )
        .concat("|").concat( r.getString(55) )
        .concat("|").concat( r.getString(56) )
        .concat("|").concat( r.getString(57) )
        .concat("|").concat( r.getString(58) )
        .concat("|").concat( r.getString(59) )
        .concat("|").concat( r.getString(60) )
        .concat("|").concat( r.getString(61) )
        .concat("|").concat( r.getString(62) )
        .concat("|").concat( r.getString(63) )
        .concat("|").concat( r.getString(64) )
        .concat("|").concat( r.getString(65) )
        .concat("|").concat( r.getString(66) )
        .concat("|").concat( r.getString(67) )
        .concat("|").concat( r.getString(68) )
        .concat("|").concat( r.getString(69) )
        .concat("|").concat( r.getString(70) )
        .concat("|").concat( r.getString(71) )
        .concat("|").concat( r.getString(72) )
        .concat("|").concat( r.getString(73) )
        .concat("|").concat( r.getString(74) )
        .concat("|").concat( r.getString(75) )
        .concat("|").concat( r.getString(76) )
        .concat("|").concat( r.getString(77) )
        .concat("|").concat( r.getString(78) )
        .concat("|").concat( r.getString(79) )
        .concat("|").concat( r.getString(80) )
        .concat("|").concat( r.getString(81) )
        .concat("|").concat( r.getString(82) )
        .concat("|").concat( r.getString(83) )
        .concat("|").concat( r.getString(84) )
        .concat("|").concat( r.getString(85) )
        .concat("|").concat( r.getString(86) )
        .concat("|").concat( r.getString(87) )
        .concat("|").concat( r.getString(88) )
        .concat("|").concat( r.getString(89) )
        .concat("|").concat( r.getString(90) )
        .concat("|").concat( r.getString(91) )
        .concat("|").concat( r.getString(92) )
        .concat("|").concat( r.getString(93) )
        .concat("|").concat( r.getString(94) )
        .concat("|").concat( r.getString(95) )
        .concat("|").concat( r.getString(96) )
        .concat("|").concat( r.getString(97) )
        .concat("|").concat( r.getString(98) )
        .concat("|").concat( r.getString(99) )
        .concat("|").concat( r.getString(100) )
        .concat("|").concat( r.getString(101) )
        .concat("|").concat( r.getString(102) )
        .concat("|").concat( r.getString(103) )
        .concat("|").concat( r.getString(104) ) ,
         r.getString(105) )
      )
    //formatRDD.collect foreach {case (a) => println (a)}
    
     /* 
     * Transpose DA column into records 
     * Sample input : a|b|c|RevCodeBase and DA1~moneyamt~unitamt~uom^DA2~moneyamt~unitamt~uom
     * Sample output : a|b|c|RevCodeBase|moneyamt|unitamt|uom|DA1 & a|b|c|RevCodeBase|moneyamt|unitamt|uom|DA2
     */
    //--20161001--formatRDD.cache()
    val cs5TransposeRDD = formatRDD.flatMapValues(word => word.split("\\^")).map{case (k,v) => k.concat("|").concat( Common.getTuple(v, "~", 2)+"|"+Common.getTuple(v, "~", 3)+"|"+Common.getTuple(v, "~", 4)+"|"+Common.getTuple(v, "~", 0)+"|"+Common.getTuple(v, "~", 1) ) };
    //cs5TransposeRDD.collect foreach {case (a) => println (a)}
    
    
     /*
     * Create new dataframe from CS5 transpose result
     */    
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
    val cs5TransposeDF = sqlContext.createDataFrame(cs5TransposeRow, CS5Schema.CS5Transpose)
    cs5TransposeDF.persist(storageLevel)
    cs5TransposeDF.registerTempTable("cs5Transpose")
    //cs5TransposeDF.show()
    
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
    val cs5LookupMADADF = sqlContext.sql(
        """select a.*,
           genRevenueCode(a.revenueCodeBase,a.Account,a.ServiceUsageType) revenueCode,
           nvl(b.lv1,'') revCodeLv1, nvl(b.lv2,'') revCodeLv2, nvl(b.lv3,'') revCodeLv3, nvl(b.lv4,'') revCodeLv4,
           nvl(b.lv5,'') revCodeLv5, nvl(b.lv6,'') revCodeLv6, nvl(b.lv7,'') revCodeLv7, nvl(b.lv8,'') revCodeLv8,
           nvl(b.lv9,'') revCodeLv9, nvl(b.svcUsgTp,'') revCodeUsgTp, nvl(b.dirTp,'') revCodeDirTp,
           nvl(b.distTp,'') revCodeDistTp,
           nvl(c.acc, '') MADAAcc,
           genSvcUsgTypeFromRevCode( b.svcUsgTp , b.lv9, b.lv5 ) finalServiceUsageType,
           genSvcUsgDirFromRevCode( b.svcUsgTp, b.lv3 ) SvcUsgDirection,
           genSvcUsgDestFromRevCode( b.svcUsgTp, b.lv2 ) SvcUsgDestination,
           case when b.svcUsgTp='VOICE' then nvl(c.voiceTrf, '')
             when b.svcUsgTp='SMS' then nvl(c.smsTrf, '') 
             when b.svcUsgTp='DATA' then nvl(c.dataVolTrf, '')
           else ''
           end TrafficFlag,
           case when b.svcUsgTp='VOICE' then nvl(c.voiceRev, '')
             when b.svcUsgTp='SMS' then nvl(c.smsRev, '') 
             when b.svcUsgTp='DATA' then nvl(c.dataVolRev, '')
             when b.svcUsgTp='VAS' then nvl(c.vasRev, '')
             when b.svcUsgTp='OTHER' then nvl(c.othRev, '')
             when b.svcUsgTp='DISCOUNT' then nvl(c.discRev, '')
             when b.svcUsgTp='DROP' then 'No'
           else 'UNKNOWN'
           end RevenueFlag
           from cs5Transpose a            
           left join refRevCode b on genRevenueCode(a.revenueCodeBase,a.Account,a.ServiceUsageType)=b.revCode
           left join refMADA c on a.prmPkgCode=c.grpCd and genAccountForMADA(a.Account)=c.acc and a.TransactionDate>=c.effDt and a.TransactionDate<c.endDt"""
        )
    cs5LookupMADADF.persist(storageLevel)
    cs5LookupMADADF.registerTempTable("cs5LookupMADA")    
    //cs5LookupMADADF.show()
    
    
    
     /*****************************************************************************************************
     * 
     * CS5 TRANSFORMATION OUTPUT
     * 
     *****************************************************************************************************/   
    val cs5Output = sqlContext.sql("""
        select
        TransactionDate TRANSACTION_DATE,
        TransactionHour TRANSACTION_HOUR,
        case when ServiceScenario = '2' then BPartyNumber else APartyNumber end A_PARTY_NUMBER,
        case when ServiceScenario = '2' then BPrefix else APrefix end A_PREFIX,
        case when ServiceScenario = '2' then DestinationCityName else ServiceCityName end SERVICE_CITY_NAME,
        case when ServiceScenario = '2' then DestinationProviderId else ServiceProviderId end SERVICE_PROVIDER_ID,
        case when ServiceScenario = '2' then DestinationCountryName else ServiceCountryName end SERVICE_COUNTRY_NM,
        hlrBranchNm HLR_BRANCH_NM,
        hlrRegionNm HLR_REGION_NM,
        case when ServiceScenario = '2' then APartyNumber else BPartyNumber end B_PARTY_NUMBER,
        case when ServiceScenario = '2' then APrefix else BPrefix end B_PREFIX,
        case when ServiceScenario = '2' then ServiceCityName else DestinationCityName end DESTINATION_CITY_NAME,
        case when ServiceScenario = '2' then ServiceProviderId else DestinationProviderId end DESTINATION_PROVIDER_ID,
        case when ServiceScenario = '2' then ServiceCountryName else DestinationCountryName end DESTINATION_COUNTRY_NM,
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
        LAC,
        CI,
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
        case when TransposeId='1' then UsageVolume 
          else 0 
        end USAGE_VOLUME,
        UsageAmount USAGE_AMOUNT,
        case when TransposeId='1' then UsageDuration
          else 0
        end USAGE_DURATION,
         case when TransposeId='1' then Hit 
          else 0
        end HIT,
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
        RsvFld2 CUSTOMER_SEGMENT
        from cs5LookupMADA
        where 
        prmPkgCode <> ""
        and revCodeUsgTp <> ""
        and MADAAcc <> ""
        """)
     cs5Output.registerTempTable("cs5Output") 
        

     /*****************************************************************************************************
     * 
     * REJECT REFERENCE
     * 
     *****************************************************************************************************/   
    sqlContext.udf.register("getRejRsnPrepaid", CS5Select.getRejRsnPrepaid _)
        
        val cs5OutputRejRef = sqlContext.sql("""
        select a.*,
        getRejRsnPrepaid(prmPkgCode, callClassBase, revCodeUsgTp, MADAAcc) RejectReason
        from cs5LookupMADA a
        where 
        prmPkgCode = ""
        or revCodeUsgTp = ""
        or MADAAcc = ""
        """)
    
     /*****************************************************************************************************
     * 
     * AGGREGATION
     * 
     *****************************************************************************************************/
     
      val cs5HourlyAggOutput = CS5Aggregate.genHourlySummary(sqlContext)
      cs5HourlyAggOutput.registerTempTable("cs5HourlyAggOutput") 
      
      val cs5DailyAggOutput = CS5Aggregate.genDailySummary(sqlContext)
      cs5DailyAggOutput.registerTempTable("cs5DailyAggOutput") 
      
      val cs5GreenReportAggOutput = CS5Aggregate.genGreenReportSummary(sqlContext)
      cs5GreenReportAggOutput.registerTempTable("cs5GreenReportAggOutput") 
      
      val cs5TdwSmyAggOutput = CS5Aggregate.genPrepaidTdwSummary(sqlContext)
     
     /*****************************************************************************************************
     * 
     * WRITE OUTPUT
     * 
     *****************************************************************************************************/
     

      if ( reprocessFlag!= "REPCS" ) {
         /*
          * Output reject BP
          */
// TEMPORARILY COMMENTED
//         Common.cleanDirectory(sc, pathRejBP)
//         rejBpRDD.saveAsTextFile(pathRejBP);   
         
         /*
          * Output CDR Inquiry
          */
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         cs5CDRInquiry.repartition(140).write.format("com.databricks.spark.csv")
           .option("delimiter", "|")
           .save(pathCS5CDRInqOutput)  
           
           /*
           * Output Addon IPCN
           */
          Common.cleanDirectoryWithPattern(sc, pathAddonIPCNOutput, "/*/JOB_ID=" + jobID)
          cs5AddonIPCNDF.repartition(1).write.partitionBy("TRANSACTION_DATE","JOB_ID")
          .mode("append")
          .save(pathAddonIPCNOutput) 
          
          /*
           * Output Microcluster No Category SMS
           */
          Common.cleanDirectoryWithPattern(sc, pathMCNoCatSMSOutput, "/*/JOB_ID=" + jobID)
          cs5MCNoCatSMSDF.repartition(1).write.partitionBy("TRANSACTION_DATE","JOB_ID")
          .mode("append")
          .save(pathMCNoCatSMSOutput) 
          
          /*
           * Output Microcluster No Category Voice
           */
          Common.cleanDirectoryWithPattern(sc, pathMCNoCatVoiceOutput, "/*/JOB_ID=" + jobID)
          cs5MCNoCatVoiceDF.repartition(1).write.partitionBy("TRANSACTION_DATE","JOB_ID")
          .mode("append")
          .save(pathMCNoCatVoiceOutput) 
      } 
      else {
          /*
           * Output Reject Permanent
           */
          Common.cleanDirectory(sc, pathRejRefPerm)
          if ( rejPermRDD.count() > 0 ) {
             rejPermRDD.repartition(10).saveAsTextFile(pathRejRefPerm);
          }
      }
    
      /*
       * Output Transformation  
       */
      Common.cleanDirectoryWithPattern(sc, pathCS5TransformOutput, "/*/JOB_ID=" + jobID)
      cs5Output.repartition(40).write.partitionBy("TRANSACTION_DATE","JOB_ID")
        .mode("append")
        .save(pathCS5TransformOutput) 
      
      /*
       * Output Reject Reference
       */
      Common.cleanDirectory(sc, pathRejRef)
      cs5OutputRejRef.repartition(5)
        .write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(pathRejRef) 
                
      /*
       * Output Aggregation Hourly
       */
      Common.cleanDirectoryWithPattern(sc, pathAggHourlyOutput, "/*/job_id=" + jobID)
      cs5HourlyAggOutput.repartition(30).write.partitionBy("transaction_date","job_id","src_tp")
        .mode("append")
        .save(pathAggHourlyOutput)
     
      /*
       * Output Aggregation Daily
       */
      Common.cleanDirectoryWithPattern(sc, pathAggDailyOutput, "/*/job_id=" + jobID)
      cs5DailyAggOutput.repartition(30).write.partitionBy("transaction_date","job_id","src_tp")
        .mode("append")
        .save(pathAggDailyOutput)
        
      /*
       * Output Aggregation Green Report
       */
      Common.cleanDirectory(sc, pathAggGreenReportOutput +"/process_id=" + prcDt + "_" + jobID + "_CS5")
      cs5GreenReportAggOutput.repartition(30).write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save( pathAggGreenReportOutput +"/process_id=" + prcDt + "_" + jobID + "_CS5")
        
      /*
       * Output Aggregation TDW Summary
       */
      Common.cleanDirectory(sc, pathTdwSmyOutput)
      cs5TdwSmyAggOutput.repartition(140).write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save( pathTdwSmyOutput )
        
        
        
        
        
    sc.stop();
    
  }
  

  
  
}