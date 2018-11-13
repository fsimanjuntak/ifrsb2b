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
object CS5PostpaidTransform {
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
    
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    
    /*
     * Assign child dir
     */
    val childDir = "/" + prcDt + "_" + jobID
    
    /*
     * Target directory assignments
     */
    val pathCS5TransformOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_POSTPAID_TRANSFORM_DIR")
    val pathCS5OrangeOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_ORANGE_REPORT_POSTPAID_DIR") + childDir
    val pathRejBP = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_POSTPAID_REJ_BP_DIR") + childDir
    val pathRejRef = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_POSTPAID_REJ_REF_DIR") + childDir
    val pathCS5CDRInqOutput = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_POSTPAID_CDR_INQ_DIR") + childDir
    val pathAggDailyOutput = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_DAILY_POSTPAID.OUTPUT_SPARK_AGG_DAILY_POSTPAID")
    val pathCCNPostpaidAddon = Common.getConfigDirectory(sc, configDir, "USAGE.CS5_POSTPAID_TRANSFORM.OUTPUT_CS5_POSTPAID_ADDON")
    
    //val path_ref = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/Reference";
    val path_ref = "/user/apps/CS5/reference";
    
    
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
          val cs5InputFilteredRDD =  
            cs5InputRDD
            .filter(line => CS5Select.cs5ConvertedRowIsValid(line) )
            .filter(line => CS5Select.getPaymentCategory(line)=="POSTPAID" )
         
          //2. Transform the rest
          cs5InputFilteredRDD
          //.map(line => CS5Select.genCS5PostTransformedRow( Common.getTuple(line, "\t", 1).concat("|").concat(Common.getTuple(line, "\t", 2)).concat("|").concat(Common.getTuple(line, "\t", 3)), prcDt, jobID ) )
          .flatMap(line => CS5Select.genCS5TransformedRowList(  Common.getTuple(line, "\t", 1).concat("|").concat(Common.getTuple(line, "\t", 2)).concat("|").concat(Common.getTuple(line, "\t", 3))  , "POSTPAID" , prcDt, jobID  ) )
      } else { 
          //GET THE REJECTED DATA
          cs5InputRDD.map(line => CS5Select.genCS5RejectedRowPostpaid(line, prcDt, jobID ))
      }
    }
    
    /*
     * Reject BP 
     * Only for main process
     */
    val rejBpRDD = {
       if ( reprocessFlag!= "REPCS" ) cs5InputRDD.filter(line => !CS5Select.cs5ConvertedRowIsValid(line) )
       else null
    }
   
    
    /*
     * DEBUG: print
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
     * Reference Service Class Postpaid
     */
//    val refPostServiceClass = sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.postServiceClass)
//    .load(path_ref+"/REF_POSTPAID_SERVICE_CLASS_20160622.txt") 
    val refPostServiceClass = ReferenceDF.getServiceClassPostpaidDF(sqlContext)
    refPostServiceClass.registerTempTable("refPostServiceClass")
    refPostServiceClass.persist(storageLevel)
    refPostServiceClass.count()
    //refPostServiceClass.show()
    
    
    /*
     * Reference Interconnect - NOT USED
     */
//    val refIntcct = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter",",")
//    .schema(ReferenceSchema.intcctNtwkShortCodeSchema)
//    .load(path_ref+"/ref_inttct_network_and_shortcode.csv") )
//    refIntcct.registerTempTable("refIntcct")
//    refIntcct.persist(storageLevel) //TEMP
//    refIntcct.count()
    //refIntcct.show()
    
    /*
     * Reference Interconnect 2
     */  
//     val intcctDF =  broadcast(sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.intcctAllSchema)
//    .load(path_ref+"/ref_intcct_length_all.txt"))
    val intcctDF = ReferenceDF.getIntcctAllDF(sqlContext)
    
    /*
     * Reference Costband
     */
//    val refCostband = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.postCostband)
//    .load(path_ref+"/REF_POSTPAID_COSTBAND_20160107.txt") )
    val refCostband = ReferenceDF.getPostpaidCostbandDF(sqlContext)
    refCostband.registerTempTable("refCostband")
    refCostband.persist(storageLevel) 
    refCostband.count()
    //refCostband.show()
        
    /*
     * Reference Location
     */
//    val refLocation = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.postLocation)
//    .load(path_ref+"/REF_POSTPAID_LOCATION_20160107.txt") )
    val refLocation = ReferenceDF.getPostpaidLocationDF(sqlContext)
    refLocation.registerTempTable("refLocation")
    refLocation.persist(storageLevel) 
    refLocation.count()
    //refLocation.show()
    
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
    refRegionBranch.persist(storageLevel) 
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
    refLacCiHierarchy.persist(storageLevel) 
    refLacCiHierarchy.count()
    //refLacCiHierarchy.show()
    
    /*
     * Reference postpaid event type revenue
     */
//    val refPostEventTypeDF = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.postEventTypeRevenue)
//    .load(path_ref+"/REF_POSTPAID_EVENTTYPE_REVENUE_20160107.txt") )
    val refPostEventTypeDF = ReferenceDF.getPostpaidEventtypeDF(sqlContext)
    refPostEventTypeDF.registerTempTable("refPostEventType")
    refPostEventTypeDF.persist(storageLevel) 
    refPostEventTypeDF.count()
    //refPostEventTypeDF.show()
    
    /*
     * Reference postpaid revcode to GL
     */
//    val refRevToGLDF = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.postRevCodeToGLSchema)
//    .load(path_ref+"/REF_POSTPAID_REVENUE_TO_GL_20160107.txt") )
    val refRevToGLDF = ReferenceDF.getPostpaidRevToGLDF(sqlContext)
    refRevToGLDF.registerTempTable("refRevToGL")
    refRevToGLDF.persist(storageLevel) 
    refRevToGLDF.count()
    //refRevToGLDF.show()
    
    /*
     * Reference postpaid GL Service Type
     */
//    val refGLSvcTypeDF = sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.postGLtoServType)
//    .load(path_ref+"/REF_POSTPAID_GL_TO_SERVICETYPE_20160107_DIST.txt") 
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
//    val refRevCodeDF = sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.revcodeSchema)
//    .load(path_ref+"/REF_REVENUE_CODE_20160430.txt")
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
     * Reference MCC MNC
     */
    val refMCCMNCDF = ReferenceDF.getMCCMNCDF(sqlContext)
    refMCCMNCDF.registerTempTable("refMCCMNC")
    refMCCMNCDF.persist() 
    refMCCMNCDF.count()
   
    /*
     * Other references (non-DataFrame)
     */
    //val postRef = new CS5Reference.postEventType("/user/apps/reference/ref_postpaid_eventtype_revenue/REF_POSTPAID_EVENTTYPE_REVENUE_*" , sc)
    val postRef = ReferenceDF.getPostEventTypeRef(sc)
    val intcctRef = ReferenceDF.getIntcctAllRef(sc)
    val postCorpUsageTypeRef = ReferenceDF.getPostCorpProductID(sc)
    
    /*****************************************************************************************************
     * 
     * Join references using DataFrames
     * 
     * 
     *****************************************************************************************************/
        
    /*
     * Create new dataframe from CS5 Transform 1 result
     */    
    val cs5Transform1DF = sqlContext.createDataFrame(cs5Transform1RDD, CS5Schema.CS5PostSchema)
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
//    val joinAintcctDF = Common.getIntcct(sqlContext, cs5Transform1DF, intcctDF, "MSISDN", "TransactionDate", "APrefix", "ServiceCountryName", "ServiceCityName", "ServiceProviderId")
//    val joinBintcctDF = Common.getIntcctWithSID(sqlContext, joinAintcctDF, intcctDF, "OtherPartyNum", "TransactionDate", "VasServiceId", "BPrefix", "DestinationCountryName", "DestinationCityName", "DestinationProviderId")
    
    /*
     * New Interconnect (Loop)
     */
    val joinAintcctDF = Common.getIntcctWithSID2(sqlContext, cs5Transform1DF, intcctRef, "MSISDN", "TransactionDate", "VasServiceId", "APrefix", "ServiceCountryName", "ServiceCityName", "ServiceProviderId")
    val joinBintcctDF = Common.getIntcctWithSID2(sqlContext, joinAintcctDF, intcctRef, "OtherPartyNum", "TransactionDate", "VasServiceId", "BPrefix", "DestinationCountryName", "DestinationCityName", "DestinationProviderId")
    
    joinBintcctDF.registerTempTable("cs5Lookup2")
 
       
    /*
     * Reference lookup 2: 
     * 1. Branch/region
     * 2. Service Class
     * 3. Costband
     * 4. Location
     */
    val cs5Lookup3DF = sqlContext.sql(
        """select a.*,
           nvl(b.branch,'') hlrBranchNm, nvl(b.region,'') hlrRegionNm,
           nvl(c.postSCName,'') postSCName, 
           nvl(d.CostbandName,'') CostbandName,
           nvl(e.LocationName,'') LocationName
           from cs5Lookup2 a 
           left join refRegionBranch b on a.ServiceCityName = b.city
           left join refPostServiceClass c on a.ServiceClass = c.postSCCode
           left join refCostband d on a.Costband = d.CostbandId
           left join refLocation e on a.Location = e.LocationId
        """
        )
    cs5Lookup3DF.registerTempTable("cs5Lookup3")
    //cs5Lookup3DF.show()
    
        
    /*
     * Register UDFs
     */
    sqlContext.udf.register("getPostAxis1", CS5Select.getPostAxis1 _)
    sqlContext.udf.register("getPostAxis2", CS5Select.getPostAxis2 _)
    sqlContext.udf.register("getPostAxis3", CS5Select.getPostAxis3 _)
    sqlContext.udf.register("getPostAxis4", CS5Select.getPostAxis4 _)
    sqlContext.udf.register("getPostAxis5", CS5Select.getPostAxis5 _)
    
    sqlContext.udf.register("getEventTypeRevCode", postRef.getEventTypeRevCode _)
    
    /*
     * Reference lookup 3: 
     * 1. Lac CI Hierarchy (by join)
     * 2. Event type Revenue Code (by function)
     * 
     * Transformation:
     * 1. Determine value per Axis
     */
    val cs5Lookup4DF = sqlContext.sql(
        """select a.*,
           nvl(b.clusterName,'') laciCluster, nvl(b.salesArea,'') laciSalesArea, nvl(b.area,'') laciArea, nvl(b.microCluster,'') laciMicroCluster, nvl(b.siteTech,'') siteTech, nvl(b.ratType,'') laciRatType,
           getPostAxis1(EventId, OtherPartyNum, CalltypeName, CostbandName, LocationName, RatTypeName, ServiceClass) PAxis1,
           getPostAxis2(EventId, OtherPartyNum, CalltypeName, CostbandName, LocationName, RatTypeName, ServiceClass) PAxis2,
           getPostAxis3(EventId, OtherPartyNum, CalltypeName, CostbandName, LocationName, RatTypeName, ServiceClass) PAxis3,
           getPostAxis4(EventId, OtherPartyNum, CalltypeName, CostbandName, LocationName, RatTypeName, ServiceClass) PAxis4,
           getPostAxis5(EventId, OtherPartyNum, CalltypeName, CostbandName, LocationName, RatTypeName, ServiceClass) PAxis5,
           case when ( ServiceUsageType='VAS') then VasServiceId
           else getEventTypeRevCode(EventId, OtherPartyNum, CalltypeName, CostbandName, LocationName, RatTypeName, ServiceClass) 
           end RevenueCode
           from cs5Lookup3 a 
           left join refLacCiHierarchy b on a.LAC=b.lacDec and a.CI=b.ciDec
        """
        )
    cs5Lookup4DF.registerTempTable("cs5Lookup4")
    //cs5Lookup4DF.show()
    
    /*
     * Reference lookup 4: 
     * 1. Service Type based on GL
     * 
     */
    sqlContext.udf.register("getUsageTypeCorporate", postCorpUsageTypeRef.getUsageTypePostpaid _)
    val cs5Lookup5DF = sqlContext.sql(
         """select a.*,
           nvl(b.Level4, '') GLCode,
           nvl(b.Level5, '') GLName, nvl(b.SvcType, '') ServiceType, nvl(b.SvcTypeDA1, '') ServiceTypeDA1, nvl(b.SvcTypeNDA1, '') ServiceTypeNDA1,
           nvl(getUsageTypeCorporate(TreeDefinedFields, DedicatedAccountValuesRow), '') CorpUsgType,
           DedicatedAccountValuesRow AccountRow 
           from cs5Lookup4 a 
           left join refGLSvcTypeComplete b on upper(a.RevenueCode)=upper(b.RevenueId)
        """
        )        
    cs5Lookup5DF.persist(storageLevel)
    cs5Lookup5DF.registerTempTable("CCNCS5PostpaidAddon")
    //cs5Lookup5DF.show()
    
    
    val CCSNCS5PostPaidAddonDF = CS5FilteredOutput.genCCNCS5PostpaidAddonOutput(sqlContext)
         
    cs5Lookup5DF.drop("SSPTransId").drop("APartyNumber").drop("NormCCRTriggerTime").registerTempTable("cs5Lookup5")
    
    
    /*****************************************************************************************************
     * 
     * CDR INQUIRY OUTPUT
     * 
     *****************************************************************************************************/
    
    val cs5CDRInquiry = CS5CDRInquiry.genPostpaidOutput(sqlContext, postCorpUsageTypeRef)
    
    
     /*****************************************************************************************************
     * 
     * TRANSFORMATION PART 2
     * 
     *****************************************************************************************************/
    
    /*
     * Convert back to RDD
     * Separating the records as (key, value) for transpose
     * Only records NOT REJECT REFERENCE
     */
    val formatRDD = cs5Lookup5DF
        .filter("RevenueCode <> ''  and GLCode <> ''")
        .rdd.map( r => 
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
         .concat("|").concat( r.getDouble(10).toString() )
        .concat("|").concat( r.getString(11) )
        .concat("|").concat( r.getString(12) )
        .concat("|").concat( r.getString(13) )
        .concat("|").concat( r.getString(14) )
        .concat("|").concat( r.getString(15) )
        .concat("|").concat( r.getString(16) )
        .concat("|").concat( r.getString(17) )
        .concat("|").concat( r.getString(18) )
        .concat("|").concat( r.getDouble(19).toString() )
        .concat("|").concat( r.getDouble(20).toString() )
        .concat("|").concat( r.getDouble(21).toString() )
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
        .concat("|").concat( r.getString(40) )
        .concat("|").concat( r.getString(41) )
        .concat("|").concat( r.getString(42) )
        .concat("|").concat( r.getString(43) )
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
        .concat("|").concat( r.getString(104) )
        .concat("|").concat( r.getString(105) )
        .concat("|").concat( r.getString(106) )
        .concat("|").concat( r.getString(107) )
        .concat("|").concat( r.getString(108) )
        .concat("|").concat( r.getString(109) ),
         r.getString(110) )
      )
    //formatRDD.collect foreach {case (a) => println (a)}
    
     /* 
     * Transpose DA column into records 
     * Sample input : a|b|c|RevCodeBase[DA1~moneyamt~unitamt~uom^DA2~moneyamt~unitamt~uom
     * Sample output : a|b|c|RevCodeBase|DA1|moneyamt|unitamt|uom & a|b|c|RevCodeBase|DA2|moneyamt|unitamt|uom
     */
    val cs5TransposeRDD = formatRDD.flatMapValues(word => word.split("\\^")).map{case (k,v) => k.concat("|").concat( Common.getTuple(v, "~", 1)+"|"+Common.getTuple(v, "~", 2)+"|"+Common.getTuple(v, "~", 3)+"|"+Common.getTuple(v, "~", 0) ) };
    //cs5TransposeRDD.collect foreach {case (a) => println (a)}
    
     /*
     * Create new dataframe from CS5 transpose result
     */    
    val cs5TransposeRow = cs5TransposeRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
        p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10).toDouble, p(11), p(12),p(13), p(14), p(15), p(16),
        p(17), p(18), p(19).toDouble, p(20).toDouble, p(21).toDouble, p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29),
        p(30), p(31), p(32), p(33), p(34), p(35), p(36), p(37), p(38), p(39), p(40), p(41), p(42), 
        p(43), p(44), p(45), p(46), p(47), p(48), p(49), p(50), p(51), p(52), p(53), p(54), p(55), 
        p(56), p(57), p(58), p(59), p(60), p(61), p(62), p(63), p(64), p(65), p(66), p(67), p(68),
        p(69), p(70), p(71), p(72), p(73), p(74), p(75), p(76), p(77), p(78), p(79), p(80), p(81),
        p(82), p(83), p(84), p(85), p(86), p(87), p(88), p(89), p(90), p(91), p(92), p(93), p(94),
        p(95), p(96), p(97), p(98), p(99), p(100), p(101), p(102), p(103), p(104), p(105), p(106), p(107), p(108), p(109),
        Common.toDouble(p(110)), p(111), p(112), p(113)
        ))
    val cs5TransposeDF = sqlContext.createDataFrame(cs5TransposeRow, CS5Schema.CS5PostTranspose)
    cs5TransposeDF.persist(storageLevel)
    cs5TransposeDF.registerTempTable("cs5Transpose")
    //cs5TransposeDF.show()
    
    
    
     /*****************************************************************************************************
     * 
     * CS5 TRANSFORMATION OUTPUT
     * 
     *****************************************************************************************************/
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
        LAC,
        CI,
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
        end USAGE_TYPE
        from cs5Transpose
        where 
        RevenueCode <> "" 
        and GLCode <> ""
        """)
        //cs5PostOutput.show()
        cs5PostOutput.persist(storageLevel)
        cs5PostOutput.registerTempTable("cs5PostOutput")
        
     /*****************************************************************************************************
     * 
     * CS5 TRANSFORMATION ORANGE REPORT OUTPUT
     * 
     *****************************************************************************************************/
    
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
          USAGE_TYPE
          from cs5PostOutput
        """)
        

     /*****************************************************************************************************
     * 
     * REJECT REFERENCE
     * 
     *****************************************************************************************************/   
    val cs5OutputRejRef = sqlContext.sql("""
        select a.*,
        case 
        when ( GLCode = "" and RevenueCode = "" ) then 'Revenue Code and GL Code not found'
        when RevenueCode = "" then 'Revenue Code not found'
        when GLCode = "" then 'GL Code not found'
        end RejectReason
        from cs5Lookup5 a
        where 
        RevenueCode = "" 
        or GLCode = ""
        """)
     cs5OutputRejRef.persist(storageLevel)
        
    /*****************************************************************************************************
     * 
     * AGGREGATION
     * 
     *****************************************************************************************************/
      
      val cs5DailyAggOutput = CS5Aggregate.genDailyPostpaidSummary(sqlContext)
      cs5DailyAggOutput.registerTempTable("cs5DailyAggOutput") 
     
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
      * Output CDR Inquiry
      */
      if ( reprocessFlag!= "REPCS" ) {
        
////       Reject BP not required, already done in prepaid
//         Common.cleanDirectory(sc, pathRejBP)
//         rejBpRDD.saveAsTextFile(pathRejBP);   
          
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         cs5CDRInquiry.repartition(30).write.format("com.databricks.spark.csv")
           .option("delimiter", "|")
           .save(pathCS5CDRInqOutput)  
      }  
      
       /*
       * Output Reject Reference
       */
      Common.cleanDirectory(sc, pathRejRef)
      cs5OutputRejRef.repartition(5)
        .write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(pathRejRef) 
      
      /*
       * Output Transformation
       */
      Common.cleanDirectoryWithPattern(sc, pathCS5TransformOutput, "/*/JOB_ID=" + jobID)
      cs5PostOutput.repartition(25).write.partitionBy("TRANSACTION_DATE","JOB_ID")
        .mode("append")
        .save(pathCS5TransformOutput) 
      
      /*
       * Output Orange output
       */
      Common.cleanDirectory(sc, pathCS5OrangeOutput)
      cs5PostOrangeOutput.repartition(30).write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(pathCS5OrangeOutput)  
        
     /*
       * Output Aggregation Daily
       */
      Common.cleanDirectoryWithPattern(sc, pathAggDailyOutput, "/*/job_id=" + jobID)
      cs5DailyAggOutput.repartition(5).write.partitionBy("transaction_date","job_id","src_tp")
        .mode("append")
        .save(pathAggDailyOutput)
        
      Common.cleanDirectoryWithPattern(sc, pathCCNPostpaidAddon, "/*/JOB_ID=" + jobID)
      CCSNCS5PostPaidAddonDF.repartition(5).write.partitionBy("TRANSACTION_DATE","JOB_ID")
          .mode("append")
          .save(pathCCNPostpaidAddon)   
        
    sc.stop();
    
  }
  
}