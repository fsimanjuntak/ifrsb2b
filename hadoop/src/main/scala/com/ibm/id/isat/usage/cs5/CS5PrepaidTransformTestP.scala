package com.ibm.id.isat.usage.cs5

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.spark.storage.StorageLevel



 /*****************************************************************************************************
 * 
 * CS5 Transformation
 * Input: CCN CS5 Conversion
 * Output: CS5 CDR Inquiry, CS5 Transformation, CS5 Reject Reference, CS5 Reject BP
 * 
 * Author: anico@id.ibm.com
 * IBM Indonesia for Indosat Ooredoo
 * June 2016
 * 
 *****************************************************************************************************/   
object CS5PrepaidTransformTestP {
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
    var numOfPartitions = 0
    var outputLevel = ""
    var cacheInBp = ""
      
    try {
      prcDt = args(0)
      jobID = args(1)
      reprocessFlag = args(2)
      configDir = args(3)
      inputDir = args(4)
      numOfPartitions = args(5).toInt
      outputLevel = args(6)
      cacheInBp = args (7)
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
    
    //val path_ref = "C:/Users/IBM_ADMIN/Documents/Indosat Project/Indosat Hadoop/2 New Hadoop/Development/Reference";
    val path_ref = "/user/apps/CS5/reference";
    
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
    val cs5InputRDD = sc.textFile(inputDir, numOfPartitions);
    
    if (cacheInBp=="1") cs5InputRDD.cache()

    
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
          //cs5InputFilteredRDD.cache()
          
          //2. Transform the rest
          cs5InputFilteredRDD
            //.map(line => CS5NewSelect.genCS5TransformedRow(  Common.getTuple(line, "\t", 1).concat("|").concat(Common.getTuple(line, "\t", 2)).concat("|").concat(Common.getTuple(line, "\t", 3)), prcDt, jobID ) )
            .flatMap(line => CS5Select.genCS5TransformedRowList(  Common.getTuple(line, "\t", 1).concat("|").concat(Common.getTuple(line, "\t", 2)).concat("|").concat(Common.getTuple(line, "\t", 3))  , "PREPAID" , prcDt, jobID  ) )
          
      } else { 
          //GET THE REJECTED DATA
          cs5InputRDD.map(line => CS5Select.genCS5RejectedRow(line, prcDt, jobID))
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
    val refServiceClassOfr = ReferenceDF.getServiceClassOfferDF(sqlContext)
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
//    val refRevCodeDF = broadcast( sqlContext.read
//    .format("com.databricks.spark.csv")
//    .option("header", "false") // Use first line of all files as header
//    .option("delimiter","|")
//    .schema(ReferenceSchema.revcodeSchema)
//    .load(path_ref+"/REF_REVENUE_CODE_20160430.txt") )
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
     * Join Interconnect A and B
     */
    /*
     * Old Interconnect (Multiple joins)
     */
    /*
     * Old Interconnect (Double Loop)
     */
//    val joinAintcctDF = Common.getIntcctWithSID2(sqlContext, cs5Transform1DF, intcctRef, "APartyNumber", "TransactionDate", "VasServiceId", "APrefix", "ServiceCountryName", "ServiceCityName", "ServiceProviderId")
//    val joinBintcctDF = Common.getIntcctWithSID2(sqlContext, joinAintcctDF, intcctRef, "BPartyNumber", "TransactionDate", "VasServiceId", "BPrefix", "DestinationCountryName", "DestinationCityName", "DestinationProviderId")

    /*
     * New Interconnect (Single Loop)
     */
    val joinBintcctDF = Common.getIntcctWithSID3(sqlContext, cs5Transform1DF, intcctRef, "APartyNumber", "BPartyNumber", "TransactionDate", "VasServiceId", 
         "APrefix", "ServiceCountryName", "ServiceCityName", "ServiceProviderId", "BPrefix", "DestinationCountryName", "DestinationCityName", "DestinationProviderId")
    joinBintcctDF.registerTempTable("cs5Lookup2")

      
    /*
     * Join service class and branch/region
     * TEMP=and a.UsedOfferSC=d.offerId 
     */
    val cs5Lookup3DF = sqlContext.sql(
        """select a.*, nvl(b.baseNm,'') callClassBase, nvl(b.ruleId,'') callClassRuleId, nvl(b.tariffZone,'') callClassTariff, nvl(c.branch,'') hlrBranchNm, nvl(c.region,'') hlrRegionNm,
           nvl(d.prmPkgCode,'') prmPkgCode, nvl(d.prmPkgName,'') prmPkgName, nvl(d.brndSCName,'') brndSCName, nvl(d.svcClassCodeOld,'') mgrSvcClassId,
           nvl(d.offerAttrName,'') offerAttrName, nvl(d.offerAttrValue,'') offerAttrValue, nvl(d.areaName,'') areaName,
           nvl(e.microclusterName,'') microclusterName
           from cs5Lookup2 a 
           left join refCallClass b on a.CallClassCat = b.callClassId 
           left join refRegionBranch c on a.DestinationCityName = c.city
           left join refServiceClassComplete d            
           on a.ServiceClass=d.svcClassCode  
           and a.OfferAttributeAreaname=d.offerAttrValue
           and a.TransactionDate>=d.svcClassEffDt and a.TransactionDate<d.svcClassEndDt and a.TransactionDate>=d.brndSCEffDt and a.TransactionDate<d.brndSCEndDt
           left join refMicrocluster e on a.MicroclusterId=e.microclusterId"""
        )
    cs5Lookup3DF.registerTempTable("cs5Lookup3")
    //cs5Lookup3DF.persist(storageLevel)
    //cs5Lookup3DF.show()
    
    /*
     * Join LAC CI hierarchy
     * Define revenue code base (without Account ID [MA/DA] )
     * Create row of account for transpose : MA~MAvalue^DA1~DA1value^DA2~DA2value ...
     * For REPROCESS, no need to add MA to the account since it has been transposed before
     */
    sqlContext.udf.register("getTotalDAFromDedicatedAccountValuesRow", CS5Select.getTotalDAFromDedicatedAccountValuesRow _)
    sqlContext.udf.register("getAccountRowForTranspose", CS5Select.getAccountRowForTranspose _)
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
              a.DedicatedAccountValuesRow AccountRow 
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
        .concat("|").concat( r.getString(91) )  ,
         r.getString(92) )
      )
    //formatRDD.collect foreach {case (a) => println (a)}
    
     /* 
     * Transpose DA column into records 
     * Sample input : a|b|c|RevCodeBase[DA1~moneyamt~unitamt~uom^DA2~moneyamt~unitamt~uom
     * Sample output : a|b|c|RevCodeBase|DA1|moneyamt|unitamt|uom & a|b|c|RevCodeBase|DA2|moneyamt|unitamt|uom
     */
    formatRDD.cache()
    val cs5TransposeRDD = formatRDD.flatMapValues(word => word.split("\\^")).map{case (k,v) => k.concat("|").concat( Common.getTuple(v, "~", 1)+"|"+Common.getTuple(v, "~", 2)+"|"+Common.getTuple(v, "~", 3)+"|"+Common.getTuple(v, "~", 0) ) };
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
        p(82), p(83), p(84), p(85), p(86), p(87), p(88), p(89), p(90), p(91), p(92).toDouble, p(93), p(94), p(95)
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
     * Join Revenue Code and MADA
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
        LAC,
        CI,
        '' LACI_CLUSTER_ID,
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
        UsageVolume USAGE_VOLUME,
        UsageAmount USAGE_AMOUNT,
        UsageDuration USAGE_DURATION,
        Hit HIT,
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
        microclusterName MICROCLUSTER_NAME
        from cs5LookupMADA
        where 
        prmPkgCode <> ""
        and revCodeUsgTp <> ""
        and MADAAcc <> ""
        """)
        
        

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
     * WRITE OUTPUT
     * 
     *****************************************************************************************************/
     
         Common.cleanDirectory(sc, pathRejBP)
         rejBpRDD.saveAsTextFile(pathRejBP);   
 
       if (outputLevel == "0") { //after transform cs5Transform1DF
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         cs5Transform1DF.write.mode("append").save(pathCS5CDRInqOutput) 
       }
       else if (outputLevel == "1") { //after interconnect joinBintcctDF
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         joinBintcctDF.write.mode("append").save(pathCS5CDRInqOutput) 
       }
       else if (outputLevel == "2") { //after lookup 1 cs5Lookup3DF
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         cs5Lookup3DF.write.mode("append").save(pathCS5CDRInqOutput) 
       }
       else if (outputLevel == "3") { //after lookup 2 refs cs5Lookup6DF
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         cs5Lookup6DF.write.mode("append").save(pathCS5CDRInqOutput) 
       }
       else if (outputLevel == "4") { //after transpose cs5TransposeDF
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         cs5TransposeDF.write.mode("append").save(pathCS5CDRInqOutput) 
       }
       else if (outputLevel == "5") { //after lookup refs #3 cs5LookupMADADF
         Common.cleanDirectory(sc, pathCS5CDRInqOutput)
         cs5LookupMADADF.write.mode("append").save(pathCS5CDRInqOutput) 
       }
         
       else if (outputLevel == "6") { //after transform output
         Common.cleanDirectoryWithPattern(sc, pathCS5TransformOutput, "/*/JOB_ID=" + jobID)
         cs5Output.write.partitionBy("TRANSACTION_DATE","JOB_ID").mode("append").save(pathCS5TransformOutput) 
       }
     
    /*
      if ( reprocessFlag!= "REPCS" ) {
         Common.cleanDirectory(sc, pathRejBP)
         rejBpRDD.saveAsTextFile(pathRejBP);   
          
         

      }  
        
      Common.cleanDirectory(sc, pathRejRef)
      cs5OutputRejRef.write.format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .save(pathRejRef) 
      */
        
      
        
    sc.stop();
    
  }
  

  
  
}