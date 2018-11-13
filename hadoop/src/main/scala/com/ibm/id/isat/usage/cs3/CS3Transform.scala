package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
//import org.apache.spark.sql.DataFrame
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import com.ibm.id.isat.usage.cs3._
import com.ibm.id.isat.utils._
import java.util.concurrent.atomic.AtomicInteger
import java.net._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date._
import com.ibm.id.isat.utils._
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import java.io._
import sys.process._

object CS3Transform {
  def main(args: Array[String]): Unit = {
    
    var prcDt = ""
    var jobID = ""
    var jobType = ""
    var configDir = ""
    var inputDir = ""
    
    try {
    
       prcDt = args(0)
       jobID = args(1)
       jobType = args(2)
       //jobType = "MAIN"
       //configDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/CS3_TRANSFORM.conf"
       //configDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/USAGE.conf"
       configDir = args(3)
       //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/SAMPLE_CONV_OUTPUT_CS3_SIT_20160728_V1.txt"
       //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3.txt"
       //inputDir = "C:/user/apps/CS3/reject/ref/20160712_1241/*"
       //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3_SIT.txt"
       //inputDir = "/user/apps/CS3/input/sample_conv_output_CS3_SIT.txt"
       //inputDir = "/user/apps/CS3/input/sample_conv_output_CS3.txt"       
       //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/postpaid_used_da.txt"
       //inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/output_split_cs3/"
       inputDir = args(4)
       
       
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Please provide correct parameter. Application exiting...")
      return
    }
    
    
    println("CS3_LOGS: Start Job ID : " + jobID + ", Reprocesss Flag : " + jobType)
    
    //val sc = new SparkContext("local", "CS3 Transform", new SparkConf());
    val sc = new SparkContext(new SparkConf())
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    //import hc.implicits._
    
    //println("sc.applicationId : " + sc.applicationId)
    //val logDir = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_DETAIL_TRANSFORM.LOG_APP_ID_DIR")
    
    
    
    /*--- Get Population Date Time ---*/
    val dateFormat = new SimpleDateFormat("yyyyMMdd");    //("yyyy/MM/dd HH:mm:ss");    
    //val prcDt = dateFormat.format(Calendar.getInstance.getTime)
    /*--- Get Host ID ---*/
    //val localhost = InetAddress.getLocalHost.getHostAddress.split("\\.")
    //val hostID = localhost(2)+localhost(3)        
   
    /*--- Get SYSDATE -90 for Filtering Reject Permanent ---*/
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -90);
    val minDate = dateFormat.format(cal.getTime())
    println("Date = "+ minDate);

    //println(dateFormat.format(Calendar.getInstance.getTime - 30))
    //println(CS3Functions.concatDA("1#+499710209.00#+499709709.00##0[7#+499710209.00#+499709909.00##0"))
    
    /*--- Get Output Parent Directory Path ---*/
    
    val OUTPUT_REJ_BP_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_REJ_BP_DIR")
    val OUTPUT_REJ_REF_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_REJ_REF_DIR")    
    val OUTPUT_REJ_REF_PERM_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_REJ_REF_PERM_DIR")    
    val OUTPUT_DETAIL_PREPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_DETAIL_PREPAID_DIR")
    val OUTPUT_DETAIL_POSTPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_DETAIL_POSTPAID_DIR")
    val OUTPUT_CDR_INQ_PREPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_CDR_INQ_PREPAID_DIR")
    val OUTPUT_CDR_INQ_POSTPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_CDR_INQ_POSTPAID_DIR")
    val OUTPUT_ORANGE_REPORT_POSTPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_ORANGE_REPORT_POSTPAID_DIR")
    
    val OUTPUT_AGG_HOURLY_PREPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_HOURLY_PREPAID.OUTPUT_AGG_HOURLY_PREPAID")
    val OUTPUT_AGG_DAILY_PREPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_DAILY_PREPAID.OUTPUT_AGG_DAILY_PREPAID")    
    val OUTPUT_GREEN_REPORT_PREPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.GREEN_REPORT_PREPAID.OUTPUT_GREEN_REPORT_PREPAID")
    val OUTPUT_AGG_DAILY_POSTPAID_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.AGG_DAILY_POSTPAID.OUTPUT_AGG_DAILY_POSTPAID")    
    val OUTPUT_CS5_PREPAID_TDW_SUMMARY_DIR = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS5_PREPAID_TDW_SUMMARY_DIR")
    //val OUTPUT_LOG_APP_ID = Common.getConfigDirectory(sc, configDir, "USAGE.CS3_TRANSFORM.OUTPUT_CS3_LOG_APP_ID")
    
   
    /*--- Set Child Directory ---*/
    val childDir = Common.getChildDirectory(prcDt, jobID)
    
    /*--- Write Application ID to HDFS ---*/
    //val app_id = sc.applicationId    
    //Common.cleanDirectory(sc, OUTPUT_LOG_APP_ID +"/" + childDir + ".log") 
    //Common.writeApplicationID(sc, OUTPUT_LOG_APP_ID + "/" + childDir + ".log", app_id)
        
    /*println("OUTPUT_REJ_BP_DIR : " + OUTPUT_REJ_BP_DIR);
    println("OUTPUT_REJ_REF_DIR : " + OUTPUT_REJ_REF_DIR);
    println("OUTPUT_DETAIL_PREPAID_DIR : " + OUTPUT_DETAIL_PREPAID_DIR);
    println("OUTPUT_DETAIL_POSTPAID_DIR : " + OUTPUT_DETAIL_POSTPAID_DIR);
    println("OUTPUT_CDR_INQ_PREPAID_DIR : " + OUTPUT_CDR_INQ_PREPAID_DIR);
    println("OUTPUT_CDR_INQ_POSTPAID_DIR : " + OUTPUT_CDR_INQ_POSTPAID_DIR);
    println("OUTPUT_EXADATA_DETAIL_POSTPAID_DIR : " + OUTPUT_EXADATA_DETAIL_POSTPAID_DIR);
    
    println("1 : " + Common.getConfigDirectory(sc, configDir, "USAGE.SPLIT.OUTPUT_SPLIT_CS5"));
    println("1 : " + Common.getConfigDirectory(sc, configDir, "USAGE.SPLIT.OUTPUT_SPLIT_CS3"));*/
    
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val inputRDD = sc.textFile(inputDir);
    //inputRDD.collect foreach {case (a) => println (a)}
    
    //println("count input dir :" + inputRDD.count()) 
     /*----- if not reprocess -------*/
     if (jobType != "REPCS")
     { 
         /* Split the record by tab
         * Sample input : CS3 a|b|c|d|e filename filedate
         * Sample output : a|b|c|d|e|filename|filedate
         * */
        val splitRDD = inputRDD.filter(line => line.split("\t").length == 4).map(line => line.split("\t")).map(col => col(1).concat("|" + col(2) + "|" + col(3)));
        
        //println("inputRDD.filter(line => line.split(\t).length == 4)")
        //inputRDD.filter(line => line.split("\t").length != 4).collect foreach {case (a) => println (a + a.split("\t").length)}
        
        //println("SPLIT RDD")
        //splitRDD.collect foreach {case (a) => println (a)}
        
        /* Filtering to Reject BP
         * Reject BP rules :  null subscriber id, null service class, null trigger time 
         */
        val rejBPRDD = splitRDD.filter(line => line.split("\\|")(6).isEmpty() || line.split("\\|")(21).isEmpty() || line.split("\\|")(9).isEmpty()).map(line => line.concat("|null subscriber id or null service class or null trigger time"))
        //rejBPRDD.collect foreach {case (a) => println (a)}
    
        /*--- Save reject BP records ---*/
        //if(Common.isDirectoryExists(sc, OUTPUT_REJ_BP_DIR +"/" + childDir) == false)
        Common.cleanDirectory(sc, OUTPUT_REJ_BP_DIR +"/" + childDir)
        rejBPRDD.saveAsTextFile(OUTPUT_REJ_BP_DIR +"/" + childDir) 
        
        //rejBPRDD.collect foreach {case (a) => println (a)}
        
        /* Formatting records by selecting used column
         * Sample input : a,b,c,d,e,f,g
         * Sample output : a,b,c,g
         * */
        val formatRDD = splitRDD.filter(line => ! line.split("\\|")(6).isEmpty() && ! line.split("\\|")(21).isEmpty() && ! line.split("\\|")(9).isEmpty()).map(line => CS3Functions.formatCS3(line, "\\|"))
        //splitRDD.filter(line => ! line.split("\\|")(6).isEmpty() || ! line.split("\\|")(21).isEmpty() || ! line.split("\\|")(9).isEmpty() || line.split("\\|")(1) == "6e73623033_REF_MADA_POST").collect foreach {case (a) => println (a)} 
        //println("AFTER FORMAT")
        //formatRDD.collect foreach {case (a) => println (a)}    
        
        //val debugRDD = formatRDD.map(line => (line.split("\\~")(0),line.split("\\~")(1))).flatMapValues(word => word.split("\\["))
        //val debugRDD = formatRDD.filter(line => line.split("\\~")(1).split("\\[").length > 1 ) //256988
        
        /* Transpose DA column into records 
         * Sample input : a,b,c~1;2;3[4;5;6 
         * Sample output : a,b,c,1;2;3 & a,b,c,4;5;6
         */
        //val transposeRDD = formatRDD.map(line => line.split("\\~")).map(field => (field(0), field(1))).flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)};
        val transposeRDD = formatRDD.map(line => (line.split("\\~")(0),line.split("\\~")(1))).flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)};
        //println("AFTER TRANSPOSE")
        //transposeRDD.collect foreach {case (a) => println (a)};
        
        
        /* 
         * Get the Revenue Code, DA ID, Revenue, and VAS flag and concat with PRC DATE and JOB ID
         */
        val afterTransposeRDD = transposeRDD.map(line => line.concat("|" + 
            CS3Functions.getAllRev(line.split("\\|")(5), line.split("\\|")(23), line.split("\\|")(9), line.split("\\|")(31), line.split("\\|")(7), line.split("\\|")(22), line.split("\\|")(17), line.split("\\|")(18), line.split("\\|")(24), "|")
            + "|" + prcDt + "|" + jobID //+ "|" + 
            //intcct.getIntcct(line.split("\\|")(2), line.split("\\|")(0)) + "|" + 
            //intcct.getIntcctWithSID(line.split("\\|")(2), line.split("\\|")(0), line.split("\\|")(23)+"SID") + "|" + prcDt + "|" + jobID  
            ))
        //+ "|" + intcct.getIntcctWithSID(line.split("\\|")(2), line.split("\\|")(0), line.split("\\|")(23)) intcct.getIntcct(line.split("\\|")(2), line.split("\\|")(0)) + "|" +
        //afterTransposeRDD.collect foreach {case (a) => println (a)};
        //println("count after transpose  :" + afterTransposeRDD.count())
        
        /*  
         * Create CS3 Data Frame 
         */
        val cs3Row = afterTransposeRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
        p(3), p(4), p(5), p(6), p(7),
        p(8), p(9), p(10), Common.toDouble(p(11)), p(12),
        p(13), p(14), Common.toDouble(p(15)), p(16), 
        p(17), p(18), p(19), p(20),
        Common.toInt(p(21)), Common.toDouble(p(22)), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30),
        p(31), p(32), p(33), Common.toDouble(p(34)), p(35), p(36), p(37), prcDt, jobID    
        //,p(37), p(38), p(39), p(40)
        //, p(41), p(42), p(43), p(44)
        ))  //, p(41), p(42), p(43), p(44)
        val cs3DF = sqlContext.createDataFrame(cs3Row, CS3Schema.CS3Schema)//.withColumn("id", monotonicallyIncreasingId()) 
        cs3DF.registerTempTable("CS3")
        //cs3DF.persist(StorageLevel.MEMORY_AND_DISK)//.persist()
        cs3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        //println("count transposeRDD :" + transposeRDD.count())
        //println("count debugRDD :" + debugRDD.count())
        //debugRDD.collect foreach {case (a) => println (a)} 
        //println("count after transpose  :" + cs3Row.count())       
         //println("inputRDD = " + inputRDD.count())
         //println("rejBPRDD = " + rejBPRDD.count())
         //println("afterTransposeRDD = " + afterTransposeRDD.count())
         
         //cs3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/cs3_bfr_join/" + childDir)                                         
         //cs3DF.show()
         println("CS3_LOGS: FINISH PREPARE DF")
     }
     
     /*----- if reprocess -------*/
     else
     {
       
       val rejRDD = inputRDD.filter(line => line.split("\\|")(0) >= minDate)
       val rejPermRDD = inputRDD.filter(line => line.split("\\|")(0) < minDate)
       
       //rejPermRDD.collect foreach {case (a) => println (a)};
       
       //if(Common.isDirectoryExists(sc, OUTPUT_REJ_REF_PERM_DIR +"/" + childDir) == false)
       Common.cleanDirectory(sc, OUTPUT_REJ_REF_PERM_DIR +"/" + childDir)
       rejPermRDD.saveAsTextFile(OUTPUT_REJ_REF_PERM_DIR +"/" + childDir)  
       
       /*try{          
          rejPermRDD.saveAsTextFile(OUTPUT_REJ_REF_PERM_DIR +"/" + childDir);  
        }
        catch {
    	    case e: Exception => println("Reject Ref Permanent folder already exists") 
    	  }*/   
        
       /*  
        * Create CS3 Data Frame 
        */
       val cs3Row = rejRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
        p(3), p(4), p(5), p(6), p(7),
        p(8), p(9), p(10), Common.toDouble(p(11)), p(12),
        p(13), p(14), Common.toDouble(p(15)), p(16), 
        p(17), p(18), p(19), p(20),
        Common.toInt(p(21)), Common.toDouble(p(22)), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30),
        p(31), p(32), p(33), Common.toDouble(p(34)), p(35), prcDt, jobID, p(36), p(37) 
        ))
        val cs3DF = sqlContext.createDataFrame(cs3Row, CS3Schema.CS3Schema)//.repartition(10)//.withColumn("id", monotonicallyIncreasingId()) 
        cs3DF.registerTempTable("CS3")        
        //cs3DF.write.saveAsTable("CS3")
        cs3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        //cs3DF.show()
        println("CS3_LOGS: PREPARE DF REPCS")
     }
    
    
     
    
      
    /*****************************************************************************************************
     * 
     * Load Reference Data
     * 
     *****************************************************************************************************/
    // Service Class  
    val svcClassDF = ReferenceDF.getServiceClassDF(sqlContext)
    svcClassDF.registerTempTable("ServiceClass")
    svcClassDF.persist()
    svcClassDF.count()
    
    // Service Class Offer
    val svcClassOfferDF = ReferenceDF.getServiceClassOfferDF(sqlContext)
    svcClassOfferDF.registerTempTable("ServiceClassOffer")
    svcClassOfferDF.persist()
    svcClassOfferDF.count()
    
    // Service Class Postpaid
    val svcClassPostpaidDF = ReferenceDF.getServiceClassPostpaidDF(sqlContext)
    svcClassPostpaidDF.registerTempTable("ServiceClassPostpaid")
    svcClassPostpaidDF.persist()
    svcClassPostpaidDF.count()
    
    // Region Branch   
    val regionBranchDF = ReferenceDF.getRegionBranchDF(sqlContext)
    regionBranchDF.registerTempTable("RegionBranch")     
    regionBranchDF.persist()
    regionBranchDF.count()
    
    // Revenue Code
    val revcodeDF = ReferenceDF.getRevenueCodeDF(sqlContext)
    revcodeDF.registerTempTable("Revcode") 
    revcodeDF.persist()
    revcodeDF.count()
    
    // MADA
    val madaDF = ReferenceDF.getMaDaDF(sqlContext)
    madaDF.registerTempTable("MADA")     
    madaDF.persist()
    madaDF.count()
    
    // Record Type : Mentari 49K, MOBO, SID Multiple Benefit    
    val recTypeDF = ReferenceDF.getRecordTypeDF(sqlContext)
    recTypeDF.registerTempTable("RecType")     
    recTypeDF.persist()
    recTypeDF.count()
        
    val intcctAllDF = ReferenceDF.getIntcctAllDF(sqlContext)    
    intcctAllDF.registerTempTable("intcct")
    intcctAllDF.persist()
    intcctAllDF.count()
    
    val sdpOfferDF = ReferenceDF.getSDPOffer(sqlContext)
    sdpOfferDF.registerTempTable("SDPOffer") 
    
     
    sqlContext.udf.register("toDouble", Common.toDouble _)
    sqlContext.udf.register("toInteger", Common.toInt _)
    println("CS3_LOGS: FINISH PREPARE REFERENCE")
    
     /*****************************************************************************************************
     * 
     * Join Process
     * 
     *****************************************************************************************************/   
     
     val joinSDPOffer = sqlContext.sql("""select a.*, nvl(b.offerID,'') offerID
                                    from CS3 a 
                                    left join SDPOffer b on a.APartyNo = b.msisdn and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt and a.paymentCat = 'PREPAID' """)
   // and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt
     //joinSDPOffer.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/sdpoffer/" + childDir)                                         
          
     
     val joinAintcctDF = Common.getIntcct(sqlContext, joinSDPOffer, intcctAllDF, "APartyNo", "trgrdate", "aPrefix", "svcCtyName", "svcCityName", "svcProviderID")                                
     
     //joinAintcctDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/aintcct/" + childDir)                                         
     
     val joinBintcctDF = Common.getIntcctWithSID(sqlContext, joinAintcctDF, intcctAllDF, "BPartyNo", "trgrdate", "extText", "bPrefix", "dstCtyName", "dstCityName", "dstProviderID")                                
     joinBintcctDF.registerTempTable("InterconnectCS3")
     //joinBintcctDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/bintcct/" + childDir)                                         
     
     
     val joinSvsClssDF = sqlContext.sql("""SELECT a.*, 
                                          COALESCE(b.prmPkgCode, c.prmPkgCode,'') prmPkgCode, 
                                          COALESCE(b.prmPkgName, c.prmPkgName,'') prmPkgName, 
                                          COALESCE(b.brndSCName, c.brndSCName,'') brndSCName,
                                          nvl(b.svcClassCodeOld,'') mgrSvcClssId,
                                          nvl(b.offerAttrName,'') offerAttrKey, 
                                          nvl(b.offerAttrValue,'') offerAttrValue, 
                                          '' offerAttrName, 
                                          /*nvl(d.postSCName,'') postSCName,*/
                                          nvl(e.branch,'') hlrBranchName, nvl(e.region,'') hlrRegionName    
                                          FROM InterconnectCS3 a 
                                          left join ServiceClassOffer b on a.svcClss = b.svcClassCode and a.offerID = b.offerId and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt and a.paymentCat = 'PREPAID'
                                          left join ServiceClass c on a.svcClss = c.svcClassCode and a.trgrdate >= c.svcClassEffDt and a.trgrdate <= c.svcClassEndDt and a.trgrdate >= c.brndSCEffDt and a.trgrdate <= c.brndSCEndDt and a.paymentCat = 'PREPAID'
                                          /*left join ServiceClassPostpaid d on a.svcClss = d.postSCCode and a.paymentCat = 'POSTPAID'*/
                                          left join RegionBranch e on a.svcCityName = e.city
                                       """)
    joinSvsClssDF.registerTempTable("ServiceClassCS3")
    //joinSvsClssDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/svcclass/" + childDir)                                         
     
    
    //joinSvsClssDF.select("APartyNo", "trgrdate", "offerID", "svcClss", "paymentCat", "prmPkgCode", "prmPkgName", "brndSCName", "postSCName", "mgrSvcClssId", "offerAttrKey", "offerAttrValue", "offerAttrName").show()
    
    val joinRevcodeDF = sqlContext.sql("""SELECT a.*, 
                                          nvl(b.svcUsgTp,'') svcTp, nvl(lv1,'') revcodeL1, nvl(lv2,'') revcodeL2, nvl(lv3,'') revcodeL3, nvl(lv4,'') revcodeL4, nvl(lv5,'') revcodeL5, nvl(lv6,'') revcodeL6, nvl(lv7,'') revcodeL7, nvl(lv8,'') revcodeL8, nvl(lv9,'') revcodeL9, nvl(distTp,'') distanceTp, 
                                          CASE WHEN svcUsgTp LIKE '%VOICE%' OR svcUsgTp LIKE '%SMS%' THEN
                                                CASE WHEN lv7 like '%REG%' THEN 'Reg'
                                                     WHEN lv3 like '%Off-net%' THEN 'Offnet'
                                                     WHEN lv3 like '%On-net%' THEN 'Onnet'
                                                     WHEN lv3 like '%International%' THEN 'International'
                                                     ELSE 'Other' END
                                               WHEN svcUsgTp LIKE '%DATA%' THEN 
                                                 CASE WHEN lv7 = 'DATA' THEN 'PPU'
                                                      WHEN lv7 = 'DATA REG' AND lv5 like '%Screen%' THEN 'Add On'
                                                      WHEN lv7 = 'DATA REG' AND lv5 like '%Blackberry%' THEN 'Blackberry'
                                                      ELSE 'Bonus' END
                                               WHEN svcUsgTp LIKE '%VAS%' THEN 'VAS'
                                               ELSE '' END svcUsgTp,
                                          nvl(c.recType,'') recType     
                                          FROM ServiceClassCS3 a 
                                          left join Revcode b on a.revCode = b.revCode
                                          left join RecType c on a.revCode = c.revCode and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt    
                                      """)
    joinRevcodeDF.registerTempTable("RevCodeCS3")
    //joinRevcodeDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/revcode/" + childDir)                                         
    
    
    val joinMaDaDF = sqlContext.sql("""SELECT a.*, 
                                       case a.svcTp when 'VOICE' then b.voiceTrf 
                                                    when 'SMS' then b.smsTrf
                                                    when 'DATA' then b.dataVolTrf 
                                                    else '' end trafficFlag,
                                       case when a.paymentCat = 'POSTPAID' and accountID = 'REGULER' then 'Yes'
                                                    when a.paymentCat = 'POSTPAID' and accountID != 'REGULER' then 'No'
                                                    when a.paymentCat = 'PREPAID' and a.svcTp = 'VOICE' then b.voiceRev
                                                    when a.paymentCat = 'PREPAID' and a.svcTp = 'SMS' then b.smsRev
                                                    when a.paymentCat = 'PREPAID' and a.svcTp = 'DATA' then (case when b.dataVolRev = 'Yes' or b.dataDurRev = 'Yes' then 'Yes' else 'No' end)
                                                    when a.paymentCat = 'PREPAID' and a.svcTp = 'VAS' then b.vasRev
                                                    when a.paymentCat = 'PREPAID' and a.svcTp = 'OTHER' then b.othRev
                                                    when a.paymentCat = 'PREPAID' and a.svcTp = 'DISCOUNT' then b.discRev
                                                    when a.paymentCat = 'PREPAID' and a.svcTp = 'DROP' then 'No' 
                                                    else 'Unknown' end revenueFlag,
                                       nvl(b.grpNm,'') madaName
                                       FROM RevCodeCS3 a 
                                       left join MADA b on a.prmPkgCode = b.grpCd and a.accountID = b.acc and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt""")//.coalesce(5)//.repartition(100)
    
    joinMaDaDF.registerTempTable("FinalCS3")
    joinMaDaDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //joinMaDaDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/mada/" + childDir)                                         
    
    //joinMaDaDF.show()
    //joinMaDaDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    
   
    
    println("CS3_LOGS: FINISH JOIN")
    
    /*****************************************************************************************************
     * 
     * Filtering to Output
     * Rules : not null Promo Package (for PREPAID), not null Interconnect, not null Usage Type, not null MADA Name
     * 
     *****************************************************************************************************/
    
    if (jobType != "REPCS")
		{
        val cdrInqPrepaidDF = sqlContext.sql("""SELECT key KEY, subsID MSISDN, triggerTime TRANSACTIONSTARTTIME, '' TRANSACTIONENDTIME, case when originRealm like '%mms%' then 'MMS' else 'SMS' end TRAFFICTYPES, 
        																			directionType DIRECTIONTYPES, '' ROAMINGAREANAME, '' CALLCLASSID, svcClss ACCOUNTSERVICECLASS, paymentCat SUBSCRIBERTYPE, prmPkgName PROMOPACKAGE,
                                              case when trafficCase in (20,3) then APartyNo else BPartyNo end CALLINGNUMBER,
    																					case when trafficCase in (20,3) then concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) else concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) end CALINGNUMBERDESCRIPTION,																					 			
    																					case when trafficCase in (20,3) then BPartyNo else APartyNo end CALLEDNUMBER,
                                              case when trafficCase in (20,3) then concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) else concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) end CALLEDNUMBERDESCRIPTION,																					 			
    																					usageDuration DURATION, finalCharge COST, acValBfr MAINACCOUNTBALANCEBEFORE, acValAfr MAINACCOUNTBALANCEAFTER,  
    																					/*case when daUsed = 0 and accountID = 'REGULER' then '' else concat("DA",daInfo) end DADETAILS,*/
                                              daInfoOrigin DADETAILS, 
    																					accValueInfo ACCUMULATORDETAILS, case when trafficCase in (20,3) then svcCityName else dstCityName end LOCATIONNUMBER, 
                                              nodeID PPSNODEID, case when trafficCase in (20,3) then svcProviderID else dstProviderID end SERVICEPROVIDERID, usgVol VOLUME,
    																					'' FAFINDICATOR, '' COMMUNITYINDICATOR1, '' COMMUNITYINDICATOR2, '' COMMUNITYINDICATOR3, '' ACCOUNTGROUPID, '' SERVICEOFFERING, '' LAC, '' CELLID, '' CLUSTERNAME, '' NETWORKTYPE, '' RATINGGROUP, '' MCCMNC, 'Charging' SERVICETYPE,
    																					realFileName FILENAMEINPUT, 
                                              /*case when daUsed = 0 and accountID = 'REGULER' then extText
                                                   when daUsed = 1 then concat(extText,accountID) end REVENUE_CODE,*/ 
                                              revCode SERVICE_ID_REVENUE_CODE,
                                              jobID JOBID, 'CS3' SERVICENAME, mgrSvcClssId MGR_SVCCLSS_ID, offerID OFFER_ID, offerAttrKey OFFER_ATTR_KEY, offerAttrValue OFFER_ATTR_VALUE, offerAttrName OFFER_AREA_NAME,
                                              '' SPMADA, '' SPCONSUME 
                                              FROM FinalCS3 
                                              WHERE paymentCat = 'PREPAID' 
                                              and (daUsed = 0 or (daUsed <> 0 and accountID <> 'REGULER')) """)
                                              
        /*--- Save CDR INQ Prepaid ---*/
        Common.cleanDirectory(sc, OUTPUT_CDR_INQ_PREPAID_DIR +"/" + childDir)
        cdrInqPrepaidDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_CDR_INQ_PREPAID_DIR +"/" + childDir)                                         
        
        println("CS3_LOGS: FINSIH WRITE CDR INQ PREPAID")
        
        val cdrInqPostpaidDF = sqlContext.sql("""SELECT key KEY, subsID MSISDN, triggerTime TRANSACTIONSTARTTIME, '' TRANSACTIONENDTIME, case when originRealm like '%mms%' then 'MMS' else 'SMS' end TRAFFICTYPES, 
        																			directionType DIRECTIONTYPES, '' ROAMINGAREANAME, '' CALLCLASSID, svcClss ACCOUNTSERVICECLASS, paymentCat SUBSCRIBERTYPE, prmPkgName PROMOPACKAGE,
                                              case when trafficCase in (20,3) then APartyNo else BPartyNo end CALLINGNUMBER,
    																					case when trafficCase in (20,3) then concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) else concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) end CALINGNUMBERDESCRIPTION,																					 			
    																					case when trafficCase in (20,3) then BPartyNo else APartyNo end CALLEDNUMBER,
                                              case when trafficCase in (20,3) then concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) else concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) end CALLEDNUMBERDESCRIPTION,																					 			
    																					usageDuration DURATION, finalCharge COST, acValBfr MAINACCOUNTBALANCEBEFORE, acValAfr MAINACCOUNTBALANCEAFTER,  
    																					
      																			  /*case when daUsed = 0 and accountID = 'REGULER' then '' else concat("DA",daInfo) end DADETAILS,*/
    																					daInfoOrigin DADETAILS, 
                                              accValueInfo ACCUMULATORDETAILS, case when trafficCase in (20,3) then svcCityName else dstCityName end LOCATIONNUMBER, 
                                              nodeID PPSNODEID, case when trafficCase in (20,3) then svcProviderID else dstProviderID end SERVICEPROVIDERID, usgVol VOLUME,
    																					'' FAFINDICATOR, '' COMMUNITYINDICATOR1, '' COMMUNITYINDICATOR2, '' COMMUNITYINDICATOR3, '' ACCOUNTGROUPID, '' SERVICEOFFERING, '' LAC, '' CELLID, '' CLUSTERNAME, '' NETWORKTYPE, '' RATINGGROUP, '' MCCMNC, 'Charging' SERVICETYPE,
    																					realFileName FILENAMEINPUT,                                               

                                              case when daUsed = 0 then extText
                                                   when daUsed = 1 then concat(extText,accountID) end SERVICE_ID_REVENUE_CODE, /*revCode revCode2, extText,*/
                                              jobID JOBID, 'CS3' SERVICENAME, mgrSvcClssId MGR_SVCCLSS_ID, offerID OFFER_ID, offerAttrKey OFFER_ATTR_KEY, offerAttrValue OFFER_ATTR_VALUE, offerAttrName OFFER_AREA_NAME,
                                              '' SPMADA, '' SPCONSUME 
                                              FROM FinalCS3 
                                              WHERE paymentCat = 'POSTPAID' 
                                              and (daUsed = 0 or (daUsed <> 0 and accountID <> 'REGULER')) """)//.show()
        
        /*--- Save CDR INQ Postpaid ---*/
        Common.cleanDirectory(sc, OUTPUT_CDR_INQ_POSTPAID_DIR +"/" + childDir)
        cdrInqPostpaidDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_CDR_INQ_POSTPAID_DIR +"/" + childDir)                                      
        
        println("CS3_LOGS: FINSIH WRITE CDR INQ POSTPAID")
		}
		
		 val prepaidCS3DF = sqlContext.sql("""SELECT trgrdate TRANSACTION_DATE, trgrtime TRANSACTION_HOUR, APartyNo A_PARTY_NUMBER, 
                                          aPrefix A_PREFIX, svcCityName SERVICE_CITY_NAME, svcProviderID SERVICE_PROVIDER_ID, 
                                          svcCtyName SERVICE_COUNTRY_NM, hlrBranchName HLR_BRANCH_NM, hlrRegionName HLR_REGION_NM, 
                                          BPartyNo B_PARTY_NUMBER, bPrefix B_PREFIX, dstCityName DESTINATION_CITY_NAME, dstProviderID DESTINATION_PROVIDER_ID, 
                                          dstCtyName DESTINATION_COUNTRY_NM, svcClss SERVICE_CLASS_ID, prmPkgCode PROMO_PACKAGE_CODE, prmPkgName PROMO_PACKAGE_NAME, brndSCName BRAND_NAME,
                                          '' CALL_CLASS_CAT, '' CALL_CLASS_BASE, '' CALL_CLASS_RULEID, '' CALL_CLASS_TARIF, '' MSC_ADDRESS,  originRealm ORIGINAL_REALM, originHost ORIGINAL_HOST, 
                                          '' MCC_MNC, ''LAC, '' CI, '' LACI_CLUSTER_ID, '' LACI_CLUSTER_NM, '' LACI_REGION_ID, '' LACI_REGION_NM, '' LACI_AREA_ID, '' LACI_AREA_NM, '' LACI_SALESAREA_ID, '' LACI_SALESAREA_NM, 
                                          '' IMSI, '' APN, '' SERVICE_SCENARIO, '' ROAMING_POSITION, '' FAF, '' FAF_NUMBER, '' RATING_GROUP, '' CONTENT_TYPE, '' IMEI, '' GGSN_ADDRESS, '' SGSN_ADDRESS, '' CALL_REFERENCE, '' CHARGING_ID, '' RAT_TYPE, '' SERVICE_OFFER_ID,
                                          paymentCat PAYMENT_CATEGORY, accountID ACCOUNT_ID, '' ACCOUNT_GROUP_ID, '' FAF_NAME, trafficCase TRAFFIC_CASE, trafficCaseName TRAFFIC_CASE_NAME, revCode REVENUE_CODE, 
                                          directionType DIRECTION_TYPE, distanceTp DISTANCE_TYPE, svcTp SERVICE_TYPE, svcUsgTp SERVICE_USG_TYPE, 
                                          revcodeL1 REVENUE_CODE_L1, revcodeL2 REVENUE_CODE_L2, revcodeL3 REVENUE_CODE_L3, revcodeL4 REVENUE_CODE_L4, revcodeL5 REVENUE_CODE_L5, 
                                          revcodeL6 REVENUE_CODE_L6, revcodeL7 REVENUE_CODE_L7, revcodeL8 REVENUE_CODE_L8, revcodeL9 REVENUE_CODE_L9, svcUsgDrct SVC_USG_DIRECTION, svcUsgDest SVC_USG_DESTINATION, trafficFlag TRAFFIC_FLAG, revenueFlag REVENUE_FLAG, realFileName REAL_FILENAME, 
                                          usgVol USAGE_VOLUME, usgAmount USAGE_AMOUNT, usageDuration USAGE_DURATION, hit HIT, '' ACCUMULATOR, '' COMMUNITY_ID_1, '' COMMUNITY_ID_2, '' COMMUNITY_ID_3, toDouble('') ACCUMULATED_COST, 
                                          recType RECORD_TYPE, triggerTime TRIGGER_TIME, '' EVENT_TIME, '' RECORD_ID_NUMBER, 
                                          acValBfr CCR_CCA_ACCOUNT_VALUE_BEFORE, acValAfr CCR_CCA_ACCOUNT_VALUE_AFTER, '' ECI, '' UNIQUE_KEY, '' SITE_TECH, '' SITE_OPERATOR, 
                                          mgrSvcClssId MGR_SVCCLSS_ID, offerID OFFER_ID, offerAttrKey OFFER_ATTR_KEY, offerAttrValue OFFER_ATTR_VALUE, offerAttrName OFFER_AREA_NAME,
                                          '' DA_UNIT_AMOUNT, '' DA_UNIT_TYPE,
                                          jobID JOB_ID, key RCRD_ID, prcDt PRC_DT, 'CS3' SRC_TP, fileDate FILEDATE
                                          FROM FinalCS3 
                                          WHERE paymentCat = 'PREPAID' and prmPkgCode <> '' and  
                                          aPrefix <> ''  and svcTp <> '' 
                                          and madaName <> '' """)
                                          
                                          
    //prepaidCS3DF.show()
                                          
    println("CS3_LOGS: FINISH WRITE PREPAID DETAIL")
    
    //Common.cleanDirectory(sc, OUTPUT_DETAIL_PREPAID_DIR + "/PRC_DT=" + prcDt + "/JOB_ID=" + jobID)
    Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_PREPAID_DIR, "/*/JOB_ID=" + jobID)
    prepaidCS3DF.write.partitionBy("TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_PREPAID_DIR)  
    
    prepaidCS3DF.registerTempTable("PrepaidSOR")
    prepaidCS3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
   
                              
    val postpaidCS3DF = sqlContext.sql("""SELECT trgrdate TRANSACTION_DATE, trgrtime TRANSACTION_HOUR, APartyNo A_PARTY_NUMBER, 
                                          aPrefix A_PREFIX, svcCityName SERVICE_CITY_NAME, svcProviderID SERVICE_PROVIDER_ID, 
                                          svcCtyName SERVICE_COUNTRY_NM, hlrBranchName HLR_BRANCH_NM, hlrRegionName HLR_REGION_NM, 
                                          BPartyNo B_PARTY_NUMBER, bPrefix B_PREFIX, dstCityName DESTINATION_CITY_NAME, dstProviderID DESTINATION_PROVIDER_ID, 
                                          dstCtyName DESTINATION_COUNTRY_NM, svcClss SERVICE_CLASS_ID, prmPkgCode PROMO_PACKAGE_CODE, prmPkgName PROMO_PACKAGE_NAME, brndSCName BRAND_NAME,
                                          '' CALL_CLASS_CAT, '' CALL_CLASS_BASE, '' CALL_CLASS_RULEID, '' CALL_CLASS_TARIF, '' MSC_ADDRESS,  originRealm ORIGINAL_REALM, originHost ORIGINAL_HOST, 
                                          '' MCC_MNC, ''LAC, '' CI, '' LACI_CLUSTER_ID, '' LACI_CLUSTER_NM, '' LACI_REGION_ID, '' LACI_REGION_NM, '' LACI_AREA_ID, '' LACI_AREA_NM, '' LACI_SALESAREA_ID, '' LACI_SALESAREA_NM, 
                                          '' IMSI, '' APN, '' SERVICE_SCENARIO, '' ROAMING_POSITION, '' FAF, '' FAF_NUMBER, '' RATING_GROUP, '' CONTENT_TYPE, '' IMEI, '' GGSN_ADDRESS, '' SGSN_ADDRESS, '' CALL_REFERENCE, '' CHARGING_ID, '' RAT_TYPE, '' SERVICE_OFFER_ID,
                                          paymentCat PAYMENT_CATEGORY, accountID ACCOUNT_ID, '' ACCOUNT_GROUP_ID, '' FAF_NAME, trafficCase TRAFFIC_CASE, trafficCaseName TRAFFIC_CASE_NAME, revCode REVENUE_CODE, 
                                          directionType DIRECTION_TYPE, distanceTp DISTANCE_TYPE, svcTp SERVICE_TYPE, svcUsgTp SERVICE_USG_TYPE, 
                                          revcodeL1 REVENUE_CODE_L1, revcodeL2 REVENUE_CODE_L2, revcodeL3 REVENUE_CODE_L3, revcodeL4 REVENUE_CODE_L4, revcodeL5 REVENUE_CODE_L5, 
                                          revcodeL6 REVENUE_CODE_L6, revcodeL7 REVENUE_CODE_L7, revcodeL8 REVENUE_CODE_L8, revcodeL9 REVENUE_CODE_L9, svcUsgDrct SVC_USG_DIRECTION, svcUsgDest SVC_USG_DESTINATION, trafficFlag TRAFFIC_FLAG, revenueFlag REVENUE_FLAG, realFileName REAL_FILENAME, 
                                          usgVol USAGE_VOLUME, usgAmount USAGE_AMOUNT, usageDuration USAGE_DURATION, hit HIT, '' ACCUMULATOR, '' COMMUNITY_ID_1, '' COMMUNITY_ID_2, '' COMMUNITY_ID_3, toDouble('') ACCUMULATED_COST, 
                                          recType RECORD_TYPE, triggerTime TRIGGER_TIME, '' EVENT_TIME, '' RECORD_ID_NUMBER, 
                                          acValBfr CCR_CCA_ACCOUNT_VALUE_BEFORE, acValAfr CCR_CCA_ACCOUNT_VALUE_AFTER, '' ECI, '' UNIQUE_KEY, '' SITE_TECH, '' SITE_OPERATOR, 
                                          mgrSvcClssId MGR_SVCCLSS_ID, offerID OFFER_ID, offerAttrKey OFFER_ATTR_KEY, offerAttrValue OFFER_ATTR_VALUE, offerAttrName OFFER_AREA_NAME,
                                          '' DA_UNIT_AMOUNT, '' DA_UNIT_TYPE,
                                          jobID JOB_ID, key RCRD_ID, prcDt PRC_DT, 'CS3' SRC_TP, fileDate FILEDATE
                                          FROM FinalCS3 
                                          WHERE paymentCat = 'POSTPAID' and  
                                          aPrefix <> ''  and svcTp <> '' """)//.repartition(3)
    
    //postpaidCS3DF.show()
    //val postpaidCS3Dir_exists = fs.exists(new org.apache.hadoop.fs.Path(OUTPUT_DETAIL_POSTPAID_DIR + "/PRC_DT=" + prcDt + "/JOB_ID=" + jobID))
    //println (postpaidCS3Dir_exists)
    
    println("CS3_LOGS: FINISH WRITE POSTPAID DETAIL")    
    
    Common.cleanDirectoryWithPattern(sc, OUTPUT_DETAIL_POSTPAID_DIR, "/*/JOB_ID=" + jobID)
    postpaidCS3DF.write.partitionBy("TRANSACTION_DATE","JOB_ID").mode("append").save(OUTPUT_DETAIL_POSTPAID_DIR)
   
    postpaidCS3DF.registerTempTable("PostpaidSOR")
    postpaidCS3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
        
    val orangeReportDF = sqlContext.sql("""SELECT APartyNo SERVED_MSISDN, trgrdate CCR_TRIGGER_TIME, revenueFlag REVENUE_FLAG, '' SERVICE_IDENTIFIER, svcClss CCR_CCA_SERVICE_CLASS_ID, 
                                           '' CCR_SERVICE_SCENARIO, '' CCR_ROAMING_POSITION, '' CCR_CCA_FAF_ID, '' CCR_CCA_DEDICATED_ACCOUNTS, '' CCR_CCA_ACCOUNT_VALUE_DEDUCTED,
                                           '' CCR_TREE_DEFINED_FIELDS, '' ACCUMULATEDCOST_AMOUNT, '' BONUS_ADJ_VALUE_CHANGE, '' RECORD_NUMBER, '' CHARGING_CONTEXT_ID, realFileName REAL_FILENAME, triggerTime ORIGINAL_TRIGGER_TIME,
                                           '' MNCMCC, '' LAC, '' CI, '' APN, usageDuration DURATION, nvl(usgVol,0) VOLUME, 
                                           '' EVENT_DUR, '' RATING_GROUP, '' CONTENT_TYPE, '' RAT_TYPE, '' IMEI, '' EVENT_TIME, '' IMSI, '' GGSN_IPADDR, '' SGSN_IPADDR, '' MSC_IPADDDR, '' CALL_REFFERENCE, '' CHARGING_ID, 
                                           originRealm ORIGIN_REALM, originHost ORIGIN_HOST, aPrefix PREFIX, svcCityName CITY, svcProviderID PROVIDERID, BPartyNo OTHER_PARTY_NUMBER, bPrefix B_PREFIX, dstCityName DEST_CITY, dstProviderID DEST_PROVID, hlrRegionName REGION, hlrBranchName BRANCH,  
                                           '' COSTBAND, '' COSTBAND_NAME, '' CALLTYPE, '' CALLTYPE_NAME, '' LOCATION, '' LOCATION_NAME, '' DISCOUNT,
                                           '' RATTYPE_NAME, '' EVENT_ID, '' POSTPAID_SERVICE_NAME, '' P_AXIS_1, '' P_AXIS_2, '' P_AXIS_3, '' P_AXIS_4, '' P_AXIS_5,  
                                           revCode REVENUE_CODE, revcodeL4 GL_CODE, revcodeL5 GL_NAME, svcTp SERVICE_TYPE, '' SERVICE_TYPE_DA1, '' SERVICE_TYPE_NDA1,
                                           accountID ACCOUNT_ID, nvl(usgAmount,0) CHARGE_AMOUNT, svcUsgTp SERVICE_USAGE_TYPE, '' LOAD_DATE,
                                           hit HITS, mgrSvcClssId USG_MGR_SVCCLS_ID, offerID USG_OFFER_ID, offerAttrKey USG_OFFER_ATTR_KEY, offerAttrValue USG_OFFER_ATTR_VALUE, offerAttrName USG_OFFER_AREA_NAME,                                          
                                           jobID JOB_ID, key RECORD_ID, prcDt PRC_DT, 'CS3' SOURCE_TYPE, fileDate FILE_DATE
                                           FROM FinalCS3 
                                           WHERE paymentCat = 'POSTPAID' and  
                                           aPrefix <> ''  and svcTp <> '' """)//.show()                                 
    
    println("CS3_LOGS: FINISH WRITE POSTPAID ORANGE REPORT")
    
    /*--- Save Detail Postpaid Exadata ---*/
    Common.cleanDirectory(sc, OUTPUT_ORANGE_REPORT_POSTPAID_DIR +"/" + childDir)
    orangeReportDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_ORANGE_REPORT_POSTPAID_DIR +"/" + childDir)    
    //postpaidCS3exaDF.write.partitionBy("PRC_DT", "JOB_ID").write.format("com.databricks.spark.csv").save("C:/user/apps/CS3/output/test_partial_join/" + childDir)
   
    
                                        
    /* Filtering to Reject Reference
     * Rules : not null Promo Package and not null Mada (for PREPAID), not null Interconnect, not null Usage Type 
     */
    val rejRefCS3DF = sqlContext.sql("""SELECT trgrdate TRIGGER_DATE, trgrtime TRIGGER_TIME, APartyNo A_PARTY_NO, BPartyNo B_PARTY_NO, svcClss SVC_CLASS, originRealm ORIGIN_REALM, originHost ORIGIN_HOST, paymentCat PAYMENT_CATEGORY, 
                                          trafficCase TRAFFIC_CASE, trafficCaseName TRAFFIC_CASE_NM, directionType DIRECTION_TP, usageDuration USG_DUR,
                                          APartyNoPad A_PARTY_NO_PAD, BPartyNoPad B_PARTY_NO_PAD, realFileName REAL_FILE_NM, usgVol USG_VOL, triggerTime REAL_TRIGGER_TIME, 
                                          acValBfr AC_VAL_BFR, acValAfr AC_VAL_AFR, svcUsgDrct SVC_USG_DIRECTION, svcUsgDest SVC_USG_DEST, hit HIT, finalCharge FINAL_CHARGE,    
                                          extText EXT_TEXT, daUsed DA_USED, subsID SUBSCRIBER_ID, nodeID NODE_ID, accValueInfo AC_VALUE_INFO, fileDate FILE_DATE, key KEY, 
                                          daInfoOrigin DA_INFO_ORIGIN, daInfo DA_INFO, accountID ACCOUNT_ID, revCode REV_CODE, usgAmount USG_AMT, SID, prcDtOld PRC_DT, jobIDOld JOBID,
                                          case when paymentCat = 'PREPAID' and prmPkgCode = '' then 'Service Class not found' 
                                          		 when aPrefix = '' then 'Origin Interconnect not found'
                                          		 when svcTp = '' then 'Revenue Code not found'
                                          		 when paymentCat = 'PREPAID' and madaName = '' then 'MA DA not found'
                                         	end REJ_RSN
                                          FROM FinalCS3 a
                                          WHERE (paymentCat = 'PREPAID' and prmPkgCode = '') or  
                                          aPrefix = ''  or svcTp = '' 
                                          or (paymentCat = 'PREPAID' and madaName = '') """)
                                                                               
    
    //rejRefCS3DF.show()
    /*--- Save Reject Ref ---*/
    Common.cleanDirectory(sc, OUTPUT_REJ_REF_DIR +"/" + childDir)
    rejRefCS3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_REJ_REF_DIR +"/" + childDir) 
    
    
    
    //prepaidCS3DF.show()
    
   
    /*****************************************************************************************************
     * 
     * Output Agggregate
     * 
     *****************************************************************************************************/ 
    
    /*--- Save Agg Hourly Prepaid ---*/
    val hourlyPrepaidCS3DF = sqlContext.sql("""select TRANSACTION_HOUR HOUR_ID, A_PARTY_NUMBER MSISDN, A_PREFIX PREFIX_NUMBER, SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM COUNTRY_NAME, HLR_BRANCH_NM, HLR_REGION_NM, 
                            B_PREFIX OTHER_PREFIX_NO, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM DESTINATION_COUNTRY_NAME, 
                            SERVICE_CLASS_ID, PROMO_PACKAGE_CODE, PROMO_PACKAGE_NAME, BRAND_NAME, MSC_ADDRESS, ORIGINAL_REALM, ORIGINAL_HOST, 
                            MCC_MNC MCCMNC, LAC, CI, LACI_CLUSTER_ID, LACI_CLUSTER_NM, LACI_REGION_ID, LACI_REGION_NM, LACI_AREA_ID, LACI_AREA_NM, 
                            LACI_SALESAREA_ID, LACI_SALESAREA_NM, IMSI, APN, SERVICE_SCENARIO, ROAMING_POSITION, FAF,RATING_GROUP, 
                            CONTENT_TYPE, IMEI, GGSN_ADDRESS, SGSN_ADDRESS, RAT_TYPE, SERVICE_OFFER_ID SERVICE_OFFERING_ID, 
                            PAYMENT_CATEGORY SUBSCRIBER_TYPE, ACCOUNT_ID, ACCOUNT_GROUP_ID ACCOUNT_GROUPID, TRAFFIC_CASE, REVENUE_CODE, DIRECTION_TYPE, 
                            DISTANCE_TYPE, SERVICE_TYPE, SERVICE_USG_TYPE SERVICE_USAGE_TYPE, SVC_USG_DIRECTION, SVC_USG_DESTINATION, TRAFFIC_FLAG, REVENUE_FLAG, 
                            sum(USAGE_VOLUME) TOTAL_VOLUME, 
                            sum(USAGE_AMOUNT) TOTAL_AMOUNT, 
                            sum(USAGE_DURATION) TOTAL_DURATION,  
                            toInteger(sum(HIT)) TOTAL_HIT,
                            COMMUNITY_ID_1, COMMUNITY_ID_2, COMMUNITY_ID_3, sum(ACCUMULATED_COST) ACCUMULATED_COST, 
                            RECORD_TYPE, ECI, SITE_TECH, SITE_OPERATOR, REVENUE_CODE_L7 SVC_USG_TRAFFIC, MGR_SVCCLSS_ID MGR_SVCCLSS_ID, 
                            OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME, 
                            PRC_DT, TRANSACTION_DATE transaction_date, JOB_ID job_id, SRC_TP src_tp
                            from PrepaidSOR 
                            group by  
                            TRANSACTION_HOUR, A_PARTY_NUMBER, A_PREFIX, SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM, HLR_BRANCH_NM, HLR_REGION_NM, 
                            B_PREFIX, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM, SERVICE_CLASS_ID, PROMO_PACKAGE_CODE, PROMO_PACKAGE_NAME, BRAND_NAME, MSC_ADDRESS, ORIGINAL_REALM, ORIGINAL_HOST, 
                            MCC_MNC, LAC, CI, LACI_CLUSTER_ID, LACI_CLUSTER_NM, LACI_REGION_ID, LACI_REGION_NM, LACI_AREA_ID, LACI_AREA_NM, 
                            LACI_SALESAREA_ID, LACI_SALESAREA_NM, IMSI, APN, SERVICE_SCENARIO, ROAMING_POSITION, FAF,RATING_GROUP, 
                            CONTENT_TYPE, IMEI, GGSN_ADDRESS, SGSN_ADDRESS, RAT_TYPE, SERVICE_OFFER_ID, 
                            PAYMENT_CATEGORY, ACCOUNT_ID, ACCOUNT_GROUP_ID, TRAFFIC_CASE, REVENUE_CODE, DIRECTION_TYPE, 
                            DISTANCE_TYPE, SERVICE_TYPE, SERVICE_USG_TYPE, SVC_USG_DIRECTION, SVC_USG_DESTINATION, TRAFFIC_FLAG, REVENUE_FLAG,
                            COMMUNITY_ID_1, COMMUNITY_ID_2, COMMUNITY_ID_3,RECORD_TYPE, ECI, SITE_TECH, SITE_OPERATOR, REVENUE_CODE_L7, MGR_SVCCLSS_ID, 
                            OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME, PRC_DT, TRANSACTION_DATE, JOB_ID, SRC_TP""")
    
    println("CS3_LOGS: FINISH WRITE PREPAID AGG HOURLY")
    
    Common.cleanDirectoryWithPattern(sc, OUTPUT_AGG_HOURLY_PREPAID_DIR, "/*/job_id=" + jobID + "/src_tp=CS3*")
    hourlyPrepaidCS3DF.write.partitionBy("transaction_date","job_id","src_tp").mode("append").save(OUTPUT_AGG_HOURLY_PREPAID_DIR)  
    
    hourlyPrepaidCS3DF.registerTempTable("hourlyPrepaidCS3")
    hourlyPrepaidCS3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    
    /*--- Save Agg Daily Prepaid ---*/
    val dailyPrepaidCS3DF = sqlContext.sql("""SELECT MSISDN,PREFIX_NUMBER,SERVICE_CITY_NAME,SERVICE_PROVIDER_ID,COUNTRY_NAME,HLR_BRANCH_NM,HLR_REGION_NM,
                            OTHER_PREFIX_NO,DESTINATION_CITY_NAME,DESTINATION_PROVIDER_ID,DESTINATION_COUNTRY_NAME,SERVICE_CLASS_ID,PROMO_PACKAGE_CODE,PROMO_PACKAGE_NAME,BRAND_NAME,
                            MSC_ADDRESS,ORIGINAL_REALM,ORIGINAL_HOST,MCCMNC,LAC,CI,LACI_CLUSTER_ID,LACI_CLUSTER_NM,LACI_REGION_ID,LACI_REGION_NM,LACI_AREA_ID,LACI_AREA_NM,LACI_SALESAREA_ID,LACI_SALESAREA_NM,
                            IMSI,APN,SERVICE_SCENARIO,ROAMING_POSITION,FAF,RATING_GROUP,CONTENT_TYPE,IMEI,GGSN_ADDRESS,SGSN_ADDRESS,RAT_TYPE,SERVICE_OFFERING_ID,
                            SUBSCRIBER_TYPE,ACCOUNT_ID,ACCOUNT_GROUPID,TRAFFIC_CASE,REVENUE_CODE,
                            DIRECTION_TYPE,DISTANCE_TYPE,SERVICE_TYPE,SERVICE_USAGE_TYPE,SVC_USG_DIRECTION,SVC_USG_DESTINATION,
                            TRAFFIC_FLAG,REVENUE_FLAG,
                            SUM(TOTAL_VOLUME) TOTAL_VOLUME,
                            SUM(TOTAL_AMOUNT) TOTAL_AMOUNT,
                            SUM(TOTAL_DURATION) TOTAL_DURATION,
                            toInteger(SUM(TOTAL_HIT)) TOTAL_HIT,
                            COMMUNITY_ID_1,COMMUNITY_ID_2,COMMUNITY_ID_3,
                            SUM(ACCUMULATED_COST) ACCUMULATED_COST,
                            RECORD_TYPE,ECI,SITE_TECH,SITE_OPERATOR,SVC_USG_TRAFFIC,
                            MGR_SVCCLSS_ID,OFFER_ID,OFFER_ATTR_KEY,OFFER_ATTR_VALUE,OFFER_AREA_NAME,
                            PRC_DT,transaction_date, job_id, src_tp
                            from hourlyPrepaidCS3 
                            group by MSISDN,PREFIX_NUMBER,SERVICE_CITY_NAME,SERVICE_PROVIDER_ID,COUNTRY_NAME,HLR_BRANCH_NM,HLR_REGION_NM,
                            OTHER_PREFIX_NO,DESTINATION_CITY_NAME,DESTINATION_PROVIDER_ID,DESTINATION_COUNTRY_NAME,SERVICE_CLASS_ID,PROMO_PACKAGE_CODE,PROMO_PACKAGE_NAME,BRAND_NAME,
                            MSC_ADDRESS,ORIGINAL_REALM,ORIGINAL_HOST,MCCMNC,LAC,CI,LACI_CLUSTER_ID,LACI_CLUSTER_NM,LACI_REGION_ID,LACI_REGION_NM,LACI_AREA_ID,LACI_AREA_NM,LACI_SALESAREA_ID,LACI_SALESAREA_NM,
                            IMSI,APN,SERVICE_SCENARIO,ROAMING_POSITION,FAF,RATING_GROUP,CONTENT_TYPE,IMEI,GGSN_ADDRESS,SGSN_ADDRESS,RAT_TYPE,SERVICE_OFFERING_ID,
                            SUBSCRIBER_TYPE,ACCOUNT_ID,ACCOUNT_GROUPID,TRAFFIC_CASE,REVENUE_CODE,
                            DIRECTION_TYPE,DISTANCE_TYPE,SERVICE_TYPE,SERVICE_USAGE_TYPE,SVC_USG_DIRECTION,SVC_USG_DESTINATION,
                            TRAFFIC_FLAG,REVENUE_FLAG,
                            COMMUNITY_ID_1,COMMUNITY_ID_2,COMMUNITY_ID_3,
                            RECORD_TYPE,ECI,SITE_TECH,SITE_OPERATOR,SVC_USG_TRAFFIC,
                            MGR_SVCCLSS_ID,OFFER_ID,OFFER_ATTR_KEY,OFFER_ATTR_VALUE,OFFER_AREA_NAME,
                            PRC_DT,transaction_date, job_id, src_tp""")
    
    println("CS3_LOGS: FINISH WRITE PREPAID AGG DAILY")
    
    Common.cleanDirectoryWithPattern(sc, OUTPUT_AGG_DAILY_PREPAID_DIR, "/*/job_id=" + jobID + "/src_tp=CS3*")
    dailyPrepaidCS3DF.write.partitionBy("transaction_date","job_id","src_tp").mode("append").save(OUTPUT_AGG_DAILY_PREPAID_DIR) 
    
    dailyPrepaidCS3DF.registerTempTable("dailyPrepaidCS3")
    dailyPrepaidCS3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    /*--- Save Green Report Prepaid ---*/ //CONCAT(PRC_DT,'_',job_id,'_',src_tp) process_id
    val greenReportCS3DF = sqlContext.sql("""SELECT 
                                              transaction_date USG_TRX_DATE,
                                              SUM(TOTAL_AMOUNT) USG_REVENUE,
                                              REVENUE_CODE USG_REVENUE_CODE,
                                              SERVICE_CITY_NAME USG_CITY,
                                              SERVICE_TYPE USG_SERVICE_TYPE,
                                              FROM_UNIXTIME( UNIX_TIMESTAMP()) USG_LOAD_DATE,
                                              SERVICE_CLASS_ID USG_SERVICE_CLASSCODE,
                                              BRAND_NAME USG_BRAND_NAME,
                                              MCCMNC USG_MCCMNC,
                                              LAC USG_LAC,
                                              CI USG_CI,
                                              APN USG_APN,
                                              SUM(TOTAL_DURATION) USG_DURATION,
                                              SUM(TOTAL_VOLUME) USG_VOLUME,
                                              toInteger(SUM(TOTAL_HIT)) USG_HITS,
                                              SUM(TOTAL_DURATION) USG_EVENT_DUR,
                                              PROMO_PACKAGE_CODE USG_PROMO_PKG_CODE,
                                              PROMO_PACKAGE_NAME USG_PROMO_PKG_NAME,
                                              REVENUE_FLAG USG_REVENUE_FLAG,
                                              MGR_SVCCLSS_ID USG_MGR_SVCCLS_ID,
                                              OFFER_ID USG_OFFER_ID,
                                              OFFER_ATTR_KEY USG_OFFER_ATTR_KEY,
                                              OFFER_ATTR_VALUE USG_OFFER_ATTR_VALUE,
                                              OFFER_AREA_NAME USG_OFFER_AREA_NAME,
                                              PRC_DT, job_id, src_tp                                              
                                              
                                              from dailyPrepaidCS3 
                                              group by REVENUE_CODE,SERVICE_CITY_NAME,SERVICE_TYPE,
                                              SERVICE_CLASS_ID,BRAND_NAME,MCCMNC,LAC,CI,APN,
                                              PROMO_PACKAGE_CODE,PROMO_PACKAGE_NAME,REVENUE_FLAG,
                                              MGR_SVCCLSS_ID,OFFER_ID,OFFER_ATTR_KEY,OFFER_ATTR_VALUE,OFFER_AREA_NAME,
                                              PRC_DT,
                                              transaction_date, job_id, src_tp""")
    
    println("CS3_LOGS: FINISH WRITE GREEN REPORT")
    
    //Common.cleanDirectoryWithPattern(sc, OUTPUT_GREEN_REPORT_PREPAID_DIR, "/process_id=" + prcDt + "_" + jobID + "_CS3*")
    //greenReportCS3DF.write.partitionBy("process_id").mode("append").save(OUTPUT_GREEN_REPORT_PREPAID_DIR) 
    
    Common.cleanDirectory(sc, OUTPUT_GREEN_REPORT_PREPAID_DIR +"/process_id=" + prcDt + "_" + jobID + "_CS3")
    greenReportCS3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_GREEN_REPORT_PREPAID_DIR +"/process_id=" + prcDt + "_" + jobID + "_CS3") 
    
    
    /*--- Save Agg Daily Postpaid ---*/
    val dailyPostpaidCS3DF = sqlContext.sql("""SELECT
                            A_PARTY_NUMBER MSISDN, A_PREFIX PREFIX_NUMBER, SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM, HLR_BRANCH_NM, HLR_REGION_NM,
                            B_PREFIX OTHER_PREFIX_NO, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM DESTINATION_COUNTRY_NAME, 
                            SERVICE_CLASS_ID, "" SERVICE_CLASS_NAME,
                            MSC_ADDRESS, ORIGINAL_REALM, ORIGINAL_HOST, MCC_MNC MCCMNC, LAC, CI, LACI_CLUSTER_ID, LACI_CLUSTER_NM, LACI_REGION_ID,
                            LACI_REGION_NM, LACI_AREA_ID, LACI_AREA_NM, LACI_SALESAREA_ID, LACI_SALESAREA_NM, IMSI, APN, SERVICE_SCENARIO,
                            ROAMING_POSITION, FAF, RATING_GROUP, CONTENT_TYPE, IMEI, GGSN_ADDRESS, SGSN_ADDRESS, RAT_TYPE, PAYMENT_CATEGORY SUBSCRIBER_TYPE,
                            ACCOUNT_ID, REVENUE_CODE, REVENUE_CODE_L4 GL_CODE, REVENUE_CODE_L5 GL_NAME, SERVICE_TYPE, SERVICE_USG_TYPE SERVICE_USAGE_TYPE, REVENUE_FLAG,
                            SUM(USAGE_VOLUME) TOTAL_VOLUME,
                            SUM(USAGE_AMOUNT) TOTAL_AMOUNT,
                            SUM(USAGE_DURATION) TOTAL_DURATION,
                            toInteger(SUM(HIT)) TOTAL_HIT,
                            SUM(ACCUMULATED_COST) ACCUMULATED_COST,
                            ECI, SITE_TECH, SITE_OPERATOR, MGR_SVCCLSS_ID MGR_SVCCLS_ID, OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME, PRC_DT,
                            TRANSACTION_DATE transaction_date, JOB_ID job_id, SRC_TP src_tp
                            from PostpaidSOR
                            GROUP BY 
                            A_PARTY_NUMBER, A_PREFIX, SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM, HLR_BRANCH_NM, HLR_REGION_NM,
                            B_PREFIX, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM, SERVICE_CLASS_ID, MSC_ADDRESS,
                            ORIGINAL_REALM, ORIGINAL_HOST, MCC_MNC, LAC, CI, LACI_CLUSTER_ID, LACI_CLUSTER_NM, LACI_REGION_ID,
                            LACI_REGION_NM, LACI_AREA_ID, LACI_AREA_NM, LACI_SALESAREA_ID, LACI_SALESAREA_NM, IMSI, APN, SERVICE_SCENARIO, ROAMING_POSITION,
                            FAF, RATING_GROUP, CONTENT_TYPE, IMEI, GGSN_ADDRESS, SGSN_ADDRESS, RAT_TYPE, PAYMENT_CATEGORY,
                            ACCOUNT_ID, REVENUE_CODE, REVENUE_CODE_L4, REVENUE_CODE_L5, SERVICE_TYPE, SERVICE_USG_TYPE,
                            REVENUE_FLAG, ECI, SITE_TECH, SITE_OPERATOR, MGR_SVCCLSS_ID, OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME,
                            PRC_DT, TRANSACTION_DATE,JOB_ID,SRC_TP""")
    
    println("CS3_LOGS: FINISH WRITE POSTPAID AGG DAILY")
    
    Common.cleanDirectoryWithPattern(sc, OUTPUT_AGG_DAILY_POSTPAID_DIR, "/*/job_id=" + jobID + "/src_tp=CS3*")
    dailyPostpaidCS3DF.write.partitionBy("transaction_date","job_id","src_tp").mode("append").save(OUTPUT_AGG_DAILY_POSTPAID_DIR) 
    
    
    /*--- Save DWH Prepaid ---*/
    val DWHCS3DF = sqlContext.sql("""select TRANSACTION_DATE, TRANSACTION_HOUR, A_PARTY_NUMBER, A_PREFIX, 
                            SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM,
                            B_PREFIX, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM, 
                            SERVICE_CLASS_ID, PROMO_PACKAGE_CODE, BRAND_NAME, MSC_ADDRESS, 
                            MCC_MNC, LAC, CI, LACI_CLUSTER_ID, APN, SERVICE_SCENARIO, ROAMING_POSITION, FAF, RATING_GROUP, 
                            IMEI, RAT_TYPE, 
                            ACCOUNT_ID, ACCOUNT_GROUP_ID, REVENUE_CODE, DIRECTION_TYPE, 
                            DISTANCE_TYPE, SERVICE_TYPE, SVC_USG_DIRECTION, SVC_USG_DESTINATION, TRAFFIC_FLAG, REVENUE_FLAG,
                            REVENUE_CODE_L1,REVENUE_CODE_L2,REVENUE_CODE_L3,REVENUE_CODE_L4,REVENUE_CODE_L5,REVENUE_CODE_L6,
                            REVENUE_CODE_L7,REVENUE_CODE_L8,REVENUE_CODE_L9,
                            sum(CASE WHEN ACCOUNT_ID = 'REGULER' THEN USAGE_VOLUME ELSE 0 END) TOTAL_VOLUME, 
                            sum(USAGE_AMOUNT) TOTAL_AMOUNT, 
                            sum(CASE WHEN ACCOUNT_ID = 'REGULER' THEN USAGE_DURATION ELSE 0 END) TOTAL_DURATION,  
                            toInteger(sum(CASE WHEN ACCOUNT_ID = 'REGULER' THEN HIT ELSE 0 END)) TOTAL_HIT, 
                            COMMUNITY_ID_1, COMMUNITY_ID_2, COMMUNITY_ID_3, 
                            ECI, MGR_SVCCLSS_ID, 
                            OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME, 
                            ACCUMULATOR,  DA_UNIT_AMOUNT, DA_UNIT_TYPE, CALL_CLASS_CAT,
                            PRC_DT, JOB_ID, SRC_TP, '' MICROCLUSTER_NAME                            
                            from PrepaidSOR 
                            group by
                            TRANSACTION_DATE, TRANSACTION_HOUR, A_PARTY_NUMBER, A_PREFIX, 
                            SERVICE_CITY_NAME, SERVICE_PROVIDER_ID, SERVICE_COUNTRY_NM,
                            B_PREFIX, DESTINATION_CITY_NAME, DESTINATION_PROVIDER_ID, DESTINATION_COUNTRY_NM, 
                            SERVICE_CLASS_ID, PROMO_PACKAGE_CODE, BRAND_NAME, MSC_ADDRESS, 
                            MCC_MNC, LAC, CI, LACI_CLUSTER_ID, APN, SERVICE_SCENARIO, ROAMING_POSITION, FAF,RATING_GROUP, 
                            IMEI, RAT_TYPE, 
                            ACCOUNT_ID, ACCOUNT_GROUP_ID, REVENUE_CODE, DIRECTION_TYPE, 
                            DISTANCE_TYPE, SERVICE_TYPE, SVC_USG_DIRECTION, SVC_USG_DESTINATION, TRAFFIC_FLAG, REVENUE_FLAG,
                            REVENUE_CODE_L1,REVENUE_CODE_L2,REVENUE_CODE_L3,REVENUE_CODE_L4,REVENUE_CODE_L5,REVENUE_CODE_L6,
                            REVENUE_CODE_L7,REVENUE_CODE_L8,REVENUE_CODE_L9,
                            COMMUNITY_ID_1, COMMUNITY_ID_2, COMMUNITY_ID_3, 
                            ECI, MGR_SVCCLSS_ID, 
                            OFFER_ID, OFFER_ATTR_KEY, OFFER_ATTR_VALUE, OFFER_AREA_NAME, 
                            ACCUMULATOR, DA_UNIT_AMOUNT, DA_UNIT_TYPE, CALL_CLASS_CAT,
                            PRC_DT, TRANSACTION_DATE, JOB_ID, SRC_TP
                            """)
    
    println("CS3_LOGS: FINISH WRITE TDW SUMMARY DWH")
    
    //Common.cleanDirectoryWithPattern(sc, OUTPUT_GREEN_REPORT_PREPAID_DIR, "/process_id=" + prcDt + "_" + jobID + "_CS3*")
    //greenReportCS3DF.write.partitionBy("process_id").mode("append").save(OUTPUT_GREEN_REPORT_PREPAID_DIR) 
    
    Common.cleanDirectory(sc, OUTPUT_CS5_PREPAID_TDW_SUMMARY_DIR + "/" + childDir)
    DWHCS3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").save(OUTPUT_CS5_PREPAID_TDW_SUMMARY_DIR + "/" + childDir) 
                            
                            
    println("CS3_LOGS: FINISH ALL")
    sc.stop();
    
  }
    
}
