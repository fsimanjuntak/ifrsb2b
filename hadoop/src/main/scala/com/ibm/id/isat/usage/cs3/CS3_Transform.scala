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
//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import scala.reflect.runtime.universe
//import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
//import org.apache.spark.sql.hive.HiveContext

class IdGenerator {
  private val id = new AtomicInteger
  def next: Int = id.incrementAndGet
}

object CS3_Transform {
  def main(args: Array[String]): Unit = {
    
    val jobID = args(0)
    val reprocessFlag = args(1)
    val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3.txt"
    //val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/rej ref/*"
    //val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3_SIT.txt"
    //val inputDir = "/user/apps/CS3/input/sample_conv_output_CS3_SIT.txt"
    //val inputDir = "/user/apps/CS3/input/sample_conv_output_CS3.txt"
    //val inputDir = args(2)
    
    println("Start Job ID : " + jobID + ", Reprocesss Flag : " + reprocessFlag);    
    
    val sc = new SparkContext("local", "CS3 Transform", new SparkConf());
    //val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    //import hc.implicits._
    
    val intcct = new TestInttct.Interconnect3(sc, "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_all.txt")
    //val intcct = new TestInttct.Interconnect3(sc, "/user/apps/CS3/reference/ref_intcct_length_all.txt")
    
    
    
    /*--- Get Population Date Time ---*/
    val dateFormat = new SimpleDateFormat("yyyyMMdd");    //("yyyy/MM/dd HH:mm:ss");    
    val prcDt = dateFormat.format(Calendar.getInstance.getTime)
    /*--- Get Host ID ---*/
    val localhost = InetAddress.getLocalHost.getHostAddress.split("\\.")
    val hostID = localhost(2)+localhost(3)        
   
    /*--- Get SYSDATE -90 for Filtering Reject Permanent ---*/
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -90);
    val minDate = dateFormat.format(cal.getTime())
    println("Date = "+ minDate);

    //println(dateFormat.format(Calendar.getInstance.getTime - 30))
   
    
    val inputRDD = sc.textFile(inputDir);
    
    
     /*----- if not reprocess -------*/
     if (reprocessFlag == "0")
     { 
         /* Split the record by tab
         * Sample input : CS3 a|b|c|d|e filename filedate
         * Sample output : a|b|c|d|e|filename|filedate
         * */
        val splitRDD = inputRDD.map(line => line.split("\t")).map(col => col(1).concat("|" + col(2) + "|" + col(3)));
        
        
        /* Filtering to Reject BP
         * Reject BP rules :  null subscriber id, null service class, null trigger time 
         */
        val rejBPRDD = splitRDD.filter(line => line.split("\\|")(6).isEmpty() || line.split("\\|")(21).isEmpty() || line.split("\\|")(9).isEmpty()).map(line => line.concat("|null subscriber id or null service class or null trigger time"))
    
    
        /*--- Save reject BP records ---*/
        //rejBPRDD.saveAsTextFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/rej_bp2");
        try{
          rejBPRDD.saveAsTextFile("/user/apps/CS3/reject/bp/"+prcDt+jobID);   
        }
        catch {
    	    case e: Exception => println("File Exit") 
    	  }     
        //rejBPRDD.collect foreach {case (a) => println (a)}
        
        /* Formatting records by selecting used column
         * Sample input : a,b,c,d,e,f,g
         * Sample output : a,b,c,g
         * */
        val formatRDD = splitRDD.filter(line => ! line.split("\\|")(6).isEmpty() && ! line.split("\\|")(21).isEmpty() && ! line.split("\\|")(9).isEmpty()).map(line => CS3Functions.formatCS3(line, "\\|"))
            
        
        /* Transpose DA column into records 
         * Sample input : a,b,c~1;2;3[4;5;6 
         * Sample output : a,b,c,1;2;3 & a,b,c,4;5;6
         */
        //val transposeRDD = formatRDD.map(line => line.split("\\~")).map(field => (field(0), field(1))).flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)};
        val transposeRDD = formatRDD.map(line => (line.split("\\~")(0),line.split("\\~")(1))).flatMapValues(word => word.split("\\[")).map{case (k,v) => k.concat("|"+v)};
        
        //transposeRDD.collect foreach {case (a) => println (a)};
        
        
        /* 
         * Get the Revenue Code, DA ID, Revenue, and VAS flag and concat with PRC DATE and JOB ID
         */
        val afterTransposeRDD = transposeRDD.map(line => line.concat("|" + 
            CS3Functions.getAllRev(line.split("\\|")(5), line.split("\\|")(23), line.split("\\|")(9), line.split("\\|")(30), line.split("\\|")(7), line.split("\\|")(22), line.split("\\|")(17), line.split("\\|")(18), line.split("\\|")(24), "|")
            + "|" + prcDt + "|" + jobID //+ "|" + 
            //intcct.getIntcct(line.split("\\|")(2), line.split("\\|")(0)) + "|" + 
            //intcct.getIntcctWithSID(line.split("\\|")(2), line.split("\\|")(0), line.split("\\|")(23)+"SID") + "|" + prcDt + "|" + jobID  
            ))
        //+ "|" + intcct.getIntcctWithSID(line.split("\\|")(2), line.split("\\|")(0), line.split("\\|")(23)) intcct.getIntcct(line.split("\\|")(2), line.split("\\|")(0)) + "|" +
        afterTransposeRDD.collect foreach {case (a) => println (a)};
        /*  
         * Create CS3 Data Frame 
         */
        val cs3Row = afterTransposeRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
        p(3), p(4), p(5), p(6), p(7),
        p(8), p(9), p(10), Common.toInt(p(11)), p(12),
        p(13), p(14), Common.toInt(p(15)), p(16), 
        p(17), p(18), p(19), p(20),
        Common.toInt(p(21)), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30),
        p(31), p(32), Common.toDouble(p(33)), p(34), p(35), p(36) 
        //,p(37), p(38), p(39), p(40)
        //, p(41), p(42), p(43), p(44)
        ))  //, p(41), p(42), p(43), p(44)
        val cs3DF = sqlContext.createDataFrame(cs3Row, CS3Schema.CS3Schema)//.withColumn("id", monotonicallyIncreasingId()) 
        cs3DF.registerTempTable("CS3")
        //cs3DF.write.saveAsTable("CS3")
        //cs3DF.persist(StorageLevel.MEMORY_AND_DISK)//.persist()
        cs3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        //cs3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/reject/ref/"+prcDt+jobID)                                      
        //cs3DF.show()
     }
     
     /*----- if reprocess -------*/
     else
     {
       
       val rejRDD = inputRDD.filter(line => line.split("\\|")(0) >= minDate)
       val rejPermRDD = inputRDD.filter(line => line.split("\\|")(0) < minDate)
       
       
       try{
          rejPermRDD.saveAsTextFile("/user/apps/CS3/reject/ref_perm/"+prcDt+jobID);   
        }
        catch {
    	    case e: Exception => println("File Exit") 
    	  }   
        
       /*  
        * Create CS3 Data Frame 
        */
       val cs3Row = rejRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
        p(3), p(4), p(5), p(6), p(7),
        p(8), p(9), p(10), Common.toInt(p(11)), p(12),
        p(13), p(14), Common.toInt(p(15)), p(16),
        p(17), p(18), p(19), p(20),
        Common.toInt(p(21)), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30),
        p(31), p(32), Common.toDouble(p(33)), p(34), p(35), jobID, jobID+hostID))
        val cs3DF = sqlContext.createDataFrame(cs3Row, CS3Schema.CS3Schema).repartition(10)//.withColumn("id", monotonicallyIncreasingId()) 
        cs3DF.registerTempTable("CS3")        
        //cs3DF.write.saveAsTable("CS3")
        cs3DF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        //cs3DF.show()
     }
    
    
    
     
    
    
    //val cs3DF = sqlContext.createDataFrame(cs3Row, CS3Schema.CS3Schema).withColumn("id", monotonicallyIncreasingId()) 
    //val cs3DF = sqlContext.createDataFrame(cs3Row, CS3Schema.CS3Schema).withColumn("id", rowNumber())    
    //cs3DF.registerTempTable("CS3")
    //cs3DF.show()
    //val cs3rdd = cs3DF.rdd.map(line => line.mkString("|"))
    //cs3rdd.collect foreach {case (a) => println (a)}
    //cs3DF.repartition(3).write.format("com.databricks.spark.csv").option("delimiter", "|").save("/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/CS3_SIT")
    
    //cs3DF.persist()
    
    
    
   
    /*****************************************************************************************************
     * 
     * Load Reference Data
     * 
     *****************************************************************************************************/
    // Service Class  
    val svcClassDF = ReferenceDF.getServiceClassDF(sqlContext)
    svcClassDF.registerTempTable("ServiceClass")
    //sqlContext.cacheTable("ServiceClass")
    svcClassDF.persist()
    svcClassDF.count()
    //val svcClassDF = ReferenceDF2.getServiceClassDF(hc)
    //svcClassDF.write.saveAsTable("ServiceClass") 
    
    // Service Class Offer
    val svcClassOfferDF = ReferenceDF.getServiceClassOfferDF(sqlContext)
    svcClassOfferDF.registerTempTable("ServiceClassOffer")
    //sqlContext.cacheTable("ServiceClass")
    svcClassOfferDF.persist()
    svcClassOfferDF.count()
    
    //Interconnect Network and Short Code
    //val intcctDF = ReferenceDF.getInterconnectDF(sqlContext)   
    //intcctDF.registerTempTable("Interconnect") 
    //sqlContext.cacheTable("Interconnect")
    //intcctDF.persist()
    //intcctDF.count()
    //val intcctDF = ReferenceDF2.getInterconnectDF(hc) 
    //intcctDF.write.saveAsTable("Interconnect") 
    
    // Region Branch   
    val regionBranchDF = ReferenceDF.getRegionBranchDF(sqlContext)
    regionBranchDF.registerTempTable("RegionBranch")     
    //sqlContext.cacheTable("RegionBranch")
    regionBranchDF.persist()
    regionBranchDF.count()
    //val regionBranchDF = ReferenceDF2.getRegionBranchDF(hc)
    //regionBranchDF.write.saveAsTable("RegionBranch") 
    
    // Revenue Code
    val revcodeDF = ReferenceDF.getRevenueCodeDF(sqlContext)
    revcodeDF.registerTempTable("Revcode") 
    //sqlContext.cacheTable("Revcode")
    revcodeDF.persist()
    revcodeDF.count()
    //val revcodeDF = ReferenceDF2.getRevenueCodeDF(hc)
    //revcodeDF.write.saveAsTable("Revcode") 
    
    // MADA
    val madaDF = ReferenceDF.getMaDaDF(sqlContext)
    madaDF.registerTempTable("MADA")     
    //sqlContext.cacheTable("MADA")
    madaDF.persist()
    madaDF.count()
    //val madaDF = ReferenceDF2.getMaDaDF(hc)
    //madaDF.write.saveAsTable("MADA") 
    
    // Record Type : Mentari 49K, MOBO, SID Multiple Benefit    
    val recTypeDF = ReferenceDF.getRecordTypeDF(sqlContext)
    recTypeDF.registerTempTable("RecType")     
    //sqlContext.cacheTable("RecType")
    recTypeDF.persist()
    recTypeDF.count()
    //val recTypeDF = ReferenceDF2.getRecordTypeDF(hc)
    //recTypeDF.write.saveAsTable("RecType") 
    
    
    val intcctAllDF = ReferenceDF.getIntcctAllDF(sqlContext)
    
    intcctAllDF.registerTempTable("intcct")
    intcctAllDF.persist()
    intcctAllDF.count()
    
    /*
    val intcct14DF = intcctAllDF.where("length = 14")
    intcct14DF.registerTempTable("Intcct14")
    intcct14DF.persist()
    intcct14DF.count()
    
    //val intcct13DF = ReferenceDF.getIntcct13DF(sqlContext)
    val intcct13DF = intcctAllDF.where("length = 13")
    //intcct13DF.show()
    intcct13DF.registerTempTable("Intcct13")
    intcct13DF.persist()
    intcct13DF.count()
    
    //val intcct12DF = ReferenceDF.getIntcct12DF(sqlContext)
    val intcct12DF = intcctAllDF.where("length = 12")
    intcct12DF.registerTempTable("Intcct12")
    intcct12DF.persist()
    intcct12DF.count()
    
    //val intcct11DF = ReferenceDF.getIntcct11DF(sqlContext)
    val intcct11DF = intcctAllDF.where("length = 11")
    intcct11DF.registerTempTable("Intcct11")
    intcct11DF.persist()
    intcct11DF.count()
    
    //val intcct10DF = ReferenceDF.getIntcct10DF(sqlContext)
    val intcct10DF = intcctAllDF.where("length = 10")
    intcct10DF.registerTempTable("Intcct10")
    intcct10DF.persist()
    intcct10DF.count()
    
    //val intcct9DF = ReferenceDF.getIntcct9DF(sqlContext)
    val intcct9DF = intcctAllDF.where("length = 9")
    intcct9DF.registerTempTable("Intcct9")
    intcct9DF.persist()
    intcct9DF.count()
    
    //val intcct8DF = ReferenceDF.getIntcct8DF(sqlContext)
    val intcct8DF = intcctAllDF.where("length = 8")
    intcct8DF.registerTempTable("Intcct8")
    intcct8DF.persist()
    intcct8DF.count()
    
    //val intcct7DF = ReferenceDF.getIntcct7DF(sqlContext)
    val intcct7DF = intcctAllDF.where("length = 7")
    intcct7DF.registerTempTable("Intcct7")
    intcct7DF.persist()
    intcct7DF.count()
    
    //val intcct6DF = ReferenceDF.getIntcct6DF(sqlContext)
    val intcct6DF = intcctAllDF.where("length = 6")
    intcct6DF.registerTempTable("Intcct6")
    intcct6DF.persist()
    intcct6DF.count()
    
    //val intcct5DF = ReferenceDF.getIntcct5DF(sqlContext)
    val intcct5DF = intcctAllDF.where("length = 5")
    intcct5DF.registerTempTable("Intcct5")
    intcct5DF.persist()
    intcct5DF.count()
    
    val intcct4DF = intcctAllDF.where("length = 4")
    intcct4DF.registerTempTable("Intcct4")
    intcct4DF.persist()
    intcct4DF.count()
    
    val intcct3DF = intcctAllDF.where("length = 3")
    intcct3DF.registerTempTable("Intcct3")
    intcct3DF.persist()
    intcct3DF.count()
    
    val intcct2DF = intcctAllDF.where("length = 2")
    intcct2DF.registerTempTable("Intcct2")
    intcct2DF.persist()
    intcct2DF.count()
    
    val intcct1DF = intcctAllDF.where("length = 1")
    intcct1DF.registerTempTable("Intcct1")
    intcct1DF.persist()
    intcct1DF.count()
    */
    val sdpOfferDF = ReferenceDF.getSDPOffer(sqlContext)
    sdpOfferDF.registerTempTable("SDPOffer") 
    
    
    
    
    //val intcct = new TestInttct.Interconnect("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_10.txt")
    //sqlContext.udf.register("getIntcct", intcct.getIntcct2 _)
    //sqlContext.udf.register("getTupple", Common.getTuple _)
    //sqlContext.udf.register("getIntcctWithSID", intcct.getIntcctWithSID _)
  
    
    //val intcct = new TestInttct.InterconnectRDD(sc, "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_all.txt")
    //val intcct = new TestInttct.Interconnect2(sc, "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_all.txt")
    //val intcct = new TestInttct.Interconnect2(sc, "/user/apps/CS3/reference/ref_intcct_length_all.txt")
    //val intcct = new TestInttct.Interconnect3(sc, "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_all.txt")
    
      
    //println("hasil2 : " + intcct.getIntcctFinal("6285709297018", "20160423","SID"))    
    //sqlContext.udf.register("getIntcct", intcct.getIntcct _)
    //sqlContext.udf.register("getIntcctWithSID", intcct.getIntcctWithSID _)
    //sqlContext.udf.register("getTupple", Common.getTuple _)
    
    
    
     /*****************************************************************************************************
     * 
     * Join Process
     * 
     *****************************************************************************************************/   
    
    /*val joinIntcctDF = sqlContext.sql("""SELECT a.*, nvl(d.prmPkgCode,'') prmPkgCode, nvl(d.prmPkgName,'') prmPkgName, nvl(d.brndSCName,'') brndSCName, 
                                          nvl(b.intcctPfx,'') aPrefix, nvl(b.locName,'') svcCityName, nvl(b.intcctOpr,'') svcProviderID, nvl(b.intcctCty,'') svcCtyName, 
                                          nvl(c.intcctPfx,'') bPrefix, nvl(c.locName,'') dstCityName, nvl(c.intcctOpr,'') destProviderID, nvl(c.intcctCty,'') destCtyName,
                                          nvl(e.branch,'') hlrBranchName, nvl(e.region,'') hlrRegionName 
                                         FROM CS3 a left join ServiceClass d on a.svcClss = d.svcClassCode and a.trgrdate >= d.svcClassEffDt and a.trgrdate <= d.svcClassEndDt and a.trgrdate >= d.brndSCEffDt and a.trgrdate <= d.brndSCEndDt and a.paymentCat = 'PREPAID'
                                         left join Interconnect b on a.APartyNoPad >= b.intcctPfxStart and a.APartyNoPad <= b.intcctPfxEnd and a.trgrdate >= effDt and a.trgrdate <= endDt 
                                         left join Interconnect c on a.BPartyNoPad >= c.intcctPfxStart and a.BPartyNoPad <= c.intcctPfxEnd and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt and a.extText = c.sid
                                         left join RegionBranch e on b.locName = e.city""")//.coalesce(5)//.repartition(100)*/
                                        
                          
    /*val joinIntcctDF = sqlContext.sql("""SELECT a.*, nvl(d.prmPkgCode,'') prmPkgCode, nvl(d.prmPkgName,'') prmPkgName, nvl(d.brndSCName,'') brndSCName,
                                          nvl(b.intcctPfx,'') aPrefix, nvl(b.locName,'') svcCityName, nvl(b.intcctOpr,'') svcProviderID, nvl(b.intcctCty,'') svcCtyName 
                                          FROM CS3 a left join ServiceClass d on a.svcClss = d.svcClassCode and a.trgrdate >= d.svcClassEffDt and a.trgrdate <= d.svcClassEndDt and a.trgrdate >= d.brndSCEffDt and a.trgrdate <= d.brndSCEndDt and a.paymentCat = 'PREPAID'
                                          left join Interconnect b on a.APartyNoPad between b.intcctPfxStart and b.intcctPfxEnd and a.trgrdate between effDt and endDt 
                                           
                                     """)
    joinIntcctDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    */                                  
    /*val joinIntcctDF = sqlContext.sql("""SELECT a.*, nvl(d.prmPkgCode,'') prmPkgCode, nvl(d.prmPkgName,'') prmPkgName, nvl(d.brndSCName,'') brndSCName, 
                                          nvl(e.branch,'') hlrBranchName, nvl(e.region,'') hlrRegionName 
                                         FROM CS3 a left join ServiceClass d on a.svcClss = d.svcClassCode and a.trgrdate >= d.svcClassEffDt and a.trgrdate <= d.svcClassEndDt and a.trgrdate >= d.brndSCEffDt and a.trgrdate <= d.brndSCEndDt and a.paymentCat = 'PREPAID'
                                         left join RegionBranch e on b.locName = e.city""")//.coalesce(5)//.repartition(100)
                                        */
                                        
    //joinIntcctDF.show()
    //joinIntcctDF.persist()
    //joinIntcctDF.registerTempTable("InterconnectCS3")
    //joinIntcctDF.write.saveAsTable("InterconnectCS3")
    //joinIntcctDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    
    
    val joinRevcodeDF = sqlContext.sql("""SELECT a.*, nvl(d.prmPkgCode,'') prmPkgCode, nvl(d.prmPkgName,'') prmPkgName, nvl(d.brndSCName,'') brndSCName,  
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
                                          FROM CS3 a left join ServiceClass d on a.svcClss = d.svcClassCode and a.trgrdate >= d.svcClassEffDt and a.trgrdate <= d.svcClassEndDt and a.trgrdate >= d.brndSCEffDt and a.trgrdate <= d.brndSCEndDt and a.paymentCat = 'PREPAID'
                                          left join Revcode b on a.revCode = b.revCode
                                          left join RecType c on a.revCode = c.revCode and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt    
                                      """)
                                      
    /*val joinRevcodeDF = sqlContext.sql("""SELECT a.*, nvl(b.svcUsgTp,'') svcTp, nvl(lv1,'') revcodeL1, nvl(lv2,'') revcodeL2, nvl(lv3,'') revcodeL3, nvl(lv4,'') revcodeL4, nvl(lv5,'') revcodeL5, nvl(lv6,'') revcodeL6, nvl(lv7,'') revcodeL7, nvl(lv8,'') revcodeL8, nvl(lv9,'') revcodeL9, nvl(distTp,'') distanceTp, 
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
                                          FROM InterconnectCS3 a left join Revcode b on a.revCode = b.revCode
                                          left join RecType c on a.revCode = c.revCode and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt    
                                      """)*///.coalesce(5)//.repartition(100)
    //joinRevcodeDF.show();
    joinRevcodeDF.registerTempTable("RevCodeCS3")    
    //joinRevcodeDF.write.saveAsTable("RevCodeCS3")
    
    val joinMaDaDF = sqlContext.sql("""SELECT a.*, case a.svcTp when 'VOICE' then b.voiceTrf 
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
                                b.grpNm madaName
                                FROM RevCodeCS3 a left join MADA b on a.prmPkgCode = b.grpCd and a.accountID = b.acc and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt""")//.coalesce(5)//.repartition(100)
    //joinMaDaDF.show();    
    joinMaDaDF.registerTempTable("madaCS3")//("FinalCS3")          
    //joinMaDaDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    
    /*
    //,getIntcctWithSID(a.APartyNo, a.trgrdate, CONCAT( a.extText,'SID'))
    val joinDF = sqlContext.sql("""select a.*, getIntcct(a.APartyNo, a.trgrdate) Aintcct,
                                          getIntcctWithSID(a.BPartyNo, a.trgrdate, CONCAT( a.extText,'SID')) Bintcct
                                          from madaCS3 a""")
    joinDF.registerTempTable("temp")
    
    val joinDF2 = sqlContext.sql("""select a.*, getTupple(Aintcct,"|",0),getTupple(Aintcct,"|",1),getTupple(Aintcct,"|",2),getTupple(Aintcct,"|",3),
                                           getTupple(Bintcct,"|",0),getTupple(Bintcct,"|",1),getTupple(Bintcct,"|",2),getTupple(Bintcct,"|",3)   
                  from temp a""")
    //joinDF2.show()
    joinDF2.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    */
    //joinMaDaDF.persist()
    //joinDF2.write.partitionBy("trgrdate").partitionBy("jobID").mode("append").format("com.databricks.spark.csv").option("delimiter", "|").save("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Output/"+prcDt+jobID)
    //joinDF2.write.partitionBy("trgrdate","jobID").mode("append").save("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Output/"+prcDt+jobID)
    
    /*
    val joinDF = sqlContext.sql("""select a.*, 
                                          COALESCE(getIntcct(13, substr(a.APartyNo,1,13),a.trgrdate),
                                                   getIntcct(12, substr(a.APartyNo,1,12),a.trgrdate),
                                                   getIntcct(11, substr(a.APartyNo,1,11),a.trgrdate),
                                                   getIntcct(10, substr(a.APartyNo,1,10),a.trgrdate),
                                                   getIntcct(9, substr(a.APartyNo,1,9),a.trgrdate),
                                                   getIntcct(8, substr(a.APartyNo,1,8),a.trgrdate),
                                                   getIntcct(7, substr(a.APartyNo,1,7),a.trgrdate),
                                                   getIntcct(6, substr(a.APartyNo,1,6),a.trgrdate),
                                                   getIntcct(5, substr(a.APartyNo,1,5),a.trgrdate),
                                                   "|||") Aintcct,
                                           COALESCE(getIntcctWithSID(13, substr(a.BPartyNo,1,13),a.trgrdate, a.extText),
                                                   getIntcctWithSID(12, substr(a.BPartyNo,1,12),a.trgrdate, a.extText),
                                                   getIntcctWithSID(11, substr(a.BPartyNo,1,11),a.trgrdate, a.extText),
                                                   getIntcctWithSID(10, substr(a.BPartyNo,1,10),a.trgrdate, a.extText),
                                                   getIntcctWithSID(9, substr(a.BPartyNo,1,9),a.trgrdate, a.extText),
                                                   getIntcctWithSID(8, substr(a.BPartyNo,1,8),a.trgrdate, a.extText),
                                                   getIntcctWithSID(7, substr(a.BPartyNo,1,7),a.trgrdate, a.extText),
                                                   getIntcctWithSID(6, substr(a.BPartyNo,1,6),a.trgrdate, a.extText),
                                                   getIntcctWithSID(5, substr(a.BPartyNo,1,5),a.trgrdate, a.extText),
                                                   "|||") Bintcct
                                  from madaCS3 a""")
    joinDF.registerTempTable("temp")    
    joinDF.show()
    
    val joinDF2 = sqlContext.sql("""select a.*, getTupple(Aintcct,"|",0),getTupple(Aintcct,"|",1),getTupple(Aintcct,"|",2),getTupple(Aintcct,"|",3),
                                           getTupple(Bintcct,"|",0),getTupple(Bintcct,"|",1),getTupple(Bintcct,"|",2),getTupple(Bintcct,"|",3)   
                 from temp a""")
    joinDF2.show()
    */
    
    
    val joinAintcctDF = Common.getIntcct(sqlContext, joinMaDaDF, intcctAllDF, "APartyNo", "trgrdate", "aPrefix", "svcCtyName", "svcCityName", "svcProviderID")                                
    /*val joinAintcctDF = sqlContext.sql("""SELECT a.*, 
                                          COALESCE(b.intcctPfx, c.intcctPfx, d.intcctPfx, e.intcctPfx, f.intcctPfx, g.intcctPfx, h.intcctPfx, i.intcctPfx, j.intcctPfx, k.intcctPfx, l.intcctPfx, m.intcctPfx, n.intcctPfx, o.intcctPfx) aPrefix,
                                          COALESCE(b.intcctCty, c.intcctCty, d.intcctCty, e.intcctCty, f.intcctCty, g.intcctCty, h.intcctCty, i.intcctCty, j.intcctCty, k.intcctCty, l.intcctCty, m.intcctCty, n.intcctCty, o.intcctCty) svcCtyName,
                                          COALESCE(b.intcctOpr, c.intcctOpr, d.intcctOpr, e.intcctOpr, f.intcctOpr, g.intcctOpr, h.intcctOpr, i.intcctOpr, j.intcctOpr, k.intcctOpr, l.intcctOpr, m.intcctOpr, n.intcctOpr, o.intcctOpr) svcProviderID,
                                          COALESCE(b.locName, c.locName, d.locName, e.locName, f.locName, g.locName, h.locName, i.locName, j.locName, k.locName, l.locName, m.locName, n.locName, o.locName) svcCityName   
                                          FROM madaCS3 a 
                                          left join Intcct14 b on substr(a.APartyNo,1,14) = b.intcctPfx and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt
                                          left join Intcct13 c on substr(a.APartyNo,1,13) = c.intcctPfx and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt 
                                          left join Intcct12 d on substr(a.APartyNo,1,12) = d.intcctPfx and a.trgrdate >= d.effDt and a.trgrdate <= d.endDt 
                                          left join Intcct11 e on substr(a.APartyNo,1,11) = e.intcctPfx and a.trgrdate >= e.effDt and a.trgrdate <= e.endDt 
                                          left join Intcct10 f on substr(a.APartyNo,1,10) = f.intcctPfx and a.trgrdate >= f.effDt and a.trgrdate <= f.endDt 
                                          left join Intcct9 g on substr(a.APartyNo,1,9) = g.intcctPfx and a.trgrdate >= g.effDt and a.trgrdate <= g.endDt 
                                          left join Intcct8 h on substr(a.APartyNo,1,8) = h.intcctPfx and a.trgrdate >= h.effDt and a.trgrdate <= h.endDt 
                                          left join Intcct7 i on substr(a.APartyNo,1,7) = i.intcctPfx and a.trgrdate >= i.effDt and a.trgrdate <= i.endDt 
                                          left join Intcct6 j on substr(a.APartyNo,1,6) = j.intcctPfx and a.trgrdate >= j.effDt and a.trgrdate <= j.endDt 
                                          left join Intcct5 k on substr(a.APartyNo,1,5) = k.intcctPfx and a.trgrdate >= k.effDt and a.trgrdate <= k.endDt 
                                          left join Intcct4 l on substr(a.APartyNo,1,4) = l.intcctPfx and a.trgrdate >= l.effDt and a.trgrdate <= l.endDt 
                                          left join Intcct3 m on substr(a.APartyNo,1,3) = m.intcctPfx and a.trgrdate >= m.effDt and a.trgrdate <= m.endDt 
                                          left join Intcct2 n on substr(a.APartyNo,1,2) = n.intcctPfx and a.trgrdate >= n.effDt and a.trgrdate <= n.endDt 
                                          left join Intcct1 o on substr(a.APartyNo,1,1) = o.intcctPfx and a.trgrdate >= o.effDt and a.trgrdate <= o.endDt 
                                      """)*/
    joinAintcctDF.registerTempTable("intcctACS3")
    
                                          
    val joinBintcctDF = Common.getIntcctWithSID(sqlContext, joinAintcctDF, intcctAllDF, "BPartyNo", "trgrdate", "extText", "bPrefix", "dstCtyName", "dstCityName", "dstProviderID")                                
                                          
    /*val joinBintcctDF = sqlContext.sql("""SELECT a.*, 
                                          COALESCE(b.intcctPfx, c.intcctPfx, d.intcctPfx, e.intcctPfx, f.intcctPfx, g.intcctPfx, h.intcctPfx, i.intcctPfx, j.intcctPfx, k.intcctPfx, l.intcctPfx, m.intcctPfx, n.intcctPfx, o.intcctPfx) bPrefix,
                                          COALESCE(b.intcctCty, c.intcctCty, d.intcctCty, e.intcctCty, f.intcctCty, g.intcctCty, h.intcctCty, i.intcctCty, j.intcctCty, k.intcctCty, l.intcctCty, m.intcctCty, n.intcctCty, o.intcctCty) dstCtyName,
                                          COALESCE(b.intcctOpr, c.intcctOpr, d.intcctOpr, e.intcctOpr, f.intcctOpr, g.intcctOpr, h.intcctOpr, i.intcctOpr, j.intcctOpr, k.intcctOpr, l.intcctOpr, m.intcctOpr, n.intcctOpr, o.intcctOpr) dstProviderID,
                                          COALESCE(b.locName, c.locName, d.locName, e.locName, f.locName, g.locName, h.locName, i.locName, j.locName, k.locName, l.locName, m.locName, n.locName, o.locName) destCityName   
                                          FROM intcctACS3 a 
                                          left join Intcct14 b on substr(a.BPartyNo,1,14) = b.intcctPfx and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt and a.extText = b.sid
                                          left join Intcct13 c on substr(a.BPartyNo,1,13) = c.intcctPfx and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt and a.extText = c.sid 
                                          left join Intcct12 d on substr(a.BPartyNo,1,12) = d.intcctPfx and a.trgrdate >= d.effDt and a.trgrdate <= d.endDt and a.extText = d.sid
                                          left join Intcct11 e on substr(a.BPartyNo,1,11) = e.intcctPfx and a.trgrdate >= e.effDt and a.trgrdate <= e.endDt and a.extText = e.sid
                                          left join Intcct10 f on substr(a.BPartyNo,1,10) = f.intcctPfx and a.trgrdate >= f.effDt and a.trgrdate <= f.endDt and a.extText = f.sid
                                          left join Intcct9 g on substr(a.BPartyNo,1,9) = g.intcctPfx and a.trgrdate >= g.effDt and a.trgrdate <= g.endDt and a.extText = g.sid
                                          left join Intcct8 h on substr(a.BPartyNo,1,8) = h.intcctPfx and a.trgrdate >= h.effDt and a.trgrdate <= h.endDt and a.extText = h.sid
                                          left join Intcct7 i on substr(a.BPartyNo,1,7) = i.intcctPfx and a.trgrdate >= i.effDt and a.trgrdate <= i.endDt and a.extText = i.sid
                                          left join Intcct6 j on substr(a.BPartyNo,1,6) = j.intcctPfx and a.trgrdate >= j.effDt and a.trgrdate <= j.endDt and a.extText = j.sid
                                          left join Intcct5 k on substr(a.BPartyNo,1,5) = k.intcctPfx and a.trgrdate >= k.effDt and a.trgrdate <= k.endDt and a.extText = k.sid
                                          left join Intcct4 l on substr(a.BPartyNo,1,4) = l.intcctPfx and a.trgrdate >= l.effDt and a.trgrdate <= l.endDt and a.extText = l.sid
                                          left join Intcct3 m on substr(a.BPartyNo,1,3) = m.intcctPfx and a.trgrdate >= m.effDt and a.trgrdate <= m.endDt and a.extText = m.sid
                                          left join Intcct2 n on substr(a.BPartyNo,1,2) = n.intcctPfx and a.trgrdate >= n.effDt and a.trgrdate <= n.endDt and a.extText = n.sid
                                          left join Intcct1 o on substr(a.BPartyNo,1,1) = o.intcctPfx and a.trgrdate >= o.effDt and a.trgrdate <= o.endDt and a.extText = o.sid
                                      """)*/
    joinBintcctDF.registerTempTable("intcctBCS3")    
    //joinBintcctDF.show()
    //joinBintcctDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    // 
    val joinsdp = sqlContext.sql("""select a.*, b.offerID,
                                    nvl(e.branch,'') hlrBranchName, nvl(e.region,'') hlrRegionName 
                                    from intcctBCS3 a 
                                    left join SDPOffer b on a.APartyNo = b.msisdn and a.trgrdate >= b.effDt and a.trgrdate <= b.endDt
                                    left join RegionBranch e on a.svcCityName = e.city""")
    
    joinsdp.registerTempTable("sdpCS3")
    
    //val joinSC = sqlContext.sql("""select""")
    
    //joinsdp.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    joinsdp.registerTempTable("FinalCS3")                                  
    joinsdp.show()
    //val intcct13DF2 = intcctAllDF.where("intcctPfx = '335502' AND sid = '33550027008002'")
    //intcct13DF2.show()
    
    //sqlContext.sql("""select * from Intcct7 where intcctPfx = '6281571' """).show()
    
    //joinMaDaDF.write.saveAsTable("FinalCS3")
    /*
    val joinIntcctDF2 = sqlContext.sql("""SELECT a.*, 
                                          nvl(b.intcctPfx,'') aPrefix, nvl(b.locName,'') svcCityName, nvl(b.intcctOpr,'') svcProviderID, nvl(b.intcctCty,'') svcCtyName, 
                                          nvl(c.intcctPfx,'') bPrefix, nvl(c.locName,'') dstCityName, nvl(c.intcctOpr,'') destProviderID, nvl(c.intcctCty,'') destCtyName,
                                          nvl(e.branch,'') hlrBranchName, nvl(e.region,'') hlrRegionName 
                                         FROM madaCS3 a left join Interconnect b on a.APartyNoPad >= b.intcctPfxStart and a.APartyNoPad <= b.intcctPfxEnd and a.trgrdate >= effDt and a.trgrdate <= endDt 
                                         left join Interconnect c on a.BPartyNoPad >= c.intcctPfxStart and a.BPartyNoPad <= c.intcctPfxEnd and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt and a.extText = c.sid
                                         left join RegionBranch e on b.locName = e.city""")//.coalesce(5)//.repartition(100)
    joinIntcctDF2.registerTempTable("FinalCS3")               
    sqlContext.cacheTable("FinalCS3")*/
    //joinIntcctDF2.show()
    /*
    val alljoin =  sqlContext.sql("""explain SELECT a.*, nvl(d.prmPkgCode,'') prmPkgCode, nvl(d.prmPkgName,'') prmPkgName, nvl(d.brndSCName,'') brndSCName, 
                                          nvl(b.intcctPfx,'') aPrefix, nvl(b.locName,'') svcCityName, nvl(b.intcctOpr,'') svcProviderID, nvl(b.intcctCty,'') svcCtyName, 
                                          nvl(c.intcctPfx,'') bPrefix, nvl(c.locName,'') dstCityName, nvl(c.intcctOpr,'') destProviderID, nvl(c.intcctCty,'') destCtyName,
                                          nvl(e.branch,'') hlrBranchName, nvl(e.region,'') hlrRegionName,
                                          nvl(f.svcUsgTp,'') svcTp, nvl(lv1,'') revcodeL1, nvl(lv2,'') revcodeL2, nvl(lv3,'') revcodeL3, nvl(lv4,'') revcodeL4, nvl(lv5,'') revcodeL5, nvl(lv6,'') revcodeL6, nvl(lv7,'') revcodeL7, nvl(lv8,'') revcodeL8, nvl(lv9,'') revcodeL9, nvl(distTp,'') distanceTp, 
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
                                          nvl(g.recType,'') recType,
                                          case f.svcUsgTp when 'VOICE' then h.voiceTrf 
                                                        when 'SMS' then h.smsTrf
                                                        when 'DATA' then h.dataVolTrf 
                                                        else '' end trafficFlag,
                                                        case when a.paymentCat = 'POSTPAID' and accountID = 'REGULER' then 'Yes'
                                                        when a.paymentCat = 'POSTPAID' and accountID != 'REGULER' then 'No'
                                                        when a.paymentCat = 'PREPAID' and f.svcUsgTp = 'VOICE' then h.voiceRev
                                                        when a.paymentCat = 'PREPAID' and f.svcUsgTp = 'SMS' then h.smsRev
                                                        when a.paymentCat = 'PREPAID' and f.svcUsgTp = 'DATA' then (case when h.dataVolRev = 'Yes' or h.dataDurRev = 'Yes' then 'Yes' else 'No' end)
                                                        when a.paymentCat = 'PREPAID' and f.svcUsgTp = 'VAS' then h.vasRev
                                                        when a.paymentCat = 'PREPAID' and f.svcUsgTp = 'OTHER' then h.othRev
                                                        when a.paymentCat = 'PREPAID' and f.svcUsgTp = 'DISCOUNT' then h.discRev
                                                        when a.paymentCat = 'PREPAID' and f.svcUsgTp = 'DROP' then 'No' 
                                                        else 'Unknown' end revenueFlag,
                                                        h.grpNm madaName

                                         FROM CS3 a left join ServiceClass d on a.svcClss = d.svcClassCode and a.trgrdate >= d.svcClassEffDt and a.trgrdate <= d.svcClassEndDt and a.trgrdate >= d.brndSCEffDt and a.trgrdate <= d.brndSCEndDt and a.paymentCat = 'PREPAID'
                                         left join Interconnect b on a.APartyNoPad >= b.intcctPfxStart and a.APartyNoPad <= b.intcctPfxEnd and a.trgrdate >= effDt and a.trgrdate <= endDt 
                                         left join Interconnect c on a.BPartyNoPad >= c.intcctPfxStart and a.BPartyNoPad <= c.intcctPfxEnd and a.trgrdate >= c.effDt and a.trgrdate <= c.endDt and a.extText = c.sid
                                         left join RegionBranch e on b.locName = e.city
                                         left join Revcode f on a.revCode = f.revCode
                                         left join RecType g on a.revCode = g.revCode and a.trgrdate >= g.effDt and a.trgrdate <= g.endDt    
                                         left join MADA h on d.prmPkgCode = h.grpCd and a.accountID = h.acc and a.trgrdate >= h.effDt and a.trgrdate <= h.endDt
                              """)//.coalesce(5)//.repartition(100)
    alljoin.show()	
    */													
    
    //alljoin.registerTempTable("FinalCS3")
    //joinRecTypeDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/CS3_SIT_ALLREF")
    
    //alljoin.show()
    
    
    //alljoin.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    /*****************************************************************************************************
     * 
     * Filtering to Output
     * Rules : not null Promo Package (for PREPAID), not null Interconnect, not null Usage Type, not null MADA Name
     * 
     *****************************************************************************************************/
    /*
    val prepaidCS3DF = sqlContext.sql("""SELECT trgrdate TRANSACTION_DATE, trgrtime TRANSACTION_HOUR, APartyNo A_PARTY_NUMBER, 
                                          aPrefix A_PREFIX, svcCityName SERVICE_CITY_NAME, svcProviderID SERVICE_PROVIDER_ID, 
                                          svcCtyName SERVICE_COUNTRY_NM, hlrBranchName HLR_BRANCH_NM, hlrRegionName HLR_REGION_NM, 
                                          BPartyNo B_PARTY_NUMBER, bPrefix B_PREFIX, dstCityName DESTINATION_CITY_NAME, dstProviderID DESTINATION_PROVIDER_ID, 
                                          dstCtyName DESTINATION_COUNTRY_NM, svcClss SERVICE_CLASS_ID, prmPkgCode PROMO_PACKAGE_CODE, prmPkgName PROMO_PACKAGE_NAME, brndSCName BRAND_NAME,
                                          '' CALL_CLASS_CAT, '' CALL_CLASS_BASE, '' CALL_CLASS_RULEID, '' CALL_CLASS_TARIF, '' MSC_ADDRESS,  originRealm ORIGINAL_REALM, originHost ORIGINAL_HOST, 
                                          '' MNC_MCC, ''LAC, '' CI, '' LACI_CLUSTER_ID, '' LACI_CLUSTER_NM, '' LACI_REGION_ID, '' LACI_REGION_NM, '' LACI_AREA_ID, '' LACI_AREA_NM, '' LACI_SALESAREA_ID, '' LACI_SALESAREA_NM, 
                                          '' IMSI, '' APN, '' SERVICE_SCENARIO, '' ROAMING_POSITION, '' FAF, '' FAF_NUMBER, '' RATING_GROUP, '' CONTENT_TYPE, '' IMEI, '' GGSN_ADDRESS, '' SGSN_ADDRESS, '' CALL_REFERENCE, '' CHARGING_ID, '' RAT_TYPE, '' SERVICE_OFFER_ID,
                                          paymentCat PAYMENT_CATEGORY, accountID ACCOUNT_ID, '' ACCOUNT_GROUP_ID, '' FAF_NAME, trafficCase TRAFFIC_CASE, trafficCaseName TRAFFIC_CASE_NAME, revCode REVENUE_CODE, 
                                          directionType DIRECTION_TYPE, distanceTp DISTANCE_TYPE, svcTp SERVICE_TYPE, svcUsgTp SERVICE_USG_TYPE, 
                                          revcodeL1 REVENUE_CODE_L1, revcodeL2 REVENUE_CODE_L2, revcodeL3 REVENUE_CODE_L3, revcodeL4 REVENUE_CODE_L4, revcodeL5 REVENUE_CODE_L5, 
                                          revcodeL6 REVENUE_CODE_L6, revcodeL7 REVENUE_CODE_L7, revcodeL8 REVENUE_CODE_L8, revcodeL9 REVENUE_CODE_L9, svcUsgDrct SVC_USG_DIRECTION, svcUsgDest SVC_USG_DESTINATION, trafficFlag TRAFFIC_FLAG, revenueFlag REVENUE_FLAG, realFileName REAL_FILENAME, 
                                          usgVol USAGE_VOLUME, usgAmount USAGE_AMOUNT, usageDuration USAGE_DURATION, hit HIT, '' ACCUMULATOR, '' COMMUNITY_ID_1, '' COMMUNITY_ID_2, '' COMMUNITY_ID_3, '' ACCUMULATED_COST, 
                                          recType RECORD_TYPE, triggerTime TRIGGER_TIME, '' EVENT_TIME, '' RECORD_ID_NUMBER, 
                                          acValBfr CCR_CCA_ACCOUNT_VALUE_BEFORE, acValAfr CCR_CCA_ACCOUNT_VALUE_AFTER, '' ECI, '' UNIQUE_KEY, '' SITE_TECH, '' SITE_OPERATOR, 
                                          '' MGR_SVCCLSS_ID, '' OFFER_ID, '' OFFER_ATTR_KEY, '' OFFER_ATTR_VALUE, '' OFFER_AREA_NAME,
                                          '' DA_UNIT_AMOUNT, '' DA_UNIT_TYPE,
                                          jobID JOB_ID, key RCRD_ID, prcDt PRC_DT, 'CS3' SRC_TP, fileDate FILEDATE
                                          FROM FinalCS3 
                                          WHERE paymentCat = 'PREPAID' and prmPkgCode <> '' and  
                                          aPrefix <> ''  and svcTp <> '' 
                                          and madaName <> '' """)//.repartition(3)
    
    
    prepaidCS3DF.show()*/
    //prepaidCS3DF.write.partitionBy("trgrdate").mode("append").format("com.databricks.spark.csv").option("delimiter", "|").save("/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/CS3_SIT_PREPAID2")
    //prepaidCS3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").partitionBy("trgrdate").save("/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/CS3_SIT_PREPAID3")                                      
   
    //prepaidCS3DF.write.partitionBy("TRANSACTION_DATE").mode("append").save("/user/apps/CS3/output/prepaidnew")
    //prepaidCS3DF.write.mode("append").save("/user/apps/CS3/output/prepaidnew")
    
    
    
   
           /*                          
    val postpaidCS3DF = sqlContext.sql("""SELECT trgrdate TRANSACTION_DATE, trgrtime TRANSACTION_HOUR, APartyNo A_PARTY_NUMBER, 
                                          aPrefix A_PREFIX, svcCityName SERVICE_CITY_NAME, svcProviderID SERVICE_PROVIDER_ID, 
                                          svcCtyName SERVICE_COUNTRY_NM, hlrBranchName HLR_BRANCH_NM, hlrRegionName HLR_REGION_NM, 
                                          BPartyNo B_PARTY_NUMBER, bPrefix B_PREFIX, dstCityName DESTINATION_CITY_NAME, dstProviderID DESTINATION_PROVIDER_ID, 
                                          dstCtyName DESTINATION_COUNTRY_NM, svcClss SERVICE_CLASS_ID, prmPkgCode PROMO_PACKAGE_CODE, prmPkgName PROMO_PACKAGE_NAME, brndSCName BRAND_NAME,
                                          '' CALL_CLASS_CAT, '' CALL_CLASS_BASE, '' CALL_CLASS_RULEID, '' CALL_CLASS_TARIF, '' MSC_ADDRESS,  originRealm ORIGINAL_REALM, originHost ORIGINAL_HOST, 
                                          '' MNC_MCC, ''LAC, '' CI, '' LACI_CLUSTER_ID, '' LACI_CLUSTER_NM, '' LACI_REGION_ID, '' LACI_REGION_NM, '' LACI_AREA_ID, '' LACI_AREA_NM, '' LACI_SALESAREA_ID, '' LACI_SALESAREA_NM, 
                                          '' IMSI, '' APN, '' SERVICE_SCENARIO, '' ROAMING_POSITION, '' FAF, '' FAF_NUMBER, '' RATING_GROUP, '' CONTENT_TYPE, '' IMEI, '' GGSN_ADDRESS, '' SGSN_ADDRESS, '' CALL_REFERENCE, '' CHARGING_ID, '' RAT_TYPE, '' SERVICE_OFFER_ID,
                                          paymentCat PAYMENT_CATEGORY, accountID ACCOUNT_ID, '' ACCOUNT_GROUP_ID, '' FAF_NAME, trafficCase TRAFFIC_CASE, trafficCaseName TRAFFIC_CASE_NAME, revCode REVENUE_CODE, 
                                          directionType DIRECTION_TYPE, distanceTp DISTANCE_TYPE, svcTp SERVICE_TYPE, svcUsgTp SERVICE_USG_TYPE, 
                                          revcodeL1 REVENUE_CODE_L1, revcodeL2 REVENUE_CODE_L2, revcodeL3 REVENUE_CODE_L3, revcodeL4 REVENUE_CODE_L4, revcodeL5 REVENUE_CODE_L5, 
                                          revcodeL6 REVENUE_CODE_L6, revcodeL7 REVENUE_CODE_L7, revcodeL8 REVENUE_CODE_L8, revcodeL9 REVENUE_CODE_L9, svcUsgDrct SVC_USG_DIRECTION, svcUsgDest SVC_USG_DESTINATION, trafficFlag TRAFFIC_FLAG, revenueFlag REVENUE_FLAG, realFileName REAL_FILENAME, 
                                          usgVol USAGE_VOLUME, usgAmount USAGE_AMOUNT, usageDuration USAGE_DURATION, hit HIT, '' ACCUMULATOR, '' COMMUNITY_ID_1, '' COMMUNITY_ID_2, '' COMMUNITY_ID_3, '' ACCUMULATED_COST, 
                                          recType RECORD_TYPE, triggerTime TRIGGER_TIME, '' EVENT_TIME, '' RECORD_ID_NUMBER, 
                                          acValBfr CCR_CCA_ACCOUNT_VALUE_BEFORE, acValAfr CCR_CCA_ACCOUNT_VALUE_AFTER, '' ECI, '' UNIQUE_KEY, '' SITE_TECH, '' SITE_OPERATOR, 
                                          '' MGR_SVCCLSS_ID, '' OFFER_ID, '' OFFER_ATTR_KEY, '' OFFER_ATTR_VALUE, '' OFFER_AREA_NAME,
                                          '' DA_UNIT_AMOUNT, '' DA_UNIT_TYPE,
                                          jobID JOB_ID, key RCRD_ID, prcDt PRC_DT, 'CS3' SRC_TP, fileDate FILEDATE
                                          FROM FinalCS3 
                                          WHERE paymentCat = 'POSTPAID' and  
                                          aPrefix <> ''  and svcTp <> '' """)//.repartition(3)
                                          
    //postpaidCS3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/CS3_SIT_POSTPAID")
    //postpaidCS3DF.show()
    postpaidCS3DF.write.partitionBy("TRANSACTION_DATE").mode("append").save("/user/apps/CS3/output/postpaidnew")
    //postpaidCS3DF.write.mode("append").save("/user/apps/CS3/output/postpaidnew")
    
    val postpaidCS3exaDF = sqlContext.sql("""SELECT APartyNo servedMSISDN, trgrdate CCRtrgrTime, revenueFlag, '' svcIdentifier, svcClss, '' ccrSvcScenario, '' ccrRoamingPost, '' ccrCCAfafID, '' ccrCCAda, '' ccrCCAacValDeducted,
                                           '' ccrTreeDefinedFields, '' accCostAmt, '' bonusAdjValueChange, '' recNumber, '' chargingContextID, realFileName, triggerTime,  '' MNCMCC, '' LAC, '' CI, '' APN, usageDuration, nvl(usgVol,0) usgVol, 
                                           '' eventDuration, '' ratGrp, '' contentTp, '' ratTp, '' IMEI, '' eventTime, '' IMSI, '' ggsnIPaddr, '' sgsnIPaddr, '' mscIPaddr, '' callRef, '' chargingID, 
                                           originRealm, originHost, aPrefix, svcCityName, svcProviderID, BPartyNo otherPartyNo, bPrefix, dstCityName, destProviderID, hlrBranchName, hlrRegionName, '' costband, '' costbandNm, '' callTp, '' callTpNm, '' location, '' locNm, '' discount,
                                          '' ratTpNm, '' evID, '' postSvcNm, '' axis1, '' axis2, '' axis3, '' axis4, '' axis5,  revCode, revcodeL4 gl_code, revcodeL5 gl_acc, svcTp, '' svcTpDA1, '' svcTpDA2,
                                          accountID, nvl(usgAmount,0) usgAmount, svcTp svcUsgTp, '' srcID, prcDt loadDate, '' loadJobID,
                                          jobID, key, prcDt, 'CS3' srcType, fileDate
                                          FROM FinalCS3 
                                          WHERE paymentCat = 'POSTPAID' and  
                                          aPrefix <> ''  and svcTp <> '' """)//.show()                                 
    postpaidCS3exaDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/postpaidEXADATA/"+prcDt+jobID)                                      
    //postpaidCS3exaDF.show() 
    //postpaidCS3exaDF.write.partitionBy("trgrdate").mode("append").save("/user/apps/CS3/output/postpaidEXADATA")
    
    */
		if (reprocessFlag == "0")
		{
        val cdrInqPrepaidDF = sqlContext.sql("""SELECT key, subsID, triggerTime transStartTime, '' transEndTime, case when originRealm like '%mms%' then 'MMS' else 'SMS' end trafficType, 
        																			directionType, '' roamingArea, '' callClassID, svcClss, paymentCat, prmPkgName,
                                              case when trafficCase in (20,3) then APartyNo else BPartyNo end callingNo,
    																					case when trafficCase in (20,3) then concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) else concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) end callingNoDesc,																					 			
    																					case when trafficCase in (20,3) then BPartyNo else APartyNo end calledNo,
                                              case when trafficCase in (20,3) then concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) else concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) end calledNoDesc,																					 			
    																					usageDuration, finalCharge, acValBfr, acValAfr,  
    																					case when daUsed = 0 and daInfo = 'REGULER' then '' else concat("DA",daInfo) end daDetails,
    																					accValueInfo, case when trafficCase in (20,3) then svcCityName else dstCityName end locNo, 
                                              nodeID, case when trafficCase in (20,3) then svcProviderID else dstProviderID end svcProviderID, usgVol volume,
    																					'' fafIndicator, '' commID1, '' commID2, '' commID3, '' acGrpID, '' svcOffering, '' lac, '' ci, '' clusterNm, '' ntwrkTp, '' rtgGrp, '' mmcmnc, 'Charging' svcType,
    																					realFileName, 
                                              case when daUsed = 0 and daInfo = 'REGULER' then extText
                                                   when daUsed = 1 then concat(extText,accountID) end revCode, revCode revCode2,
                                              jobID
                                              FROM FinalCS3 
                                              WHERE paymentCat = 'PREPAID' 
                                              and (daUsed = 0 or (daUsed <> 0 and daInfo <> 'REGULER')) """)
                                              
                                              
        //cdrInqPrepaidDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/prepaidCDRINQ/"+prcDt+jobID)                                      
        cdrInqPrepaidDF.show()
        //cdrInqPrepaidDF.write.partitionBy("trgrdate").mode("append").save("/user/apps/CS3/output/prepaidCDRINQ")
        
        val cdrInqPostpaidDF = sqlContext.sql("""SELECT key, subsID, triggerTime transStartTime, '' transEndTime, case when originRealm like '%mms%' then 'MMS' else 'SMS' end trafficType, 
        																			directionType, '' roamingArea, '' callClassID, svcClss, paymentCat, prmPkgName,
                                              case when trafficCase in (20,3) then APartyNo else BPartyNo end callingNo,
    																					case when trafficCase in (20,3) then concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) else concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) end callingNoDesc,																					 			
    																					case when trafficCase in (20,3) then BPartyNo else APartyNo end calledNo,
                                              case when trafficCase in (20,3) then concat(bPrefix,';',dstCityName,';',dstProviderID,';',dstCtyName) else concat(aPrefix,';',svcCityName,';',svcProviderID,';',svcCtyName) end calledNoDesc,																					 			
    																					usageDuration, finalCharge, acValBfr, acValAfr,  
    																					case when daUsed = 0 and daInfo = 'REGULER' then '' else concat("DA",daInfo) end daDetails,
    																					accValueInfo, case when trafficCase in (20,3) then svcCityName else dstCityName end locNo, 
                                              nodeID, case when trafficCase in (20,3) then svcProviderID else dstProviderID end svcProviderID, usgVol volume,
    																					'' fafIndicator, '' commID1, '' commID2, '' commID3, '' acGrpID, '' svcOffering, '' lac, '' ci, '' clusterNm, '' ntwrkTp, '' rtgGrp, '' mmcmnc, 'Charging' svcType,
    																					realFileName, 
                                              case when daUsed = 0 then extText
                                                   when daUsed = 1 then concat(extText,accountID) end revCode, revCode revCode2, extText,
                                              jobID
                                              FROM FinalCS3 
                                              WHERE paymentCat = 'POSTPAID' 
                                              and (daUsed = 0 or (daUsed <> 0 and daInfo <> 'REGULER')) """)//.show()
        
        //cdrInqPostpaidDF.write.partitionBy("trgrdate").mode("append").save("/user/apps/CS3/output/postpaidCDRINQ")
        //cdrInqPostpaidDF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/postpaidCDRINQ/"+prcDt+jobID)                                      
        cdrInqPostpaidDF.show()
		}
        /*                                  
    /* Filtering to Reject Reference
     * Rules : not null Promo Package (for PREPAID), not null Interconnect, not null Usage Type, not null Revenue Flag 
     */
    val rejRefCS3DF = sqlContext.sql("""SELECT trgrdate, trgrtime, APartyNo, BPartyNo, svcClss, originRealm, originHost, paymentCat, trafficCase, trafficCaseName, directionType, usageDuration,
                                          APartyNoPad, BPartyNoPad, realFileName, usgVol, triggerTime, acValBfr, acValAfr, svcUsgDrct, svcUsgDest, hit, finalCharge,    
                                          extText, daUsed, subsID, nodeID, accValueInfo, fileDate, key, daInfo, accountID, revCode, usgAmount, SID, prcDt, jobID,
                                          case when paymentCat = 'PREPAID' and prmPkgCode = '' then 'Service Class not found' 
                                          		 when aPrefix = '' then 'Origin Interconnect not found'
                                          		 when svcTp = '' then 'Revenue Code not found'
                                          		 when paymentCat = 'PREPAID' and madaName = '' then 'MA DA not found'
                                         	end rejReason
                                          FROM FinalCS3 a
                                          WHERE (paymentCat = 'PREPAID' and prmPkgCode = '') or  
                                          aPrefix = ''  or svcTp = '' 
                                          or (paymentCat = 'PREPAID' and madaName = '') """)//.show()
                                          
    rejRefCS3DF.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/reject/ref/"+prcDt+jobID)                                      
    
    //rejRefCS3DF.write.partitionBy("trgrdate").mode("append").save("/user/apps/CS3/reject/ref")
                                      
    //prepaidCS3DF.write.partitionBy("trgrdate").mode("append").save("/user/apps/CS3/output/test_partition_date")
    
    */
    
    sc.stop();
    
  }
    
}
