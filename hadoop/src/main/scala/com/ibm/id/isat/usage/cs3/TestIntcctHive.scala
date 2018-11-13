package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner
import com.ibm.id.isat.utils._
import java.util.Calendar
import java.text.SimpleDateFormat
import com.ibm.id.isat.usage.cs3._
import com.ibm.id.isat.utils._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.expressions.Window

object TestIntcctHive {
  def main(args: Array[String]): Unit = {
    
    //val jobID = args(0)
    val jobID = "1238"
    val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3.txt"
    //val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/rej ref/*"
    //val inputDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3_SIT.txt"
    //val inputDir = "/user/apps/CS3/input/sample_conv_output_CS3_SIT.txt"
    //val inputDir = "/user/apps/CS3/input/sample_conv_output_CS3.txt"
    //val inputDir = args(1)
    //val refDir = args(2)
    //val refDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/sdp_offer.txt"
    
    println("Start Job ID : ");    
    
    val sc = new SparkContext("local", "CS3 Transform", new SparkConf());
    //val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._
    
    //val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    //hc.setConf("hive.exec.dynamic.partition", "true") 
    //hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict") 
    
    /*--- Get Population Date Time ---*/
    val dateFormat = new SimpleDateFormat("yyyyMMdd");    //("yyyy/MM/dd HH:mm:ss");    
    val prcDt = dateFormat.format(Calendar.getInstance.getTime)
    
    val inputRDD = sc.textFile(inputDir)    
    val splitRDD = inputRDD.map(line => line.split("\t")).map(col => col(1).concat("|" + col(2) + "|" + col(3)))
    
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
        + "|" + prcDt + "|" + jobID ))
    
    /*  
     * Create CS3 Data Frame 
     */
    val cs3Row = afterTransposeRDD.map(line => line.split("\\|")).map(p => Row(p(0), p(1), p(2), 
    p(3), p(4), p(5), p(6), p(7),
    p(8), p(9), p(10), Common.toInt(p(11)), p(12),
    p(13), p(14), Common.toInt(p(15)), p(16),
    p(17), p(18), p(19), p(20),
    Common.toInt(p(21)), p(22), p(23), p(24), p(25), p(26), p(27), p(28), p(29), p(30),
    p(31), p(32), Common.toDouble(p(33)), p(34), p(35), p(36)))  
    val cs3DF = sqlContext.createDataFrame(cs3Row, CS3Schema.CS3Schema)//.withColumn("id", monotonicallyIncreasingId()) 
    cs3DF.registerTempTable("CS3")
    //cs3DF.saveAsTable("revenue.CS3")
    cs3DF.persist()                                      
    //cs3DF.show()
    val intcctDF = ReferenceDF.getIntcctAllDF(sqlContext)
    intcctDF.registerTempTable("intcct")
    intcctDF.persist()
    intcctDF.count()
    //intcctDF.show()
    
    /*val results = hc.sql("""select * from (
                  select a.*, b.intcctPfx, b.intcctCty, b.intcctOpr, b.locName,
                  rank() over (partition by a.key order by length(b.intcctPfx) desc) rank from
                  (select * from CS3) a left join intcct b 
                  where substring(a.APartyNo,1,cast( b.length as int)) == b.intcctPfx and
                  and a.trgrdate >= effDt and a.trgrdate <= endDt) a where a.rank == 1""");*/
    
    //val results = hc.sql("""select trgrdate, sum(usgAmount) from revenue.CS3TransformPrepaid group by trgrdate""");
    
    val overCategory = Window.partitionBy("key").orderBy("length") //partition by a.key order by length(b.intcctPfx) desc
    
    /*val results = sqlContext.sql("""select * from (
                  select a.*, b.intcctPfx, b.intcctCty, b.intcctOpr, b.locName,
                  dense_rank().over(overCategory) rank from
                  (select * from CS3) a left join intcct b 
                  on substring(a.APartyNo,1,cast( b.length as int)) = b.intcctPfx 
                  and a.trgrdate >= effDt and a.trgrdate <= endDt) a where a.rank = 1""")*/
                  
    val results = sqlContext.sql("""select a.* from (
                  select a.*,b.length, b.intcctPfx, b.intcctCty, b.intcctOpr, b.locName
                  from
                  (select * from CS3) a left join intcct b 
                  on a.APartyNo like concat(b.intcctPfx,'%')
                  and a.trgrdate >= effDt and a.trgrdate <= endDt) a """)            
    
     
    //val rank = dense_rank.over(overCategory)
    
    //val ranked = results.withColumn("rank", rank)
    //val results = sqlContext.sql("""select * from CS3""")
    results.show()
    //ranked.show()
    //results.write.format("com.databricks.spark.csv").option("delimiter", "|").save("/user/apps/CS3/output/test_partial_join/"+prcDt+jobID)
    
  }
}