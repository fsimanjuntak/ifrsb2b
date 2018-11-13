package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import scala.io.Source
import scala.util.control.Breaks._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner
import com.ibm.id.isat.utils._
import java.util.Calendar
import java.text.SimpleDateFormat
import com.ibm.id.isat.usage.cs3._
import com.ibm.id.isat.utils._
import org.apache.spark.sql.types.{StructType, StructField, StringType}


object TestInttct  {
  def main(args: Array[String]): Unit = {
    
    //val jobID = args(0)
    val jobID = "1238"
    val prcDt = "20160623"
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
    import sqlContext.implicits._
    //-------------------------------------------
    //----------------------------------------
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
    cs3DF.persist()       
    
    
    val intcct = new Interconnect("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_10.txt")
    sqlContext.udf.register("getIntcct", intcct.getIntcct2 _)
    sqlContext.udf.register("getTupple", Common.getTuple _)
    sqlContext.udf.register("getIntcctWithSID", intcct.getIntcctWithSID _)
    
     
    val value = "6281105057"
    val date = "20160501"
    
    val intcctrdd = new InterconnectRDD(sc,"C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_all.txt")
    //println("hasil1 : " + intcctrdd.getIntcctBasedLength(13, "6283194065420", "20160501","SID"))
    //println("hasil3 : " + intcctrdd.getIntcctBasedLength(7, "6285692", "20160501","SID"))
    //println("hasil2 : " + intcctrdd.getIntcct("6285709297018", "20160423","SID"))    
    //println("hasil4 : " + intcctrdd.getIntcct("933399", "20160501","93339095007005SID"))
    
    val intcctr2 = new Interconnect2(sc,"C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_all.txt")
    
    //println("hasil intcct2 : " + intcctr2.getIntcctFinal("6285709297018", "20160423","SID"))
    //println("hasil intcct2 2 : " + intcctr2.getIntcctWithSIDFinal("335502", "20160423","33550027008002SID"))
    //eventTypeList.foreach { a => println(a) }
    //intcctArr.foreach { a => println(a(0)) }
    
    //println(intcct.getIntcct(value, date))
    
    val intcctr3 = new Interconnect3(sc,"C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_all.txt")
    println("hasil intcctr3 : " + intcctr3.getIntcct("6285709297018", "20160423"))//
    println("hasil intcctr3 2 : " + intcctr3.getIntcct("6285694200050", "20160423"))
    println("hasil intcctr3 3 : " + intcctr3.getIntcctWithSID("335502", "20160423","33550027008002SID"))
    
    
    val joinDF = sqlContext.sql("""select a.APartyNo, 
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
                                  from CS3 a""")
    joinDF.registerTempTable("temp")    
    joinDF.show()
    
    val joinDF2 = sqlContext.sql("""select a.APartyNo, getTupple(Aintcct,"|",0),getTupple(Aintcct,"|",1),getTupple(Aintcct,"|",2),getTupple(Aintcct,"|",3),
                                           getTupple(Bintcct,"|",0),getTupple(Bintcct,"|",1),getTupple(Bintcct,"|",2),getTupple(Bintcct,"|",3)   
                 from temp a""")
    joinDF2.show()
    
    //val intcctDF = ReferenceDF.getInterconnectDF(sqlContext)   
    //intcctDF.registerTempTable("Interconnect") 
    //sqlContext.cacheTable("Interconnect")
    
    
                 
    sc.stop(); 
  }
  
  
   class InterconnectRDD(sc: SparkContext, path: String) extends java.io.Serializable {
        val intcctRDD = sc.textFile(path)
        //intcctRDD.persist()
        
        val intcct14RDD = intcctRDD.filter(line => line.split("\\|")(0) == "14" ).map(line => line.split("\\|"))
        val intcct13RDD = intcctRDD.filter(line => line.split("\\|")(0) == "13" ).map(line => line.split("\\|"))
        val intcct12RDD = intcctRDD.filter(line => line.split("\\|")(0) == "12" ).map(line => line.split("\\|"))
        val intcct11RDD = intcctRDD.filter(line => line.split("\\|")(0) == "11" ).map(line => line.split("\\|"))
        val intcct10RDD = intcctRDD.filter(line => line.split("\\|")(0) == "10" ).map(line => line.split("\\|"))
        val intcct9RDD = intcctRDD.filter(line => line.split("\\|")(0) == "9" ).map(line => line.split("\\|"))
        val intcct8RDD = intcctRDD.filter(line => line.split("\\|")(0) == "8" ).map(line => line.split("\\|"))
        val intcct7RDD = intcctRDD.filter(line => line.split("\\|")(0) == "7" ).map(line => line.split("\\|"))
        val intcct6RDD = intcctRDD.filter(line => line.split("\\|")(0) == "6" ).map(line => line.split("\\|"))
        val intcct5RDD = intcctRDD.filter(line => line.split("\\|")(0) == "5" ).map(line => line.split("\\|"))
        val intcct4RDD = intcctRDD.filter(line => line.split("\\|")(0) == "4" ).map(line => line.split("\\|"))
        val intcct3RDD = intcctRDD.filter(line => line.split("\\|")(0) == "3" ).map(line => line.split("\\|"))
        val intcct2RDD = intcctRDD.filter(line => line.split("\\|")(0) == "2" ).map(line => line.split("\\|"))
        val intcct1RDD = intcctRDD.filter(line => line.split("\\|")(0) == "1" ).map(line => line.split("\\|"))
        
        val intcctMapRDD = Map(14 -> intcct14RDD, 13 -> intcct13RDD, 12 -> intcct12RDD, 11 -> intcct11RDD, 
                                10 -> intcct10RDD, 9 -> intcct9RDD, 8 -> intcct8RDD,
                                7 -> intcct7RDD, 6 -> intcct6RDD, 5 -> intcct5RDD, 4 -> intcct4RDD,
                                3 -> intcct3RDD, 2 -> intcct2RDD, 1 -> intcct1RDD
                                );
    
       // intcct13RDD.collect().toList                        
        //&& sid == col(7)
         def getIntcctBasedLength(lengthType: Int, msisdn: String, eventDate: String, sid: String): String = {
            try {
              
            println (lengthType + "::" + msisdn + "," + sid + "," + eventDate + "," + intcct13RDD.collect().toList.size)
            println("size : " + intcct13RDD.collect().toList.size)
            val intcctRDD = intcctMapRDD.getOrElse(lengthType, intcct1RDD)
            println ("1")
            val resultRDD = intcctRDD.filter(col => col(1) == msisdn && col(5) <= eventDate && col(6) >= eventDate).first()
            println ("2")
            //intcct13RDD.foreach(a => "print 13 intct : " + println( a(1) + "|" + a(2) + "|" + a(3) + "|" +  a(4) + "|" +  a(7) ))
            return resultRDD(1) + "|" + resultRDD(2) + "|" + resultRDD(3) + "|" + resultRDD(4) 
            }
            
            catch {
        	    case e: Exception => return e.getMessage
        	  }
            
         }
         
         def getIntcct(msisdn: String, eventDate: String, sid: String): String = {
             var result = "|||"
             var final_result = "|||"
             
             for (length <- 14 until 0 by -1)
             {     //println(length)
               if(msisdn.length >= length)
                   {
                     //println(length + ":" + msisdn.substring(0,length) )
                     result = getIntcctBasedLength(length, msisdn.substring(0,length), eventDate, sid)
                     println("resultt : " + result)
                     if(!result.isEmpty() || result != "" )
                     {     
                       println("masuk result")
                       final_result = result
                       return final_result
                     } 
                       
                   }
             }  
               
             return final_result
         }
         
   }
   
   class Interconnect( eventTypeFileName:String ) extends java.io.Serializable {
   
   val intcct13List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_13.txt").getLines.toList
   val intcct13ListArr = intcct13List.map( line => getIntcctArr(line) )
   
   val intcct12List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_12.txt").getLines.toList
   val intcct12ListArr = intcct12List.map( line => getIntcctArr(line) )
   
   val intcct11List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_11.txt").getLines.toList
   val intcct11ListArr = intcct11List.map( line => getIntcctArr(line) )
   
   val intcct10List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_10.txt").getLines.toList
   val intcct10ListArr = intcct10List.map( line => getIntcctArr(line) )
   
   val intcct9List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_9.txt").getLines.toList
   val intcct9ListArr = intcct9List.map( line => getIntcctArr(line) )
   
   val intcct8List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_8.txt").getLines.toList
   val intcct8ListArr = intcct8List.map( line => getIntcctArr(line) )
   
   val intcct7List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_7.txt").getLines.toList
   val intcct7ListArr = intcct7List.map( line => getIntcctArr(line) )
   
   val intcct6List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_6.txt").getLines.toList
   val intcct6ListArr = intcct6List.map( line => getIntcctArr(line) )
   
   val intcct5List = Source.fromFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Reference/ref_intcct_length_5.txt").getLines.toList
   val intcct5ListArr = intcct5List.map( line => getIntcctArr(line) )
   
   val intcctMap = Map(13 -> intcct13ListArr, 12 -> intcct12ListArr, 11 -> intcct11ListArr, 
                            10 -> intcct10ListArr, 9 -> intcct9ListArr, 8 -> intcct8ListArr,
                            7 -> intcct7ListArr, 6 -> intcct6ListArr, 5 -> intcct5ListArr
                            );
   
   /*
     * Return as array
     */
    def getIntcctArr ( line:String ) : Array[String] = {
      return Array(
          Common.getTuple(line, "|", 0),
          Common.getTuple(line, "|", 1),
          Common.getTuple(line, "|", 2),
          Common.getTuple(line, "|", 3),
          Common.getTuple(line, "|", 4),
          Common.getTuple(line, "|", 5),
          Common.getTuple(line, "|", 6)
          )
    }
   
   def getIntcct( msisdn: String, eventDate: String) : String = {
      
     var result = ""
     
      intcct13ListArr.foreach { a => 
      if ( msisdn == a(0) && eventDate >= a(4) && eventDate <= a(5))
      {
        result = a(0) + "|" + a(1) + "|" + a(2) + "|" + a(3)
        return result
      }      
    }
     println("masuk")
      return "aa"
    }
   
   def getIntcct2(lengthType: Int, msisdn: String, eventDate: String): String = {
        
      val intcctListArr = intcctMap.getOrElse(lengthType, intcct13ListArr)
      intcctListArr.foreach { a => 
          if ( msisdn == a(0) && eventDate >= a(4) && eventDate <= a(5))
          {
            return a(0) + "|" + a(1) + "|" + a(2) + "|" + a(3)              
          }    
       }
      
      return null
   }
   
   def getIntcctWithSID(lengthType: Int, msisdn: String, eventDate: String, sid: String): String = {
        
      val intcctListArr = intcctMap.getOrElse(lengthType, intcct13ListArr)
      intcctListArr.foreach { a => 
          if ( msisdn == a(0) && sid == a(6) && eventDate >= a(4) && eventDate <= a(5))
          {
            return a(0) + "|" + a(1) + "|" + a(2) + "|" + a(3)              
          }    
       }
      
      return null
   }
   
   def getIntcct(msisdn: String, eventDate: String, sid: String): String = {
     var result = ""
     
     for (length <- 14 to 1)
     {
         result = getIntcct2(length, msisdn, eventDate)
         if(!result.isEmpty())
           return result
     }  
       
          
      
      
      return null
   }
   
   
} 
    
    
   class Interconnect2(sc: SparkContext,  path:String ) extends java.io.Serializable {
   
   val intcctRDD = sc.textFile(path)
    intcctRDD.persist()
    
    val intcct14RDD = intcctRDD.filter(line => line.split("\\|")(0) == "14" ).map(line => line.split("\\|")).collect().toList
    val intcct13RDD = intcctRDD.filter(line => line.split("\\|")(0) == "13" ).map(line => line.split("\\|")).collect().toList
    val intcct12RDD = intcctRDD.filter(line => line.split("\\|")(0) == "12" ).map(line => line.split("\\|")).collect().toList
    val intcct11RDD = intcctRDD.filter(line => line.split("\\|")(0) == "11" ).map(line => line.split("\\|")).collect().toList
    val intcct10RDD = intcctRDD.filter(line => line.split("\\|")(0) == "10" ).map(line => line.split("\\|")).collect().toList
    val intcct9RDD = intcctRDD.filter(line => line.split("\\|")(0) == "9" ).map(line => line.split("\\|")).collect().toList
    val intcct8RDD = intcctRDD.filter(line => line.split("\\|")(0) == "8" ).map(line => line.split("\\|")).collect().toList
    val intcct7RDD = intcctRDD.filter(line => line.split("\\|")(0) == "7" ).map(line => line.split("\\|")).collect().toList
    val intcct6RDD = intcctRDD.filter(line => line.split("\\|")(0) == "6" ).map(line => line.split("\\|")).collect().toList
    val intcct5RDD = intcctRDD.filter(line => line.split("\\|")(0) == "5" ).map(line => line.split("\\|")).collect().toList
    val intcct4RDD = intcctRDD.filter(line => line.split("\\|")(0) == "4" ).map(line => line.split("\\|")).collect().toList
    val intcct3RDD = intcctRDD.filter(line => line.split("\\|")(0) == "3" ).map(line => line.split("\\|")).collect().toList
    val intcct2RDD = intcctRDD.filter(line => line.split("\\|")(0) == "2" ).map(line => line.split("\\|")).collect().toList
    val intcct1RDD = intcctRDD.filter(line => line.split("\\|")(0) == "1" ).map(line => line.split("\\|")).collect().toList
      
   
   val intcctMapRDD = Map(14 -> intcct14RDD, 13 -> intcct13RDD, 12 -> intcct12RDD, 11 -> intcct11RDD, 
                                10 -> intcct10RDD, 9 -> intcct9RDD, 8 -> intcct8RDD,
                                7 -> intcct7RDD, 6 -> intcct6RDD, 5 -> intcct5RDD, 4 -> intcct4RDD,
                                3 -> intcct3RDD, 2 -> intcct2RDD, 1 -> intcct1RDD
                                );
   
   /*
     * Return as array
     */
    def getIntcctArr ( line:String ) : Array[String] = {
      return Array(
          Common.getTuple(line, "|", 0),
          Common.getTuple(line, "|", 1),
          Common.getTuple(line, "|", 2),
          Common.getTuple(line, "|", 3),
          Common.getTuple(line, "|", 4),
          Common.getTuple(line, "|", 5),
          Common.getTuple(line, "|", 6)
          )
    }
   
    
   def getIntcct( msisdn: String, eventDate: String) : String = {
      
     var result = ""
     
      intcct1RDD.foreach { a => 
      if ( msisdn == a(0) && eventDate >= a(4) && eventDate <= a(5))
      {
        result = a(0) + "|" + a(1) + "|" + a(2) + "|" + a(3)
        return result
      }      
    }
     println("masuk")
      return "aa"
    }
   
   def getIntcct2(lengthType: Int, msisdn: String, eventDate: String): String = {
        
      val intcctListArr = intcctMapRDD.getOrElse(lengthType, intcct1RDD)
      println("length : " + lengthType + ",size:" + intcctListArr.size)
      intcctListArr.foreach { a => 
          //println(a(1) )
          if ( msisdn == a(1) && eventDate >= a(5) && eventDate <= a(6))
          {
            return a(1) + "|" + a(2) + "|" + a(3) + "|" + a(4)              
          }    
       }
      
      return ""
   }
   
   def getIntcctWithSID(lengthType: Int, msisdn: String, eventDate: String, sid: String): String = {
        
     val intcctListArr = intcctMapRDD.getOrElse(lengthType, intcct1RDD)
      println("length : " + lengthType + ",size:" + intcctListArr.size)
      intcctListArr.foreach { a => 
          //println(a(1) )
          if ( msisdn == a(1) && eventDate >= a(5) && eventDate <= a(6) && sid == a(7))
          {
            return a(1) + "|" + a(2) + "|" + a(3) + "|" + a(4)              
          }    
       }
      
      return ""
   }
   
 
   def getIntcctFinal(msisdn: String, eventDate: String, sid: String): String = {
         var result = "|||"
         var final_result = "|||"
         
         for (length <- 14 until 0 by -1)
         {     //println(length)
           if(msisdn.length >= length)
               {
                 //println(length + ":" + msisdn.substring(0,length) )
                 result = getIntcct2(length, msisdn.substring(0,length), eventDate)
                 println("resultt : " + result)
                 if(!result.isEmpty()  )
                 {     
                   println("masuk result")
                   final_result = result
                   return final_result
                 } 
                   
               }
         }  
           
         return final_result
     }
   
   def getIntcctWithSIDFinal(msisdn: String, eventDate: String, sid: String): String = {
         var result = "|||"
         var final_result = "|||"
         
         for (length <- 14 until 0 by -1)
         {     //println(length)
           if(msisdn.length >= length)
               {
                 //println(length + ":" + msisdn.substring(0,length) )
                 result = getIntcctWithSID(length, msisdn.substring(0,length), eventDate, sid)
                 println("resultt : " + result)
                 if(!result.isEmpty()  )
                 {     
                   println("masuk result")
                   final_result = result
                   return final_result
                 } 
                   
               }
         }  
           
         return final_result
     }
   
   
} 
   
   class Interconnect3(sc: SparkContext,  path:String ) extends java.io.Serializable {
   
   val intcctRDD = sc.textFile(path)
   intcctRDD.persist()
    
   val intcct = intcctRDD.map(line => line.split("\\|")).collect().toList
   
   
   def getIntcct(msisdn: String, eventDate: String): String = {
        
      //println("size:" + intcct.size)
      intcct.foreach { a => 
          //println(a(1) )
          var msisdn_new =  if(msisdn.length >= a(0).toInt ) msisdn.substring(0,a(0).toInt)  else msisdn 
          if ( msisdn_new == a(1) && eventDate >= a(5) && eventDate <= a(6))
          {
            return a(1) + "|" + a(2) + "|" + a(3) + "|" + a(4)              
          }    
       }
      
      return "|||"
   }
   
   
   def getIntcctWithSID(msisdn: String, eventDate: String, sid: String): String = {
        
     //println("size:" + intcct.size)
      intcct.foreach { a => 
          //println(a(1) )
          var msisdn_new =  if(msisdn.length >= a(0).toInt ) msisdn.substring(0,a(0).toInt)  else msisdn 
          if ( msisdn_new == a(1) && eventDate >= a(5) && eventDate <= a(6) && sid == a(7))
          {
            return a(1) + "|" + a(2) + "|" + a(3) + "|" + a(4)              
          }    
       }
      
      return "|||"
   }
   
 
   
   
   
} 
   
}