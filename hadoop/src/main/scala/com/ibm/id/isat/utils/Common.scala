package com.ibm.id.isat.utils

import scala.util.control.Breaks._
import java.net._
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io._
import com.google.common.base.Splitter
import com.google.common.collect.Iterables
import com.google.common.collect.FluentIterable
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat

 /*****************************************************************************************************
 * 
 * Common functions for Spark Hadoop
 * 
 * 
 * @author IBM Indonesia for Indosat Ooredoo June 2016
 * 
 * 
 *****************************************************************************************************/ 
object Common {
   
	
   /**
    * Get tuple function
    * @param str String to split
    * @param del Delimiter
    * @param tupleNum Tuple number n to return
    * @return String by delimiter
    */
  def getTuple(str:String,del:String,tupleNum:Int):String={
	  val strSplit = Splitter.on(del).split(str)
    return Iterables.get(strSplit,tupleNum, "")  
	} 
  
     /**
    * Get tuple function
    * @param str String to split
    * @param del Delimiter
    * @param tupleNum Tuple number n to return
    * @return String by delimiter
    */
	def getTuple2(str:String,del:String,tupleNum:Int):String={
		var str_ARR = str.split("[\\"+del+"]");
		if(str_ARR.length>tupleNum){
			return str_ARR(tupleNum)
		}
		else{
			return "";
		}
	}
	
	/**
    * Get tuple function
    * @param str String to split
    * @param delLV1 Delimiter level 1
    * @param tupleLV1Num Tuple number n to return
    * @param delLV2 Delimiter level 1
    * @param tupleLV2Num Tuple number n to return
    * @return String by delimiter
	 */
	def getTupleInTuple(str:String,delLV1:String,tupleLV1Num:Int,delLV2:String,tupleLV2Num:Int):String={
	    val level1Split = Splitter.on(delLV1).split(str)
	    val level1Str = Iterables.get(level1Split, tupleLV1Num, "")
	    
	    val level2Split = Splitter.on(delLV2).split(level1Str)
	    val level2Str = Iterables.get(level2Split, tupleLV2Num, "")
	    
	    return level2Str
	}
	
		/**
    * Get tuple function
    * @param str String to split
    * @param delLV1 Delimiter level 1
    * @param tupleLV1Num Tuple number n to return
    * @param delLV2 Delimiter level 1
    * @param tupleLV2Num Tuple number n to return
    * @return String by delimiter
	 */
	def getLoanBalcDAId(str:String):String={
	    
	    val DAid=str.split("#",-1)(7).split("\\]",-1).filter(t => t.split("\\*",-1)(0)=="24007050")
	    
	    if (DAid.length >= 1){
	      return DAid(0).split("\\*",-1)(0)
	    } else {
	      return str.split("#",-1)(7).split("\\*",-1)(0)
	    }
	} 
	def getLoanBalcString(str:String):String={
	    
	    val DAid=str.split("#",-1)(7).split("\\]",-1).filter(t => t.split("\\*",-1)(0)=="24007050")
	    
	    if (DAid.length >= 1){
	      return DAid(0)
	    } else {
	      return str.split("#",-1)(7).split("\\]",-1)(0)
	    }
	}
	def getTupleLoanBalc(str:String,tupleLV1Num:Int):String={
	    return str.split("\\*",-1)(tupleLV1Num)
	}
	def getTupleInTuple2(str:String,delLV1:String,tupleLV1Num:Int,delLV2:String,tupleLV2Num:Int):String={
	  try{
  		var LV1_ARR=str.split("[\\"+delLV1+"]");
  		if(LV1_ARR.length<tupleLV1Num){
  			return "";
  		}
  			
  		var tupleLV1 =LV1_ARR(tupleLV1Num).replaceAll("[\'\"]", "");
  		var LV2_ARR=tupleLV1.split("[\\"+delLV2+"]");
  
  		if(LV2_ARR.length<tupleLV2Num){
  			return "";
  		}		
  		var tupleLV2=LV2_ARR(tupleLV2Num);
  		return tupleLV2;
	  }
	  catch {
	    case e: Exception => return ""
	  }
	}
	
	 /**
    * Hex to Integer transform 
    * @param hex Hexadecimal value
    * @return Integer value in String
    */
	def hexToInt (hex: String): String = {
	  try {
	    return java.lang.Long.parseLong(hex, 16).toString() 
	  }
	  catch {
	    case e: Exception => return ""
	  }
	}
	
 /**
  * Swap character in String by Order
  * Ex: 
  * 	str: abcde
  * 	odr: 2,1,4,5,3
  * 	output: badec
  * @param str String swap
  * @param odr Index Order
  * @return Swapped string
  */
	def swapChar(str:String,odr:String):String={
		var str_ARR = str.toCharArray;
		var odr_ARR = odr.split(',');
		var new_str_ARR = str_ARR;
		var new_ord="";
		var idx =0;
		for( x <- odr_ARR){
			var new_idx=x.toInt-1;
			
			new_ord=new_ord+str_ARR(new_idx);
			//new_ord(idx)=str_ARR(new_idx);
			idx=idx+1;
		}
		return new_ord;
		
	}
	
  	/**
   * Get value by key
   * 
  * @param str String swap
  * @param key Key string
  * @param keyDelim Delimiter between key,value pairs
  * @param valDelim Delimiter between key and value
  * @return Swapped string
	 */
	def getKeyVal(str:String,key:String,keyDelim:String,valDelim:String):String={
    val leadingStr = key+keyDelim
    val leadingIdx = str.indexOf(leadingStr)
    val leadingIdxEnd = leadingIdx+leadingStr.length()
    val endIdx = str.indexOf(valDelim, leadingIdxEnd)
    
    if (leadingIdx >= 0 && endIdx >= 0) {
      return str.substring(leadingIdxEnd, endIdx)
    } 
    else if ( leadingIdx >= 0 && endIdx < 0 ) {
      return str.substring(leadingIdxEnd)
    }
    else return ""
  }
	
	/**
	 * Get value by key
	 * 
  * @param str String swap
  * @param key Key string
  * @param keyDelim Delimiter between key,value pairs
  * @param valDelim Delimiter between key and value
  * @return Swapped string
	 */
  def getKeyVal3(str:String,key:String,keyDelim:String,valDelim:String):String={
    var value = ""
    val splitStr = str.split("[\\"+valDelim+"]")
    
    for ( item <- splitStr ) {
       if ( Common.getTuple(item, keyDelim, 0) == key )
         value = Common.getTuple(item, keyDelim, 1)
    }    
    return value
  }
  
  /**
	 * Get value by key
	 * 
  * @param str String swap
  * @param key Key string
  * @param keyDelim Delimiter between key,value pairs
  * @param valDelim Delimiter between key and value
  * @return Swapped string
	 */
  def getKeyVal5(str:String,key:String,keyDelim:String,valDelim:String):String={
    var value = ""
    val splitStr = Splitter.on(valDelim).split(str)
    val iteratorStr = FluentIterable.from(splitStr).iterator()
    
    while( iteratorStr.hasNext() ) {
       val keyValPair = iteratorStr.next()
       if ( Common.getTuple( keyValPair , keyDelim, 0) == key )
         value = Common.getTuple( keyValPair, keyDelim, 1)
    }    
    return value
  }
  
    /**
	 * Get value by key
	 * 
  * @param str String swap
  * @param key Key string
  * @param keyDelim Delimiter between key,value pairs
  * @param valDelim Delimiter between key and value
  * @return Swapped string
	 */
  def getKeyVal4(str:String,key:String,keyDelim:String,valDelim:String):String={
    var value = ""
    val splitStr = str.split("[\\"+valDelim+"]")
    var i=0
    
    while( i<splitStr.length ) {
       if ( Common.getTuple(splitStr(i), keyDelim, 0) == key )
         value = Common.getTuple(splitStr(i), keyDelim, 1)
       i=i+1
    }    
    return value
  }
  
  /**
  * Get value by key
  * 
  * @param str String swap
  * @param key Key string
  * @param keyDelim Delimiter between key,value pairs
  * @param valDelim Delimiter between key and value
  * @return Swapped string
	 */
	def getKeyVal2(str:String,key:String,keyDelim:String,valDelim:String):String={
	  try {
  	  var tmpKey:String=key+keyDelim;
  	  var keyIdx=str.indexOf(tmpKey);
  		if(keyIdx<0){
  			return "";
  		}
  		var valDelim_ARR = valDelim.toCharArray;
  		var valDelimIdx=str.length;
  		
  		var ch:Int=0;
  		while(ch < valDelim_ARR.length){
  			var tmp_idx=str.indexOf(valDelim_ARR(ch),keyIdx);
  			if(tmp_idx>0){
  				valDelimIdx=tmp_idx;
  				ch=100; // force to quit from loop
  			}
  			ch=ch+1;	
  		}	
  		var partStr = str.substring(keyIdx,valDelimIdx).split("\\"+keyDelim);
  			return partStr(1);
	  }    
    catch{
      case e: Exception => return "";
		}

	}
	
	/**
	 * Normalize MSISDN
	 * 
  * @param str MSISDN
  * @return Normalized MSISDN
	 */
   def normalizeMSISDN(str:String=null):String={
		    try{
		      
		      var new_str=str.replaceFirst("^(\\+62|062|62|19|00|0)", "")
		      if(new_str.length() <= 7)
		      {
		        //var new_str=str.replaceFirst("\\+62|062|62|19|00|0", "")
		        return new_str;
		      }
		      else
		      {
		        //var new_str=str.replaceFirst("\\+62|062|62|1962|19|00|0", "")
		        return "62"+new_str.replaceFirst("^62", "");
		      }
		    }
		    catch{
		      case e: Exception => return "";
		    }
		  }
   
    /**
    * Normalize MSISDN
    * 
    * @param str MSISDN
    * @return Normalized MSISDN
    */
   def normalizeMSISDNnon62(str:String=null):String={
		    try{
		      var new_str=str.replaceFirst("^(\\+62|062|62|1962|19|00|0)", "")
		      return new_str;
		      
		    }
		    catch{
		      case e: Exception => return "";
		    }
		  }
   
    /**
    * Normalize MSISDN
    * 
    * @param str MSISDN
    * @return Normalized MSISDN
    */
   def normalizeMSISDNNew(str:String=null):String={
		    try{
		      var new_str=str.replaceFirst("^(\\+62|062|62|1962|19|00|0)", "")
		      return "62"+new_str;
		      
		    }
		    catch{
		      case e: Exception => return "";
		    }
		  }
   
   /**
    * Normalize MSISDN
    * 
    * @param str MSISDN
    * @param delimiter Delimiter to shortcode 
    * @return Normalized MSISDN
    */
   def normalizeMSISDN2(str:String=null, delimiter:String):String={
		    try{
		      if(str.length() <= 9)
		      {
		        var new_str=str.replaceFirst("\\+62|062|62|19|00|0", "")
		        return new_str+delimiter+"SHORTCODE";
		      }
		      else
		      {
		        var new_str=str.replaceFirst("\\+62|062|62|1962|19|00|0", "")
		        return "62"+new_str+delimiter+"NONSHORTCODE";
		      }
		      
		      var new_str=str.replaceFirst("\\+62|062|62|1962|19|00|0", "").replaceFirst("\\+62|062|62|1962|19|00|0", "")
		        return "62"+new_str;
		    }
		    catch{
		      case e: Exception => return "";
		    }
		  }
  
	def getUsageCategory( svcClass: String ) : String = {
    var usgCategory = "";
    
    try{
      if (svcClass.startsWith("4") || svcClass.startsWith("7")) 
        usgCategory =  "POSTPAID"
      else
        usgCategory =  "PREPAID"
      
      return usgCategory;
    }
    catch{
      case e: Exception => return "";
    }
    
   
   }
	

  //var tmap = Map("100"->"satu","200"->"Dua","300"->"tiga");

  /**
   * 
   */
  def swapChar2(str:String=null,odr:String):String={
    try{
      var str_ARR = str.toCharArray;
      var odr_ARR = odr.split(',');
      
      
      for( x <- odr_ARR){
        var tmp_ARR=x.split('|');
        var tmp_val=str_ARR(tmp_ARR(0).toInt-1);
        str_ARR(tmp_ARR(0).toInt-1)=str_ARR(tmp_ARR(1).toInt-1);
        str_ARR(tmp_ARR(1).toInt-1)=tmp_val;
      }
      return str_ARR.mkString("");
    }
    catch{
      case e: Exception => return "";
    }
  }

  /**
   * 
   */
  def getNumber(str:String=null):Integer={
    val new_val = str.replaceAll("\\D+","").toInt;
    return new_val;
  }
  
  /**
   * Hardcoded reference for CS5
   * NEED TO CHANGE TO ACTUAL REFERENCE 
   * [FIXME]
   */
  var trafficCaseRoam = Map("0" -> "Non Roaming", "1" -> "Non Roaming", "2" -> "Non Roaming", 
                            "3" -> "Roaming", "4" -> "Roaming", "5" -> "Roaming",
                            "6" -> "Roaming", "7" -> "Roaming", "8" -> "Roaming",
                            "9" -> "Roaming", "10" -> "Roaming", "11" -> "Roaming",
                            "12" -> "Roaming", "20" -> "Non Roaming", "21" -> "Non Roaming"
                            );
  
  var trafficCaseDrct = Map("0" -> "OUTGOING", "1" -> "OUTGOING", "2" -> "INCOMING", 
                            "3" -> "OUTGOING", "4" -> "OUTGOING", "5" -> "INCOMING",
                            "6" -> "INCOMING", "7" -> "OUTGOING", "8" -> "OUTGOING",
                            "9" -> "INCOMING", "10" -> "OUTGOING", "11" -> "OUTGOING",
                            "12" -> "INCOMING", "20" -> "OUTGOING", "21" -> "INCOMING"
                            );
  
  var refOfferAreaSC = Array("200000010","200000020","200000030","200000040","200000050","200000060","200000070","200000080","200000090","200000100","200000110","200000120","200000130","200000140","200000150","200000160","200000170","200000180","200000190","200000200","200000210","200000220","200000230","200000240","200000250","200000260")
	/**
	 * End of hardcoded reference for CS5	  
	 */
  
   /**
    * added 31 May 2016
   * Created for read non usage adjustment.
   * return array value
   */
  def getTupleInTupleNonUsage(str:String,delLV1:String,tupleLV1Num:Int,delLV2:String,tupleLV2Num01:Int,tupleLV2Num02:Int,tupleLV2Num03:Int,delLV3:String): Array[String] ={
    var line:Array[String] = new Array[String](10)
      
	  try{
	    
  		var LV1_ARR=str.split("[\\"+delLV1+"]");
  		if(LV1_ARR.length<tupleLV1Num){
  			return line;
  		}
  		
  		var tupleLV1 =LV1_ARR(tupleLV1Num).replaceAll("[\'\"]", "");
  		if(tupleLV1.length < 2 ){
  			return line;
  		}
  		
  		// separate "[" into array
  		var LV3_ARR=tupleLV1.split("[\\"+delLV3+"]");
  		var ch:Int=0;
  		
  		while(ch <  LV3_ARR.length ){
	      line (ch) =  getTuple(LV3_ARR(ch),delLV2,tupleLV2Num01).concat("|").concat(getTuple(LV3_ARR(ch),delLV2,tupleLV2Num02)).concat("|").concat(getTuple(LV3_ARR(ch),delLV2,tupleLV2Num03))
  			ch=ch+1;	
  		}	

  		var result = line;
  		return result;
	  }
	  catch {
	    case e: Exception => return line
	  }
	}
  
  
  /**
   * Create for read non usage refill
   * return array value
   * 
   */  
   def getTupleInTupleNonUsageRefill(str:String,delLV2:String,tupleLV2Num01:Int,delLV3:String,tupleLV2Num02:Int,tupleLV2Num03:Int): Array[String] ={
    
     // Modify : getTupleInTupleNonUsageRefill(str:String,delLV1:String,tupleLV1Num:Int,delLV2:String,tupleLV2Num01:Int,delLV3:String,tupleLV2Num02:Int,tupleLV2Num03:Int): Array[String] 
     
   //var temp=str.split("[\\"+delLV2+"]");
	  try{
	    //this below script is modified by IBM 7 Jun 2016
  		/*var LV1_ARR=str.split("[\\"+delLV1+"]");
  		if(LV1_ARR.length<tupleLV1Num){
  			return line;
  		}
  		
  		var tupleLV1 =LV1_ARR(tupleLV1Num).replaceAll("[\'\"]", "");
  		if(tupleLV1.length < 2 ){
  			return line;
  		}*/
  		
  		// separate "[" into array
//  		var LV2_ARR=tupleLV1.split("[\\"+delLV2+"]");
	    var LV2_ARR=str.split("[\\"+delLV2+"]");
  		if(LV2_ARR.length < tupleLV2Num01){
  			return null;
  		}
  		var tupleLV2 =LV2_ARR(tupleLV2Num01);
  		
  		//var LV3_ARR=tupleLV2.split("[\\"+delLV3+"]");
  		var LV3_ARR=tupleLV2.split("\\"+delLV3+"");
  		var line:Array[String] = new Array[String](LV3_ARR.length);
  		var ch:Int=0;
  		while(ch < LV3_ARR.length){
  		  val var1 = getTuple(LV3_ARR(ch),"*",tupleLV2Num02);
  		  val var2 = getTuple(LV3_ARR(ch),"*",tupleLV2Num03)
  		  //println(var1.length())
  		  //println(var2.length())
  		  if (!getTuple(LV3_ARR(ch),"*",tupleLV2Num02).equals("") && !getTuple(LV3_ARR(ch),"*",tupleLV2Num03).equals("")){
  		      line (ch) =  getTuple(LV3_ARR(ch),"*",tupleLV2Num02).concat("@").concat(getTuple(LV3_ARR(ch),"*",tupleLV2Num03)).concat("[")
  		  }else{
  		       line (ch) =  "xxx".concat("@").concat("xxx").concat("[")
  		  }
  			ch=ch+1;
  		}	

  		return line;
	  }
	  catch {
	    case e: Exception => return null
	  }
	}

   /**
    * 
    */
def getRecordID( MSISDN: String, eventDate: String) : String = {
      
      try {
        val localhost = InetAddress.getLocalHost.getHostAddress.split("\\.")
        
        return MSISDN+eventDate+localhost(2)+localhost(3)
      }
      
      catch{
        case e: Exception => return ""
		  }
  }
  
  def getKeyCDRInq( MSISDN: String, eventDate: String) : String = {
      
      try {
        val randomNumber = 10 + scala.util.Random.nextInt(90)        
        
        return MSISDN+eventDate+randomNumber
      }
      
      catch{
        case e: Exception => return ""
		  }
  }
  
  
  
  def toInt(s: String): Int = {
    try {
      s.toInt
    } 
    
    catch {
      case e: Exception => 0
    }
  }
  
  def toDouble(s: String): Double = {
    try {
      s.toDouble
    } 
    
    catch {
      case e: Exception => 0
    }
  }
  
  /**
   * 
   */
   def getIntcct(sqlContext: SQLContext, sourceDF: DataFrame, intcctAllDF: DataFrame, msisdnNm: String, eventDtNm: String, intcctPfxNm: String, intcctCtyNm: String, intcctCityNm: String, intcctProvdrNm: String) : DataFrame = {
       
      val intcct14DF = intcctAllDF.where("length = 14")
      intcct14DF.registerTempTable("Intcct14")
      intcct14DF.persist()
      intcct14DF.count()
     
      val intcct13DF = intcctAllDF.where("length = 13")
      //intcct13DF.show()
      intcct13DF.registerTempTable("Intcct13")
      intcct13DF.persist()
      intcct13DF.count()
     
      val intcct12DF = intcctAllDF.where("length = 12")
      intcct12DF.registerTempTable("Intcct12")
      intcct12DF.persist()
      intcct12DF.count()
     
      val intcct11DF = intcctAllDF.where("length = 11")
      intcct11DF.registerTempTable("Intcct11")
      intcct11DF.persist()
      intcct11DF.count()
     
      val intcct10DF = intcctAllDF.where("length = 10")
      intcct10DF.registerTempTable("Intcct10")
      intcct10DF.persist()
      intcct10DF.count()
     
      val intcct9DF = intcctAllDF.where("length = 9")
      intcct9DF.registerTempTable("Intcct9")
      intcct9DF.persist()
      intcct9DF.count()
     
      val intcct8DF = intcctAllDF.where("length = 8")
      intcct8DF.registerTempTable("Intcct8")
      intcct8DF.persist()
      intcct8DF.count()
     
      val intcct7DF = intcctAllDF.where("length = 7")
      intcct7DF.registerTempTable("Intcct7")
      intcct7DF.persist()
      intcct7DF.count()
     
      val intcct6DF = intcctAllDF.where("length = 6")
      intcct6DF.registerTempTable("Intcct6")
      intcct6DF.persist()
      intcct6DF.count()
     
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
   
      sourceDF.registerTempTable("sourceDF")
     
      val joinIntcctDF = sqlContext.sql("""SELECT a.*,
                                          COALESCE(b.intcctPfx, c.intcctPfx, d.intcctPfx, e.intcctPfx, f.intcctPfx, g.intcctPfx, h.intcctPfx, i.intcctPfx, j.intcctPfx, k.intcctPfx, l.intcctPfx, m.intcctPfx, n.intcctPfx, o.intcctPfx, '') """ + intcctPfxNm + """,
                                          COALESCE(b.intcctCty, c.intcctCty, d.intcctCty, e.intcctCty, f.intcctCty, g.intcctCty, h.intcctCty, i.intcctCty, j.intcctCty, k.intcctCty, l.intcctCty, m.intcctCty, n.intcctCty, o.intcctCty, '') """ + intcctCtyNm + """,
                                          COALESCE(b.intcctOpr, c.intcctOpr, d.intcctOpr, e.intcctOpr, f.intcctOpr, g.intcctOpr, h.intcctOpr, i.intcctOpr, j.intcctOpr, k.intcctOpr, l.intcctOpr, m.intcctOpr, n.intcctOpr, o.intcctOpr, '') """ + intcctProvdrNm + """,
                                          COALESCE(b.locName, c.locName, d.locName, e.locName, f.locName, g.locName, h.locName, i.locName, j.locName, k.locName, l.locName, m.locName, n.locName, o.locName, '') """ + intcctCityNm + """  
                                          FROM sourceDF a
                                          left join Intcct14 b on substr(a.""" + msisdnNm + """,1,14) = b.intcctPfx and a.""" + eventDtNm + """ >= b.effDt and a.""" + eventDtNm + """ <= b.endDt
                                          left join Intcct13 c on substr(a.""" + msisdnNm + """,1,13) = c.intcctPfx and a.""" + eventDtNm + """ >= c.effDt and a.""" + eventDtNm + """ <= c.endDt
                                          left join Intcct12 d on substr(a.""" + msisdnNm + """,1,12) = d.intcctPfx and a.""" + eventDtNm + """ >= d.effDt and a.""" + eventDtNm + """ <= d.endDt
                                          left join Intcct11 e on substr(a.""" + msisdnNm + """,1,11) = e.intcctPfx and a.""" + eventDtNm + """ >= e.effDt and a.""" + eventDtNm + """ <= e.endDt
                                          left join Intcct10 f on substr(a.""" + msisdnNm + """,1,10) = f.intcctPfx and a.""" + eventDtNm + """ >= f.effDt and a.""" + eventDtNm + """ <= f.endDt
                                          left join Intcct9 g on substr(a.""" + msisdnNm + """,1,9) = g.intcctPfx and a.""" + eventDtNm + """ >= g.effDt and a.""" + eventDtNm + """ <= g.endDt
                                          left join Intcct8 h on substr(a.""" + msisdnNm + """,1,8) = h.intcctPfx and a.""" + eventDtNm + """ >= h.effDt and a.""" + eventDtNm + """ <= h.endDt
                                          left join Intcct7 i on substr(a.""" + msisdnNm + """,1,7) = i.intcctPfx and a.""" + eventDtNm + """ >= i.effDt and a.""" + eventDtNm + """ <= i.endDt
                                          left join Intcct6 j on substr(a.""" + msisdnNm + """,1,6) = j.intcctPfx and a.""" + eventDtNm + """ >= j.effDt and a.""" + eventDtNm + """ <= j.endDt
                                          left join Intcct5 k on substr(a.""" + msisdnNm + """,1,5) = k.intcctPfx and a.""" + eventDtNm + """ >= k.effDt and a.""" + eventDtNm + """ <= k.endDt
                                          left join Intcct4 l on substr(a.""" + msisdnNm + """,1,4) = l.intcctPfx and a.""" + eventDtNm + """ >= l.effDt and a.""" + eventDtNm + """ <= l.endDt
                                          left join Intcct3 m on substr(a.""" + msisdnNm + """,1,3) = m.intcctPfx and a.""" + eventDtNm + """ >= m.effDt and a.""" + eventDtNm + """ <= m.endDt
                                          left join Intcct2 n on substr(a.""" + msisdnNm + """,1,2) = n.intcctPfx and a.""" + eventDtNm + """ >= n.effDt and a.""" + eventDtNm + """ <= n.endDt
                                          left join Intcct1 o on substr(a.""" + msisdnNm + """,1,1) = o.intcctPfx and a.""" + eventDtNm + """ >= o.effDt and a.""" + eventDtNm + """ <= o.endDt
                                      """)
 
      return joinIntcctDF
   
   }
   
   /**
    * 
    */
     def getIntcctDummy(sqlContext: SQLContext, sourceDF: DataFrame, intcctAllDF: DataFrame, msisdnNm: String, eventDtNm: String, intcctPfxNm: String, intcctCtyNm: String, intcctCityNm: String, intcctProvdrNm: String) : DataFrame = {
   
      sourceDF.registerTempTable("sourceDF")
     
      val joinIntcctDF = sqlContext.sql("""SELECT a.*,
                                          '' """ + intcctPfxNm + """,
                                          '' """ + intcctCtyNm + """,
                                          '' """ + intcctProvdrNm + """,
                                          '' """ + intcctCityNm + """  
                                          FROM sourceDF a
                                      """)
      return joinIntcctDF   
   }
   
   
   /**
    * 
    */
   def getIntcctWithSID(sqlContext: SQLContext, sourceDF: DataFrame, intcctAllDF: DataFrame, msisdnNm: String, eventDtNm: String, sidNm: String, intcctPfxNm: String, intcctCtyNm: String, intcctCityNm: String, intcctProvdrNm: String) : DataFrame = {
        
      val intcct14DF = intcctAllDF.where("length = 14")
      intcct14DF.registerTempTable("Intcct14")
      intcct14DF.persist()
      intcct14DF.count()
      
      val intcct13DF = intcctAllDF.where("length = 13")
      //intcct13DF.show()
      intcct13DF.registerTempTable("Intcct13")
      intcct13DF.persist()
      intcct13DF.count()
      
      val intcct12DF = intcctAllDF.where("length = 12")
      intcct12DF.registerTempTable("Intcct12")
      intcct12DF.persist()
      intcct12DF.count()
      
      val intcct11DF = intcctAllDF.where("length = 11")
      intcct11DF.registerTempTable("Intcct11")
      intcct11DF.persist()
      intcct11DF.count()
      
      val intcct10DF = intcctAllDF.where("length = 10")
      intcct10DF.registerTempTable("Intcct10")
      intcct10DF.persist()
      intcct10DF.count()
      
      val intcct9DF = intcctAllDF.where("length = 9")
      intcct9DF.registerTempTable("Intcct9")
      intcct9DF.persist()
      intcct9DF.count()
      
      val intcct8DF = intcctAllDF.where("length = 8")
      intcct8DF.registerTempTable("Intcct8")
      intcct8DF.persist()
      intcct8DF.count()
      
      val intcct7DF = intcctAllDF.where("length = 7")
      intcct7DF.registerTempTable("Intcct7")
      intcct7DF.persist()
      intcct7DF.count()
      
      val intcct6DF = intcctAllDF.where("length = 6")
      intcct6DF.registerTempTable("Intcct6")
      intcct6DF.persist()
      intcct6DF.count()
      
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
    
      sourceDF.registerTempTable("sourceDF")
      
      val joinIntcctDF = sqlContext.sql("""SELECT a.*, 
                                          COALESCE(b.intcctPfx, c.intcctPfx, d.intcctPfx, e.intcctPfx, f.intcctPfx, g.intcctPfx, h.intcctPfx, i.intcctPfx, j.intcctPfx, k.intcctPfx, l.intcctPfx, m.intcctPfx, n.intcctPfx, o.intcctPfx,'') """ + intcctPfxNm + """,
                                          COALESCE(b.intcctCty, c.intcctCty, d.intcctCty, e.intcctCty, f.intcctCty, g.intcctCty, h.intcctCty, i.intcctCty, j.intcctCty, k.intcctCty, l.intcctCty, m.intcctCty, n.intcctCty, o.intcctCty,'') """ + intcctCtyNm + """,
                                          COALESCE(b.intcctOpr, c.intcctOpr, d.intcctOpr, e.intcctOpr, f.intcctOpr, g.intcctOpr, h.intcctOpr, i.intcctOpr, j.intcctOpr, k.intcctOpr, l.intcctOpr, m.intcctOpr, n.intcctOpr, o.intcctOpr,'') """ + intcctProvdrNm + """,
                                          COALESCE(b.locName, c.locName, d.locName, e.locName, f.locName, g.locName, h.locName, i.locName, j.locName, k.locName, l.locName, m.locName, n.locName, o.locName,'') """ + intcctCityNm + """   
                                          FROM sourceDF a 
                                          left join Intcct14 b on substr(a.""" + msisdnNm + """,1,14) = b.intcctPfx and a.""" + eventDtNm + """ >= b.effDt and a.""" + eventDtNm + """ <= b.endDt and a.""" + sidNm + """ = b.sid
                                          left join Intcct13 c on substr(a.""" + msisdnNm + """,1,13) = c.intcctPfx and a.""" + eventDtNm + """ >= c.effDt and a.""" + eventDtNm + """ <= c.endDt and a.""" + sidNm + """ = c.sid
                                          left join Intcct12 d on substr(a.""" + msisdnNm + """,1,12) = d.intcctPfx and a.""" + eventDtNm + """ >= d.effDt and a.""" + eventDtNm + """ <= d.endDt and a.""" + sidNm + """ = d.sid
                                          left join Intcct11 e on substr(a.""" + msisdnNm + """,1,11) = e.intcctPfx and a.""" + eventDtNm + """ >= e.effDt and a.""" + eventDtNm + """ <= e.endDt and a.""" + sidNm + """ = e.sid
                                          left join Intcct10 f on substr(a.""" + msisdnNm + """,1,10) = f.intcctPfx and a.""" + eventDtNm + """ >= f.effDt and a.""" + eventDtNm + """ <= f.endDt and a.""" + sidNm + """ = f.sid
                                          left join Intcct9 g on substr(a.""" + msisdnNm + """,1,9) = g.intcctPfx and a.""" + eventDtNm + """ >= g.effDt and a.""" + eventDtNm + """ <= g.endDt and a.""" + sidNm + """ = g.sid
                                          left join Intcct8 h on substr(a.""" + msisdnNm + """,1,8) = h.intcctPfx and a.""" + eventDtNm + """ >= h.effDt and a.""" + eventDtNm + """ <= h.endDt and a.""" + sidNm + """ = h.sid
                                          left join Intcct7 i on substr(a.""" + msisdnNm + """,1,7) = i.intcctPfx and a.""" + eventDtNm + """ >= i.effDt and a.""" + eventDtNm + """ <= i.endDt and a.""" + sidNm + """ = i.sid
                                          left join Intcct6 j on substr(a.""" + msisdnNm + """,1,6) = j.intcctPfx and a.""" + eventDtNm + """ >= j.effDt and a.""" + eventDtNm + """ <= j.endDt and a.""" + sidNm + """ = j.sid
                                          left join Intcct5 k on substr(a.""" + msisdnNm + """,1,5) = k.intcctPfx and a.""" + eventDtNm + """ >= k.effDt and a.""" + eventDtNm + """ <= k.endDt and a.""" + sidNm + """ = k.sid
                                          left join Intcct4 l on substr(a.""" + msisdnNm + """,1,4) = l.intcctPfx and a.""" + eventDtNm + """ >= l.effDt and a.""" + eventDtNm + """ <= l.endDt and a.""" + sidNm + """ = l.sid
                                          left join Intcct3 m on substr(a.""" + msisdnNm + """,1,3) = m.intcctPfx and a.""" + eventDtNm + """ >= m.effDt and a.""" + eventDtNm + """ <= m.endDt and a.""" + sidNm + """ = m.sid
                                          left join Intcct2 n on substr(a.""" + msisdnNm + """,1,2) = n.intcctPfx and a.""" + eventDtNm + """ >= n.effDt and a.""" + eventDtNm + """ <= n.endDt and a.""" + sidNm + """ = n.sid
                                          left join Intcct1 o on substr(a.""" + msisdnNm + """,1,1) = o.intcctPfx and a.""" + eventDtNm + """ >= o.effDt and a.""" + eventDtNm + """ <= o.endDt and a.""" + sidNm + """ = o.sid
                                      """)
  
      return joinIntcctDF
    
   }
   
   /**
    * 
    */
   def getIntcctWithSIDDummy(sqlContext: SQLContext, sourceDF: DataFrame, intcctAllDF: DataFrame, msisdnNm: String, eventDtNm: String, sidNm: String, intcctPfxNm: String, intcctCtyNm: String, intcctCityNm: String, intcctProvdrNm: String) : DataFrame = {
        
     
    
      sourceDF.registerTempTable("sourceDF")
      
      val joinIntcctDF = sqlContext.sql("""SELECT a.*, 
                                          '' """ + intcctPfxNm + """,
                                          '' """ + intcctCtyNm + """,
                                          '' """ + intcctProvdrNm + """,
                                          '' """ + intcctCityNm + """   
                                          FROM sourceDF a""")
  
      return joinIntcctDF
    
   }
   
  /**
   * Read configuration file and return the value based on given key
   */
  def getConfigDirectory(sc: SparkContext, path: String, key: String) : String = {
   
   try {
       
       val hadoopConfig: Configuration = sc.hadoopConfiguration
       val fs: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
       val file: FSDataInputStream = fs.open(new org.apache.hadoop.fs.Path(path))
       val reader = new InputStreamReader(file)
       val config = ConfigFactory.parseReader(reader)
       
       return config.getString(key)
   }
   
   catch {
     case e: Exception => throw new Exception("Key : " + key + " => " + e.getMessage)         
   }
 }
  
 /**
  * Check if Directory exists or not. 
  * Return true if exists, false if not exists 
  */
 def isDirectoryExists(sc: SparkContext, path: String) : Boolean = {
   
   try {
       
       val conf = sc.hadoopConfiguration
       val fs = org.apache.hadoop.fs.FileSystem.get(conf)
       val exists_flag = fs.exists(new org.apache.hadoop.fs.Path(path))
      
       return exists_flag
   }
   
   catch {
     case e: Exception => throw new Exception("Path : " + path + " => " + e.getMessage)         
   }
 }
 
  /**
  * If the directory exists, then delete the directory and its content
  */
 def cleanDirectoryWithPattern(sc: SparkContext, path: String, pattern: String)  = {
   
   try {
       
       val conf = sc.hadoopConfiguration
       val fs = org.apache.hadoop.fs.FileSystem.get(conf)
       
       //val d = fs.globStatus(new org.apache.hadoop.fs.Path("/user/apps/CS3/output/detail_prepaid_sit/" + "/*/JOB_ID=1305"));
       val folder_list = fs.globStatus(new org.apache.hadoop.fs.Path(path + pattern));
       
       
       folder_list.foreach(x=> fs.delete(x.getPath, true))
       
   }
   
   catch {
     case e: Exception => throw new Exception("Path : " + path + " => " + e.getMessage)         
   }
 }
 
 /**
  * If the directory exists, then delete the directory and its content
  * @param sc Spark Context
  * @param path Path to be deleted
  */
 def cleanDirectory(sc: SparkContext, path: String) = {
   
   try {
       
       if (isDirectoryExists(sc, path) == true)
       {
         val conf = sc.hadoopConfiguration
         val fs = org.apache.hadoop.fs.FileSystem.get(conf)
         fs.delete(new org.apache.hadoop.fs.Path(path), true)
       }
   }
   
   catch {
     case e: Exception => throw new Exception("Path : " + path + " => " + e.getMessage)         
   }
 }
 
 /**
  * Get child directory : combination of process date and job ID
  * Sample : 20160701_1234
  */
 def getChildDirectory(prcDt : String, jobId : String) : String = {
   
   return prcDt + "_" + jobId
 }
 
 
 def writeApplicationID(sc: SparkContext, path: String, appID: String) = {
   
   try {
       val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration); 
       val output = fs.create(new org.apache.hadoop.fs.Path(path));
       val os = new BufferedOutputStream(output)
       os.write(appID.getBytes("UTF-8"))
       os.close()
   }
   
   catch {
     case e: Exception => throw new Exception("Path : " + path + " => " + e.getMessage)         
   }
 }
 
 /**
  * Safely get elements in array
  * @param arr Array
  * @param element Element number
  */
 def safeGet( arr : Array[String], element: Int ) :String = {
   return arr.lift(element).getOrElse("")
   /*try {
     return arr(element)
   }   
   catch {
     case e: Exception => return ""        
   }*/
 }
 
 /**
  * Get interconnect with SID
  * @param intcctRef Instantiation of Common.interconnectRef
  */
def getIntcctWithSID2(sqlContext: SQLContext, sourceDF: DataFrame, intcctRef: interconnectRef, msisdnNm: String, eventDtNm: String, sidNm: String, intcctPfxNm: String, intcctCtyNm: String, intcctCityNm: String, intcctProvdrNm: String) : DataFrame = {
   
      sqlContext.udf.register("getIntcctLookup", intcctRef.getIntcctLookup _)
      sqlContext.udf.register("getTuple", Common.getTuple _)
      sourceDF.registerTempTable("sourceDF")
      
      val joinIntcctDF = sqlContext.sql("""SELECT a.*,getIntcctLookup(""" + msisdnNm + """,""" + eventDtNm + """,""" + sidNm + """) """+ msisdnNm +"""Intcct FROM sourceDF a""")
      joinIntcctDF.registerTempTable("joinIntcctDF")
      
      val joinIntcctDF2 = sqlContext.sql("""SELECT a.*,
          gettuple("""+ msisdnNm +"""Intcct, "|", 0) """ + intcctPfxNm + """,
          gettuple("""+ msisdnNm +"""Intcct, "|", 1) """ + intcctCtyNm + """,
          gettuple("""+ msisdnNm +"""Intcct, "|", 2) """ + intcctProvdrNm + """,
          gettuple("""+ msisdnNm +"""Intcct, "|", 3) """ + intcctCityNm + """
          FROM joinIntcctDF a""").drop(msisdnNm +"Intcct")
       
      return joinIntcctDF2
  
 }


/**
  * Get interconnect with SID
  * @param intcctRef Instantiation of Common.interconnectRef
  */
def getIntcctWithSID3(sqlContext: SQLContext, sourceDF: DataFrame, intcctRef: interconnectRef, AmsisdnNm: String, BmsisdnNm: String, eventDtNm: String, sidNm: String, AintcctPfxNm: String, AintcctCtyNm: String, AintcctCityNm: String, AintcctProvdrNm: String, BintcctPfxNm: String, BintcctCtyNm: String, BintcctCityNm: String, BintcctProvdrNm: String) : DataFrame = {
   
      sqlContext.udf.register("getIntcctLookup", intcctRef.getIntcctLookup2 _)
      sqlContext.udf.register("getTuple", Common.getTuple _)
      sourceDF.registerTempTable("sourceDF")
      
      val joinIntcctDF = sqlContext.sql("""SELECT a.*,getIntcctLookup(""" + AmsisdnNm + """,""" + BmsisdnNm + """,""" + eventDtNm + """,""" + sidNm + """) """+ """IntcctLkp FROM sourceDF a""")
      joinIntcctDF.registerTempTable("joinIntcctDF")
      
      val joinIntcctDF2 = sqlContext.sql("""SELECT a.*,
          gettuple(IntcctLkp, "|", 0) """ + AintcctPfxNm + """,
          gettuple(IntcctLkp, "|", 1) """ + AintcctCtyNm + """,
          gettuple(IntcctLkp, "|", 2) """ + AintcctProvdrNm + """,
          gettuple(IntcctLkp, "|", 3) """ + AintcctCityNm + """,
          gettuple(IntcctLkp, "|", 4) """ + BintcctPfxNm + """,
          gettuple(IntcctLkp, "|", 5) """ + BintcctCtyNm + """,
          gettuple(IntcctLkp, "|", 6) """ + BintcctProvdrNm + """,
          gettuple(IntcctLkp, "|", 7) """ + BintcctCityNm + """
          FROM joinIntcctDF a""").drop("IntcctLkp")
       
      return joinIntcctDF2
  
 }

  /**
    * Get interconnect with SID
    * @param intcctRef Instantiation of Common.interconnectRef
    */
  def getIntcctWithSID4(sqlContext: SQLContext, sourceDF: DataFrame,
      intcctRef: interconnectRef, AmsisdnNm: String, BmsisdnNm: String,
      eventDtNm: String, sidNm: String, AintcctPfxNm: String,
      AintcctCtyNm: String, AintcctCityNm: String, AintcctProvdrNm: String,
      BintcctPfxNm: String, BintcctCtyNm: String, BintcctCityNm: String,
      BintcctProvdrNm: String, ServiceScenarioNm: String) : DataFrame = {
     
    sqlContext.udf.register("getIntcctLookup", intcctRef.getIntcctLookup3 _)
    sqlContext.udf.register("getTuple", Common.getTuple _)
    sourceDF.registerTempTable("sourceDF")
    
    val joinIntcctDF = sqlContext.sql("""
    SELECT a.*,getIntcctLookup(""" + AmsisdnNm + """,""" + BmsisdnNm + """,""" + eventDtNm + """,""" + sidNm + """,""" + ServiceScenarioNm + """) IntcctLkp FROM sourceDF a""")
    joinIntcctDF.registerTempTable("joinIntcctDF")
    
    val joinIntcctDF2 = sqlContext.sql("""SELECT a.*,
    gettuple(IntcctLkp, "|", 0) """ + AintcctPfxNm + """,
    gettuple(IntcctLkp, "|", 1) """ + AintcctCtyNm + """,
    gettuple(IntcctLkp, "|", 2) """ + AintcctProvdrNm + """,
    gettuple(IntcctLkp, "|", 3) """ + AintcctCityNm + """,
    gettuple(IntcctLkp, "|", 4) """ + BintcctPfxNm + """,
    gettuple(IntcctLkp, "|", 5) """ + BintcctCtyNm + """,
    gettuple(IntcctLkp, "|", 6) """ + BintcctProvdrNm + """,
    gettuple(IntcctLkp, "|", 7) """ + BintcctCityNm + """
    FROM joinIntcctDF a""").drop("IntcctLkp")
           
    return joinIntcctDF2
    
  }
 
/**
 * Load and perform interconnect longest match
 * 1. Instantiate this class
 * 2. Use getIntcctLookup function to perform lookup
 * @param interconnectFileName is the location of reference file in HDFS
 */
 class interconnectRef( interconnectFileName:String, sc:SparkContext ) extends java.io.Serializable {

    //val eventTypeList = Source.fromFile( interconnectFileName ).getLines.toList
    val interconnectList = sc.textFile(interconnectFileName).collect().toList
    //val interconnectListArr = sc.textFile(interconnectFileName).map(line => getIntcctArr(line))
    
    //Get list of array as input
    val interconnectListArr = interconnectList.map( line => getIntcctArr(line) )
    
    //Get list of array per length
    val interconnectListArr14 = interconnectListArr.filter { arr => arr(0)=="14" }
    val interconnectListArr13 = interconnectListArr.filter { arr => arr(0)=="13" }
    val interconnectListArr12 = interconnectListArr.filter { arr => arr(0)=="12" }
    val interconnectListArr11 = interconnectListArr.filter { arr => arr(0)=="11" }
    val interconnectListArr10 = interconnectListArr.filter { arr => arr(0)=="10" }
    val interconnectListArr9 = interconnectListArr.filter { arr => arr(0)=="9" }
    val interconnectListArr8 = interconnectListArr.filter { arr => arr(0)=="8" }
    val interconnectListArr7 = interconnectListArr.filter { arr => arr(0)=="7" }
    val interconnectListArr6 = interconnectListArr.filter { arr => arr(0)=="6" }
    val interconnectListArr5 = interconnectListArr.filter { arr => arr(0)=="5" }
    val interconnectListArr4 = interconnectListArr.filter { arr => arr(0)=="4" }
    val interconnectListArr3 = interconnectListArr.filter { arr => arr(0)=="3" }
    val interconnectListArr2 = interconnectListArr.filter { arr => arr(0)=="2" }
    val interconnectListArr1 = interconnectListArr.filter { arr => arr(0)=="1" }
    
    val interconnectMap = Map(14 -> interconnectListArr14, 13 -> interconnectListArr13, 12 -> interconnectListArr12, 11 -> interconnectListArr11, 
                                10 -> interconnectListArr10, 9 -> interconnectListArr9, 8 -> interconnectListArr8,
                                7 -> interconnectListArr7, 6 -> interconnectListArr6, 5 -> interconnectListArr5, 4 -> interconnectListArr4,
                                3 -> interconnectListArr3, 2 -> interconnectListArr2, 1 -> interconnectListArr1
                                );
    
    
    
    //Parallellize
    //val interconnectRef8 = sc.broadcast(interconnectListArr8)
    
    
    
    //Check test print
    //interconnectListArr8.collect{case (a) => println (a.mkString("|"))}
    //interconnectRef8.value
    
    /**
     * Split interconnect reference with delimiter '|'
     * Return as array
     */
    def getIntcctArr  ( line:String ) : Array[String] = {
      return Array(
          Common.getTuple(line, "|", 0), //length
          Common.getTuple(line, "|", 1), //intcctPfx
          Common.getTuple(line, "|", 2), //intcctCty
          Common.getTuple(line, "|", 3), //intcctOpr
          Common.getTuple(line, "|", 4), //locName
          Common.getTuple(line, "|", 5), //effDt
          Common.getTuple(line, "|", 6), //endDt
          Common.getTuple(line, "|", 7)  ////sid
          )
    }
    
    /**
     * Get interconnect using longest match
     * @param msisdn, event date, sid (empty string if not required)
     * @return PREFIX|COUNTRY|OPERATOR|CITY or empty string
     */
    def getIntcctLookup( msisdn: String, eventDt: String, sid: String  ) : String = {
      val msisdnLen = msisdn.length()
      val msisdn14 = msisdn.take(14)
      val msisdn13 = msisdn.take(13)
      val msisdn12 = msisdn.take(12)
      val msisdn11 = msisdn.take(11)
      val msisdn10 = msisdn.take(10)
      val msisdn9 = msisdn.take(9)
      val msisdn8 = msisdn.take(8)
      val msisdn7 = msisdn.take(7)
      val msisdn6 = msisdn.take(6)
      val msisdn5 = msisdn.take(5)
      val msisdn4 = msisdn.take(4)
      val msisdn3 = msisdn.take(3)
      val msisdn2 = msisdn.take(2)
      var returnVal = ""
      
      //Perform loop for each of length from longest to shortest
      if ( msisdnLen>=14 ) returnVal = getIntcctValueEachLength(interconnectListArr14, msisdn14, eventDt, sid)
      if ( msisdnLen>=13 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr13, msisdn13, eventDt, sid)
      if ( msisdnLen>=12 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr12, msisdn12, eventDt, sid)
      if ( msisdnLen>=11 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr11, msisdn11, eventDt, sid)
      if ( msisdnLen>=10 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr10, msisdn10, eventDt, sid)
      if ( msisdnLen>=9 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr9, msisdn9, eventDt, sid)
      if ( msisdnLen>=8 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr8, msisdn8, eventDt, sid)
      if ( msisdnLen>=7 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr7, msisdn7, eventDt, sid)
      if ( msisdnLen>=6 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr6, msisdn6, eventDt, sid)
      if ( msisdnLen>=5 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr5, msisdn5, eventDt, sid)
      if ( msisdnLen>=4 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr4, msisdn4, eventDt, sid)
      if ( msisdnLen>=3 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr3, msisdn3, eventDt, sid)
      if ( msisdnLen>=2 && returnVal.isEmpty() ) returnVal = getIntcctValueEachLength(interconnectListArr2, msisdn2, eventDt, sid)
      
      return returnVal
    }
    
    /**
     * Get interconnect value for single length
     * @param List of Array of Interconnect reference for that length, MSISDN substring, Event Date, SID (empty string if not required)
     * @return PREFIX|COUNTRY|OPERATOR|CITY or empty string
     */
    def getIntcctValueEachLength(interconnectListArr: List[Array[String]], msisdn: String, eventDt:String, sid:String) : String = {
      var returnVal = ""
      breakable {
        interconnectListArr.foreach { arr => 
            if (msisdn==arr(1) && sid==arr(7) && eventDt>=arr(5) && eventDt<=arr(6)) {
              //Returning PREFIX|COUNTRY|OPERATOR|CITY
              returnVal =  arr(1) + "|" +  arr(2) + "|" +  arr(3) + "|" +  arr(4)
              break
            }
          }
      }
      return returnVal
    }
    
    
    def getIntcctLookup2( msisdnA: String, msisdnB: String, eventDt: String, sid: String  ) : String = {
      val msisdnALen = msisdnA.length()
      val msisdnBLen = msisdnB.length()
      var returnVal = ""
      var resultArr = new Array[String](2)
      
      if ( (msisdnALen>=14 || msisdnBLen>=14) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr14, msisdnA.take(14), msisdnB.take(14), eventDt, sid, resultArr, 14)
      if ( (msisdnALen>=13 || msisdnBLen>=13) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr13, msisdnA.take(13), msisdnB.take(13), eventDt, sid, resultArr, 13)
      if ( (msisdnALen>=12 || msisdnBLen>=12) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr12, msisdnA.take(12), msisdnB.take(12), eventDt, sid, resultArr, 12)
      if ( (msisdnALen>=11 || msisdnBLen>=11) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr11, msisdnA.take(11), msisdnB.take(11), eventDt, sid, resultArr, 11)
      if ( (msisdnALen>=10 || msisdnBLen>=10) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr10, msisdnA.take(10), msisdnB.take(10), eventDt, sid, resultArr, 10)
      if ( (msisdnALen>=9 || msisdnBLen>=9) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr9, msisdnA.take(9), msisdnB.take(9), eventDt, sid, resultArr, 9)
      if ( (msisdnALen>=8 || msisdnBLen>=8) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr8, msisdnA.take(8), msisdnB.take(8), eventDt, sid, resultArr, 8)
      if ( (msisdnALen>=7 || msisdnBLen>=7) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr7, msisdnA.take(7), msisdnB.take(7), eventDt, sid, resultArr, 7)
      if ( (msisdnALen>=6 || msisdnBLen>=6) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr6, msisdnA.take(6), msisdnB.take(6), eventDt, sid, resultArr, 6)
      if ( (msisdnALen>=5 || msisdnBLen>=5) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr5, msisdnA.take(5), msisdnB.take(5), eventDt, sid, resultArr, 5)
      if ( (msisdnALen>=4 || msisdnBLen>=4) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr4, msisdnA.take(4), msisdnB.take(4), eventDt, sid, resultArr, 4)
      if ( (msisdnALen>=3 || msisdnBLen>=3) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr3, msisdnA.take(3), msisdnB.take(3), eventDt, sid, resultArr, 3)
      if ( (msisdnALen>=2 || msisdnBLen>=2) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr2, msisdnA.take(2), msisdnB.take(2), eventDt, sid, resultArr, 2)            
        
        /*
        breakable {
          for (length <- 14 until 0 by -1)
             {     
               if(msisdnA.length >= length || msisdnB.length >= length  )
               {             
                   resultArr = getIntcctValueEachLength2(interconnectMap.getOrElse(length, interconnectListArr1), msisdnA.take(length), msisdnB.take(length), eventDt, sid, resultArr, length)
               }
             
               if (resultArr(0) != null && resultArr(1) != null)
                 break
               
             } 
           }*/
           
      
      if(resultArr(0) == null)
          resultArr(0) = "|||"
          
      if(resultArr(1) == null)
          resultArr(1) = "|||"
          
      //println("a party : " + resultArr(0))   
      //println("b party : " + resultArr(1)) 
      
      return resultArr(0) + "|" + resultArr(1)
    }
    
    def getIntcctLookup3( msisdnA: String, msisdnB: String, 
        eventDt: String, sid: String, serviceScenario: String ) : String = {
      val msisdnALen = msisdnA.length()
      val msisdnBLen = msisdnB.length()
      var returnVal = ""
      var resultArr = new Array[String](2)
      
      if ( (msisdnALen>=14 || msisdnBLen>=14) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr14, msisdnA.take(14), msisdnB.take(14), eventDt, sid, resultArr, 14, serviceScenario)
      if ( (msisdnALen>=13 || msisdnBLen>=13) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr13, msisdnA.take(13), msisdnB.take(13), eventDt, sid, resultArr, 13, serviceScenario)
      if ( (msisdnALen>=12 || msisdnBLen>=12) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr12, msisdnA.take(12), msisdnB.take(12), eventDt, sid, resultArr, 12, serviceScenario)
      if ( (msisdnALen>=11 || msisdnBLen>=11) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr11, msisdnA.take(11), msisdnB.take(11), eventDt, sid, resultArr, 11, serviceScenario)
      if ( (msisdnALen>=10 || msisdnBLen>=10) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr10, msisdnA.take(10), msisdnB.take(10), eventDt, sid, resultArr, 10, serviceScenario)
      if ( (msisdnALen>=9 || msisdnBLen>=9) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr9, msisdnA.take(9), msisdnB.take(9), eventDt, sid, resultArr, 9, serviceScenario)
      if ( (msisdnALen>=8 || msisdnBLen>=8) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr8, msisdnA.take(8), msisdnB.take(8), eventDt, sid, resultArr, 8, serviceScenario)
      if ( (msisdnALen>=7 || msisdnBLen>=7) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr7, msisdnA.take(7), msisdnB.take(7), eventDt, sid, resultArr, 7, serviceScenario)
      if ( (msisdnALen>=6 || msisdnBLen>=6) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr6, msisdnA.take(6), msisdnB.take(6), eventDt, sid, resultArr, 6, serviceScenario)
      if ( (msisdnALen>=5 || msisdnBLen>=5) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr5, msisdnA.take(5), msisdnB.take(5), eventDt, sid, resultArr, 5, serviceScenario)
      if ( (msisdnALen>=4 || msisdnBLen>=4) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr4, msisdnA.take(4), msisdnB.take(4), eventDt, sid, resultArr, 4, serviceScenario)
      if ( (msisdnALen>=3 || msisdnBLen>=3) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr3, msisdnA.take(3), msisdnB.take(3), eventDt, sid, resultArr, 3, serviceScenario)
      if ( (msisdnALen>=2 || msisdnBLen>=2) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength3(interconnectListArr2, msisdnA.take(2), msisdnB.take(2), eventDt, sid, resultArr, 2, serviceScenario)            
      
      if(resultArr(0) == null)
          resultArr(0) = "|||"
          
      if(resultArr(1) == null)
          resultArr(1) = "||"+serviceScenario+"|"
          
      //println("a party : " + resultArr(0))   
      //println("b party : " + resultArr(1)) 
      
      return resultArr(0) + "|" + resultArr(1)
    }
    
    def getIntcctLookup4(
        msisdnA: String, msisdnB: String, 
        eventDt: String, sid: String) : String = {
      
      val msisdnALen = msisdnA.length()
      val msisdnBLen = msisdnB.length()
      var returnVal = ""
      var resultArr = new Array[String](2)
      
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=14 || msisdnBLen>=14) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr14, msisdnA.take(14), msisdnB.take(14), eventDt, sid, resultArr, 14)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=13 || msisdnBLen>=13) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr13, msisdnA.take(13), msisdnB.take(13), eventDt, sid, resultArr, 13)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=12 || msisdnBLen>=12) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr12, msisdnA.take(12), msisdnB.take(12), eventDt, sid, resultArr, 12)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=11 || msisdnBLen>=11) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr11, msisdnA.take(11), msisdnB.take(11), eventDt, sid, resultArr, 11)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=10 || msisdnBLen>=10) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr10, msisdnA.take(10), msisdnB.take(10), eventDt, sid, resultArr, 10)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=9 || msisdnBLen>=9) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr9, msisdnA.take(9), msisdnB.take(9), eventDt, sid, resultArr, 9)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=8 || msisdnBLen>=8) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr8, msisdnA.take(8), msisdnB.take(8), eventDt, sid, resultArr, 8)
      println(msisdnALen, msisdnBLen, resultArr(0), resultArr(1))
      if ( (msisdnALen>=7 || msisdnBLen>=7) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr7, msisdnA.take(7), msisdnB.take(7), eventDt, sid, resultArr, 7)
      println(msisdnALen, msisdnBLen, resultArr(0), resultArr(1))
      if ( (msisdnALen>=6 || msisdnBLen>=6) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr6, msisdnA.take(6), msisdnB.take(6), eventDt, sid, resultArr, 6)
      println(msisdnALen, msisdnBLen, resultArr(0), resultArr(1))
      if ( (msisdnALen>=5 || msisdnBLen>=5) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr5, msisdnA.take(5), msisdnB.take(5), eventDt, sid, resultArr, 5)
      println(msisdnALen, msisdnBLen, resultArr(0), resultArr(1))
      if ( (msisdnALen>=4 || msisdnBLen>=4) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr4, msisdnA.take(4), msisdnB.take(4), eventDt, sid, resultArr, 4)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=3 || msisdnBLen>=3) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr3, msisdnA.take(3), msisdnB.take(3), eventDt, sid, resultArr, 3)
      println(resultArr(0), resultArr(1))
      if ( (msisdnALen>=2 || msisdnBLen>=2) && (resultArr(0)==null || resultArr(1)==null) )
          resultArr = getIntcctValueEachLength2(interconnectListArr2, msisdnA.take(2), msisdnB.take(2), eventDt, sid, resultArr, 2)            
      
      println(resultArr(0), resultArr(1))
      
      if(resultArr(0) == null)
          resultArr(0) = "|||"
          
      if(resultArr(1) == null)
          resultArr(1) = "|||"
          
      return resultArr(0) + "|" + resultArr(1)
    }
    
    /**
     * Get interconnect value for single length
     * @param List of Array of Interconnect reference for that length, A MSISDN substring, B MSISDN substring, Event Date, SID (empty string if not required), array of string for saving the result
     * @return array of string : 	(0) A NUMBER : PREFIX|COUNTRY|OPERATOR|CITY or empty string
     * 														(1) B NUMBER : PREFIX|COUNTRY|OPERATOR|CITY or empty string
     */ //resultMap:collection.mutable.Map[String, String],
    def getIntcctValueEachLength2(interconnectListArr: List[Array[String]], msisdnA: String, msisdnB: String, eventDt:String, sid:String, resultArr:Array[String], length: Integer ) : Array[String] = {
      
      breakable {
        interconnectListArr.foreach { arr => 
            
            if (resultArr(0) != null && resultArr(1) != null)
               break
           
            if (resultArr(0) == null)
            { 
                if (msisdnA==arr(1) && sid==arr(7) && eventDt>=arr(5) && eventDt<=arr(6)) {
                    //Returning PREFIX|COUNTRY|OPERATOR|CITY
                    resultArr(0) = arr(1) + "|" +  arr(2) + "|" +  arr(3) + "|" +  arr(4)
                }
            }
            
            if (resultArr(1) == null)
            { 
                if (msisdnB==arr(1) && sid==arr(7) && eventDt>=arr(5) && eventDt<=arr(6)) {
                    //Returning PREFIX|COUNTRY|OPERATOR|CITY
                    resultArr(1) = arr(1) + "|" +  arr(2) + "|" +  arr(3) + "|" +  arr(4)              
                }
            }
            
          }  
      }
   
      return resultArr
    }
    
    /**
     * Get interconnect value for single length
     * @param List of Array of Interconnect reference for that length, A MSISDN substring, B MSISDN substring, Event Date, SID (empty string if not required), array of string for saving the result
     * @return array of string : 	(0) A NUMBER : PREFIX|COUNTRY|OPERATOR|CITY or empty string
     * 														(1) B NUMBER : PREFIX|COUNTRY|OPERATOR|CITY or empty string
     */ //resultMap:collection.mutable.Map[String, String],
    def getIntcctValueEachLength3(interconnectListArr: List[Array[String]], 
        msisdnA: String, msisdnB: String, eventDt:String, sid:String, 
        resultArr:Array[String], length: Integer, serviceScenario: String ) : Array[String] = {
      
      breakable {
        interconnectListArr.foreach { arr => 
            
            if (resultArr(0) != null && resultArr(1) != null)
               break
           
            if (resultArr(0) == null)
            { 
                if (msisdnA==arr(1) && sid==arr(7) && eventDt>=arr(5) && eventDt<=arr(6)) {
                    //Returning PREFIX|COUNTRY|OPERATOR|CITY
                    resultArr(0) = arr(1) + "|" +  arr(2) + "|" +  arr(3) + "|" +  arr(4)
                }
            }
            
            if (resultArr(1) == null)
            { 
                // serviceScenario=2 (Outgoing BNumber will be MSISDN which do not need SID Checking)
                if (serviceScenario == "2") {
                  if (msisdnB==arr(1) && ""==arr(7) && eventDt>=arr(5) && eventDt<=arr(6)) {
                    //Returning PREFIX|COUNTRY|OPERATOR|CITY
                    resultArr(1) = arr(1) + "|" +  arr(2) + "|" +  arr(3) + "|" +  arr(4)              
                  }
                }
                else {
                  if (msisdnB==arr(1) && sid==arr(7) && eventDt>=arr(5) && eventDt<=arr(6)) {
                    //Returning PREFIX|COUNTRY|OPERATOR|CITY
                    resultArr(1) = arr(1) + "|" +  arr(2) + "|" +  arr(3) + "|" +  arr(4)              
                  }
                }
            }
            
          }  
      }
   
      return resultArr
    }
  }
 def getMonthID(inputDate:String) : String = {
    val inputFormat = new SimpleDateFormat("yyyyMMdd")
    val monthIDFormat = new SimpleDateFormat("yyyyMM")
    val monthID = monthIDFormat.format(inputFormat.parse(inputDate))
    return monthID
  }
  
  def getPreviousMonthDays(date : String ) : String = {
    
     val inputFormat = new SimpleDateFormat("yyyyMMdd")
    val  givenDate = inputFormat.parse(date);
 
  // First of previous month
    val cal = Calendar.getInstance();
    cal.setTime(givenDate);
    cal.add(Calendar.MONTH, -1);
    cal.set(Calendar.DAY_OF_MONTH, 1);
    val firstOfPreviousMonth = inputFormat.format(cal.getTime());
    
    println ("Given Date : " + date)
    println("First of Previous month : " +     firstOfPreviousMonth )
    
    return firstOfPreviousMonth
    
  }
 /**
  * For testing only
  */
 def main(args: Array[String]): Unit = {
//    val sc = new SparkContext("local", "testing spark ", new SparkConf())
//    val line = sc.parallelize(List("Hello", "Word lagi"))
//    println(line.collect().mkString(" "));
//     println("Common Process");
//   
//
//
//		val myVal :String ="'abcdef'#aaaaa#bbbbb#'1234'='cccc'#4567*ddddd,2222*ffff,3333*gggg#;44444;;5555";
//		val myVal :String ="""abcdef|aaaaa|bbbbb|'sub1'#"sub2"#sub3|4567#ddddd|2222#ffff#3333#gggg|44444#5555""";
//		val myVal2 = myVal.split("[\\|]",3);

//		println(myVal2.length);
//		var num:Int=1;
//    for ( x <- myVal2) {
//      print (num+" :"); 
//      println( x )
//      num=num+1;
//    }
     
//     val myValAdjustmentNonValid01="EXT|ssppadpsvr4||14585715718655637479||20160524160357+0700|AJKT22|261351877|20160524160357+0700|3888||||RUP|85715718655|RUP|||||10000000|10000000|0|26686121700713|||||||||AIROUTPUTCDR_4006_AJKT22_1357_20160524-160317.AIR|20160524";
       val myValAdjustmentValid01="EXT|ssppadpsvr4||14585715718655637479||20160524160357+0700|AJKT22|261351877|20160524160357+0700|3888||||RUP|85715718655|RUP|||||10000000|10000000|0|26686121700713|||||||131##3000.000000##3000.000000##20160525###1####||AIROUTPUTCDR_4006_AJKT22_1357_20160524-160317.AIR|20160524";
       val myValAdjustmentValid02="EXT|ssppadpsvr3||13585717719136486346||20160524154446+0700|AJKT23|57048592|20160524154446+0700|2008||||RUP|85717719136|RUP|||||10000000|10000000|0|26686121700713|||||||10####0.000000#20160531#20160626###1####[20##104857600.000000##104857600.000000#20160531#20160626###1####[53####0.000000#20160531#20160626###1####[54####0.000000#20160531#20160626###1####[55####0.000000#20160531#20160626###1####[60####0.000000#20160531#20160626###1####[100####104857600.000000#20160531#20160626###1####[148####104857600.000000#20160531#20160626###1####[151####0.000000#20160531#20160626###1####[152####0.000000#20160531#20160626###1####||AIROUTPUTCDR_4006_AJKT23_0333_20160524-154059.AIR|20160524"
     val myValRefillValid03="EXT|SEV||497687875||20160526000659+0700|AJKT30|119878154|20160526000659+0700|3848|false|||500000.000000|RUP|||2|6A|8201|||85716928283|RUP|85716928283||10000000#5000.000000#####30#42****7500.000000***1********]41****0.000000***1********##3848####20160724#3#1#20160624#1='0*']2='1*']3='0*']4='0*']5='0*']6='0*']7='0*']8='0*']9='0*']10='0*']11='0*']12='0*']13='0*']14='0*']15='0*']16='0*']17='0*']18='0*']19='0*']20='0*']21='0*']22='0*']23='0*']24='0*']25='0*']26='0*']27='0*']28='0*']29='0*']30='0*']31='0*']32='0*']33='0*']34='0*']35='0*']36='0*']37='0*']38='0*']39='0*']40='0*']41='0*']42='0*']43='0*']44='0*']45='0*']46='0*']47='0*']48='0*']49='0*']50='0*']51='0*']52='0*']53='0*']79='0*']91='0*'#####|10000000######30#42**10000.000000**17500.000000***1********]41**40000.000000**40000.000000***1********##3848####20160727#3#1#20160627#1='0*']2='1*']3='0*']4='0*']5='0*']6='0*']7='0*']8='0*']9='0*']10='0*']11='0*']12='0*']13='0*']14='0*']15='0*']16='0*']17='0*']18='0*']19='0*']20='0*']21='0*']22='0*']23='0*']24='0*']25='0*']26='0*']27='0*']28='0*']29='0*']30='0*']31='0*']32='0*']33='0*']34='0*']35='0*']36='0*']37='0*']38='0*']39='0*']40='0*']41='0*']42='0*']43='0*']44='0*']45='0*']46='0*']47='0*']48='0*']49='0*']50='0*']51='0*']52='0*']53='0*']79='0*']91='0*'#####||||||120|90|20160404|true|||0|32537111420101||||||||1#Refill AIR period R103_Refill_19April2016_3880_SCOptim_1May2016_V4 Valid: 20160524T23:59:59+0700 Last modified: 20160525T07:04:05+0700[4#Account refill AIR period R64_AccReff_18April2016_4579_SCOptim_28Apr2016_V2 Valid: 20160524T23:59:59+0700 Last modified: 20160525T07:26:11+0700[7#Promotion AIR period R2_Promotion_test Valid: 20130331T16:27:45+0700 Last modified: 20160317T02:50:08+0700[1#Account division AIR period R220_AccDiv_19April2016_3880_SCOptim_28Apr2016_V4 Valid: 20160524T23:59:59+0700 Last modified: 20160525T07:34:39+0700|AccountGroupIDTDF#0|0||||0||AIROUTPUTCDR_4006_AJKT30_5454_20160526-000248.AIR|20160526"
//     
     //already OK
     for (x <- getTupleInTupleNonUsage(myValAdjustmentValid01,"|",30,"#",0,2,100,"[")){
         if(x != null){
           println(x)
         }
       }
//
//    println(getTupleInTuple(myVal,"|",3,"#",2));
     println(getTupleInTuple("2001||615090519264307000|CJK10|Core_Context_R1A@ericsson.com|CAPv2_V.1.0@ericsson.com|5-347205635-03f0b114|3008e485|0|true","|",6,":",1));
    //println(hexToInt(""));
//    println(swapChar("ABCDEF","2,1,4,3,6,5"));
    //println(getKeyVal2("bbbb=|\"abcd\"='4444',aaaa*5555]cccc=3333,dddd=","aaaa","*","]"));
//    println(getKeyVal2("0*6285866267728","0","*","["));
//    println(getKeyVal2("0*628158889098[1*510018810308930","1","*","["));
//    getKeyVal("test","*");
//    println(getKeyVal("16778217*628160971000]16778218*3830]16778219*6285781418041]16777416*6285770954807]16777616*15F010975B7759","16778219","*","]"));
//    println(getKeyVal("16778217*628160971000","16778217","*","]"));
//    println(normalizeMSISDN("+6281611111"));
//    println(normalizeMSISDN("081611111"));
//    println(normalizeMSISDN("196281611111"));
//    println(normalizeMSISDN("06281611111"));
//    println(normalizeMSISDN());
//    println(swapChar2("ABCDEF","5|5"));
//    var test=null;
//    println(swapChar2(test,"2|1"));
//    println(getNumber("100.00XX"));
		
	}


}

