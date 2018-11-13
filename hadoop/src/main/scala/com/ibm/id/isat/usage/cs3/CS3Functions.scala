package com.ibm.id.isat.usage.cs3

import org.apache.spark.Partitioner
import com.ibm.id.isat.utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

/*class DomainNamePartitioner(numParts  : Int) extends Partitioner {
  override def numPartitions : Int = numParts
  
  override def getPartitions(key : Any): Int = {
    val domain = new java.net.URL(key.toString()).getHost
    val code = (domain.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    }
    else {
      code
    }
  }
  
  override def equals(other : Any) : Boolean = other match {
    case dnp : DomainNamePartitioner => dnp.numPartitions == numPartitions
    case _=> false
  }
  
}*/

object CS3Functions  {
  
  def formatCS3 ( line: String, delimiter: String) : String = {
    val col = line.split(delimiter)
   
    val result =  
            /*--- Trigger Date ---*/
            col(9).substring(0,8).concat("|" + 
            /*--- Trigger Time ---*/
            col(9).substring(8,10) + "|" +
            /*--- A Party No ---*/
            CS3Functions.getPartyNumber("A", col(4), Common.normalizeMSISDN(col(7)), Common.normalizeMSISDN(col(8))) + "|" +
            /*--- B Party No ---*/
            CS3Functions.getPartyNumber("B", col(4), Common.normalizeMSISDN(col(7)), Common.normalizeMSISDN(col(8))) + "|" +
            /*--- Service Class, Origin Realm, Origin Host ---*/
            col(21) + "|" +  col(19) + "|" +  col(20)  + "|" +  
            /*--- Payment Category, Traffic Case ---*/
            Common.getUsageCategory(col(21)) + "|" +  col(4) + "|" +
            /*--- Traffic Case Name ---*/
            Common.trafficCaseRoam.getOrElse(col(4), "") + "|" + 
            /*--- Direction Type ---*/
            Common.trafficCaseDrct.getOrElse(col(4), "") + "|" +
            /*--- Usage Direction ---*/
            Common.hexToInt(col(10)) + "|" + 
            /*--- APartyNoPad ---*/
            CS3Functions.getPartyNumber("A", col(4), Common.normalizeMSISDN(col(7)), Common.normalizeMSISDN(col(8))).padTo(14, ".").mkString("") + "|" + 
            /*--- BPartyNoPad ---*/
            CS3Functions.getPartyNumber("B", col(4), Common.normalizeMSISDN(col(7)), Common.normalizeMSISDN(col(8))).padTo(14, ".").mkString("") + "|" +   
            /*--- Real Filename, Usage Vol, Trigger Time, Account Value Before, Account Value After ---*/
            col(56).split("/").last + "|" +  col(11) + "|" +  col(9) + "|" +  col(14) + "|" +  col(15) + "|" + 
            /*--- Service Usage Direction, Service Usage Destination, Hit, PPN DTTM ---*/
            "Others" + "|" + "Others" + "|" + "1" + "|" +
            /*--- Final Charge, Ext Text, DA Used ---*/
            col(13) + "|" + col(33) + "|" + CS3Functions.getDAUsed(Common.getUsageCategory(col(21)), col(27)) + "|" + 
            /*--- Subscriber ID, Node ID, Accumulator Value Info, FileDate ---*/
            Common.normalizeMSISDN(col(6)) + "|" + col(1) + "|" + col(26) + "|" + col(57) + "|" + 
            /*--- Key : APartyNo + Trigger Time + random number ---*/
            Common.getKeyCDRInq( CS3Functions.getPartyNumber("A", col(4), Common.normalizeMSISDN(col(7)), Common.normalizeMSISDN(col(8))), col(9)) + "|" +
            /*--- Dedicated Account Info Original Column --*/
            CS3Functions.concatDA(col(27))  +
            /*--- DA & MA Info---*/
            "~" + CS3Functions.getDAInfo(Common.getUsageCategory(col(21)), col(27))
            )
  
   
          return result;
  }
  
  def getPartyNumber( Party: String, TrafficCase: String, CallingMSISDN:String, CalledMSISDN:String ) : String = {
    var MSISDN = "";
    
    try {
        if (Party == "A") 
            MSISDN =  TrafficCase 
                            match {case "20"|"3" => CallingMSISDN 
                            case "21"|"5" => CalledMSISDN }
        if (Party == "B") 
            MSISDN =  TrafficCase 
                            match {case "20"|"3" => CalledMSISDN 
                            case "21"|"5" => CallingMSISDN }
           
        return MSISDN
    }
    
    catch {
	    case e: Exception => return "";
	  }
    
   }
  
  
  
  /*
   * To get DA Information
   * INPUT : "PREPAID," 										 												; OUTPUT : "REGULER"
   * INPUT : "PREPAID,1#499990425.00#49.." 	 												; OUTPUT : "REGULER[1#499990425.00#49.."
   * INPUT : "POSTPAID,1#71200.00#70400...." 												;	OUTPUT : "1#71200.00#70400...."
   * INPUT : "POSTPAID,1#71200.00#70400....[7#71200.00#70400...."  	; OUTPUT : 1#71200.00#70400....[7#71200.00#70400....
   */
  def getDAInfo( UsageType: String, DAinfo: String) : String = {
    var AccountID = "";
    
    try {
        if (UsageType == "PREPAID" && DAinfo != "") 
            AccountID =  "REGULER[" + DAinfo
        else if (UsageType == "PREPAID" && DAinfo == "") 
            AccountID =  "REGULER"
        else if (UsageType == "POSTPAID" && DAinfo != "")
            AccountID =  DAinfo
        else if (UsageType == "POSTPAID" && DAinfo == "")
            AccountID =  "DA1"
            
        return AccountID
    } 
      
    catch {
	    case e: Exception => return "";
	  }
    
   }
  
  /*
   * To get DA Used
   * INPUT : "PREPAID," 										 												; OUTPUT : 0
   * INPUT : "PREPAID,1#499990425.00#49.." 	 												; OUTPUT : 1
   * INPUT : "POSTPAID,1#71200.00#70400...." 												;	OUTPUT : 0
   * INPUT : "POSTPAID,1#71200.00#70400....[7#71200.00#70400...."  	; OUTPUT : 1
   */
  def getDAUsed( UsageType: String, DAinfo: String) : Int = {
    var daUsed = 0;
    
    try {
        if (UsageType == "PREPAID" && DAinfo != "") 
            daUsed = DAinfo.split("\\[").length  
        else if (UsageType == "PREPAID" && DAinfo == "") 
            daUsed = 0
        else if (UsageType == "POSTPAID" && DAinfo.split("\\[").length > 1) 
            daUsed =  DAinfo.split("\\[").length - 1  
           
        return daUsed
    }
    
    catch {
	    case e: Exception => return 0;
	  }
    
   }
  
  /*
   * To add "DA" word in every DA Info
   * INPUT  : 1#+499710209.00#+499709709.00##0[7#+499710209.00#+499709909.00##0
   * OUTPUT : DA1#+499710209.00#+499709709.00##0[DA7#+499710209.00#+499709909.00##0
   */
  def concatDA(daInfo: String) : String = {
    
    var input = daInfo.split("\\[")
    var result = ""
    
    if(daInfo != "")
    {
       for (index <- 0 until daInfo.split("\\[").length by 1)
       {     
         if (index > 0)
           result = result + "[DA" + input(index)
         else
           result = result + "DA" + input(index)
       }
    }
    
    return result
  }
  /*
   * To get combination of account ID, revenue code, usage amount, and SID
   * sample output :  REGULER|33000352011006|65.0|33000352011006
   * sample output :  DA1|33000352011006DA1|935.0|33000352011006
   */
  def getAllRev( originRealm: String, extText: String, trafficCaseName: String, dedicatedAccInfo:String, paymentCat: String, finalCharge: String, acValBfr: String, acValAft: String, daUsed: String, delimiter: String ) : String = {
    var accountID = "";
    var revcode = "";
    var usgAmount = 0.0;
    var SID = "";
    
    try {
        //println (originRealm.contains("mmsc") + ":" + originRealm + "," + extText + "," + finalCharge.toDouble + "," + acValBfr + "," + acValAft + "," +  dedicatedAccInfo)
        //println (originRealm.contains("mmsc") + ":" +  dedicatedAccInfo)
        //println (originRealm.contains("mmsc") + ":" + originRealm + "," + extText )
        
        if (originRealm.contains("mmsc"))
        {
            accountID =  dedicatedAccInfo
            usgAmount = finalCharge.toDouble
            
            if (trafficCaseName.toLowerCase().contains("non roaming"))
                revcode = "MMSLOCAL"
            else if (trafficCaseName.toLowerCase().contains("roaming"))
                revcode = "MMSROAMING"
            else
                revcode = ""
        }        
        else if (!originRealm.contains("mmsc") && extText != "" && !extText.isEmpty())
        {
            if (dedicatedAccInfo == "REGULER" && daUsed == "0")  /*--- for PREPAID AND USED MA ONLY ---*/
            {
                accountID =  dedicatedAccInfo
                revcode =  extText
                usgAmount = finalCharge.toDouble
            }
            else if (dedicatedAccInfo == "REGULER" && daUsed != "0") /*--- for PREPAID AND USED MA & DA ---*/
            {
                accountID =  dedicatedAccInfo
                revcode =  extText
                usgAmount = finalCharge.toDouble - ( acValBfr.toDouble - acValAft.toDouble)
            }
            else if (paymentCat == "POSTPAID"  && daUsed == "0") /*--- for POSTPAID  AND USED MA ONLY ---*/
            {
                accountID =  "REGULER"
                revcode =  extText
                usgAmount = finalCharge.toDouble
            }
            else if (paymentCat == "POSTPAID" && dedicatedAccInfo.split("#")(0) == "1" && daUsed != "0") /*--- for POSTPAID AND USED MA & DA ---*/
            {
                accountID =  "REGULER"
                revcode =  extText
                usgAmount = finalCharge.toDouble - ( acValBfr.toDouble - acValAft.toDouble)
            }
            
            else /*--- for DA both PREPAID and POSTPAID ---*/
            {   
              accountID =  "DA" + dedicatedAccInfo.split("#")(0)
              revcode =  extText + "DA" + dedicatedAccInfo.split("#")(0)                  
              usgAmount = acValBfr.toDouble - acValAft.toDouble
              
              /*if (paymentCat == "PREPAID")
              {
                  revcode =  extText + "DA" + dedicatedAccInfo.split("#")(0)                  
                  usgAmount = acValBfr.toDouble - acValAft.toDouble
              }
              else /*--POSTPAID--*/
              {
                  revcode =  extText
                  usgAmount = finalCharge.toDouble
                  
                  if(accountID == "DA1")
                    accountID = "REGULER"
                    
              }*/
            }
        }
        else
            ""
        
        if(extText != "")
          SID = extText
        else
          SID = ""

        return accountID + delimiter + revcode + delimiter + usgAmount + delimiter + SID;
    }
    
    catch {
	    case e: Exception => return accountID + delimiter + revcode + delimiter + usgAmount + delimiter + SID;
	  }
    
   }
  
   def getIntcctPfx(intcctPfx13:String, intcctPfx12:String, intcctPfx11:String, intcctPfx10:String, intcctPfx9:String, intcctPfx8:String, intcctPfx7:String, intcctPfx6:String, intcctPfx5:String) : String =
   {
     
      if ( ! intcctPfx13.isEmpty() )
        return intcctPfx13
      else if ( ! intcctPfx12.isEmpty() )
        return intcctPfx12
      else if ( ! intcctPfx11.isEmpty() )
        return intcctPfx11
      else 
        return ""
   }
   
   def getServiceClassDF(sqlContext: SQLContext) : DataFrame = {
    
     val svcClassDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter",",")
                      .schema(ReferenceSchema.svcClassSchema)
                      .load("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/ref_service_class no -99.csv"))
                      //.load("/user/apps/CS3/reference/ref_service_class.csv"))
     return svcClassDF
    
   }
}