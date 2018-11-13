package com.ibm.id.isat.usage.cs5

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._

 /*****************************************************************************************************
 * 
 * CS5 Transformation 
 * Select columns from tuples
 * Input: CCN CS5 Conversion
 * 
 * Author: Nico Anandito
 * IBM Indonesia for Indosat Ooredoo
 * June 2016
 * 
 *****************************************************************************************************/ 
object CS5OldSelect {
  
    
  /*
   * Generate Transformed String and select columns for CS5
   * @param Conversion result for CS5, delimited by pipe (|)
   * @return Transformed row
   */
   def genCS5TransformedRow( line:String, prcDt:String, jobId:String) : Row = {  
    return Row(this.getTransactionDate(line)
          ,this.getTransactionHour(line)
          ,this.getMSISDN(line)
          ,this.getCCNNode(line)
          ,this.getAPartyNumber(line)              //LOOKUP INTCCT
          ,this.getBPartyNumber(line)              //LOOKUP INTCCT
          ,this.getServiceClass(line)              //LOOKUP PP_BRAND
          ,this.getCallClassCat(line)              //LOOKUP CALLCLASS
          ,this.getMSCAddress(line)
          ,this.getOriginRealm(line)
          ,this.getOriginHost(line)
          ,this.getMCCMNC(line)
          ,this.getLAC(line)                        //Lookup to LACCI REF
          ,this.getCI(line)                         //Lookup to LACCI REF
          ,this.getECI(line)                         
          ,this.getIMSI(line)
          ,this.getAPN(line)
          ,this.getServiceScenario(line)
          ,this.getRoamingPosition(line)
          ,this.getFAF(line)
          ,this.getFAFNumber(line)  
          ,this.getRatingGroup(line)  
          ,this.getContentType(line)
          ,this.getIMEI(line)  
          ,this.getGGSNAddress(line)  
          ,this.getSGSNAddress(line)
          ,this.getCorrelationId(line)              //CALL_REFERENCE
          ,this.getChargingId(line)
          ,this.getRATType(line)
          ,this.getServiceOfferId(line)
          ,this.getPaymentCategory(line)
          ,this.getDedicatedAccountsRow(line)
          ,this.getDedicatedAccountValuesRow(line)
          ,this.getAccountGroup(line)
          ,this.getFAFName(line)
          ,this.getAccountValueDeducted(line)
          ,this.getAccumulator(line)
          ,this.getCommunityId(line, 0) 
          ,this.getCommunityId(line, 1) 
          ,this.getCommunityId(line, 2) 
          ,this.getAccumulatedCost(line)
          ,this.getHit(line)
          ,this.getUsageVolume(line)
          ,this.getUsageDuration(line)          
          ,this.getIntcctLkpPad( this.getAPartyNumber(line) )
          ,this.getIntcctLkpPad( this.getBPartyNumber(line) )
          ,this.getSpecConsumpSPMADA(line)
          ,this.getSpecConsumpSPCONS(line)
          ,this.getVasServiceId(line)
          ,this.getTrafficCase(line)  
          ,this.getUsedOfferSC(line)
          ,this.getOfferAttributeAreaname(line)
          ,this.getAccountValueBefore(line)
          ,this.getAccountValueAfter(line)
          ,this.getServiceUsageType(line)
          ,this.getCCRTriggerTime(line)
          ,this.getCCREventTime(line) 
          ,this.getJobId(line, jobId)
          ,this.getRecordId(line)         
          ,this.getPrcDt(line, prcDt)
          ,this.getSourceType(line)
          ,this.getFileName(line)
          ,this.getFileDate(line) )
  }
   
  /*
   * Generate Transformed String and select columns for CS5 Postpaid
   * @param Conversion result for CS5, delimited by pipe (|)
   * @return Transformed postpaid row
   */
  def genCS5PostTransformedRow( line:String, prcDt:String, jobId:String ) : Row = {  
    return Row(
        this.getMSISDN(line),
        this.getTransactionDate(line),
        this.getServiceIdentifier(line),
        this.getServiceClass(line),
        this.getServiceScenario(line),
        this.getRoamingPosition(line),
        this.getFAF(line),
        this.getDedicatedAccountsRow(line),
        this.getAccountValueDeducted(line),
        this.getCCRTreeDefinedFields(line),
        this.getAccumulatedCost(line),
        this.getCCRBonusAdjustment(line),
        "", //RECORD_NUMBER
        this.getChargingContextId(line),
        this.getFileName(line),
        this.getPostCCRTriggerTime(line),
        this.getLAC(line),
        this.getCI(line),
        this.getAPN(line),
        this.getUsageDuration(line),
        this.getUsageVolume(line),
        this.getUsageDuration(line),
        this.getRatingGroup(line),
        this.getContentType(line),
        this.getRATType(line),
        this.getIMEI(line),
        this.getPostCCREventTime(line),
        this.getIMSI(line),
        this.getGGSNAddress(line),
        this.getSGSNAddress(line),
        this.getMSCAddress(line),
        this.getCallReference(line),
        this.getChargingId(line),
        this.getOriginRealm(line),
        this.getOriginHost(line),
        this.getPostOtherPartyNum(line),
        this.getTDFCostBand(line),
        this.getTDFCallType(line),
        this.getCallTypeName(this.getTDFCallType(line)),
        this.getTDFLocation(line),
        this.getTDFDiscount(line),
        this.getPostRatTypeName(this.getRATType(line)),
        this.getPostEventId(line),
        this.getCCRTriggerTime(line),
        this.getCCREventTime(line), 
//        this.getIntcctLkpPad( this.getMSISDN(line) ) ,
//        this.getIntcctLkpPad( this.getPostOtherPartyNum(line) ) ,
        this.getDedicatedAccountValuesRowPostpaid(line),
        this.getPostVasServiceId(line),
        this.getUsedOfferSC(line),
        this.getOfferAttributeAreaname(line),
        this.getAccountValueBefore(line),
        this.getAccountValueAfter(line),
        this.getCCNNode(line),
        this.getCommunityId(line, 0),
        this.getCommunityId(line, 1),
        this.getCommunityId(line, 2),
        this.getAccountGroup(line),
        this.getServiceOfferId(line),
        this.getSpecConsumpSPMADA(line),
        this.getSpecConsumpSPCONS(line),
        this.getAccumulator(line),
        this.getMCCMNC(line),        
        this.getECI(line),
        this.getServiceUsageTypePostpaid(line),
        this.getJobId(line, jobId),
        this.getRecordId(line),
        this.getPrcDt(line, prcDt),
        this.getSourceType(line),
        this.getFileDate(line)
        
        
          )
  } 
   
  /*
   * Generate Rejected Row for Processing
   * Note: for column #32, replaced with single DA, to prevent transpose in reprocess
   * @param Rejected reference result for CS5, delimited by pipe (|)
   * @return Transformed row
   */
   def genCS5RejectedRow( line:String ) : Row = {  
     val r = line.split("\\|")
     return Row(
         r(0), r(1), r(2), r(3), r(4),  r(5), r(6),  r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17),  r(18), r(19), r(20),
         r(21), r(22), r(23), r(24), r(25), r(26).stripPrefix("\"").stripSuffix("\""), r(27), r(28), r(29), r(30), r(31), r(93)+"~"+r(90)+"~"+r(91)+"~"+r(92), r(33), r(34), r(35), r(36), r(37), r(38), r(39), Common.toDouble(r(40)),
         Common.toInt(r(41)), Common.toDouble(r(42)), Common.toDouble(r(43)), r(44), r(45), r(46), r(47), r(48),r(49), r(50), r(51), r(52), r(53), r(54), r(55), r(56), r(57), r(58), r(59), r(60), 
         r(61), r(62)         
         )
   }
   
     /*
   * Generate Rejected Row for Processing
   * @param Rejected reference result for CS5 Postpaid, delimited by pipe (|)
   * @return Transformed row
   */
   def genCS5RejectedRowPostpaid( line:String ) : Row = {  
     val r = line.split("\\|")
     return Row(
         r(0), r(1), r(2), r(3), r(4),  r(5), r(6),  r(7), r(8), r(9), Common.toDouble(r(10)), r(11), r(12), r(13), r(14), r(15), r(16), r(17),  r(18), Common.toDouble(r(19)), Common.toDouble(r(20)),
         Common.toDouble(r(21)), r(22), r(23), r(24), r(25), r(26), r(27), r(28), r(29), r(30), r(31).stripPrefix("\"").stripSuffix("\""), r(32), r(33), r(34), r(35), r(36), r(37), r(38), r(39), r(40),
         r(41), r(42), r(43), r(44), r(45), r(46), r(47), r(48),r(49), r(50), r(51), r(52), r(53), r(54), r(55), r(56), r(57), r(58), r(59), r(60), 
         r(61), r(62), r(63), r(64), r(65), r(66), r(67)                
         )
   }
  
  /*
   * 
   */
  def getSourceType( content:String ) : String = {  
    return "CS5"
  }
  
  /*
   * 
   */
  def getCCNNode( content:String ) : String = {  
    return Common.getTuple(content, "|", 3)
  }
  
  /**
   * Get A Party Number
   * @return MSISDN of A Party string
   */
  def getAPartyNumber( content:String ) : String = {
    
    var msisdn = ""            
    if ( this.getNetworkDirection(content) == "MO" ) {
      // Get SERVED_MSISDN
      msisdn = Common.getKeyVal( Common.getTuple(content, "|", 10) , "0", "#", "[" )
    } else {
      // Get CCR_CHARGING_CONTEXT_SPECIFICS with param 16778219
      msisdn = Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), "16778219", "*", "]" )
    }       
    return Common.normalizeMSISDN(msisdn)
  }
  
  /**
   * Get B Party Number
   * @return MSISDN of B Party string
   */
  def getBPartyNumber( content:String ) : String = {
    
    var msisdn = ""            
    if ( this.getNetworkDirection(content) == "MO" ) {
      // Get CCR_CHARGING_CONTEXT_SPECIFICS with param 16778219
      msisdn = Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), "16778219", "*", "]" )
    } else {
      // Get SERVED_MSISDN
      msisdn = Common.getKeyVal( Common.getTuple(content, "|", 10) , "0", "#", "[" )
    }       
    return Common.normalizeMSISDN(msisdn)
  }
  
  /**
   * Get network direction string
   * @returns "MO" or "MT"
   */
  def getNetworkDirection( content:String ) : String = {
    val ccrServiceScenario = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 )
    
    if ( ccrServiceScenario == "0" || ccrServiceScenario == "1" ) 
      return "MO"
    else 
      return "MT"    
  }
  
  /**
   * Get service usage type definition from network
   */
  def getServiceUsageType( content:String ) : String = {
    val chargingContextId = Common.getTuple( content, "|", 5 )
    val ccrServiceIdentifier = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 0 )
    val ccrServiceScenario = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 ) 
    val callClassId = Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "DWSCallClassCategory", "*", "]" )
    
    if ( chargingContextId.startsWith("Ericsson") )  return "DATA"
    else if ( chargingContextId.startsWith("CAP") )  return "VOICE"    
    else if ( chargingContextId.startsWith("SCAP") && ccrServiceScenario=="2" && (callClassId=="1" || callClassId=="-1") )  return "VAS"
    else if ( chargingContextId.startsWith("SCAP") )  return "SMS"
    else return "UNKNOWN"     
  }
  
  /**
   * Get service usage type definition from network for postpaid
   */
  def getServiceUsageTypePostpaid( content:String ) : String = {
    val chargingContextId = Common.getTuple( content, "|", 5 )
    val ccrServiceIdentifier = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 0 )
    val ccrServiceScenario = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 ) 
    val callClassId = Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "DWSCallClassCategory", "*", "]" )
    
    if ( chargingContextId.startsWith("Ericsson") )  return "DATA"
    else if ( chargingContextId.startsWith("CAP") )  return "VOICE"    
    else if ( chargingContextId.startsWith("SCAP") && ccrServiceScenario=="2" )  return "VAS"
    else if ( chargingContextId.startsWith("SCAP") )  return "SMS"
    else return "UNKNOWN"     
  }
  
  /**
   * Get MSC address (from Voice CDR only)
   */
  def getMSCAddress( content:String ) : String = {
    if ( getServiceUsageType(content) == "VOICE" ) {
      return Common.getTuple(content, "|", 12)
    }
    else return ""
  }
  
  /**
   * Get origin realm (from SMS CDR only)
   */
  def getOriginRealm( content:String ) : String = {
    if ( getServiceUsageType(content) == "SMS" ) {
      return Common.getTupleInTuple(content, "|", 12, "#", 0)
    }
    else return ""
  }
   
  /**
   * Get origin host (from SMS CDR only)
   */
   def getOriginHost( content:String ) : String = {
    if ( getServiceUsageType(content) == "SMS" ) {
      return Common.getTupleInTuple(content, "|", 12, "#", 1)
    }
    else return ""
  }
  
   /**
    * Get transaction date from CCR_TRIGGER_TIME
    */
  def getTransactionDate( content:String ) : String = {
     return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).take(8)//.substring(0, 8)
  }
  
  /**
    * Get transaction hour from CCR_TRIGGER_TIME
    */
  def getTransactionHour( content:String ) : String = {
     return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).substring(8, 10)
  }
  
  /**
    * Get service class
    */
  def getServiceClass( content:String ) : String = {
     return Common.getTupleInTuple(Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 1)
  }
  
  /**
   * 
   */
  def getChargingContextSpecific( content:String, param:String ) : String = {
     return Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), param, "*", "]" )
  }
  
  /**
   * Get MCC MNC from multiple service type:
   * For SMS: sample is "15F01074058647" 
   */
  def getMCCMNC( content:String ) : String = {
     if ( this.getServiceUsageType(content) == "DATA" )  return this.getChargingContextSpecific(content, "16778232")
     else if ( this.getServiceUsageType(content) == "VOICE" ) return this.getChargingContextSpecific(content, "16778249")
     else if ( this.getServiceUsageType(content) == "SMS" )
       try return Common.swapChar( substring(getChargingContextSpecific(content, "16777616"),0,6).replace("F", ""), "2,1,3,5,4" )
       catch {
         case e: Exception => return ""
       }
     else return ""     
  }
  
  /*
   * 
   */
  def getRATType( content:String ) : String = {
     return this.getChargingContextSpecific(content, "16778240")
  }
  
  /*
   * 
   */
  def getLAC( content:String ) : String = {
     if ( this.getServiceUsageType(content) == "DATA" && this.getRATType(content) == "6" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778333") )
     else if ( this.getServiceUsageType(content) == "DATA" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778250") )
     else if ( this.getServiceUsageType(content) == "VOICE" ) return Common.hexToInt( this.getChargingContextSpecific(content, "16778250") ) 
     else if ( this.getServiceUsageType(content) == "SMS" )   return Common.hexToInt( substring(getChargingContextSpecific(content, "16777616"), 7,10) )
     else return ""     
  }
  
  /*
   * 
   */
  def getCI( content:String ) : String = {
     if ( this.getServiceUsageType(content) == "DATA" && this.getRATType(content) == "6" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778332") )
     else if ( this.getServiceUsageType(content) == "DATA" && this.getRATType(content) == "1" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778252") )
     else if ( this.getServiceUsageType(content) == "DATA" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778251") )
     else if ( this.getServiceUsageType(content) == "VOICE" ) return Common.hexToInt( this.getChargingContextSpecific(content, "16778251") ) 
     else if ( this.getServiceUsageType(content) == "SMS" )   return Common.hexToInt( substring(getChargingContextSpecific(content, "16777616"), 11,14) )
     else return ""     
  }
  
  /*
   * 
   */
  def getECI( content:String ) : String = {
     if ( this.getServiceUsageType(content) == "SMS" || this.getServiceUsageType(content) == "VAS")   
       return Common.hexToInt( substring(getChargingContextSpecific(content, "16777616"), 6,14) ) 
     else return ""     
  }
  
  /*
   * 
   */
  def getIMSI( content:String ) : String = {
     return Common.getKeyVal( Common.getTuple(content, "|", 10) , "1", "#", "[" )
  }
  
  /*
   * 
   */
  def getAPN( content:String ) : String = {
     return getChargingContextSpecific(content, "16778235")
  }
  
  /*
   * 
   */
  def getServiceScenario( content:String ) : String = {
     return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 )
  }
  
  /*
   * 
   */
  def getRoamingPosition( content:String ) : String = {
     return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 6 )
  }
  
  /*
   * 
   */
  def getFAF( content:String ) : String = {
     return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 , "*", 8)
  }
  
  /*
   * 
   */
  def getFAFNumber( content:String ) : String = {
     return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 , "*", 9)
     
  }
  
  /*
   * Need further checking to other service type
   */
  def getRatingGroup( content:String ) : String = {
    return Common.getTupleInTuple(content, "|", 6, ":", 0)    
  }
  
  /*
   * Need further checking to other service type
   */
  def getContentType( content:String ) : String = {
     return Common.getTupleInTuple(content, "|", 6, ":", 1) 
  }
  
  /*
   * 
   */
  def getIMEI( content:String ) : String = {
     return getChargingContextSpecific(content, "16778243")
  }
    
  /*
   * 
   */
  def getGGSNAddress( content:String ) : String = {
     return getChargingContextSpecific(content, "16778230")
  }
    
  /*
   * 
   */
  def getSGSNAddress( content:String ) : String = {
     return getChargingContextSpecific(content, "16778229")
  }
  
  /*
   * 
   */
  def getCallReference( content:String ) : String = {
     return getChargingContextSpecific(content, "16778229")
  }
  
  /*
   * 
   */
  def getCorrelationId( content:String ) : String = {
     return Common.getTuple(content, "|", 11)
  }
  
  /*
   * 
   */
  def getChargingId( content:String ) : String = {
     return getChargingContextSpecific(content, "16778226")
  }
  
  /*
   * 
   */
  def getServiceOfferId( content:String ) : String = {
     return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 , "*", 10)
  }
  
  /*
   * 
   */
  def getPaymentCategory( content:String ) : String = {
     return Common.getUsageCategory( this.getServiceClass(content) )
  }
  
  /*
 	* Get dedicated account values by row
 	* @returns DA_ID,DA_AMOUNT,DA_UOM separated by 
 	*/
  def getDedicatedAccountsRow( content:String ) : String = {
    return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 )
  }
  
  /*
 	* Get dedicated account values by row 
 	* @returns DA_ID,DA_AMOUNT,DA_UNIT_AMOUNT,DA_UNIT_TYPE(UOM) separated by ~. Multiple DA by ^
 	*/
  def getDedicatedAccountValuesRow( content:String ) : String = {
    val daRow = this.getDedicatedAccountsRow(content)
    
    if (daRow == "") return ""
    //if more than 1 DA
    else if ( daRow.contains("^") ) {
      var daReturn = ""
      var i = 0
      val daArr = daRow.split("\\^")
      
      daArr.foreach { case (a) => 
        //add delimiter if more than 1 DA
        if (i > 0) daReturn = daReturn.concat("^")
        
        //Normalize DA Change
        var daChange = getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 3) )
        if ( daChange.isEmpty() ) daChange = "0.0"
        
        daReturn = daReturn.concat("DA").concat( Common.getTuple(a, "~", 0) )
          .concat( "~" ).concat( daChange )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 13) ) ) //DA UNIT CHANGE
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 14) ) ) //DA UNIT UOM
          
        i += 1
        }
      return daReturn
    }
    //if only 1 DA
    else {
      //Normalize DA Change
      var daChange = getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 3) )
      if ( daChange.isEmpty() ) daChange = "0.0"
      
      return "DA".concat( Common.getTuple(daRow, "~", 0) )
      .concat( "~" ).concat( daChange )
      .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 13) ) ) //DA UNIT CHANGE
      .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 14) ) ) //DA UNIT UOM
    }
         

  }
  
  /*
   * Get total DA amount
   * @params: Input sample: DA54~1200.00~~1^DA20~375.00~~1
   * @returns: 1575.00
   */
  def getTotalDAFromDedicatedAccountValuesRow( content:String ) : Double = {
    
    var total = 0.0
    if ( content.contains("^") ) {
      val daArr = content.split("\\^")
      
      //Loop for each DA
      daArr.foreach { case (a) => 
        if ( Common.getTuple(a, "~", 3) == "1" ) //If DA type is money
        total = total + Common.toDouble( Common.getTuple(a, "~", 1) )
      }
    } else if ( Common.getTuple(content, "~", 3) == "1" )  {  //If DA type is money
      total = Common.toDouble( Common.getTuple(content, "~", 1) )
    }
    
    return total
  }
  
    /*
   * Get AccountRow for Transpose
   * @params: Input sample DedicatedAccountValuesRow: DA20~375.00~~1
   * @params: Input sample AccountValueDeducted: 375.00
   * @returns: DA20~375.00~~1
   */
  def getAccountRowForTranspose( DedicatedAccountValuesRow:String, AccountValueDeducted:Double ) : String = {
    
    val totalDA = this.getTotalDAFromDedicatedAccountValuesRow(DedicatedAccountValuesRow)
    val totalMA = AccountValueDeducted - totalDA    
    val AccountValueDeductedStr = AccountValueDeducted.toString()
    
    if (DedicatedAccountValuesRow == "" ) {
      //MA Only 
      return "MA~"+AccountValueDeductedStr
    } else {
      //With DA
      if ( totalMA == 0 ) {
        //MA is zero 
        return DedicatedAccountValuesRow
      } else {
        //MA is not zero 
        return "MA~"+totalMA+"^"+DedicatedAccountValuesRow
      }      
    }
    
    return ""
  }
  
  /**
 	* Get dedicated account values by row for Postpaid
 	* @returns DA_ID,DA_AMOUNT,DA_UNIT_AMOUNT,DA_UNIT_TYPE(UOM) separated by ~. Multiple DA by ^
 	*/
  def getDedicatedAccountValuesRowPostpaid( content:String ) : String = {
    val daRow = this.getDedicatedAccountsRow(content)
    
    if (daRow == "") return "MA".concat("~"+"0"+"~~")
    //if more than 1 DA
    else if ( daRow.contains("^") ) {
      var daReturn = ""
      var i = 0
      val daArr = daRow.split("\\^")
      
      daArr.foreach { case (a) => 
        //add delimiter if more than 1 DA
        if (i > 0) daReturn = daReturn.concat("^")
        
        daReturn = daReturn.concat("DA").concat( Common.getTuple(daRow, "~", 0) )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 3) ) )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 13) ) )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 14) ) )
          
        i += 1
        }
      return daReturn
    }
    //if only 1 DA
    else return "DA".concat( Common.getTuple(daRow, "~", 0) )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 3) ) )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 13) ) )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(daRow, "~", 14) ) )

  }
  
  /**
   * Example: input 2477.00\6\360 will return 2477.00
   */
  def getAmountFromMonetaryUnit( content:String ) : String = {
     try {
       return content.split("\\\\")(0)
     } catch {
       case e: Exception => return ""
     }
  }
  
   /*
   * 
   */
  def getAccountGroup( content:String ) : String = {
     return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 2 )
  }
  
   /**
   * 
   */
  def getTrafficCase( content:String ) : String = {
     if ( this.getServiceScenario(content) == "0" && this.getRoamingPosition(content) == "0" ) return "H"
     else if ( this.getServiceScenario(content) == "0" && this.getRoamingPosition(content) == "1" ) return "RCAP"
     else if ( this.getServiceScenario(content) == "1" && this.getRoamingPosition(content) == "0" ) return "HCF"
     else if ( this.getServiceScenario(content) == "0" && this.getRoamingPosition(content) == "0" ) return "RCAPCF"
     else return ""
  }
  
   /*
   * 
   */
  def getFAFName( content:String ) : String = {
     if ( this.getFAF(content).nonEmpty ) return "FF"
     else return ""
  }
  
   /*
   * 
   */
  def getUsageVolume( content:String ) : Double = {
    val volume = Common.toDouble( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 1, "*", 3 ) )
    return volume
  }
  
   /*
   * 
   */
  def getUsageDuration( content:String ) : Double = {
     val duration = Common.toDouble( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 1, "*", 1 ) )
     return duration
  }
  
  /*
   * 
   */
  def getFileName( content:String ) : String = {
     return this.getFilenameFromPath( Common.getTuple(content, "|", 15) )
  }
  
  /*
   * 
   */
  def getAccountValueDeducted( content:String ) : String = {
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 13 ) , "~" , 0)
  }
  
  /*
   * 
   */
  def getAccumulator( content:String ) : String = {
     return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 6 )
  }

  
  /**
   * Get community from 
   * @param item Community number
   */
  def getCommunityId( content:String, item:Integer ) : String = {
     val servedCommunityIDs = Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 5 ) , "~", 1)
     try {
       //require four backslashes for regex backslash escape (\)
       return servedCommunityIDs.split("\\\\")(item)  
     }
     catch {
       case e: Exception => return ""
     }
  }
  
   /*
   * 
   */
  def getAccumulatedCost( content:String ) : Double = {
    val accumulatedCost = Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 11 ) , "~", 0)
    return Common.toDouble( accumulatedCost )
  }

  /*
   * 
   */
  def getHit( content:String ) : Integer = {
     return  1
  }
  
  /*
   * 
   */
  def getCallClassCat( content:String ) : String = {
     return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "DWSCallClassCategory", "*", "]" )
  }
  
  /*
   * 
   */
  def getIntcctLkpPad( msisdn:String ) : String = {
     return  msisdn.padTo(14, ".").mkString("")
  }
  
  /*
   * 
   */
  def getIntcctType ( content:String ) : String = {
     if ( this.getServiceUsageType(content) == "VAS") return "VAS"
     else return "NONVAS"
  }
  
  /*
   * 
   */
  def getVasServiceId ( content:String ) : String = {
    if (this.getServiceUsageType(content) == "VAS") return getChargingContextSpecific(content, "16777416")
    else return ""
  }
  
  /*
   * 
   */
  def substring ( content:String, start:Integer, end:Integer ) : String = {
    try {
        return content.substring(start, end)
     }
     catch {
       case e: Exception => return ""
     }
  }
  
  /*
   * 
   */
  def getUsedOfferId ( content:String , int:Integer ) : String = {
    return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ) , "]", int)  
  }
  
  /*
   * 
   */
  def getUsedOfferId ( content:String ) : String = {
    return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ) 
  }
  
  /*
   * Check to offer reference (hardcoded in Common) 
   * @return offer id for SC
   */
  def getUsedOfferSC ( content:String ) : String = {
    val offers = Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ) 
    
    if ( offers.contains("]") == true)  {
      offers.split("\\]").foreach { case (a) => if ( Common.refOfferAreaSC.contains(a) ) return a }
      return ""
    }
    else if (offers == "") return ""
    else return offers
      
  }
  
  /*
   * 
   */
  def getOfferAttributeAreaname ( content:String ) : String = {
    return Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "AREANAME", "*", "]" )  
  }
  
  /*
   * @Return d from /a/b/c/d
   */
  def getFilenameFromPath ( fullPath:String ) : String = {    
    val index = fullPath.lastIndexOf("/");
    return fullPath.substring(index + 1);  
  }
  
  /*
   * CCR_CCA_ACCOUNT_VALUE_BEFORE
   */
  def getAccountValueBefore( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 3 ) , "~" , 0)
  }
  
  
  /*
   * CCR_CCA_ACCOUNT_VALUE_AFTER
   */
  def getAccountValueAfter( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 4 ) , "~" , 0)
  }
  
  /*
   * CCR_TRIGGER_TIME
   */
  def getCCRTriggerTime( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).substring(0, 14)
  } 
  
   /*
   * CCR_EVENT_TIME
   */
  def getCCREventTime( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 2 ).substring(0, 14)
  } 
  
  /*
   * 
   */
  def getCcrCcaSpecConsump( content:String ) : String = {    
    return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 ,"*", 19)
  } 
  
  /*
   * 
   */
  def getSpecConsumpSPMADA( content:String ) : String = {
    try {
      return this.getCcrCcaSpecConsump(content).split("\\\\")(3).split("~")(1)
    }
    catch {
      case e: Exception => return ""
    }
  } 
  
  /*
   * 
   */
  def getSpecConsumpSPCONS( content:String ) : String = {
    try {
      return this.getCcrCcaSpecConsump(content).split("\\\\")(1)
    }
    catch {
      case e: Exception => return ""
    }
  } 
  
  
  /*
   * 
   */
  def getMSISDN( content:String ) : String = {  
    return Common.getKeyVal( Common.getTuple(content, "|", 10) , "0", "#", "[" )
  }
 
  
  /*
   * 
   */
  def getRecordId( content:String ) : String = {  
    val msisdn = Common.getKeyVal( Common.getTuple(content, "|", 10) , "0", "#", "[" )
    val ccrTriggerTime = this.getCCRTriggerTime(content)
    return Common.getRecordID(Common.normalizeMSISDNnon62(msisdn), ccrTriggerTime )
  }
 
  /*
   * 
   */
  def getJobId( content:String, jobId:String) : String = {  
    return jobId
  }  
  
  /*
   * @return date in format of YYYYMMDD
   */
  def getPrcDt( content:String, prcDt:String ) : String = {  
    //return new SimpleDateFormat("yyyyMMdd").format(new Date());
    return prcDt
  }  
  
  /*
   * 
   */
  def getFileDate( content:String ) : String = {  
    return this.getFilenameFromPath( Common.getTuple(content, "|", 16) )
  }
  
  /********************************************************************
   * 
   * AFTER TRANSPOSE
   * 
   *******************************************************************/
  
  /*
   * 
   */
  def genRevenueCode( revCodeBase:String, account:String, serviceUsageType:String ) : String = {  
    if ( serviceUsageType == "VAS" && account == "MA" ) return revCodeBase
    else if ( serviceUsageType == "CALLNOMAP" ) return revCodeBase
    else return revCodeBase.concat(account)
  }
  
  /*
   * Generate lookup account for MADA lookup
   */
  def genAccountForMADA( account:String ) : String = {  
    if ( account == "MA" ) return "REGULER"
    else return account
  }
  
  /*
   * Generate SERVICE_USG_TYPE from revCodeUsgTp by rules
   */
  def genSvcUsgTypeFromRevCode( revCodeUsgTp:String, revCodeLv9:String, revCodeLv5:String ) : String = {   
    if ( revCodeUsgTp == null  ) return ""
    else if ( revCodeUsgTp == "DATA" && revCodeLv9.toLowerCase() == "reguler" && revCodeLv5.toLowerCase().contains("screen usage") ) return "DATA PPU"
    else if ( revCodeUsgTp == "DATA" && revCodeLv5.toLowerCase().contains("blackberry") ) return "DATA Blackberry"
    else if ( revCodeUsgTp == "DATA" && revCodeLv5.toLowerCase().contains("screen") ) return "DATA ADDON"
    else if ( revCodeUsgTp == "DATA" ) return "DATA OTHERS"
    else return revCodeUsgTp
  }
  
  /*
   * Generate SERVICE_USG_DIRECTION from revCode by rules
   */
  def genSvcUsgDirFromRevCode( revCodeUsgTp:String, revCodeLv3:String ) : String = {
    if ( revCodeUsgTp == null  ) return ""
    else if ( revCodeUsgTp == "VOICE" ) return revCodeLv3
    else if ( revCodeUsgTp == "SMS" ) return revCodeLv3
    else return "Others"
  }
  
  /*
   * Generate SERVICE_USG_DIRECTION from revCode by rules
   */
  def genSvcUsgDestFromRevCode( revCodeUsgTp:String, revCodeLv2:String ) : String = {
    if ( revCodeUsgTp == null  ) return ""
    else if ( revCodeUsgTp == "VOICE" && revCodeLv2.startsWith("Local") ) return "Local Distance"
    else  if ( revCodeUsgTp == "VOICE" && revCodeLv2.startsWith("DLD") ) return "Long Distance"
    else return "Others"
  }
  
  /*
   * Reject BP Condition
   */
  def cs5ConvertedRowIsValid( line:String ) : Boolean = {
    
    try {
        val msisdn = CS5Select.getMSISDN(line)
        val time = CS5Select.getCCRTriggerTime(line)
        val sc = CS5Select.getServiceClass(line)
        
        return !msisdn.isEmpty() && !time.isEmpty() && !sc.isEmpty()
    }
    catch {
      case e: Exception => return false
    }
    
    //!Common.getTuple(line, "\\|", 0).isEmpty() && !Common.getTuple(line, "\\|", 2).isEmpty() && !Common.getTuple(line, "\\|", 6).isEmpty()
    //CS5Select.getMSISDN(line).isEmpty() || CS5Select.getCCRTriggerTime(line).isEmpty() || CS5Select.getServiceClass(line).isEmpty()
  }
  
  /**
   * Filter CS5 Garbage from E/// August 2016
   * where CHARGING_CONTEXT_ID like'%@3gpp.org%'
   * and CCR_CCA_USED_OFFERS is null
   * and CCR_RATED_UNITS like'%:0:%:%:%,%:0:%:%:%,%:0:%:%:%';
   */
  def cs5RecordsIsNotGarbage( line:String ) : Boolean = {
    
    try {
        val chargingContext = CS5Select.getChargingContextId(line)
        val usedOffers = CS5Select.getUsedOfferId(line)
        val ratedUnits = CS5Select.getRatedUnits(line)
        
        //create match using regex
        val patt = ".~0~.~.~.*.~0~.~.~.*.~0~.~.~.".r
        val ratedUnitsStatus = patt.findFirstIn(ratedUnits) //if valid then empty
        
        return !(chargingContext.contains("@3gpp.org") && usedOffers.isEmpty() && (ratedUnits.isEmpty() || !ratedUnitsStatus.isEmpty) )
    }
    catch {
      case e: Exception => return false
    }
  }
  


  
  /*
   * 
   */
  def getServiceIdentifier( content:String ) : String = {
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 0 )  
  }
  
  /*
   * 
   */
  def getRatedUnits( content:String ) : String = {
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 14 )  
  }
  
  /*
   * 
   */
  def getCCRTreeDefinedFields( content:String ) : String = {
     return  Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) 
  }
  
  /*
   * 
   */
  def getCCRBonusAdjustment( content:String ) : String = {
     return  Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 11 ) 
  }
  
  /*
   * Get service usage type definition from network
   */
  def getChargingContextId( content:String ) : String = {
    return Common.getTuple( content, "|", 5 )
  }
  
  /********************************************************************
   * 
   * Postpaid functions
   * 
   *******************************************************************/
  
  /*
   * Get costband from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFCostBand( content:String ) : String = {
     return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "COSTBAND", "*", "]" )
  }
  
  /*
   * Get calltype from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFCallType( content:String ) : String = {
     return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "CALLTYPE", "*", "]" )
  }
  
  /*
   * Get location from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFLocation( content:String ) : String = {
     return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "LOCATION", "*", "]" )
  }
  
  /*
   *  Get discount from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFDiscount( content:String ) : String = {
     return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "Discount Labelling", "*", "]" )
  }
  
  /*
   * Get RBM event type id for postpaid
   * @returns: 54 for SMS, 51 for VOICE, 40 for ???, 41 for 3G data, 55 for ??? [FIXME]
   */
  def getPostEventId( content:String ) : String = {
     val chargingContextId=this.getChargingContextId(content)
     val ccrSvcScn=this.getServiceScenario(content)
     val ccrSvcIdent=this.getServiceIdentifier(content)
     val apn=this.getAPN(content)
     
     if ( chargingContextId == "SCAP_V.2.0@ericsson.com" && ( ccrSvcScn == "0" || ccrSvcScn == "1" ) ) return "54"
     else if ( chargingContextId == "CAPv2_V.1.0@ericsson.com" )
       if ( ccrSvcIdent=="0" || ccrSvcIdent=="1" || ccrSvcIdent=="2" ) return "51"
       else return "40"
     else if ( chargingContextId.contains("@3gpp.org") )
       if ( apn == "indosat3g" ) return "41"
       else return "55"
     else return ""
  }
  
  /*
   * 
   */
  def getPostOtherPartyNum( content:String ) : String = {
     return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), "16778219", "*", "]" )
  }
  
  /*
   * Hardcoded value for postpaid EVENTCLASS (RATTYPE NAME)
   */
  def getPostRatTypeName( ratType:String ) : String = {
     return { if (ratType=="") "" else CS5Reference.eventClass(ratType) }
  }
  
  /*
   * Hardcoded value for postpaid CALLTYPE
   */
  def getCallTypeName( callType:String ) : String = {
     return { if (callType=="") "" else CS5Reference.callTypeArr(callType) }
  }
  
  /*
   * 
   */
  def getPostAxis1( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {    
     return  { if ( EventTp=="54" ) otherPartyNum
       else if ( EventTp=="51" ) callType
       else if ( EventTp=="55" || EventTp=="41" ) costband
       else "" }
  }
  
  /*
   * 
   */
   def getPostAxis2( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
     val pAxis2 = { if ( EventTp=="54" ) callType
       else if ( EventTp=="51" || EventTp=="40" ) costband
       else if ( EventTp=="55" || EventTp=="41" ) ratTypeName
       else "" }
    
    return pAxis2
     
  }
  
  /*
   * 
   */
  def getPostAxis3( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
    val pAxis3 = { if ( EventTp=="54" || EventTp=="55" || EventTp=="41" ) costband
       else if ( EventTp=="51" ) location
       else if ( EventTp=="40" ) otherPartyNum
       else "" }
    
    return pAxis3
     
  }
  
   /*
   * 
   */
  def getPostAxis4( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
    val pAxis4 = { if ( EventTp=="54" || EventTp=="40" ) costband
       else if ( EventTp=="51" || EventTp=="41" ) svcClass
       else "" }
    
    return pAxis4
     
  }
  
   /*
   * 
   */
  def getPostAxis5( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
    val pAxis5 = { if ( EventTp=="40" ) location
       else if ( EventTp=="51" ) otherPartyNum
       else if ( EventTp=="55" || EventTp=="54" ) svcClass
       else "" }
    
    return pAxis5
     
  }
  
  /*
   * 
   */
  def getPostVasServiceId ( content:String ) : String = {
    val vasSvcId=getChargingContextSpecific(content, "16777416")
    if ( vasSvcId.length() == 14 ) return vasSvcId
    else return ""
  }
  
  /*
   * CCR_TRIGGER_TIME for postpaid
   */
  def getPostCCRTriggerTime( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 )
  } 
  
  /*
   * CCR_EVENT_TIME for postpaid
   */
  def getPostCCREventTime( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 2 )
  } 
  
  /*
   * Get Reject Reason for Prepaid
   */
  def getRejRsnPrepaid( prmPkgCode:String, callClassBase:String, revCodeUsgTp:String, MADAAcc:String  ) : String = {  
    
    var rejectReason = ""
    
    if ( prmPkgCode.isEmpty() ) rejectReason = rejectReason.concat("Service Class, ")
    if ( callClassBase.isEmpty() ) rejectReason = rejectReason.concat("Call Class, ")
    if ( revCodeUsgTp.isEmpty() ) rejectReason = rejectReason.concat("Revenue Code, ")
    if ( MADAAcc.isEmpty() ) rejectReason = rejectReason.concat("MADA, ")    

    if (rejectReason.isEmpty()) {
      return ""
    } else {
      return rejectReason + "is not found"
    }
  } 
  

}