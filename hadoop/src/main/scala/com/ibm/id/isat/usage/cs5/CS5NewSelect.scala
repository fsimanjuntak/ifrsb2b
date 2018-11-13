package com.ibm.id.isat.usage.cs5

import java.util.Date
import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import scala.collection.mutable.ListBuffer

 /*****************************************************************************************************
 * 
 * CS5 Transformation 
 * Select columns from tuples
 * Input: CCN CS5 Conversion
 * 
 * For anyone taking over this code, good luck!
 * 
 * @author anico@id.ibm.com
 * @author IBM Indonesia for Indosat Ooredoo June 2016
 * 
 * 
 *****************************************************************************************************/ 
object CS5NewSelect {
  
  /**
   * Split CS5 conversion result based on delimiter
   * @param line Conversion result of CS5
   * @return lineMap Map of array of CS5 tuples
   */
  def getMapOfLine( line:String , paymentType:String) : Map[String, Array[String]] = {
    //Split content by tuple delimiters
    
    //First level of delimiter
    val level1 = line.split("\\|")
    
    //Second level: credit control record
    val level2_ccr = {
      if ( paymentType=="PREPAID" ) mergeCcrDAandValueDeductedFromCCR( level1(13) )
      else  level1(13).split("#")
    }
    
    //Second level: serving element
    val level2_serv_elmt = level1(12).split("#")
    
    //Second level: service session id
    val level2_serv_sess_id = level1(6).split("\\:")
    
    //Third level: ccr used service unit
    val level3_usu = level2_ccr(1).split("\\*")
    
    //Third level: ccr ccaccount data
    val level3_ccr_cca = level2_ccr(8).split("\\*")
    
    //Merge in one map collection
    val lineMap = Map(
        "level1" -> level1, 
        "level2_ccr" -> level2_ccr, 
        "level2_serv_sess_id" -> level2_serv_sess_id,
        "level2_serv_elmt" -> level2_serv_elmt,
        "level3_usu" -> level3_usu,
        "level3_ccr_cca" -> level3_ccr_cca)
    
    
    //println(lineMap("level2_ccr").mkString(">>"))
        
    return lineMap
  }
  
  /**
   * Get List of Row of CS5 values.
   * Split values based on delimiter.
   * Transpose data based on multiple CCR records.
   * Similar with getMapOfLine but has multiple returns.
   * 
   * @param line String of conversion result CS5 for postpaid data
   * @param prcDt Process date in string
   * @param paymentType "PREPAID" or "POSTPAID"
   * @param jobId Job ID in string
   * @return List of row of CS5 postpaid values. 
   *   Using schema: CS5Schema.CS5Schema or CS5Schema.CS5PostSchema
   */
  def genCS5TransformedRowList( line:String , paymentType:String, prcDt:String, jobId:String ) : List[Row] = {
    
    //Create list for return value
    var listTransformedRow = new ListBuffer[Row]()
    
    //First level of delimiter
    val level1 = line.split("\\|")
    
    //Get multiple second level: credit control records    
    val level2_ccr_multiple = level1(13).split("\\[")
    
    //Second level: serving element
    val level2_serv_elmt = level1(12).split("#")
    
    //Second level: service session id
    val level2_serv_sess_id = level1(6).split("\\:")
    
    //Build list per CCR records
    level2_ccr_multiple.foreach { case (a) => 
        
      //Get single second level: credit control record    
       val level2_ccr = a.split("#")
       
      //Third level: ccr used service unit
      val level3_usu = level2_ccr(1).split("\\*")
  
      //Third level: ccr ccaccount data
      val level3_ccr_cca = level2_ccr(8).split("\\*")
      
      //Create map of transformed string (for quick access)
      val transformedString = Array(
           Common.safeGet( level1,5 ) //0 : chargingContextId
          
          )

      
      //Merge in one map collection
      val lineMap = Map(
        "level1" -> level1, 
        "level2_ccr" -> level2_ccr, 
        "level2_serv_sess_id" -> level2_serv_sess_id,
        "level2_serv_elmt" -> level2_serv_elmt,
        "level3_usu" -> level3_usu,
        "level3_ccr_cca" -> level3_ccr_cca        
      )
      
      //Check record validity
      if ( this.cs5RecordsIsNotGarbage(lineMap) ) {
          //Add map to list
        if ( paymentType=="POSTPAID" )
          listTransformedRow.append(this.genCS5PostTransformedRow(lineMap, prcDt, jobId))
          else //PREPAID
            listTransformedRow.append(this.genCS5TransformedRow(lineMap, prcDt, jobId))
      }
    }
    
    //Return List
    return listTransformedRow.toList

  }
    
  /**
   * Generate Transformed String and select columns for CS5 PREPAID
   * @param lineMap Map of String from genCS5TransformedRowList 
   * @return Transformed row
   */
   def genCS5TransformedRow( lineMap:Map[String, Array[String]], prcDt:String, jobId:String) : Row = {  
     
    //Get Map of line    
    //val lineMap = this.getMapOfLine(line, "PREPAID")   
    
     
    return Row(this.getTransactionDate(lineMap)
          ,this.getTransactionHour(lineMap)
          ,this.getMSISDN(lineMap)
          ,this.getCCNNode(lineMap)
          ,this.getAPartyNumber(lineMap)              //LOOKUP INTCCT
          ,this.getBPartyNumber(lineMap)              //LOOKUP INTCCT
          ,this.getServiceClass(lineMap)              //LOOKUP PP_BRAND
          ,this.getCallClassCat(lineMap)              //LOOKUP CALLCLASS
          ,this.getMSCAddress(lineMap)
          ,this.getOriginRealm(lineMap)
          ,this.getOriginHost(lineMap)
          ,this.getMCCMNC(lineMap)
          ,this.getLAC(lineMap)                        //Lookup to LACCI REF
          ,this.getCI(lineMap)                         //Lookup to LACCI REF
          ,this.getECI(lineMap)                         
          ,this.getIMSI(lineMap)
          ,this.getAPN(lineMap)
          ,this.getServiceScenario(lineMap)
          ,this.getRoamingPosition(lineMap)
          ,this.getFAF(lineMap)
          ,this.getFAFNumber(lineMap)  
          ,this.getRatingGroup(lineMap)  
          ,this.getContentType(lineMap)
          ,this.getIMEI(lineMap)  
          ,this.getGGSNAddress(lineMap)  
          ,this.getSGSNAddress(lineMap)
          ,this.getCorrelationId(lineMap)              //CALL_REFERENCE
          ,this.getChargingId(lineMap)
          ,this.getRATType(lineMap)
          ,this.getServiceOfferId(lineMap)
          ,this.getPaymentCategory(lineMap)
          ,this.getDedicatedAccountsRow(lineMap)
          ,this.getDedicatedAccountValuesRow(lineMap)
          ,this.getAccountGroup(lineMap)
          ,this.getFAFName(lineMap)
          ,this.getAccountValueDeducted(lineMap)
          ,this.getAccumulator(lineMap)
          ,this.getCommunityId(lineMap, 0) 
          ,this.getCommunityId(lineMap, 1) 
          ,this.getCommunityId(lineMap, 2) 
          ,this.getAccumulatedCost(lineMap)
          ,this.getHit(lineMap)
          ,this.getUsageVolume(lineMap)
          ,this.getUsageDuration(lineMap)          
          ,""//this.getIntcctLkpPad( this.getAPartyNumber(line) )
          ,""//this.getIntcctLkpPad( this.getBPartyNumber(line) )
          ,this.getSpecConsumpSPMADA(lineMap)
          ,this.getSpecConsumpSPCONS(lineMap)
          ,this.getVasServiceId(lineMap)
          ,this.getTrafficCase(lineMap)  
          ,this.getUsedOfferSC(lineMap)
          ,this.getOfferAttributeAreaname(lineMap)
          ,this.getAccountValueBefore(lineMap)
          ,this.getAccountValueAfter(lineMap)
          ,this.getServiceUsageType(lineMap)
          ,this.getCCRTriggerTime(lineMap)
          ,this.getCCREventTime(lineMap) 
          ,this.getJobId(jobId)
          ,this.getRecordId(lineMap)         
          ,this.getPrcDt(prcDt)
          ,this.getSourceType()
          ,this.getFileName(lineMap)
          ,this.getFileDate(lineMap) )
  }
   
  /**
   * Generate Transformed String and selected columns for CS5 POSTPAID
   * @param lineMap Map of String from genCS5TransformedRowList
   * @return Transformed postpaid row
   */
  def genCS5PostTransformedRow( lineMap:Map[String, Array[String]], prcDt:String, jobId:String ) : Row = {  
    
    return Row(
        this.getMSISDN(lineMap),
        this.getTransactionDate(lineMap),
        this.getServiceIdentifier(lineMap),
        this.getServiceClass(lineMap),
        this.getServiceScenario(lineMap),
        this.getRoamingPosition(lineMap),
        this.getFAF(lineMap),
        this.getDedicatedAccountsRow(lineMap),
        this.getAccountValueDeducted(lineMap),
        this.getCCRTreeDefinedFields(lineMap),
        this.getAccumulatedCost(lineMap),
        this.getCCRBonusAdjustment(lineMap),
        "", //RECORD_NUMBER
        this.getChargingContextId(lineMap),
        this.getFileName(lineMap),
        this.getPostCCRTriggerTime(lineMap),
        this.getLAC(lineMap),
        this.getCI(lineMap),
        this.getAPN(lineMap),
        this.getUsageDuration(lineMap),
        this.getUsageVolume(lineMap),
        this.getUsageDuration(lineMap),
        this.getRatingGroup(lineMap),
        this.getContentType(lineMap),
        this.getRATType(lineMap),
        this.getIMEI(lineMap),
        this.getPostCCREventTime(lineMap),
        this.getIMSI(lineMap),
        this.getGGSNAddress(lineMap),
        this.getSGSNAddress(lineMap),
        this.getMSCAddress(lineMap),
        this.getCallReference(lineMap),
        this.getChargingId(lineMap),
        this.getOriginRealm(lineMap),
        this.getOriginHost(lineMap),
        this.getPostOtherPartyNum(lineMap),
        this.getTDFCostBand(lineMap),
        this.getTDFCallType(lineMap),
        this.getCallTypeName(this.getTDFCallType(lineMap)),
        this.getTDFLocation(lineMap),
        this.getTDFDiscount(lineMap),
        this.getPostRatTypeName(this.getRATType(lineMap)),
        this.getPostEventId(lineMap),
        this.getCCRTriggerTime(lineMap),
        this.getCCREventTime(lineMap), 
//        this.getIntcctLkpPad( this.getMSISDN(line) ) ,
//        this.getIntcctLkpPad( this.getPostOtherPartyNum(line) ) ,
        this.getDedicatedAccountValuesRowPostpaid(lineMap),
        this.getPostVasServiceId(lineMap),
        this.getUsedOfferSC(lineMap),
        this.getOfferAttributeAreaname(lineMap),
        this.getAccountValueBefore(lineMap),
        this.getAccountValueAfter(lineMap),
        this.getCCNNode(lineMap),
        this.getCommunityId(lineMap, 0),
        this.getCommunityId(lineMap, 1),
        this.getCommunityId(lineMap, 2),
        this.getAccountGroup(lineMap),
        this.getServiceOfferId(lineMap),
        this.getSpecConsumpSPMADA(lineMap),
        this.getSpecConsumpSPCONS(lineMap),
        this.getAccumulator(lineMap),
        this.getMCCMNC(lineMap),        
        this.getECI(lineMap),
        this.getServiceUsageTypePostpaid(lineMap),
        this.getJobId(jobId),
        this.getRecordId(lineMap),
        this.getPrcDt(prcDt),
        this.getSourceType(),
        this.getFileDate(lineMap)
        
        
          )
  } 
  
  /**
   * Merge CCR_CCA_VALUE_DEDUCTED and CCR_CCA_DEDICATED_ACCOUNTS from multiple CCRs to the first CCR
   * Not the best, but what we can come up at this time
   * Future improvement: Other column should use their own respective CCR, not merged into the first one
   * 
   * @param level2_ccr CCRs String From Conversion
   * @return Array of CCR values
   */
  def mergeCcrDAandValueDeductedFromCCR( level2_ccr:String ) : Array[String] = {
    
    val ccrDelim = "#"         //delimiter for CCR data
    val ccrRecordsDelim = "["  //delimiter for multiple CCRs
    val ccrCcaDelim = "*"      //delimiter for CCR_CCA data
    
    //Check if there are multiple CCR records
    if (level2_ccr.contains( ccrRecordsDelim )) {
      
      //Split per CCR records       
      val level2_ccrArray = level2_ccr.split( "\\"+ccrRecordsDelim )
      
      //Get first CCR record
      val level2_ccrFirst = level2_ccrArray(0).split( ccrDelim )
      val level3_ccr_ccaFirst = level2_ccrFirst(8).split( "\\"+ccrCcaDelim )
      
      //Initialize vars
      var ccrCcaDedicatedAccountOthers = new StringBuilder
      var ccrccaAccValDeductedOthers = 0.0    
      var i=0      
      
      //Loop per CCR records
      level2_ccrArray.foreach { case (a) => 
        breakable {
          if (i==0) break //break out of the breakable (alias continue) -- SKIP first record
          
          //Get account value from records number 2+
          try {          
            val level3_ccr_cca = Common.getTuple(a, "#", 8)
            val ccrCcaDedicatedAccount = Common.getTuple(level3_ccr_cca, "*", 7) 
            val ccrccaAccValDeducted = Common.toDouble( Common.getTuple( Common.getTuple(level3_ccr_cca, "*", 13) , "~", 0 ) )
            val ccrCcaDedicatedAccountChgVal = Common.toDouble( this.getAmountFromMonetaryUnit( Common.getTuple(ccrCcaDedicatedAccount, "~", 3) ) )

            //Append all remaining CCR_CCA_DEDICATED_ACCOUNTS only if NOT EMPTY and HAS CHANGED VALUE
            if ( ccrCcaDedicatedAccount != "" && ccrCcaDedicatedAccountChgVal != 0) {
              if (ccrCcaDedicatedAccountOthers.isEmpty) ccrCcaDedicatedAccountOthers.append(ccrCcaDedicatedAccount)
              else ccrCcaDedicatedAccountOthers.append("^").append(ccrCcaDedicatedAccount)
            }              
            //Sum all remaining CCR_CCA_VALUE_DEDUCTED
            ccrccaAccValDeductedOthers = ccrccaAccValDeductedOthers + ccrccaAccValDeducted
          }
          catch {
            case e: Exception => return level2_ccrFirst
          }
        }
        i+=1
      }
      //End of loop
      
      //Get total account value deducted
      val ccrccaDedicatedAccountFirst = level3_ccr_ccaFirst(7)
      val ccrccaAccValDeductedTotal = Common.toDouble( Common.getTuple( level3_ccr_ccaFirst(13), "~", 0 ) ) + ccrccaAccValDeductedOthers

      //Add value to first CCR_CCA record
      if (ccrccaDedicatedAccountFirst.isEmpty())  
        level3_ccr_ccaFirst(7) = ccrCcaDedicatedAccountOthers.toString()
      else level3_ccr_ccaFirst(7) = ccrccaDedicatedAccountFirst+"^"+ccrCcaDedicatedAccountOthers.toString()
      level3_ccr_ccaFirst(13) = ccrccaAccValDeductedTotal.toString() + "~6~360"
      
      //Add CCR_CCA to first CCR record
      level2_ccrFirst(8) = level3_ccr_ccaFirst.mkString(ccrCcaDelim)
      
      //Return combined CCR record
      return level2_ccrFirst
      
    }
    //Otherwise, return first CCR records only
    else return level2_ccr.split("#")
     
    
  }
   
  /**
   * Generate Rejected Row for Processing
   * Note: for column #32, replaced with single DA, to prevent transpose in reprocess
   * @param Rejected reference result for CS5, delimited by pipe (|)
   * @return Transformed row
   */
   def genCS5RejectedRow( line:String, prcDt:String, jobId:String ) : Row = {  
     val r = line.split("\\|")
     return Row(
         r(0), r(1), r(2), r(3), r(4),  r(5), r(6),  r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17),  r(18), r(19), r(20),
         r(21), r(22), r(23), r(24), r(25), r(26).stripPrefix("\"").stripSuffix("\""), r(27), r(28), r(29), r(30), r(31), r(93)+"~"+r(90)+"~"+r(91)+"~"+r(92), r(33), r(34), r(35), r(36), r(37), r(38), r(39), Common.toDouble(r(40)),
         Common.toInt(r(41)), Common.toDouble(r(42)), Common.toDouble(r(43)), r(44), r(45), r(46), r(47), r(48),r(49), r(50), r(51), r(52), r(53), r(54), r(55), r(56), jobId, r(58), prcDt, r(60), 
         r(61), r(62)         
         )
   }
   
  /**
   * Generate Rejected Row for Processing
   * @param Rejected reference result for CS5 Postpaid, delimited by pipe (|)
   * @return Transformed row
   */
   def genCS5RejectedRowPostpaid( line:String, prcDt:String, jobId:String ) : Row = {  
     val r = line.split("\\|")
     return Row(
         r(0), r(1), r(2), r(3), r(4),  r(5), r(6),  r(7), r(8), r(9), Common.toDouble(r(10)), r(11), r(12), r(13), r(14), r(15), r(16), r(17),  r(18), Common.toDouble(r(19)), Common.toDouble(r(20)),
         Common.toDouble(r(21)), r(22), r(23), r(24), r(25), r(26), r(27), r(28), r(29), r(30), r(31).stripPrefix("\"").stripSuffix("\""), r(32), r(33), r(34), r(35), r(36), r(37), r(38), r(39), r(40),
         r(41), r(42), r(43), r(44), r(45), r(46), r(47), r(48),r(49), r(50), r(51), r(52), r(53), r(54), r(55), r(56), r(57), r(58), r(59), r(60), 
         r(61), r(62), jobId, r(64), prcDt, r(66), r(67)                
         )
   }
  
  /*
   * 
   */
  def getSourceType(  ) : String = {  
    return "CS5"
  }
  
  /**
   * Get CCN node
   * @return CCN node
   */
  def getCCNNode( lineMap: Map[String, Array[String]] ) : String = {  
    return Common.safeGet( lineMap("level1"),3 )
  }
  
  /**
   * Get A Party Number
   * @return MSISDN of A Party string
   */
  def getAPartyNumber( lineMap: Map[String, Array[String]] ) : String = {
    
    var msisdn = ""            
    if ( this.getNetworkDirection(lineMap) == "MO" ) {
      // Get SERVED_MSISDN
      msisdn = Common.getKeyVal( Common.safeGet( lineMap("level1"),10 ) , "0", "#", "[" )
    } else {
      // Get CCR_CHARGING_CONTEXT_SPECIFICS with param 16778219
      msisdn = Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),9 ), "16778219", "*", "]" )
    }       
    return Common.normalizeMSISDN(msisdn)
  }
  
  /**
   * Get B Party Number
   * @return MSISDN of B Party string
   */
  def getBPartyNumber( lineMap: Map[String, Array[String]] ) : String = {
    
    var msisdn = ""            
    if ( this.getNetworkDirection(lineMap) == "MO" ) {
      // Get CCR_CHARGING_CONTEXT_SPECIFICS with param 16778219
      msisdn = Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),9 ), "16778219", "*", "]" )
    } else {
      // Get SERVED_MSISDN
      msisdn = Common.getKeyVal( Common.safeGet( lineMap("level1"),10 ) , "0", "#", "[" )
    }       
    return Common.normalizeMSISDN(msisdn)
  }
  
  /**
   * Get network direction string
   * @returns "MO" or "MT"
   */
  def getNetworkDirection( lineMap: Map[String, Array[String]] ) : String = {
    val ccrServiceScenario = Common.safeGet( lineMap("level2_ccr"),4 )
    
    if ( ccrServiceScenario == "0" || ccrServiceScenario == "1" ) 
      return "MO"
    else 
      return "MT"    
  }
  
  /**
   * Get service usage type definition from network
   * @return Service usage type based on network
   */
  def getServiceUsageType( lineMap: Map[String, Array[String]] ) : String = {
    val chargingContextId = Common.safeGet( lineMap("level1"),5 )
    val ccrServiceIdentifier = Common.safeGet( lineMap("level2_ccr"),0 )
    val ccrServiceScenario = Common.safeGet( lineMap("level2_ccr"),4 ) 
    val callClassId = this.getCallClassCat(lineMap) 
    
    if ( chargingContextId.startsWith("Ericsson") )  return "DATA"
    else if ( chargingContextId.startsWith("CAP") )  return "VOICE"    
    else if ( chargingContextId.startsWith("SCAP") && ccrServiceScenario=="2" && (callClassId=="1" || callClassId=="-1") )  return "VAS"
    else if ( chargingContextId.startsWith("SCAP") )  return "SMS"
    else return "UNKNOWN"     
  }
  
  /**
   * Get service usage type definition from network for postpaid
   * @return Service usage type based on network for postpaid
   */
  def getServiceUsageTypePostpaid( lineMap: Map[String, Array[String]] ) : String = {
    //val chargingContextId = Common.getTuple( content, "|", 5 )
    //val ccrServiceIdentifier = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 0 )
    //val ccrServiceScenario = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 ) 
    //val callClassId = Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "DWSCallClassCategory", "*", "]" )
    
    val chargingContextId = this.getChargingContextId(lineMap)
    val ccrServiceIdentifier = this.getServiceIdentifier(lineMap)
    val ccrServiceScenario = this.getServiceScenario(lineMap)
    val callClassId = this.getCallClassCat(lineMap)
    
    if ( chargingContextId.startsWith("Ericsson") )  return "DATA"
    else if ( chargingContextId.startsWith("CAP") )  return "VOICE"    
    else if ( chargingContextId.startsWith("SCAP") && ccrServiceScenario=="2" )  return "VAS"
    else if ( chargingContextId.startsWith("SCAP") )  return "SMS"
    else return "UNKNOWN"     
  }
  
  /**
   * Get MSC address (from Voice CDR only)
   */
  def getMSCAddress( lineMap: Map[String, Array[String]] ) : String = {
    if ( getServiceUsageType(lineMap) == "VOICE" ) {
      //return Common.getTuple(content, "|", 12)
      return Common.safeGet( lineMap("level1"),12 )
    }
    else return ""
  }
  
  /**
   * Get origin realm (from SMS CDR only)
   */
  def getOriginRealm( lineMap: Map[String, Array[String]] ) : String = {
    if ( getServiceUsageType(lineMap) == "SMS" ) {
      //return Common.getTupleInTuple(content, "|", 12, "#", 0)
      return Common.safeGet( lineMap("level2_serv_elmt"),0 )
    }
    else return ""
  }
   
  /**
   * Get origin host (from SMS CDR only)
   */
   def getOriginHost( lineMap: Map[String, Array[String]] ) : String = {
    if ( getServiceUsageType(lineMap) == "SMS" ) {
      //return Common.getTupleInTuple(content, "|", 12, "#", 1)
      return Common.safeGet( lineMap("level2_serv_elmt"),1 )
    }
    else return ""
  }
  
   /**
    * Get transaction date from CCR_TRIGGER_TIME
    */
  def getTransactionDate( lineMap: Map[String, Array[String]] ) : String = {
    return Common.safeGet( lineMap("level2_ccr"),3 ).take(8)
  }
  
  /**
    * Get transaction hour from CCR_TRIGGER_TIME
    */
  def getTransactionHour( lineMap: Map[String, Array[String]] ) : String = {
     return Common.safeGet( lineMap("level2_ccr"),3 ).substring(8, 10)
  }
  
  /**
    * Get service class
    */
  def getServiceClass( lineMap: Map[String, Array[String]] ) : String = {
     return Common.safeGet( lineMap("level3_ccr_cca"),1 )
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
  def getChargingContextSpecific( lineMap: Map[String, Array[String]], param:String ) : String = {
     //return Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), param, "*", "]" )
    return Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),9 ), param, "*", "]" ) 
  }
  
  /**
   * Get MCC MNC from multiple service type:
   * For SMS: sample is "15F01074058647" 
   */
  def getMCCMNC( content: Map[String, Array[String]] ) : String = {
     if ( this.getServiceUsageType(content) == "DATA" )  return this.getChargingContextSpecific(content, "16778232")
     else if ( this.getServiceUsageType(content) == "VOICE" ) return this.getChargingContextSpecific(content, "16778249")
     else if ( this.getServiceUsageType(content) == "SMS" )
       try return Common.swapChar( substring(getChargingContextSpecific(content, "16777616"),0,6).replace("F", ""), "2,1,3,5,4" )
       catch {
         case e: Exception => return ""
       }
     else return ""     
  }
  
  /**
   * Get RAT type from charging context specific
   * @return RAT Type 
   */
  def getRATType( lineMap: Map[String, Array[String]] ) : String = {
     return this.getChargingContextSpecific(lineMap, "16778240")
  }
  
  /**
   * Get LAC from charging context specific
   * @return 
   */
  def getLAC( content: Map[String, Array[String]] ) : String = {
     if ( this.getServiceUsageType(content) == "DATA" && this.getRATType(content) == "6" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778333") )
     else if ( this.getServiceUsageType(content) == "DATA" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778250") )
     else if ( this.getServiceUsageType(content) == "VOICE" ) return Common.hexToInt( this.getChargingContextSpecific(content, "16778250") ) 
     else if ( this.getServiceUsageType(content) == "SMS" )   return Common.hexToInt( substring(getChargingContextSpecific(content, "16777616"), 7,10) )
     else return ""     
  }
  
  /**
   * Get CI from charging context specific
   * @return
   */
  def getCI( content: Map[String, Array[String]] ) : String = {
     if ( this.getServiceUsageType(content) == "DATA" && this.getRATType(content) == "6" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778332") )
     else if ( this.getServiceUsageType(content) == "DATA" && this.getRATType(content) == "1" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778252") )
     else if ( this.getServiceUsageType(content) == "DATA" )  return Common.hexToInt( this.getChargingContextSpecific(content, "16778251") )
     else if ( this.getServiceUsageType(content) == "VOICE" ) return Common.hexToInt( this.getChargingContextSpecific(content, "16778251") ) 
     else if ( this.getServiceUsageType(content) == "SMS" )   return Common.hexToInt( substring(getChargingContextSpecific(content, "16777616"), 11,14) )
     else return ""     
  }
  
  /**
   * Get ECI from charging context specific
   * @return
   */
  def getECI( content: Map[String, Array[String]] ) : String = {
     if ( this.getServiceUsageType(content) == "SMS" || this.getServiceUsageType(content) == "VAS")   
       return Common.hexToInt( substring(getChargingContextSpecific(content, "16777616"), 6,14) ) 
     else return ""     
  }
  
  /*
   * 
   */
  def getIMSI( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getKeyVal( Common.getTuple(content, "|", 10) , "1", "#", "[" )
    return Common.getKeyVal( Common.safeGet( lineMap("level1"),10 ) , "1", "#", "[" )
  }
  
  /*
   * 
   */
  def getAPN( lineMap: Map[String, Array[String]] ) : String = {
     return getChargingContextSpecific(lineMap, "16778235")
  }
  
  /*
   * 
   */
  def getServiceScenario( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 )
     return Common.safeGet( lineMap("level2_ccr"),4 )
  }
  
  /*
   * 
   */
  def getRoamingPosition( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 6 )
    return Common.safeGet( lineMap("level2_ccr"),6 )
  }
  
  /*
   * 
   */
  def getFAF( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 , "*", 8)
    return Common.safeGet( lineMap("level3_ccr_cca"),8 )
  }
  
  /*
   * 
   */
  def getFAFNumber( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 , "*", 9)
     return Common.safeGet( lineMap("level3_ccr_cca"),9 )
  }
  
  /*
   * Need further checking to other service type
   */
  def getRatingGroup( lineMap: Map[String, Array[String]] ) : String = {
    //return Common.getTupleInTuple(content, "|", 6, ":", 0)
    return Common.safeGet( lineMap("level2_serv_sess_id"),0 )
  }
  
  /*
   * Need further checking to other service type
   */
  def getContentType( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTupleInTuple(content, "|", 6, ":", 1) 
     return Common.safeGet( lineMap("level2_serv_sess_id"),1 )
  }
  
  /*
   * 
   */
  def getIMEI( lineMap: Map[String, Array[String]] ) : String = {
     return getChargingContextSpecific(lineMap, "16778243")
  }
    
  /*
   * 
   */
  def getGGSNAddress( lineMap: Map[String, Array[String]] ) : String = {
     return getChargingContextSpecific(lineMap, "16778230")
  }
    
  /*
   * 
   */
  def getSGSNAddress( lineMap: Map[String, Array[String]] ) : String = {
     return getChargingContextSpecific(lineMap, "16778229")
  }
  
  /*
   * 
   */
  def getCallReference( lineMap: Map[String, Array[String]] ) : String = {
     return getChargingContextSpecific(lineMap, "16778229")
  }
  
  /*
   * 
   */
  def getCorrelationId( lineMap: Map[String, Array[String]] ) : String = {
     // return Common.getTuple(content, "|", 11)
     return Common.safeGet( lineMap("level1"),11 )
  }
  
  /*
   * 
   */
  def getChargingId( lineMap: Map[String, Array[String]] ) : String = {
     return getChargingContextSpecific(lineMap, "16778226")
  }
  
  /*
   * 
   */
  def getServiceOfferId( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 , "*", 10)
     return Common.safeGet( lineMap("level3_ccr_cca"),10 )
  }
  
  /*
   * 
   */
  def getPaymentCategory( lineMap: Map[String, Array[String]]  ) : String = {
     return Common.getUsageCategory( this.getServiceClass(lineMap) )
  }
  
  /*
   * 
   */
  def getPaymentCategory( line: String  ) : String = {
     return Common.getUsageCategory( this.getServiceClass(line) )
  }
  
  /**
 	* Get dedicated account values by row
 	* @returns DA from conversion result 
 	*/
  def getDedicatedAccountsRow( lineMap: Map[String, Array[String]] ) : String = {
    //return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 )
    return Common.safeGet( lineMap("level3_ccr_cca"),7 )
  }
  
  /**
 	* Get dedicated account values by row 
 	* @returns DA_ID,DA_AMOUNT,DA_UNIT_AMOUNT,DA_UNIT_TYPE(UOM) separated by ~. Multiple DA by ^
 	*/
  def getDedicatedAccountValuesRow( lineMap: Map[String, Array[String]] ) : String = {
    val daRow = this.getDedicatedAccountsRow(lineMap)
    
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
        var daChange = getAmountFromMonetaryUnit( Common.getTuple(a, "~", 3) )
        if ( daChange.isEmpty() ) daChange = "0.0"
        
        daReturn = daReturn.concat("DA").concat( Common.getTuple(a, "~", 0) )
          .concat( "~" ).concat( daChange )
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(a, "~", 13) ) ) //DA UNIT CHANGE
          .concat( "~" ).concat( getAmountFromMonetaryUnit( Common.getTuple(a, "~", 14) ) ) //DA UNIT UOM
          
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
  
  /**
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
  
    /**
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
  def getDedicatedAccountValuesRowPostpaid( lineMap: Map[String, Array[String]] ) : String = {
    val daRow = this.getDedicatedAccountsRow(lineMap)

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
  
   /**
   * Get account group
   */
  def getAccountGroup( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 2 )
     return Common.safeGet( lineMap("level3_ccr_cca"),2 )
  }
  
   /**
   * Get traffic case
   */
  def getTrafficCase( content: Map[String, Array[String]]) : String = {
     if ( this.getServiceScenario(content) == "0" && this.getRoamingPosition(content) == "0" ) return "H"
     else if ( this.getServiceScenario(content) == "0" && this.getRoamingPosition(content) == "1" ) return "RCAP"
     else if ( this.getServiceScenario(content) == "1" && this.getRoamingPosition(content) == "0" ) return "HCF"
     else if ( this.getServiceScenario(content) == "0" && this.getRoamingPosition(content) == "0" ) return "RCAPCF"
     else return ""
  }
  
   /**
   * Get FAF name
   */
  def getFAFName( lineMap: Map[String, Array[String]] ) : String = {
     if ( this.getFAF(lineMap).nonEmpty ) return "FF"
     else return ""
  }
  
   /**
   * Get usage volume
   */
  def getUsageVolume( lineMap: Map[String, Array[String]] ) : Double = {
    //val volume = Common.toDouble( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 1, "*", 3 ) )
    val volume = Common.toDouble( Common.safeGet( lineMap("level3_usu"),3 ) )
    return volume
  }
  
   /**
   * Get usage duration
   */
  def getUsageDuration( lineMap: Map[String, Array[String]] ) : Double = {
     //val duration = Common.toDouble( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 1, "*", 1 ) )
     val duration = Common.toDouble( Common.safeGet( lineMap("level3_usu"),1 ) )
     return duration
  }
  
  /**
   * Get filename
   */
  def getFileName( lineMap: Map[String, Array[String]] ) : String = {
     //return this.getFilenameFromPath( Common.getTuple(content, "|", 15) )
     return this.getFilenameFromPath( Common.safeGet( lineMap("level1"),15 ) )
  }
  
  /**
   * Get account value deducted
   */
  def getAccountValueDeducted( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 13 ) , "~" , 0)
     return Common.getTuple( Common.safeGet( lineMap("level3_ccr_cca"),13 ) , "~" , 0)
  }
  
  /**
   * Get accumulator
   */
  def getAccumulator( lineMap: Map[String, Array[String]] ) : String = {
     //return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 6 )
     return Common.safeGet( lineMap("level3_ccr_cca"),6 )
  }

  
  /**
   * Get community from 
   * @param item Community number
   */
  def getCommunityId( lineMap: Map[String, Array[String]], item:Integer ) : String = {
     //val servedCommunityIDs = Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 5 ) , "~", 1)
    val servedCommunityIDs = Common.getTuple( Common.safeGet( lineMap("level3_ccr_cca"),5 ) , "~", 1)    
     try {
       //require four backslashes for regex backslash escape (\)
       return servedCommunityIDs.split("\\\\")(item)  
     }
     catch {
       case e: Exception => return ""
     }
  }
  
   /**
   * Get accumulated cost
   */
  def getAccumulatedCost( lineMap: Map[String, Array[String]] ) : Double = {
    //val accumulatedCost = Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 11 ) , "~", 0)
    val accumulatedCost = Common.getTuple( Common.safeGet( lineMap("level3_ccr_cca"),11 ) , "~", 0)
    return Common.toDouble( accumulatedCost )
  }

  /**
   * Get hits
   */
  def getHit( lineMap: Map[String, Array[String]] ) : Integer = {
     return  1
  }
  
  /**
   * Get call class category
   */
  def getCallClassCat( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "DWSCallClassCategory", "*", "]" )
     return  Common.getKeyVal( lineMap("level2_ccr")(10 ) , "DWSCallClassCategory", "*", "]" )
  }
  
  /**
   * Get tariff category
   */
  def getTariffCat( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "DWSCallClassCategory", "*", "]" )
     return  Common.getKeyVal( lineMap("level2_ccr")(10 ) , "DWSTariffCategory", "*", "]" )
  }
  
  /**
   * Get interconnect lookup padding ---- DEPRECATED
   */
  def getIntcctLkpPad( msisdn:String ) : String = {
     return  msisdn.padTo(14, ".").mkString("")
  }
  
  /**
   * Get interconnect type ---- DEPRECATED
   */
  def getIntcctType ( lineMap: Map[String, Array[String]] ) : String = {
     if ( this.getServiceUsageType(lineMap) == "VAS") return "VAS"
     else return "NONVAS"
  }
  
  /**
   * Get VAS Service Id (16 digits product ID)
   */
  def getVasServiceId ( lineMap: Map[String, Array[String]] ) : String = {
    if (this.getServiceUsageType(lineMap) == "VAS") return getChargingContextSpecific(lineMap, "16777416")
    else return ""
  }
  
  /**
   * Safely get substring
   */
  def substring ( content:String, start:Integer, end:Integer ) : String = {
    try {
        return content.substring(start, end)
     }
     catch {
       case e: Exception => return ""
     }
  }
  
  /**
   * Get CCR used offer id with parameter number
   */
  def getUsedOfferId ( content:String , int:Integer ) : String = {
    return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ) , "]", int)  
  }
  
  /**
   * Get CCR used offer id
   */
  def getUsedOfferId ( content:String ) : String = {
    return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ) 
  }
  
  /**
   * Get CCR used offer id
   */
  def getUsedOfferId ( lineMap: Map[String, Array[String]] ) : String = {
    return Common.safeGet( lineMap("level3_ccr_cca"),20 )
  }
  
  /**
   * Check to offer reference (hardcoded in Common) 
   * @return offer id for SC
   */
  def getUsedOfferSC ( lineMap: Map[String, Array[String]] ) : String = {
    //val offers = Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ) 
    val offers = Common.safeGet( lineMap("level3_ccr_cca"),20 )
    
    if ( offers.contains("]") == true)  {
      offers.split("\\]").foreach { case (a) => if ( Common.refOfferAreaSC.contains(a) ) return a }
      return ""
    }
    else if (offers == "") return ""
    else return offers
      
  }
  
  /**
   * Get offer attribute value AREANAME from Charging context specific
   */
  def getOfferAttributeAreaname ( lineMap: Map[String, Array[String]] ) : String = {
    //return Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "AREANAME", "*", "]" )  
    return Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),10 ) , "AREANAME", "*", "]" )
  }
  
  /**
   * @return d from /a/b/c/d
   */
  def getFilenameFromPath ( fullPath:String ) : String = {    
    val index = fullPath.lastIndexOf("/");
    return fullPath.substring(index + 1);  
  }
  
  /**
   * CCR_CCA_ACCOUNT_VALUE_BEFORE
   */
  def getAccountValueBefore( lineMap: Map[String, Array[String]] ) : String = {    
    //return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 3 ) , "~" , 0)
    return Common.getTuple( Common.safeGet( lineMap("level3_ccr_cca"),3 ) , "~" , 0)
  }
  
  
  /**
   * CCR_CCA_ACCOUNT_VALUE_AFTER
   */
  def getAccountValueAfter( lineMap: Map[String, Array[String]] ) : String = {    
    //return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 4 ) , "~" , 0)
    return Common.getTuple( Common.safeGet( lineMap("level3_ccr_cca"),4 ) , "~" , 0)
  }
  
  /**
   * CCR_TRIGGER_TIME
   */
  def getCCRTriggerTime( lineMap: Map[String, Array[String]] ) : String = {    
    //return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).substring(0, 14)
    return Common.safeGet( lineMap("level2_ccr"), 3 ).substring(0, 14)
  } 
  
  /**
   * CCR_TRIGGER_TIME
   */
  def getCCRTriggerTime( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).substring(0, 14)
  } 
  
   /**
   * CCR_EVENT_TIME
   */
  def getCCREventTime( lineMap: Map[String, Array[String]] ) : String = {    
    //return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 2 ).substring(0, 14)
    return Common.safeGet( lineMap("level2_ccr"), 2 ).substring(0, 14)
  } 
  
  /**
   * CCR_CCA_SPECIFIED_CONSUMPTION
   */
  def getCcrCcaSpecConsump( lineMap: Map[String, Array[String]] ) : String = {    
    //return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8 ,"*", 19)
    return Common.safeGet( lineMap("level3_ccr_cca"),19 )
  } 
  
  /**
   * CCR_CCA_SPECIFIED_CONSUMPTION SP MADA
   */
  def getSpecConsumpSPMADA( lineMap: Map[String, Array[String]] ) : String = {
    try {
      return this.getCcrCcaSpecConsump(lineMap).split("\\\\")(3).split("~")(1)
    }
    catch {
      case e: Exception => return ""
    }
  } 
  
  /**
   * CCR_CCA_SPECIFIED_CONSUMPTION SP CONS
   */
  def getSpecConsumpSPCONS( lineMap: Map[String, Array[String]] ) : String = {
    try {
      return this.getCcrCcaSpecConsump(lineMap).split("\\\\")(1)
    }
    catch {
      case e: Exception => return ""
    }
  } 
  
  
  /**
   * Get MSISDN  
   */
  def getMSISDN( lineMap: Map[String, Array[String]] ) : String = {  
    return Common.getKeyVal( lineMap("level1")(10) , "0", "#", "[" )
  }
  
  /**
   * Get MSISDN  
   */
  def getMSISDN( content:String ) : String = {  
    return Common.getKeyVal( Common.getTuple(content, "|", 10) , "0", "#", "[" )
  }
 
  
  /**
   * Get record ID
   * @return msisdn+ccrTriggerTime+randomnumber(99)
   */
  def getRecordId( lineMap: Map[String, Array[String]] ) : String = {  
    val msisdn = this.getMSISDN(lineMap)
    val ccrTriggerTime = this.getCCRTriggerTime(lineMap)
    val r = scala.util.Random
    
    return Common.normalizeMSISDNnon62(msisdn)+ccrTriggerTime+r.nextInt(999)
    //return Common.getRecordID(Common.normalizeMSISDNnon62(msisdn), ccrTriggerTime )
  }
 
  /**
   * Get Job ID
   */
  def getJobId( jobId:String) : String = {  
    return jobId
  }  
  
  /**
   * @return date in format of YYYYMMDD
   */
  def getPrcDt( prcDt:String ) : String = {  
    //return new SimpleDateFormat("yyyyMMdd").format(new Date());
    return prcDt
  }  
  
  /**
   * Get filedate from conversion
   */
  def getFileDate( lineMap: Map[String, Array[String]] ) : String = {  
    //return this.getFilenameFromPath( Common.getTuple(content, "|", 16) )
    return Common.safeGet( lineMap("level1"),16 )
  }
  
  /********************************************************************
   * 
   * AFTER TRANSPOSE -- PREPAID
   * 
   *******************************************************************/
  
  /**
   * Generate Revenue Code
   */
  def genRevenueCode( revCodeBase:String, account:String, serviceUsageType:String ) : String = {  
    if ( serviceUsageType == "VAS" && account == "MA" ) return revCodeBase
    else if ( revCodeBase == "CALLNOMAP" ) return revCodeBase
    else return revCodeBase.concat(account)
  }
  
  /**
   * Generate lookup account for MADA lookup
   */
  def genAccountForMADA( account:String ) : String = {  
    if ( account == "MA" ) return "REGULER"
    else return account
  }
  
  /**
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
  
  /**
   * Generate SERVICE_USG_DIRECTION from revCode by rules
   */
  def genSvcUsgDirFromRevCode( revCodeUsgTp:String, revCodeLv3:String ) : String = {
    if ( revCodeUsgTp == null  ) return ""
    else if ( revCodeUsgTp == "VOICE" ) return revCodeLv3
    else if ( revCodeUsgTp == "SMS" ) return revCodeLv3
    else return "Others"
  }
  
  /**
   * Generate SERVICE_USG_DIRECTION from revCode by rules
   */
  def genSvcUsgDestFromRevCode( revCodeUsgTp:String, revCodeLv2:String ) : String = {
    if ( revCodeUsgTp == null  ) return ""
    else if ( revCodeUsgTp == "VOICE" && revCodeLv2.startsWith("Local") ) return "Local Distance"
    else  if ( revCodeUsgTp == "VOICE" && revCodeLv2.startsWith("DLD") ) return "Long Distance"
    else return "Others"
  }
  
  /**
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
   * @return true if record is valid
   * 	false if record is not valid
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
  def cs5RecordsIsNotGarbage( lineMap: Map[String, Array[String]] ) : Boolean = {
    
    try {
        val chargingContext = CS5Select.getChargingContextId(lineMap)
        val usedOffers = CS5Select.getUsedOfferId(lineMap)
        val ratedUnits = CS5Select.getRatedUnits(lineMap)
        
        //create match using regex
        val patt = ".~0~.~.~.*.~0~.~.~.*.~0~.~.~.".r
        val ratedUnitsStatus = patt.findFirstIn(ratedUnits) //if valid then empty

        return !(chargingContext.contains("@3gpp.org") && usedOffers.isEmpty() && (ratedUnits.isEmpty() || !ratedUnitsStatus.isEmpty) )
    }
    catch {
      case e: Exception => return true
    }
  }
  


  /**
   * Get service identifier
   */
  def getServiceIdentifier( lineMap: Map[String, Array[String]] ) : String = {
    //return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 0 )  
    return Common.safeGet( lineMap("level2_ccr"),0 )
  }
  
  /**
   * Get rated units
   */
  def getRatedUnits( content:String ) : String = {
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 14 )  
  }
  
  /**
   * 
   */
  def getRatedUnits( lineMap: Map[String, Array[String]] ) : String = {
    return Common.safeGet( lineMap("level2_ccr"),14 ) 
  }
  
  /**
   * Get CCR tree defined fields
   */
  def getCCRTreeDefinedFields( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 )
    return  return Common.safeGet( lineMap("level2_ccr"),10 )
  }
  
  /**
   * Get CCR bonus adjustment
   */
  def getCCRBonusAdjustment( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 11 )
     return Common.safeGet( lineMap("level2_ccr"),11 )
  }
  
  /**
   * Get charging context ID
   */
  def getChargingContextId( lineMap: Map[String, Array[String]] ) : String = {
    return Common.safeGet( lineMap("level1"),5 )
  }
  
   /**
   * Get charging context ID
   */
  def getChargingContextId( content:String ) : String = {
    return Common.getTuple( content, "|", 5 )
  }
  
  /********************************************************************
   * 
   * Postpaid functions
   * 
   *******************************************************************/
  
  /**
   * Get costband from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFCostBand( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "COSTBAND", "*", "]" )
    return Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),10 ) , "COSTBAND", "*", "]" )
  }
  
  /**
   * Get calltype from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFCallType( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "CALLTYPE", "*", "]" )
    return  Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),10 ) , "CALLTYPE", "*", "]" )
  }
  
  /**
   * Get location from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFLocation( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "LOCATION", "*", "]" )
     return  Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),10 ) , "LOCATION", "*", "]" )
  }
  
  /**
   *  Get discount from CCR_TREE_DEFINED_FIELDS for postpaid
   */
  def getTDFDiscount( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ) , "Discount Labelling", "*", "]" )
     return  Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),10 ) , "Discount Labelling", "*", "]" )
  }
  
  /**
   * Get RBM event type id for postpaid
   * @returns: 54 for SMS, 51 for VOICE, 40 for ???, 41 for 3G data, 55 for ??? [FIXME]
   */
  def getPostEventId( lineMap: Map[String, Array[String]] ) : String = {
     val chargingContextId=this.getChargingContextId(lineMap)
     val ccrSvcScn=this.getServiceScenario(lineMap)
     val ccrSvcIdent=this.getServiceIdentifier(lineMap)
     val apn=this.getAPN(lineMap)
     
     if ( chargingContextId == "SCAP_V.2.0@ericsson.com" && ( ccrSvcScn == "0" || ccrSvcScn == "1" ) ) return "54"
     else if ( chargingContextId == "CAPv2_V.1.0@ericsson.com" )
       if ( ccrSvcIdent=="0" || ccrSvcIdent=="1" || ccrSvcIdent=="2" ) return "51"
       else return "40"
     else if ( chargingContextId.contains("@3gpp.org") )
       if ( apn == "indosat3g" ) return "41"
       else return "55"
     else return ""
  }
  
  /**
   * Get other party number for postpaid
   */
  def getPostOtherPartyNum( lineMap: Map[String, Array[String]] ) : String = {
     //return  Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), "16778219", "*", "]" )
    return  Common.getKeyVal( Common.safeGet( lineMap("level2_ccr"),9 ), "16778219", "*", "]" )
  }
  
  /**
   * Hardcoded value for postpaid EVENTCLASS (RATTYPE NAME)
   */
  def getPostRatTypeName( ratType:String ) : String = {
     return { if (ratType=="") "" else CS5Reference.eventClass(ratType) }
  }
  
  /**
   * Hardcoded value for postpaid CALLTYPE
   */
  def getCallTypeName( callType:String ) : String = {
     return { if (callType=="") "" else CS5Reference.callTypeArr(callType) }
  }
  
  /**
   * Get postpaid axis 1 based on rules
   */
  def getPostAxis1( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {    
     return  { if ( EventTp=="54" ) otherPartyNum
       else if ( EventTp=="51" ) callType
       else if ( EventTp=="55" || EventTp=="41" ) costband
       else "" }
  }
  
  /**
   * Get postpaid axis 2 based on rules
   */
   def getPostAxis2( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
     val pAxis2 = { if ( EventTp=="54" ) callType
       else if ( EventTp=="51" || EventTp=="40" ) costband
       else if ( EventTp=="55" || EventTp=="41" ) ratTypeName
       else "" }
    
    return pAxis2
     
  }
  
  /**
   * Get postpaid axis 3 based on rules
   */
  def getPostAxis3( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
    val pAxis3 = { if ( EventTp=="54" || EventTp=="55" || EventTp=="41" ) costband
       else if ( EventTp=="51" ) location
       else if ( EventTp=="40" ) otherPartyNum
       else "" }
    
    return pAxis3
     
  }
  
   /**
   * Get postpaid axis 4 based on rules
   */
  def getPostAxis4( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
    val pAxis4 = { if ( EventTp=="54" || EventTp=="40" ) costband
       else if ( EventTp=="51" || EventTp=="41" ) svcClass
       else "" }
    
    return pAxis4
     
  }
  
   /**
   * Get postpaid axis 5 based on rules
   */
  def getPostAxis5( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {  
     
    val pAxis5 = { if ( EventTp=="40" ) location
       else if ( EventTp=="51" ) otherPartyNum
       else if ( EventTp=="55" || EventTp=="54" ) svcClass
       else "" }
    
    return pAxis5
     
  }
  
  /**
   * Get postpaid axis 6 based on rules
   */
  def getPostVasServiceId ( lineMap: Map[String, Array[String]] ) : String = {
    val vasSvcId=getChargingContextSpecific(lineMap, "16777416")
    if ( vasSvcId.length() == 14 ) return vasSvcId
    else return ""
  }
  
  /**
   * CCR_TRIGGER_TIME for postpaid
   */
  def getPostCCRTriggerTime( lineMap: Map[String, Array[String]] ) : String = {    
    //return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 )
    return Common.safeGet( lineMap("level2_ccr"),3 )
  } 
  
  /**
   * CCR_EVENT_TIME for postpaid
   */
  def getPostCCREventTime( lineMap: Map[String, Array[String]] ) : String = {    
    //return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 2 )
    return Common.safeGet( lineMap("level2_ccr"),2 )
  } 
  
  /**
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