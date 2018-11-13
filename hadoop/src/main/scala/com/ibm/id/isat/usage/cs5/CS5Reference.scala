package com.ibm.id.isat.usage.cs5

import scala.io.Source
import org.apache.spark.SparkContext;
import com.ibm.id.isat.utils._
import scala.util.control.Breaks._

object CS5Reference {
  
  /*
   * Postpaid RAT TYPE NAME
   */
  val eventClass = Map ("1" -> "3G Event Class", "2" -> "2G Event Class", "6" -> "4G Event Class" )
  
  /*
   * Postpaid CALLTYPE 
   */
  val callTypeArr = Map ("1" -> "Voice MOC", "2" -> "Voice MTC", "3" -> "Voice CFW", "4" -> "SMS MO", "5" -> "SMS MT",
       "6" -> "MMS MO", "7" -> "MMS MT", "8" -> "GPRS")
       

  

  /**
   * Postpaid EVENTTYPE 
   * 1. Get reference from file first (instantiate this class!)
   * 2. Split as array
   * 3. Match with inserted string
   */
  class postEventType( eventTypeFileName:String, sc:SparkContext ) extends java.io.Serializable {

    //val eventTypeList = Source.fromFile( eventTypeFileName ).getLines.toList
    val eventTypeList = sc.textFile(eventTypeFileName).collect().toList
    
    val eventTypeListArr = eventTypeList.map( line => getEventTypeArr(line) )
    
    //These variables are not used for now
    val eventType1List = eventTypeListArr.filter { line => hasNAxis(line, 1) }
    val eventType2List = eventTypeListArr.filter { line => hasNAxis(line, 2) }
    val eventType3List = eventTypeListArr.filter { line => hasNAxis(line, 3) }
    val eventType4List = eventTypeListArr.filter { line => hasNAxis(line, 4) }
    val eventType5List = eventTypeListArr.filter { line => hasNAxis(line, 5) }
    val eventType6List = eventTypeListArr.filter { line => hasNAxis(line, 6) }
    //End of not used variables
    
    def getEventTypeList() : List[String] = {
      return eventTypeList
    }
    
    def test (a:String) : String = {return eventType3List(2)(2)}
    

    /*
     * return condition with n parameter
     */
    def hasNAxis( line:Array[String], numAxis:Integer ) :Boolean = {
        var i=0
        if ( line(2) != "" ) i+=1
        if ( line(3) != "" ) i+=1
        if ( line(4) != "" ) i+=1
        if ( line(5) != "" ) i+=1
        if ( line(6) != "" ) i+=1
        if ( line(7) != "" ) i+=1
        if (i==numAxis) return true
        else return false
    }
    
    /*
     * Split event type reference with delimiter '|'
     * Return as array
     */
    def getEventTypeArr ( line:String ) : Array[String] = {
      return Array(
          Common.getTuple(line, "|", 0),
          Common.getTuple(line, "|", 1),
          Common.getTuple(line, "|", 2),
          Common.getTuple(line, "|", 3),
          Common.getTuple(line, "|", 4),
          Common.getTuple(line, "|", 5),
          Common.getTuple(line, "|", 6),
          Common.getTuple(line, "|", 7)
          )
    }
    
    /*
     * Get the EventType from values
     */
    def getEventTypeRevCode( EventTp:String, otherPartyNum:String, callType:String, costband:String, location:String, ratTypeName:String, svcClass:String ) : String = {
      return getRevCodeFromAxis(
            EventTp,
            CS5Select.getPostAxis1(EventTp, otherPartyNum, callType, costband, location, ratTypeName, svcClass),
            CS5Select.getPostAxis2(EventTp, otherPartyNum, callType, costband, location, ratTypeName, svcClass),
            CS5Select.getPostAxis3(EventTp, otherPartyNum, callType, costband, location, ratTypeName, svcClass),
            CS5Select.getPostAxis4(EventTp, otherPartyNum, callType, costband, location, ratTypeName, svcClass),
            CS5Select.getPostAxis5(EventTp, otherPartyNum, callType, costband, location, ratTypeName, svcClass),
            ""
        )
    }
        
    /*
     * Get the EventType from Axis
     * Loop for each reference 
     * Get reference with most matched column
     */
    def getRevCodeFromAxis( eventTypeId:String, pAxis1:String, pAxis2:String, pAxis3:String, pAxis4:String, pAxis5:String, pAxis6:String ) : String = {
         
      
      var maxScore=0
      var score=0
      var returnVal=""
      eventTypeListArr.foreach { arr =>
        breakable {
          //Loop for each reference
          score=0
          
          //Get match, and ignore if the reference is empty
          if ( eventTypeId==arr(0) ) score+=1 else if ( !arr(0).isEmpty() ) break
          if ( pAxis1==arr(2) ) score+=1 else if ( !arr(2).isEmpty() ) break
          if ( pAxis2==arr(3) ) score+=1 else if ( !arr(3).isEmpty() ) break 
          if ( pAxis3==arr(4) ) score+=1 else if ( !arr(4).isEmpty() ) break 
          if ( pAxis4==arr(5) ) score+=1 else if ( !arr(5).isEmpty() ) break 
          if ( pAxis5==arr(6) ) score+=1 else if ( !arr(6).isEmpty() ) break
          if ( pAxis6==arr(7) ) score+=1 else if ( !arr(7).isEmpty() ) break
          
          if ( score>maxScore && score>1 ) {
            maxScore=score
            returnVal=arr(1) 
          }
        }
        
      }
      
      return returnVal
    }
  }
  
  /**
   * Postpaid Corporate Usage type 
   * 1. Get reference from file first (instantiate this class!)
   * 2. Split as array
   * 3. Match with inserted string
   */
  class postCorpProductID( postCorpProductIDFile:String, sc:SparkContext ) extends java.io.Serializable {
    //Get from file
    val postCorpProductIDList = sc.textFile(postCorpProductIDFile).collect().toList
    
    //Get first column
    val corpProductIDList = postCorpProductIDList.map( a=> Common.getTuple(a, "|", 0) ).toList
       
   /**
   * Usage type for postpaid corporate CDR Inquiry
   * First implemented for Alfamart corp account
   * @return 'C' for corporate CDR, 'P' for personal CDR, empty string for other CDR
   */
  def getUsageTypePostpaid( TreeDefinedFields:String, DedicatedAccountValuesRow:String ) : String = {
    val productId = Common.getKeyVal( TreeDefinedFields , "ProductID", "*", "]" )
    val daCorporateIdentifier = "DA1~"
    val daPersonalIdentifier = "DA35~"    
    
    if (productId.isEmpty()) return ""
    else if ( corpProductIDList.contains(productId) && DedicatedAccountValuesRow.contains(daCorporateIdentifier) ) return "C"
    else if ( corpProductIDList.contains(productId) && DedicatedAccountValuesRow.contains(daPersonalIdentifier) )  return "P"    
    else return ""
  }

  }
  
  
}