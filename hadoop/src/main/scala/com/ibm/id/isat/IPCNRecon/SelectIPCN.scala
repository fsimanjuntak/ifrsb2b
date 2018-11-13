package com.ibm.id.isat.IPCNRecon

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import org.apache.commons.lang3.time.DateUtils

object SelectIPCN {
  
   def addonpurchase( line:String, prcDt:String, jobId:String) : Row = { 
  //def addonpurchase( line:String) : Row = {
    return Row(this.getCCRTriggerTime(line),
        this.getNormalizedCCRTriggerTime(line),
        this.getServedMsisdn(line),
        this.getCCNNode(line),
        this.getOriginRealm(line),
        this.getOriginHost(line),
        this.getSSPTransactionId(line),
        this.getServiceClassId(line),
        this.getAreaNum(line),
        this.getServiceId(line),
        this.getANumCharging(line),
        this.getOfferId1(line),
        this.getOfferId2(line),
        this.getOfferId3(line),
        this.getOfferId4(line),
        this.getOfferId5(line),
        this.getAccountValueDeducted(line),
        this.getDAID1(line),
        this.getDAValueChange1(line),
        this.getDAUOM1(line),
        this.getDAID2(line),
        this.getDAValueChange2(line),
        this.getDAUOM2(line),
        this.getDAID3(line),
        this.getDAValueChange3(line),
        this.getDAUOM3(line),
        this.getDAID4(line),
        this.getDAValueChange4(line),
        this.getDAUOM4(line),
        this.getDAID5(line),
        this.getDAValueChange5(line),
        this.getDAUOM5(line)
        )
  }
  
    /*
    * Served Msisdn
    * */
    
  def getServedMsisdn( content:String ) : String = {
    
    var msisdn = ""     
    val contextId2 = this.getChargingContextSpecific2(content);
    if ( this.getNetworkDirection(content) == "MO" ) {
      // Get SERVED_MSISDN
      msisdn = Common.getKeyVal( Common.getTuple(content, "|", 10) , "0", "#", "[" )
    } else {
      // Get CCR_CHARGING_CONTEXT_SPECIFICS with param 16778219
     // msisdn = Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), "16778219", "*", "]" )
      msisdn = contextId2;
    }    
    return Common.normalizeMSISDN(msisdn)
  }
  
  
   /* *
   * Get network direction string
   * @returns "MO" or "MT"
   */
  def getNetworkDirection( content:String ) : String = {
    val ccrServiceScenario = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 )
    
    if ( ccrServiceScenario == "0" || ccrServiceScenario == "1" ) 
    {
      return "MO"
    }
    else  (ccrServiceScenario == "2")
     return "MT"  
    
  }
  
   def getChargingContextSpecific2( content:String ) : String = {
   return Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), "16778219", "*", "]" )
  } 
  
  
   /*
   * Methods for the getAmount Calculation
   */
   
   def getDedicatedAccounts( content:String ) : String = {
    
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 ) , "^" , 0)
  }
   
     def getAccountValueDeducted( content:String ) : String = {
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 13 ) , "~" , 0)
  }
   
    /*
     * Dedicated Account 1 Family Column
     */
      
   def getVDAInfo1( content:String ) : String = {
    
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 ) , "^" , 0)
     //return  Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 )
  }
   
      def getDAFirstTuple( line:String ) : String = {
       
       val getVDAInfo1 = SelectIPCN.getVDAInfo1(line)
       return Common.getTuple(getVDAInfo1, " ~" , 0)
  }   
    
   
    def getDAID1( line:String ) : String = {
       
       val dafirsttuple = SelectIPCN.getDAFirstTuple(line)
       if (dafirsttuple == "")
            return ""
       else 
          return "DA".concat(dafirsttuple)
  } 
    
       def getDAValueChange1( line:String ) : String = {
       
       val daidvaluechange = SelectIPCN.getDAID1ValueChange(line)
       
       if (daidvaluechange == "")
         return ""
       else
       return Common.getTuple(daidvaluechange, "\\", 0)
  }   
    
       def getDAID1ValueChange( line:String ) : String = {
       val vdainfo1 = SelectIPCN.getVDAInfo1(line)
        return Common.getTuple(vdainfo1,"~", 3)
  }  
       
    def getDAUOM1( line:String ) : String = {
       val vdainfo1 = SelectIPCN.getVDAInfo1(line)
       
       if (vdainfo1 == "")
         return ""
       else
        return Common.getTuple(vdainfo1,"~", 14)
  }  
  
     /*
     * Dedicated Account 2 Family Column
     */
     def getVDAInfo2( content:String ) : String = {
    
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 ) , "^" , 1)
     //return  Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 )
  }
        
      def getDA2FirstTuple( line:String ) : String = {
       
       val getVDAInfo2 = SelectIPCN.getVDAInfo2(line)
       return Common.getTuple(getVDAInfo2, " ~" , 0)
  }    
     
       def getDAID2( line:String ) : String = {
       
       val da2firsttuple = SelectIPCN.getDA2FirstTuple(line)
       if (da2firsttuple == "")
         return ""
       else
       return "DA".concat(da2firsttuple)
  } 
    
       def getDAValueChange2( line:String ) : String = {
       
       val daidvaluechange2 = SelectIPCN.getDAID2ValueChange(line)
       if (daidvaluechange2 == "")
         return ""
       else
       return Common.getTuple(daidvaluechange2, " \\ " , 0)
  }   
    
       def getDAID2ValueChange( line:String ) : String = {
       val vdainfo2 = SelectIPCN.getVDAInfo2(line)     
        return Common.getTuple(vdainfo2,"~", 3)
  }  
       
        def getDAUOM2( line:String ) : String = {
       val vdainfo2 = SelectIPCN.getVDAInfo2(line)
       if (vdainfo2 == "")
         return ""
       else
        return Common.getTuple(vdainfo2,"~", 14)
  } 
        
     /*
     * Dedicated Account 3 Family Column
     */
     
      def getVDAInfo3( content:String ) : String = {
    
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 ) , "^" , 2)
     //return  Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 )
  }
        
      def getDA3FirstTuple( line:String ) : String = {
       
       val getVDAInfo3 = SelectIPCN.getVDAInfo3(line)
       return Common.getTuple(getVDAInfo3, " ~" , 0)
  }    
     
       def getDAID3( line:String ) : String = {
       
       val da3firsttuple = SelectIPCN.getDA3FirstTuple(line)
       if (da3firsttuple == "")
         return ""
       else
       return "DA".concat(da3firsttuple)
  } 
    
       def getDAValueChange3( line:String ) : String = {
       
       val daidvaluechange3 = SelectIPCN.getDAID3ValueChange(line)
       if (daidvaluechange3 == "")
         return ""
       else
       return Common.getTuple(daidvaluechange3, " \\ " , 0)
  }   
    
       def getDAID3ValueChange( line:String ) : String = {
       val vdainfo3 = SelectIPCN.getVDAInfo3(line)     
        return Common.getTuple(vdainfo3,"~", 3)
  }  
       
        def getDAUOM3( line:String ) : String = {
       val vdainfo3 = SelectIPCN.getVDAInfo3(line)
       if (vdainfo3 == "")
         return ""
       else
        return Common.getTuple(vdainfo3,"~", 14)
  } 
        
     /*
     * Dedicated Account 4 Family Column
     */
        
       def getVDAInfo4( content:String ) : String = {
    
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 ) , "^" , 3)
     //return  Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 )
  }
        
      def getDA4FirstTuple( line:String ) : String = {
       
       val getVDAInfo4 = SelectIPCN.getVDAInfo4(line)
       return Common.getTuple(getVDAInfo4, " ~" , 0)
  }    
     
       def getDAID4( line:String ) : String = {
       
       val da4firsttuple = SelectIPCN.getDA4FirstTuple(line)
       if (da4firsttuple == "")
         return ""
       else
       return "DA".concat(da4firsttuple)
  } 
    
       def getDAValueChange4( line:String ) : String = {
       
       val daidvaluechange4 = SelectIPCN.getDAID4ValueChange(line)
       if (daidvaluechange4 == "")
         return ""
       else
       return Common.getTuple(daidvaluechange4, " \\ " , 0)
  }   
    
       def getDAID4ValueChange( line:String ) : String = {
       val vdainfo4 = SelectIPCN.getVDAInfo4(line)     
        return Common.getTuple(vdainfo4,"~", 3)
  }  
       
        def getDAUOM4( line:String ) : String = {
       val vdainfo4 = SelectIPCN.getVDAInfo4(line)
       if (vdainfo4 == "")
         return ""
       else
        return Common.getTuple(vdainfo4,"~", 14)
  } 
        
     /*
     * Dedicated Account 5 Family Column
     */
        
       def getVDAInfo5( content:String ) : String = {
    
     return Common.getTuple( Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 ) , "^" , 4)
     //return  Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 7 )
  }
        
      def getDA5FirstTuple( line:String ) : String = {
       
       val getVDAInfo5 = SelectIPCN.getVDAInfo5(line)
       return Common.getTuple(getVDAInfo5, " ~" , 0)
  }    
     
       def getDAID5( line:String ) : String = {
       
       val da5firsttuple = SelectIPCN.getDA5FirstTuple(line)
       if (da5firsttuple == "")
         return ""
       else
       return "DA".concat(da5firsttuple)
  } 
    
       def getDAValueChange5( line:String ) : String = {
       
       val daidvaluechange5 = SelectIPCN.getDAID5ValueChange(line)
       if (daidvaluechange5 == "")
         return ""
       else
       return Common.getTuple(daidvaluechange5, " \\ " , 0)
  }   
    
       def getDAID5ValueChange( line:String ) : String = {
       val vdainfo5 = SelectIPCN.getVDAInfo5(line)     
        return Common.getTuple(vdainfo5,"~", 3)
  }  
       
        def getDAUOM5( line:String ) : String = {
       val vdainfo5 = SelectIPCN.getVDAInfo5(line)
       if (vdainfo5 == "")
         return ""
       else
        return Common.getTuple(vdainfo5,"~", 14)
  } 
        
    
    /*
      * Source Type
    */
     
   def getSourceType( content:String ) : String = {  
    return "CS5"
  }
   
   /*
   * CCR_TRIGGER_TIME
   */
  def getCCRTriggerTime( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).substring(0, 14)
  } 
  
     /*
   * Normalized CCR_TRIGGER_TIME
  
  def getNormalizedCCRTriggerTime( content:String ) : String = {    
    return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).substring(0, 14)
  } 
   */
  
  
  /*
   * Normalized CCR_TRIGGER_TIME
   */
  def getNormalizedCCRTriggerTime( content:String ) : String = {    

     val  getGMT = Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 3 ).substring(15, 17)
     val  setGMT ="07"
     val differenceInGMT= ((setGMT).toInt - (getGMT).toInt)
     val inputDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
     val parseDate = inputDateFormat.parse(SelectIPCN.getCCRTriggerTime(content))
     val dateAfterNormalization= DateUtils.addHours(parseDate, differenceInGMT)
     val NormalizedCCRTriggerTime = new SimpleDateFormat("yyyyMMddHHmmss").format(dateAfterNormalization)
     return NormalizedCCRTriggerTime
  }
  
   /*
   * Get Node Num
   */
  def getCCNNode( content:String ) : String = {  
    return Common.getTuple(content, "|", 3)
  }
  
  /*
   * SSP_TRANSACTION_ID
   */
  
    def getSSPTransactionId( content:String ) : String = {
    return Common.getTuple( content, "|", 6 )
  }
  
 /*
  * Service Id
  */ 
  def getServiceId( content:String ) : String = {
  return getChargingContextSpecific(content, "16777416")  
  }
   
   def getChargingContextSpecific( content:String, param:String ) : String = {
   return Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), param, "*", "]" )
  } 
   
   
  /*
  * Get Area Number
  */ 
  def getAreaNum( content:String ) : String = {
  return getCCRTreeDefinedFields(content, "AREANAME")  
  }
   
   /*
   * get Tree defined Fields
   */
  def getCCRTreeDefinedFields( content:String, param:String ) : String = {
     return  Common.getKeyVal(Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 10 ), param,"*", "]")
  } 
   
   
    /*
    * Get service class Id
    */
  def getServiceClassId( content:String ) : String = {
     return Common.getTupleInTuple(Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 1)
  }
   
  
  /*
  * Get A Num Charging
  */ 
  def getANumCharging( content:String ) : String = {
  return getChargingContextSpecific3(content, "16778219")  
  }
   
   def getChargingContextSpecific3( content:String, param:String ) : String = {
   return Common.getKeyVal( Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 9 ), param, "*", "]" )
  } 
  
   /*
    * Methods for Filter
    */
   
    def ipcnFilterValidation( line:String ) : Boolean = {
    
    try {
       val chargingcontextid = SelectIPCN.getChargingContextId(line)
       val ccrservicescenerio = SelectIPCN.getCCRServiceScenerio(line)
      // val originrealm = SelectIPCN.getOriginRealm(line)
        
        return   chargingcontextid.toLowerCase().contains("scap") && ccrservicescenerio.equals("2") //&& originrealm.contains("ssp")
    }
   catch {
     case e: Exception => return false
   } 
}
       
   /*
   * Get Charging Context Id 
   */
  def getChargingContextId( content:String ) : String = {
    return Common.getTuple( content, "|", 5 )
  }
    
  /*
   * Get CCR Service Scenerio 
   */
    
    def getCCRServiceScenerio( content:String ) : String = {
     return Common.getTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 4 )
  }
    
   /*
   * Get origin realm 
   */
  def getOriginRealm( content:String ) : String = {
    
      return Common.getTupleInTuple(content, "|", 12, "#", 0)
  }
  
  
   /*
   * Get origin host 
   */
   def getOriginHost( content:String ) : String = {
      return Common.getTupleInTuple(content, "|", 12, "#", 1)
  }
  
   /*
    * * Get Offer Id 1
    */
   
   
  def getOfferId( content:String ) : String = {

     return Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[\\[]", 0), "#", 8, "*", 20).replace("[","]")
   }    
   
  def getOfferId1( content:String ) : String = {
   // return Common.getTuple(Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ), "[\\[]" ,0) 
   
  val offerId = this.getOfferId(content);
  return Common.getTuple(offerId,"]",0);
  
  }
  
     /*
    * * Get Offer Id 2
    */
   def getOfferId2( content:String ) : String = {
    
  // return Common.getTuple(Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ), "]" ,1) 
    
  val offerId = this.getOfferId(content);
  return Common.getTuple(offerId,"]",1);
   
  }
  
 
    
    /*
    * * Get Offer Id 3
    */
  def getOfferId3( content:String ) : String = {
   // return Common.getTuple(Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ), "]", 2) 
    
   val offerId = this.getOfferId(content);
  return Common.getTuple(offerId,"]",2);
  }
  
     /*
    * * Get Offer Id 4
    */
  def getOfferId4( content:String ) : String = {
   // return Common.getTuple(Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ), "]", 3)
    
   val offerId = this.getOfferId(content);
  return Common.getTuple(offerId,"]",3);
  }
  
     /*
    * * Get Offer Id 5
    */
  def getOfferId5( content:String ) : String = {
   // return Common.getTuple(Common.getTupleInTuple( Common.getTupleInTuple(content, "|", 13, "[", 0), "#", 8, "*", 20 ), "]" ,4) 
    
  val offerId = this.getOfferId(content);
  return Common.getTuple(offerId,"]",4);
  }
  
}