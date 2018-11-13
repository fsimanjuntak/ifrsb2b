package com.ibm.id.isat.usage.cs5

import org.apache.spark.sql._
import com.ibm.id.isat.utils._

object CS5FilteredOutput {
  
  /**
   * Addon output for IPCN reconciliation
   * 
   */
   def genAddonOutput( sqlContext:SQLContext ) : DataFrame = {
     
     sqlContext.udf.register("getTuple", Common.getTuple _)
     sqlContext.udf.register("getTupleInTuple", Common.getTupleInTuple _)
     sqlContext.udf.register("concatDA", this.concatDA _)
     sqlContext.udf.register("getDAChange", this.getDAChange _)
     
     
     return sqlContext.sql("""
        select
        CCRTriggerTime TRIGGER_TIME,
        NormCCRTriggerTime TRIGGER_TIME_NORM,        
        MSISDN SERVED_MSISDN,
        CCNNode NODE_NM,
        OriginRealm ORIGIN_REALM,
        OriginHost ORIGIN_HOST,
        SSPTransId SSP_TRANSACTION_ID,
        ServiceClass SERVICE_CLASS_ID,
        OfferAttributeAreaname AREA_NM,
        VasServiceId SERVICE_ID,
        APartyNumber A_NUM_CHARGING,
        getTuple(UsedOfferSC, ',', 0) OFFER_ID_1,
        getTuple(UsedOfferSC, ',', 1) OFFER_ID_2,
        getTuple(UsedOfferSC, ',', 2) OFFER_ID_3,
        getTuple(UsedOfferSC, ',', 3) OFFER_ID_4,
        getTuple(UsedOfferSC, ',', 4) OFFER_ID_5,
        AccountValueDeducted ACCOUNT_VALUE_DEDUCTED,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',0,'~',0 ) ) DA_ID_1,
        getDAChange(DedicatedAccountValuesRow,0) DA_VALUE_CHANGE_1,
        getTupleInTuple(DedicatedAccountValuesRow,'^',0,'~',3 ) DA_UOM_1,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',1,'~',0 ) ) DA_ID_2,
        getDAChange(DedicatedAccountValuesRow,1) DA_VALUE_CHANGE_2,
        getTupleInTuple(DedicatedAccountValuesRow,'^',1,'~',3 ) DA_UOM_2,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',2,'~',0 ) ) DA_ID_3,
        getDAChange(DedicatedAccountValuesRow,2) DA_VALUE_CHANGE_3,
        getTupleInTuple(DedicatedAccountValuesRow,'^',2,'~',3 ) DA_UOM_3,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',3,'~',0 ) ) DA_ID_4,
        getDAChange(DedicatedAccountValuesRow,3) DA_VALUE_CHANGE_4,
        getTupleInTuple(DedicatedAccountValuesRow,'^',3,'~',3 ) DA_UOM_4,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',4,'~',0 ) ) DA_ID_5,
        getDAChange(DedicatedAccountValuesRow,4) DA_VALUE_CHANGE_5,
        getTupleInTuple(DedicatedAccountValuesRow,'^',4,'~',3 ) DA_UOM_5,
        RecordId EV_ID,
        CCRCCAAccountValueBefore ACCOUNT_VALUE_BEFORE,
        CCRCCAAccountValueAfter ACCOUNT_VALUE_AFTER,
        TransactionDate TRANSACTION_DATE,
        JobId JOB_ID,
        a.*
        from cs5Lookup6 a
        where ServiceUsageType='VAS'
        """)
     
   }
   
   /**
    * Voice output for "No Category" Microcluster (-1)
    */
   def genMCNoCategoryVoice( sqlContext:SQLContext ) : DataFrame = {
     return sqlContext.sql("""
        select distinct       
        TransactionDate TRANSACTION_DATE,
        CGI,
        MSCAddress MSC_ADDRESS,
        SubsLocNo LOC_NO,
        JobId JOB_ID
        from cs5Lookup6 a
        where ServiceUsageType='VOICE'
        and ServiceScenario='0'
        and RoamingPosition='0'
        and ServiceClass in ('540','541','542','543','544','546','547','548','549','550','551','552','577','6521')
        and MicroclusterId='-1' 
        """)
     
   }
   
   /**
    * SMS output for "No Category" Microcluster (-1)
    */
   def genMCNoCategorySMS( sqlContext:SQLContext ) : DataFrame = {
     return sqlContext.sql("""
        select distinct        
        TransactionDate TRANSACTION_DATE,
        CGI,
        SubsLocNo VLR_ID,
        JobId JOB_ID
        from cs5Lookup6 a
        where ServiceUsageType='SMS'
        and ServiceScenario='0'
        and ServiceClass in ('540','541','542','543','544','546','547','548','549','550','551','552','577','6521')
        and MicroclusterId='-1' 
        """)
     
   }

   def genCCNCS5PostpaidAddonOutput( sqlContext:SQLContext ) : DataFrame = {
	 
     sqlContext.udf.register("getTuple", Common.getTuple _)
     sqlContext.udf.register("getTupleInTuple", Common.getTupleInTuple _)
     sqlContext.udf.register("concatDA", this.concatDA _)
     sqlContext.udf.register("getDAChange", this.getDAChange _)
     
     
     return sqlContext.sql("""
        select
        CCRTriggerTime TRIGGER_TIME,
        NormCCRTriggerTime TRIGGER_TIME_NORM,        
        MSISDN SERVED_MSISDN,
        CCNNode NODE_NM,
        OriginRealm ORIGIN_REALM,
        OriginHost ORIGIN_HOST,
        SSPTransId SSP_TRANSACTION_ID,
        ServiceClass SERVICE_CLASS_ID,
        OfferAttributeAreaname AREA_NM,
        VasServiceId SERVICE_ID,
        APartyNumber A_NUM_CHARGING,
        getTuple(UsedOfferSC, ',', 0) OFFER_ID_1,
        getTuple(UsedOfferSC, ',', 1) OFFER_ID_2,
        getTuple(UsedOfferSC, ',', 2) OFFER_ID_3,
        getTuple(UsedOfferSC, ',', 3) OFFER_ID_4,
        getTuple(UsedOfferSC, ',', 4) OFFER_ID_5,
        AccountValueDeducted ACCOUNT_VALUE_DEDUCTED,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',0,'~',0 ) ) DA_ID_1,
        getDAChange(DedicatedAccountValuesRow,0) DA_VALUE_CHANGE_1,
        getTupleInTuple(DedicatedAccountValuesRow,'^',0,'~',3 ) DA_UOM_1,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',1,'~',0 ) ) DA_ID_2,
        getDAChange(DedicatedAccountValuesRow,1) DA_VALUE_CHANGE_2,
        getTupleInTuple(DedicatedAccountValuesRow,'^',1,'~',3 ) DA_UOM_2,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',2,'~',0 ) ) DA_ID_3,
        getDAChange(DedicatedAccountValuesRow,2) DA_VALUE_CHANGE_3,
        getTupleInTuple(DedicatedAccountValuesRow,'^',2,'~',3 ) DA_UOM_3,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',3,'~',0 ) ) DA_ID_4,
        getDAChange(DedicatedAccountValuesRow,3) DA_VALUE_CHANGE_4,
        getTupleInTuple(DedicatedAccountValuesRow,'^',3,'~',3 ) DA_UOM_4,
        concatDA( getTupleInTuple(DedicatedAccountValuesRow,'^',4,'~',0 ) ) DA_ID_5,
        getDAChange(DedicatedAccountValuesRow,4) DA_VALUE_CHANGE_5,
        getTupleInTuple(DedicatedAccountValuesRow,'^',4,'~',3 ) DA_UOM_5,
        RecordId EV_ID,
        CCRCCAAccountValueBefore ACCOUNT_VALUE_BEFORE,
        CCRCCAAccountValueAfter ACCOUNT_VALUE_AFTER,
        TransactionDate TRANSACTION_DATE,
        JobId JOB_ID, a.*
        from CCNCS5PostpaidAddon a
        where ServiceUsageType='VAS'
        """)
     
   }

   /**
   * Add "DA" in front of DA ID if applicable
   */
  def concatDA(DADetails: String) : String = {
    return {
      if(DADetails.isEmpty())    
        return ""
      else
        return "DA" + DADetails        
    }
  }
  
  /**
   * Get change DA based on DedicatedAccountValuesRow
   */
  def getDAChange(DedicatedAccountValuesRow: String, DANumber : Integer) :String = {
    if (DedicatedAccountValuesRow.isEmpty()) return ""
    
    val uom = Common.getTupleInTuple(DedicatedAccountValuesRow, "^", DANumber, "~", 3)
    return {
      if(uom == "1")    
        return Common.getTupleInTuple(DedicatedAccountValuesRow, "^", DANumber, "~", 1)
      else
        return Common.getTupleInTuple(DedicatedAccountValuesRow, "^", DANumber, "~", 2)      
    }
  }
}
