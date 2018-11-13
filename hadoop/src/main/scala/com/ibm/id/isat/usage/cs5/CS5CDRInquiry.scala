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
import com.ibm.id.isat.usage.cs5.CS5Reference._
import scala.util.control.Breaks._

 /*****************************************************************************************************
 * 
 * CS5 Transformation 
 * Generate CDR Inquiry output
 * Input: CCN CS5 transformation pt. 1
 * 
 * Author: Nico Anandito
 * IBM Indonesia for Indosat Ooredoo
 * June 2016
 * 
 *****************************************************************************************************/ 
object CS5CDRInquiry {
   
   /**
   * Generate CDR Inquiry output from transformation and lookup results
   * @author anico@id.ibm.com
   * @param sqlContext of spark context
   * @return dataframe of CDR inquiry output
   */
  def genPrepaidOutput( sqlContext:SQLContext ) : DataFrame = {
    
    sqlContext.udf.register("getCDRInquiryRevCode", this.getCDRInquiryRevCode _)
    sqlContext.udf.register("getSPMadaFromSPConsume", this.getSPMadaFromSPConsume _)
    sqlContext.udf.register("getKeyCDRInq", Common.getKeyCDRInq _)
    sqlContext.udf.register("getCDRInquiryDADetails", this.getCDRInquiryDADetails _)
    sqlContext.udf.register("getCDRInquiryRoaming", this.getCDRInquiryRoaming _)
    
    return sqlContext.sql("""
        select
        RecordId key,
        MSISDN,
        CCRTriggerTime TRANSACTIONSTARTTIME,
        CCRTriggerTime TRANSACTIONENDTIME,
        case when ServiceUsageType = 'VAS' then 'SMS' else ServiceUsageType end TRAFFICTYPES,
        case when ServiceScenario = '0' then 'OUTGOING' when ServiceScenario = '1' then 'FORWARD (OUTGOING)' else 'INCOMING' end DIRECTIONTYPES,
        getCDRInquiryRoaming(ServiceUsageType, a.MCCMNC, RoamingPosition, SubsLocNo, ServiceScenario) ROAMINGAREANAME,
        CallClassCat CALLCLASSID,
        ServiceClass ACCOUNTSERVICECLASS,
        'PREPAID' SUBSCRIBERTYPE,
        prmPkgName PROMOPACKAGE,
        APartyNumber CALLINGNUMBER,
        case when APrefix = '' then '' else concat(APrefix, ';', ServiceCityName,';', ServiceProviderId ,';', ServiceCountryName) end CALINGNUMBERDESCRIPTION,
        BPartyNumber CALLEDNUMBER,
        case when BPrefix = '' then '' else concat(BPrefix, ';', DestinationCityName,';', DestinationProviderId ,';', DestinationCountryName) end CALLEDNUMBERDESCRIPTION,
        UsageDuration DURATION,
        AccountValueDeducted COST,
        CCRCCAAccountValueBefore MAINACCOUNTBALANCEBEFORE,
        CCRCCAAccountValueAfter MAINACCOUNTBALANCEAFTER,
        getCDRInquiryDADetails(DedicatedAccountRow) DADETAILS,
        Accumulator ACCUMULATORDETAILS,
        case when ServiceCityName != '' then ServiceCityName else 'UNKNOWN' end LOCATIONNUMBER,
        CCNNode PPSNODEID,
        case when ServiceProviderId != '' then ServiceProviderId else 'UNKNOWN' end SERVICEPROVIDERID,
        UsageVolume VOLUME,
        FAF FAFINDICATOR,
        CommunityId1 COMMUNITYINDICATOR1, CommunityId2 COMMUNITYINDICATOR2, CommunityId3 COMMUNITYINDICATOR3,
        AccountGroup ACCOUNTGROUPID,
        ServiceOfferId SERVICEOFFERING,
        LAC,
        CI CELLID, 
        case when (microclusterName!='') then microclusterName 
          when (laciCluster!='' or laciSalesArea!='' ) then concat(laciCluster,';',laciSalesArea) else '' end CLUSTERNAME,
        case when RATType='1' then 'UTRAN (UMTS/3G)' when RATType='2' then 'GERAN (2G)' when RATType='6' then 'EUTRAN (LTE)' else RATType end NETWORKTYPE,
        RatingGroup RATINGGROUP,
        concat(a.MCCMNC,';',nvl(b.MCCMNCName,'')) MCCMNC,
				'Charging' SERVICETYPE,
				FileName FILENAMEINPUT,
				getCDRInquiryRevCode(revenueCodeBase,AccountRow,ServiceUsageType) SERVICE_ID_REVENUE_CODE,
				JobId JOBID,
				SourceType SERVICENAME,
				mgrSvcClassId MGR_SVCCLS_ID,
				UsedOfferSC OFFER_ID,
				offerAttrName OFFER_ATTR_KEY,
				offerAttrValue OFFER_ATTR_VALUE,
				areaName OFFER_AREA_NAME,
        SpOfrId SPOFFER,
				getSPMadaFromSPConsume(SpConsume) SPMADA,
				SpConsume SPCONSUME,
        "" USAGETYPE
        from 
        cs5Lookup6 a
        left join
        refMCCMNC b
        on a.MCCMNC=b.MCCMNC
        """)
  }
  
  
   /*
   * Generate CDR Inquiry output from transformation and lookup results
   * @return dataframe of CDR inquiry output
   */
  def genPostpaidOutput( sqlContext:SQLContext, postCorpProductID:postCorpProductID ) : DataFrame = {
    
    sqlContext.udf.register("getCDRInquiryRevCode", this.getCDRInquiryRevCode _)
    sqlContext.udf.register("getSPMadaFromSPConsume", this.getSPMadaFromSPConsume _)
    sqlContext.udf.register("getKeyCDRInq", Common.getKeyCDRInq _)
    sqlContext.udf.register("getUsageTypeCorporate", postCorpProductID.getUsageTypePostpaid _)
    sqlContext.udf.register("getCDRInquiryDADetails", this.getCDRInquiryDADetails _)
    sqlContext.udf.register("getCDRInquiryRoaming", this.getCDRInquiryRoaming _)
    
    
    
    return sqlContext.sql("""
        select
        RecordId key,
        MSISDN,
        CCRTriggerTime TRANSACTIONSTARTTIME,
        CCREventTime TRANSACTIONENDTIME,
        ServiceType TRAFFICTYPES,
        case when ServiceScenario = '0' then 'OUTGOING' when ServiceScenario = '1' then 'FORWARD (OUTGOING)' else 'INCOMING' end DIRECTIONTYPES,
        getCDRInquiryRoaming(ServiceUsageType, a.MCCMNC, RoamingPosition, SubsLocNo, ServiceScenario) ROAMINGAREANAME,
        '' CALLCLASSID,
        ServiceClass ACCOUNTSERVICECLASS,
        'POSTPAID' SUBSCRIBERTYPE,
        '' PROMOPACKAGE,
        MSISDN CALLINGNUMBER,
        case when APrefix = '' then '' else concat(APrefix, ';', ServiceCityName,';', ServiceProviderId ,';', ServiceCountryName) end CALINGNUMBERDESCRIPTION,
        OtherPartyNum CALLEDNUMBER,
        case when BPrefix = '' then '' else concat(BPrefix, ';', DestinationCityName,';', DestinationProviderId ,';', DestinationCountryName) end CALLEDNUMBERDESCRIPTION,
        UsageDuration DURATION,
        AccountValueDeducted COST,
        CCRCCAAccountValueBefore MAINACCOUNTBALANCEBEFORE,
        CCRCCAAccountValueAfter MAINACCOUNTBALANCEAFTER,
        getCDRInquiryDADetails(DedicatedAccountRow) DADETAILS,
        Accumulator ACCUMULATORDETAILS,
        case when ServiceCityName != '' then ServiceCityName else 'UNKNOWN' end LOCATIONNUMBER,
        CCNNode PPSNODEID,
        case when ServiceProviderId != '' then ServiceProviderId else 'UNKNOWN' end SERVICEPROVIDERID,
        UsageVolume VOLUME,
        FAF FAFINDICATOR,
        CommunityId1 COMMUNITYINDICATOR1, CommunityId2 COMMUNITYINDICATOR2, CommunityId3 COMMUNITYINDICATOR3,
        AccountGroup ACCOUNTGROUPID,
        ServiceOfferId SERVICEOFFERING,
        LAC,
        CI CELLID, 
        case when (laciCluster='' and laciSalesArea='' ) then '' else concat(laciCluster,';',laciSalesArea) end CLUSTERNAME,
        case when RATType='1' then 'UTRAN (UMTS/3G)' when RATType='2' then 'GERAN (2G)' when RATType='6' then 'EUTRAN (LTE)' else RATType end NETWORKTYPE,
        RatingGroup RATINGGROUP,
        concat(a.MCCMNC,';',nvl(b.MCCMNCName,'')) MCCMNC,
				'Charging' SERVICETYPE,
				FileName FILENAMEINPUT,
				'' SERVICE_ID_REVENUE_CODE,
				JobId JOBID,
				SourceType SERVICENAME,
				'' MGR_SVCCLS_ID,
				UsedOfferSC OFFER_ID,
				'' OFFER_ATTR_KEY,
				'' OFFER_ATTR_VALUE,
				'' OFFER_AREA_NAME,
				SpOfrId SPOFFER,
				getSPMadaFromSPConsume(SpConsume) SPMADA,
				SpConsume SPCONSUME,
        CorpUsgType USAGETYPE
        from cs5Lookup5 a
        left join
        refMCCMNC b
        on a.MCCMNC=b.MCCMNC
        """)
  }
  
  
  /**
   * Input RevCodeBase, MA~[amt]^DA1~[amt]^DA2~[amt]
   * @return RevCodeBaseMA;RevCodeBaseDA1;RevCodeBaseDA2 where [amt] DA > 0
   */
  def getCDRInquiryRevCode( revCodeBase:String, accountRow:String, ServiceUsageType:String) : String = {
    
    if ( accountRow == "" ) return ""
    //If contains DA
    else if ( accountRow.contains("^") ) {
      var revCodeRow = ""
      var i = 0
      var accountRowArr = accountRow.split("\\^")
     
      //Check per DA
      while ( i<accountRowArr.length ) {
        if ( Common.getTuple(accountRowArr(i), "~", 1) == "MA" ) revCodeRow = revCodeBase.concat("MA")
        else {
           var amt = 0.00
           amt = Common.toDouble( Common.getTuple(accountRowArr(i), "~", 1) ) 
           if ( i>0 ) revCodeRow = revCodeRow.concat(";")
           if ( amt >= 0.00 ) revCodeRow = revCodeRow.concat(revCodeBase).concat(Common.getTuple(accountRowArr(i), "~", 1))
        }
        i=i+1
      }
      return revCodeRow
    }
    //If only contains MA
    else if ( ServiceUsageType=="VAS" ) return revCodeBase
    else return revCodeBase.concat( Common.getTuple(accountRow, "~", 1) )
    
  }
  
  /**
   * Return first column from ccr_specified_consumption
   */
  def getSPMadaFromSPConsume( SPConsume:String ) : String = {
     try {
      return SPConsume.split("\\\\")(1)
    }
    catch {
      case e: Exception => return ""
    }
  }
  
  /**
   * Move da unit amount (before/after/change) from unit position to monetary position
   * @param daDetails delimited by ~ from conversion result
   * @return daDetails for unit type in monetary place
   */
  def getCDRInquiryDADetails( daDetails:String ) :String = {
    val delim = "~"
    
    if ( daDetails.isEmpty() ) 
      return daDetails
      
    val daDetailsReturn = new StringBuilder
    var i=0
    val multiDAArr = daDetails.split("\\^")   
    
    //while has DA
    while ( i<multiDAArr.length ) {
      breakable {
        //add delimiter if more than 1 DA
        if (i>0) daDetailsReturn.append("^") 
        
        //get DA unit type
        val DAUnit = Common.getTuple( multiDAArr(i) , delim, 14)
        
        //If DAunit is rupiah, return as is
        if ( DAUnit=="1" ) {          
          daDetailsReturn.append( multiDAArr(i) )
          break;
        }
        
        //else, move da unit amount to monetary place
        val daRowArr = multiDAArr(i).split(delim)
        
        daDetailsReturn.append( Common.safeGet(daRowArr, 0) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 11) ).append("\\6\\360").append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 12) ).append("\\6\\360").append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 13) ).append("\\6\\360").append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 4) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 5) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 6) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 7) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 8) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 9) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 10) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 11) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 12) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 13) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 14) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 15) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 16) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 17) ).append(delim)
        daDetailsReturn.append( Common.safeGet(daRowArr, 18) )
       }
      //increment
      i=i+1
    }    
   return daDetailsReturn.toString()     
  }
  
    /**
   * Get roaming status for inquiry
   * @param svc
   * @return daDetails for unit type in monetary place
   */
  def getCDRInquiryRoaming( ServiceUsageType:String, MCCMNC:String, RoamingPosition:String, SubsLocNo:String, ServiceScenario:String ) :String = {
    if(ServiceUsageType=="DATA" ) {
      if(MCCMNC.isEmpty() || MCCMNC.startsWith("510")) return "HOME"
      else return "ROAMING"
    } else if(ServiceUsageType=="VOICE") {
      if(RoamingPosition.isEmpty() || RoamingPosition=="0") return "HOME"
      else return "ROAMING"
    } else if(ServiceUsageType=="SMS") {
      if(ServiceScenario!="2" && (!SubsLocNo.isEmpty() && !SubsLocNo.startsWith("62"))) return "ROAMING"
      else return "HOME"
    } else return "HOME"
  }
}