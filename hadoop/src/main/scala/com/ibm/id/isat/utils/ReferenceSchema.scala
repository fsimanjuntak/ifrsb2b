package com.ibm.id.isat.utils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
import org.apache.spark.sql.functions._

object ReferenceSchema {
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext("local", "testing spark ", new SparkConf());
   // val inputRDD = sc.textFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3.txt");
    
    println("Reference");
  }
  
  
    val geoAreaHlrSchema = StructType(Array(   
    StructField("city", StringType, true),
    StructField("geo_area_id", StringType, true),
    StructField("region_eff_dt", StringType, true),
    StructField("branch_eff_dt", StringType, true)));
  
  
  val svcClassSchema = StructType(Array(
    //StructField("svcClassID", StringType, true),
    StructField("svcClassCode", StringType, true),
    StructField("svcClassName", StringType, true),
   // StructField("prmPkgID", StringType, true),
    StructField("prmPkgCode", StringType, true),
    StructField("prmPkgName", StringType, true),
    StructField("svcClassEffDt", StringType, true),
    StructField("svcClassEndDt", StringType, true),
    //StructField("brndSCCode", StringType, true),
    StructField("brndSCName", StringType, true),
    StructField("brndSCEffDt", StringType, true),
    StructField("brndSCEndDt", StringType, true)));
  
  val svcClassOfrSchema = StructType(Array(
    StructField("svcClassCode", StringType, true),
    StructField("svcClassCodeOld", StringType, true),
    StructField("svcClassName", StringType, true),
    StructField("offerId", StringType, true),
    StructField("offerAttrName", StringType, true),
    StructField("offerAttrValue", StringType, true),
    StructField("areaName", StringType, true),
    StructField("prmPkgCode", StringType, true),
    StructField("prmPkgName", StringType, true),
    StructField("brndSCName", StringType, true),
    StructField("effDt", StringType, true),
    StructField("endDt", StringType, true),
    StructField("customerSegment", StringType, true)));
  val svcClassOfrSchemaVbt = StructType(Array(
    StructField("SVC_CLASS_ID", StringType, true),
    StructField("SVC_CLASS_CODE", StringType, true),
    StructField("SVC_CLASS_CODE_OLD", StringType, true),
    StructField("SVC_CLASS_NAME", StringType, true),
    StructField("OFFER_ID", StringType, true),
    StructField("OFFER_ATTR_NAME", StringType, true),
    StructField("OFFER_ATTR_VALUE", StringType, true),
    StructField("AREA_NAME", StringType, true),
    StructField("PROMO_PACKAGE_CODE", StringType, true),
    StructField("PROMO_PACKAGE_NAME", StringType, true),
    StructField("BRAND_SC_NAME", StringType, true),
    StructField("EFF_DT", StringType, true),
    StructField("END_DT", StringType, true),
    StructField("CUST_TYPE", StringType, true)));
  val acsPntNmNiSchemaVbt = StructType(Array(
    StructField("ACS_PNT_NM_NI_ID", StringType, true),
    StructField("ACS_PNT_NM_NI_NM", StringType, true)    
    ))
  val intcctSchema = StructType(Array(
    StructField("intcctPfxStart", StringType, true),
    StructField("intcctPfxEnd", StringType, true),
    StructField("intcctPfx", StringType, true),
    StructField("intcctCty", StringType, true),
    StructField("intcctOpr", StringType, true),
    StructField("locName", StringType, true),
    StructField("effDt", StringType, true),
    StructField("endDt", StringType, true),
    StructField("intcctTp", StringType, true)));
    
  val revcodeSchema = StructType(Array(
    StructField("revCode", StringType, true),
    StructField("lv1", StringType, true),
    StructField("lv2", StringType, true),
    StructField("lv3", StringType, true),
    StructField("lv4", StringType, true),
    StructField("lv5", StringType, true),
    StructField("lv6", StringType, true),
    StructField("lv7", StringType, true),
    StructField("lv8", StringType, true),
    StructField("lv9", StringType, true),
    StructField("svcUsgTp", StringType, true) ,
    StructField("dirTp", StringType, true) ,
    StructField("distTp", StringType, true),
    StructField("revSign", StringType, true)    
    ))
    
   val regionBranchSchema = StructType(Array(
    StructField("city", StringType, true),
    StructField("region", StringType, true),
    StructField("branch", StringType, true)  
    ))
   val ggsnSummaryHourlySchema= StructType(Array(
    		StructField("RECORDOPENINGTIME", StringType, true),
    		StructField("MSISDN", StringType, true),
    		StructField("IMSI", StringType, true),
    		StructField("IMEI", StringType, true),
    		StructField("SUBSTYPE", StringType, true),
    		StructField("ACCESSPOINTNAMENI", StringType, true),
    		StructField("RATTYPE", StringType, true),
    		StructField("RATINGGROUP", StringType, true),
    		StructField("USERLOCATIONINFORMATION", StringType, true),
    		StructField("MCCMNC", StringType, true),
    		StructField("LAC", StringType, true),
    		StructField("CI", StringType, true),
    		StructField("VOLUME_DOWN_LINK", StringType, true),
    		StructField("VOLUME_UP_LINK", StringType, true),
    		StructField("DURATION", StringType, true),
    		StructField("COUNTER_HIT", LongType, true),
    		StructField("HOURLY_DIMENSION", StringType, true),
    		StructField("NODE_ID", StringType, true),
    		StructField("UNKNOWN", StringType, true)
		)) 
    
   val madaSchema = StructType(Array(
    StructField("effDt", StringType, true), 
    StructField("endDt", StringType, true),
    StructField("grpCd", StringType, true),
    StructField("grpNm", StringType, true),
    StructField("acc", StringType, true),
    StructField("desc", StringType, true),
    StructField("svcTp", StringType, true),
    StructField("voiceRev", StringType, true),
    StructField("smsRev", StringType, true),
    StructField("dataVolRev", StringType, true),
    StructField("dataDurRev", StringType, true),
    StructField("vasRev", StringType, true),
    StructField("voiceTrf", StringType, true),
    StructField("smsTrf", StringType, true),
    StructField("dataVolTrf", StringType, true),
    StructField("dataDurTrf", StringType, true),
    StructField("othRev", StringType, true),
    StructField("discRev", StringType, true)
    ))
    
    val recTypeSchema = StructType(Array(
    StructField("effDt", StringType, true), 
    StructField("endDt", StringType, true),
    StructField("revCode", StringType, true),
    StructField("recType", StringType, true)
    ))
    
    val callClassSchema = StructType(Array(
    StructField("callClassId", StringType, true), 
    StructField("displayNm", StringType, true),
    StructField("baseNm", StringType, true),
    StructField("ruleId", StringType, true),
    StructField("tariffZone", StringType, true)
    ))
    
    val lacCiHierarchySchema = StructType(Array(
    StructField("area", StringType, true), 
    StructField("salesArea", StringType, true),
    StructField("clusterName", StringType, true),
    StructField("lacDec", StringType, true),
    StructField("ciDec", StringType, true),
    StructField("cellName", StringType, true),
    StructField("distro", StringType, true),
    StructField("microCluster", StringType, true),
    StructField("siteTech", StringType, true),
    StructField("ratType", StringType, true),
    StructField("siteOpr", StringType, true),
    StructField("lacHex", StringType, true),
    StructField("ciHex", StringType, true),
    StructField("lacHexOri", StringType, true),
    StructField("ciHexOri", StringType, true)
    ))
    val madaSchemaVbt = StructType(Array(
    StructField("EFF_DT", StringType, true), 
    StructField("END_DT", StringType, true),
    StructField("PROMO_PKG_CODE", StringType, true),
    StructField("PROMO_PKG", StringType, true),
    StructField("ACCOUNT1", StringType, true),
    StructField("DESCR", StringType, true),
    StructField("SERVICE_TYPE", StringType, true),
    StructField("VOICE_REV", StringType, true),
    StructField("SMS_REV", StringType, true),
    StructField("DATA_VOL_REV", StringType, true),
    StructField("DATA_DUR_REV", StringType, true),
    StructField("VAS_REV", StringType, true),
    StructField("VOICE_TRAFFIC", StringType, true),
    StructField("SMS_TRAFFIC", StringType, true),
    StructField("DATA_VOL_TRAFFIC", StringType, true),
    StructField("DATA_DUR_TRAFFIC", StringType, true),
    StructField("OTHER_REVENUE", StringType, true),
    StructField("DISCOUNT_REVENUE", StringType, true)
    ))
    val revcodeSchemaVbt = StructType(Array(
    StructField("REV_CODE", StringType, true),
    StructField("LEVEL_1", StringType, true),
    StructField("LEVEL_2", StringType, true),
    StructField("LEVEL_3", StringType, true),
    StructField("LEVEL_4", StringType, true),
    StructField("LEVEL_5", StringType, true),
    StructField("LEVEL_6", StringType, true),
    StructField("LEVEL_7", StringType, true),
    StructField("LEVEL_8", StringType, true),
    StructField("LEVEL_9", StringType, true),
    StructField("SVC_USG_TP", StringType, true) ,
    StructField("DRC_TP", StringType, true) ,
    StructField("DSTN_TP", StringType, true),
    StructField("REV_SIGN", StringType, true)    
    ))
    val scvIDShortCodeSchema = StructType(Array(
    StructField("createDate", StringType, true), 
    StructField("shortCode", StringType, true),
    StructField("contentProviderNm", StringType, true),
    StructField("activeData", StringType, true),
    StructField("lapsedDate", StringType, true),
    StructField("switch", StringType, true),
    StructField("category", StringType, true),
    StructField("svcTp", StringType, true),
    StructField("svcCategory", StringType, true),
    StructField("detailId", StringType, true),
    StructField("svcId", StringType, true),
    StructField("aChargingId", StringType, true),
    StructField("aCharing", StringType, true),
    StructField("billingModel", StringType, true),
    StructField("recTpCriteria", StringType, true),
    StructField("recTp", StringType, true),
    StructField("costBandPostpaid", StringType, true),
    StructField("mtShare", StringType, true),
    StructField("isat", StringType, true),
    StructField("cp", StringType, true),
    StructField("moPrepaid", StringType, true),
    StructField("moPostpaid", StringType, true),
    StructField("mt", StringType, true),
    StructField("endUserTarifPost", StringType, true),
    StructField("endUserTarifPrep", StringType, true),
    StructField("contectClassPercentage", StringType, true),
    StructField("svcDesc", StringType, true),
    StructField("svcPIC", StringType, true),
    StructField("services", StringType, true)
    ))
    
    val intcctNtwkShortCodeSchema = StructType(Array(
    StructField("intcctPfxStart", StringType, true),
    StructField("intcctPfxEnd", StringType, true),
    StructField("intcctPfx", StringType, true),
    StructField("intcctCty", StringType, true),
    StructField("intcctOpr", StringType, true),
    StructField("locName", StringType, true),
    StructField("effDt", StringType, true),
    StructField("endDt", StringType, true),
    StructField("intcctTp", StringType, true),
    StructField("sid", StringType, true)
    ));
  
     // added for AIR area ( 23 June 2016 )
    
    val reloadTypeSchema = StructType(Array(
      StructField("originNodeType", StringType, true), 
      StructField("originHostName", StringType, true),
      StructField("reloadType", StringType, true),
      StructField("insertDate", StringType, true)
    ))
    
     val voucherTypeSchema = StructType(Array(
      StructField("column1", StringType, true), 
      StructField("column2", StringType, true),
      StructField("column3", StringType, true),
      StructField("column4", StringType, true), 
      StructField("column5", StringType, true),
      StructField("column6", StringType, true),
      StructField("column7", StringType, true), 
      StructField("column8", StringType, true),
      StructField("column9", StringType, true)
    ))

    val bankNameSchema = StructType(Array(
      StructField("column1", StringType, true), 
      StructField("column2", StringType, true),
      StructField("column3", StringType, true),
      StructField("column4", StringType, true), 
      StructField("column5", StringType, true),
      StructField("column6", StringType, true)
    ))
 
     val bankDetailSchema = StructType(Array(
      StructField("column1", StringType, true), 
      StructField("column2", StringType, true)
    ))
    
    val postServiceClass = StructType(Array(
    	    StructField("postSCName", StringType, true),
    	    StructField("postSCCode", StringType, true)
    	    ));
    	    
    	    val postCostband = StructType(Array(
    	    StructField("CostbandId", StringType, true),
    	    StructField("CostbandName", StringType, true)
    	    ));
    	    
    	    val postLocation = StructType(Array(
    	    StructField("LocationId", StringType, true),
    	    StructField("LocationName", StringType, true)
    	    ));
    	    
    	    val postEventTypeRevenue = StructType(Array(
    	    StructField("EventTypeId", StringType, true),
    	    StructField("RevcodeName", StringType, true),
    	    StructField("Axis1Value", StringType, true),
    	    StructField("Axis2Value", StringType, true),
    	    StructField("Axis3Value", StringType, true),
    	    StructField("Axis4Value", StringType, true),
    	    StructField("Axis5Value", StringType, true),
    	    StructField("Axis6Value", StringType, true)
    	    ));
    	    
    	    val postRevCodeToGLSchema = StructType(Array(
    	    StructField("LegacyId", StringType, true),
    	    StructField("RevenueId", StringType, true),
    	    StructField("GLAcc", StringType, true)
    	    ));
    	    
    	    val postGLtoServType = StructType(Array(
    	    StructField("LegacyId", StringType, true),
    	    StructField("RevenueId", StringType, true),
    	    StructField("Level4", StringType, true),
    	    StructField("Level5", StringType, true),
    	    StructField("SvcType", StringType, true),
    	    StructField("SvcTypeDA1", StringType, true),
    	    StructField("SvcTypeNDA1", StringType, true)
    	    ));
    	    
    	    val postGLtoServTypeVAS = StructType(Array(
    	    StructField("Level4", StringType, true),
    	    StructField("Level5", StringType, true),
    	    StructField("SvcType", StringType, true),
    	    StructField("SvcTypeDA1", StringType, true),
    	    StructField("SvcTypeNDA1", StringType, true)
    	    ));
    	    
    	    val intcct2Schema = StructType(Array(
    	    StructField("intcctPfx", StringType, true),
    	    StructField("intcctCty", StringType, true),
    	    StructField("intcctOpr", StringType, true),
    	    StructField("locName", StringType, true),
    	    StructField("effDt", StringType, true),
    	    StructField("endDt", StringType, true),
    	    StructField("sid", StringType, true)));
    	    
    	    val sdpOfferSchema = StructType(Array(
    	    StructField("msisdn", StringType, true),
    	    StructField("offerID", StringType, true),
    	    StructField("effDt", StringType, true),
    	    StructField("endDt", StringType, true)));
    	    
    	    val intcctAllSchema = StructType(Array(
    	    StructField("length", StringType, true),
    	    StructField("intcctPfx", StringType, true),
    	    StructField("intcctCty", StringType, true),
    	    StructField("intcctOpr", StringType, true),
    	    StructField("locName", StringType, true),
    	    StructField("effDt", StringType, true),
    	    StructField("endDt", StringType, true),
    	    StructField("sid", StringType, true)));
    	    
    	    val microclusterSchema = StructType(Array(
    	    StructField("cgi", StringType, true),
    	    StructField("cellName", StringType, true),
    	    StructField("lac", StringType, true),
    	    StructField("ci", StringType, true),
    	    StructField("lac_hex", StringType, true),
    	    StructField("ci_hex", StringType, true),
    	    StructField("longitude", StringType, true),
    	    StructField("latitude", StringType, true),
    	    StructField("azimuth", StringType, true),
    	    StructField("serviceType", StringType, true),
    	    StructField("microclusterId", StringType, true),
    	    StructField("microclusterName", StringType, true)));
    	    
    	    val MCCMNCSchema = StructType(Array(
    	    StructField("MCCMNCName", StringType, true),
    	    StructField("MCC", StringType, true),
    	    StructField("MNC", StringType, true),
    	    StructField("MCCMNC", StringType, true)));
    	    

    	    
          val fossSchema = StructType(Array(
          StructField("ICCID", StringType, true),
          StructField("MSISDN", StringType, true),
          StructField("TOTAL_MSISDN", StringType, true),
          StructField("IMSI", StringType, true),
          StructField("ENTRY_DATE", StringType, true),
          StructField("EXPIRY_DATE", StringType, true),
          StructField("SERVICE_TYPE", StringType, true),
          StructField("BUS_STATUS", StringType, true),
          StructField("UPDATED_ON", StringType, true),
          StructField("BRAND_CODE", StringType, true),
          StructField("BRANCH_CODE", StringType, true),
          StructField("NET_STATUS", StringType, true),
          StructField("HLR_CODE", StringType, true),
          StructField("CARD_SIZE", StringType, true),
          StructField("DEALER_ID", StringType, true),
          StructField("ENTERED_BY", StringType, true),
          StructField("UPDATED_BY", StringType, true),
          StructField("ARTWORK_CODE", StringType, true),
          StructField("PROD_ORDER_NO", StringType, true),
          StructField("PACK_ORDER_NO", StringType, true),
          StructField("ALLOC_NO", StringType, true),
          StructField("HLRAREA_CODE", StringType, true),
          StructField("KI_A4KI", StringType, true),
          StructField("DISTRIBUTION_DATE", StringType, true),
          StructField("NOMINAL_VALUE", StringType, true),
          StructField("ESN", StringType, true),
          StructField("ADM1", StringType, true),
          StructField("ACTIVE_DATE", StringType, true),
          StructField("INACTIVE_DATE", StringType, true),
          StructField("PROGRAM_CODE", StringType, true),
          StructField("STOCK_ORDER_ID", StringType, true),
          StructField("PRINT_FILE_DATE", StringType, true),
          StructField("PRINT_FILE_USER", StringType, true),
          StructField("INSPECT_OK_DATE", StringType, true),
          StructField("INSPECT_OK_USER", StringType, true),
          StructField("PACK_OK_DATE", StringType, true),
          StructField("PACK_OK_USER", StringType, true),
          StructField("REPERSO_DATE", StringType, true),
          StructField("REPERSO_USER", StringType, true),
          StructField("VERSION", StringType, true),
          StructField("PIN1", StringType, true),
          StructField("PIN2", StringType, true),
          StructField("PUK1", StringType, true),
          StructField("PUK2", StringType, true),
          StructField("CARD_FLAG", StringType, true),
          StructField("COMPOSITE_AREA_CODE", StringType, true),
          StructField("IMSI_T", StringType, true),
          StructField("SERVICE_CLASS", StringType, true),
          StructField("LAC", StringType, true),
          StructField("CI", StringType, true),
          StructField("MSISDN_SEL_TYPE", StringType, true),
          StructField("SVC_BRANDCODE", StringType, true),
          StructField("PAID_VANITY", StringType, true),
          StructField("CARD_TYPE", StringType, true),
          StructField("ALGORITHM_VERSION", StringType, true)
          ));

  val refGroupOfServices = StructType(Array(
      StructField("BUSINESS_AREA_NUMBER", StringType, true),
      StructField("BUSINESS_AREA_NAME", StringType, true),
      StructField("GROUP_OF_SERVICES", StringType, true)))

  val refCustomerGroup = StructType(Array(
      StructField("CA_NAME", StringType, true),
      StructField("CUSTOMER_GROUP", StringType, true)))
          
    	    
    
}
