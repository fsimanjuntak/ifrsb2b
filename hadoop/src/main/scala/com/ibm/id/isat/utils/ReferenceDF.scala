package com.ibm.id.isat.utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.ibm.id.isat.usage.cs5.CS5Reference

object ReferenceDF {
   
   val path_ref = "/user/apps/hadoop_spark/reference" //PROD
   //val path_ref = "C:/Users/IBM_ADMIN/Desktop/Indosat/Microcluster/Dev/reference" //DEV
   //val path_ref = "/user/indosat_ops/reference" //UAT
   /**
    * Old Service Class reference 
    */
   def getServiceClassDF(sqlContext: SQLContext) : DataFrame = {
    
     val svcClassDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter",",")
                      .schema(ReferenceSchema.svcClassSchema)
                      .load(path_ref + "/ref_service_class_mv/NONTDWM.REF_SERVICE_CLASS_MV_*"))
     return svcClassDF
    
   }
   
   /**
    * ACS PNT NM NI Reference
    */
   def getAcsPntNmNiVbt(sqlContext: SQLContext) : DataFrame = {
    
      val acsPntNmNiDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.acsPntNmNiSchemaVbt)
//                      .load("C:/ScalaProjects2/microcluster/DataSource/acs_pnt_nm_ni"))
                      .load("/user/hdp-rev/spark/output_vbt/sqoop/acs_pnt_nm_ni")
                      //.load(path_ref + "/ref_revcode/SOR.REF_REVCODE_*"))
      )
      return acsPntNmNiDF
    
   }
   /**
    * Service Class Offer reference VBT
    */
   def getServiceClassOfferDFVbt(sqlContext: SQLContext) : DataFrame = {
    
     val svcClassDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "true") // Use first line of all files as header
                      .option("delimiter",",")
                      .schema(ReferenceSchema.svcClassOfrSchemaVbt)
//                      .load("C:/ScalaProjects2/microcluster/Reference/NONTDWM.REF_SERVICE_CLASS_OFR_*"))
                      .load("/user/apps/hadoop_spark/reference/ref_service_class_ofr_vbt/NONTDWM.REF_SERVICE_CLASS_OFR_*"))
                      //.load(path_ref + "/ref_service_class_ofr/NONTDWM.REF_SERVICE_CLASS_OFR_*"))
     return svcClassDF
    
   }
   
   /**
    * Region to Branch reference
    */
   def getRegionBranchDF(sqlContext: SQLContext) : DataFrame = {
    
      val regionBranchDF =  broadcast(sqlContext.read
                            .format("com.databricks.spark.csv")
                            .option("header", "false") // Use first line of all files as header
                            .option("delimiter",",")
                            .schema(ReferenceSchema.regionBranchSchema)
                            .load(path_ref + "/ref_region_branch/ref_region_branch*"))
      return regionBranchDF
    
   }
   
   /**
    * CallClass ID REference
    */
   def getCallClassDF(sqlContext: SQLContext) : DataFrame = {
    
      val callClassDF =  broadcast(sqlContext.read
                            .format("com.databricks.spark.csv")
                            .option("header", "false") // Use first line of all files as header
                            .option("delimiter",",")
                            .schema(ReferenceSchema.callClassSchema)
                            .load(path_ref + "/ref_call_class_id/SOR.REF_CALL_CLASS_ID_*"))
      return callClassDF
    
   }
   
   /**
    * Revenue Code Reference
    */
   def getRevenueCodeDF(sqlContext: SQLContext) : DataFrame = {
    
      val revcodeDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.revcodeSchema)
                      .load(path_ref + "/ref_revcode/SOR.REF_REVCODE_*"))
      return revcodeDF
    
   }
   
   /**
    * Prepaid MADA Reference
    */
   def getMaDaDF(sqlContext: SQLContext) : DataFrame = {
    
      val madaDF =  broadcast(sqlContext.read
                    .format("com.databricks.spark.csv")
                    .option("header", "false") // Use first line of all files as header
                    .option("delimiter","|")
                    .schema(ReferenceSchema.madaSchema)
                    .load(path_ref + "/ref_ma_da/NONTDWM.REF_MA_DA_*"))
      return madaDF
    
   }
   def getMaDaDFVbt(sqlContext: SQLContext) : DataFrame = {
    
      val madaDF =  broadcast(sqlContext.read
                    .format("com.databricks.spark.csv")
                    .option("header", "false") // Use first line of all files as header
                    .option("delimiter","|")
                    .schema(ReferenceSchema.madaSchemaVbt)
//                    .load("C:/ScalaProjects2/microcluster/Reference/NONTDWM.REF_MA_DA_*")) 
                    .load("/user/apps/hadoop_spark/reference/ref_ma_da/NONTDWM.REF_MA_DA_*"))
                    //.load(path_ref + "/ref_ma_da/NONTDWM.REF_MA_DA_*"))
                    //.load(path_ref + "/ref_ma_da/backup/NONTDWM.REF_MA_DA_20161231.txt"))
      return madaDF
    
   }
   def getRevenueCodeDFVbt(sqlContext: SQLContext) : DataFrame = {
    
      val revcodeDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.revcodeSchemaVbt)
//                      .load("C:/ScalaProjects2/microcluster/Reference/SOR.REF_REVCODE_*"))
                      .load("/user/apps/hadoop_spark/reference/ref_revcode/SOR.REF_REVCODE_*"))
                      //.load(path_ref + "/ref_revcode/SOR.REF_REVCODE_*"))
      return revcodeDF
    
   }
   
   
   def getRecordTypeDF(sqlContext: SQLContext) : DataFrame = {
    
     val recTypeDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter",",")
                      .schema(ReferenceSchema.recTypeSchema)
                      .load(path_ref + "/ref_record_type/all_ref_record_type_CCN*.txt"))
  
      return recTypeDF
    
   }
   
   /**
    * Get SDP Offer
    */
   def getSDPOffer(sqlContext: SQLContext) : DataFrame = {
    
     val sdpOfferDF =  sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.sdpOfferSchema)
                      .load(path_ref + "/lkp_sdp_offer/SOR.LKP_SDP_OFFER_*")
                      
      return sdpOfferDF
    
   }
   
   /**
    * Interconnect length reference - combined with SID information
    */
   def getIntcctAllDF(sqlContext: SQLContext) : DataFrame = {
    
     val intcctDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.intcctAllSchema)
                      .load(path_ref + "/ref_intcct_mv_all/HA1.REF_INTCCT_MV_ALL_*")) 
  
      return intcctDF
    
   }
   
   /**
    * Service Class Postpaid reference
    */
   def getServiceClassPostpaidDF(sqlContext: SQLContext) : DataFrame = {
    
     val intcctDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.postServiceClass)
                      .load(path_ref + "/ref_postpaid_service_class/REF_POSTPAID_SERVICE_CLASS_*")) 
  
      return intcctDF
    
   }
   
   /**
    * Service Class Offer reference
    */
   def getServiceClassOfferDF(sqlContext: SQLContext) : DataFrame = {
    
     val svcClassDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "true") // Use first line of all files as header
                      .option("delimiter",",")
                      .schema(ReferenceSchema.svcClassOfrSchema)
                      .load(path_ref + "/ref_service_class_ofr/NONTDWM.REF_SERVICE_CLASS_OFR_*"))
     return svcClassDF
    
   }
   /**
    * Service Class Offer reference with path
    */
   def getServiceClassOfferDF2(sqlContext: SQLContext, path: String) : DataFrame = {
    
     val svcClassDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "true") // Use first line of all files as header
                      .option("delimiter",",")
                      .schema(ReferenceSchema.svcClassOfrSchema)
                      .load(path + "/NONTDWM.REF_SERVICE_CLASS_OFR_*"))
     return svcClassDF
    
   }
   
   /**
    * Postpaid Costband
    */
   def getPostpaidCostbandDF(sqlContext: SQLContext) : DataFrame = {
    
     val refCostbandDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.postCostband)
                      .load(path_ref + "/ref_postpaid_costband/REF_POSTPAID_COSTBAND_*"))
     return refCostbandDF
    
   }
   
   /**
    * Postpaid Location
    */
   def getPostpaidLocationDF(sqlContext: SQLContext) : DataFrame = {
    
     val refLocationDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.postLocation)
                      .load(path_ref + "/ref_postpaid_location/REF_POSTPAID_LOCATION_*"))
     return refLocationDF
    
   }
   
   /**
    * Postpaid Event Type
    */
   def getPostpaidEventtypeDF(sqlContext: SQLContext) : DataFrame = {
    
     val refPostEventTypeDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.postEventTypeRevenue)
                      .load(path_ref + "/ref_postpaid_eventtype_revenue/REF_POSTPAID_EVENTTYPE_REVENUE_*"))
     return refPostEventTypeDF
    
   }
   
   /**
    * Postpaid Revenue to GL
    */
   def getPostpaidRevToGLDF(sqlContext: SQLContext) : DataFrame = {
    
     val refRevToGLDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.postRevCodeToGLSchema)
                      .load(path_ref + "/ref_postpaid_rev_to_gl/REF_POSTPAID_REVENUE_TO_GL_*"))
     return refRevToGLDF
    
   }
   
   /**
    * Postpaid GL to Service Type DISTINCT
    */
   def getPostpaidGLtoServTypeDF(sqlContext: SQLContext) : DataFrame = {
    
     val refRevToGLDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.postGLtoServType)
                      .load(path_ref + "/ref_postpaid_gl_to_servicetype/REF_POSTPAID_GL_TO_SERVICETYPE_*"))
     return refRevToGLDF
    
   }
   
   /**
    * Postpaid GL to Service Type VAS (without Revenue ID)
    */
   def getPostpaidGLtoServTypeVASDF(sqlContext: SQLContext) : DataFrame = {
    
     val refRevToGLVASDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.postGLtoServTypeVAS)
                      .load(path_ref + "/ref_postpaid_gl_to_servicetype_vas/REF_POSTPAID_GL_TO_SERVICETYPE_*"))
     return refRevToGLVASDF
    
   }
   
   /**
    * LAC CI Hierarchy
    */
   def getLACCIHierarchy(sqlContext: SQLContext) : DataFrame = {
    
     val refLacCiHierarchy = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.lacCiHierarchySchema)
                      .load(path_ref + "/ref_lac_ci_hierarchy/REF_LAC_CI_HIERARCHY_*"))
     return refLacCiHierarchy    
   }
   
   /**
    * NON DATAFRAME
    * @returns Interconnect Reference object
    */
    def getIntcctAllRef(sc: SparkContext) : Common.interconnectRef = {
      return new Common.interconnectRef(path_ref + "/ref_intcct_mv_all/HA1.REF_INTCCT_MV_ALL_*" , sc)
    }
    def getIntcctAllRef1(sc: SparkContext, pathRef: String) : Common.interconnectRef = {
      return new Common.interconnectRef(pathRef, sc)
    }
    
   /**
    * NON DATAFRAME
    * @returns Eventtype Reference object
    */
    def getPostEventTypeRef(sc: SparkContext) : CS5Reference.postEventType = {
      return new CS5Reference.postEventType(path_ref + "/ref_postpaid_eventtype_revenue/REF_POSTPAID_EVENTTYPE_REVENUE_*" , sc)
    }
    
   /**
    * Get record reload type
    */
    def getRecordReloadTypeDF(sqlContext: SQLContext) : DataFrame = {
     val reloadTypeFile = this.path_ref + "/ref_reload_type/REF_RELOAD_TYPE_20151027.txt";  
     val reloadTypeDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.reloadTypeSchema)
                      .load(reloadTypeFile))
      return reloadTypeDF
   }
   
   /**
    * Get record voucher
    */
   def getRecordVoucherTypeDF(sqlContext: SQLContext) : DataFrame = {
     val voucherTypeFile = this.path_ref + "/ref_voucher_type/REF_VOUCHER_TYPE_20151027.txt";  
     val voucherTypeDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.voucherTypeSchema)
                      .load(voucherTypeFile))
      return voucherTypeDF
   }
   
   /**
    * Get record bank name
    */
   def getRecordBankNameDF(sqlContext: SQLContext) : DataFrame = {
     val bankNameFile = this.path_ref + "/ref_reload_channel/REF_RELOAD_CHANNEL_20151027.txt";  
     val bankNameDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.bankNameSchema)
                      .load(bankNameFile))
      return bankNameDF
   }
   
    /**
    * GeoAreaHlr reference 
    */
   def getGeoAreaHlrDF(sqlContext: SQLContext) : DataFrame = {
    
     val geoAreaHlrDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.geoAreaHlrSchema)
                      .load(path_ref + "/ref_geo_area_hlr/SOR.REF_GEO_AREA_HLR_*"))
     return geoAreaHlrDF
    
   } 
   
   /**
    * Get record bank detail 
    */
   def getRecordBankDetailDF(sqlContext: SQLContext) : DataFrame = {
     val bankDetailFile = this.path_ref + "/ref_reload_channel_detail/REF_RELOAD_CHANNEL_DETAIL_20151027.txt";  
     val bankDetailDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.bankDetailSchema)
                      .load(bankDetailFile))
      return bankDetailDF
   }   
   
   /**
    * Microcluster reference
    */
   def getMicroClusterDF(sqlContext: SQLContext) : DataFrame = {
     val microClusterFile = this.path_ref + "/ref_microcluster/NONTDWM.REF_MICROCLUSTER*";
     val microClusterDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter",",")
                      .schema(ReferenceSchema.microclusterSchema)
                      .load(microClusterFile))
      return microClusterDF
   }  
   
   /**
    * MCCMNC reference
    */
   def getMCCMNCDF(sqlContext: SQLContext) : DataFrame = {
     val MCCMNCFile = this.path_ref + "/ref_MCC_MNC/REF_MCC_MNC*";  
     val MCCMNCDF =  broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "false") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.MCCMNCSchema)
                      .load(MCCMNCFile))
      return MCCMNCDF
   }  
   
   /**
    * NON DATAFRAME
    * @returns Reference Corporate Product ID for Usage Type of Postpaid CDR Inquiry
    */
   def getPostCorpProductID(sc: SparkContext) : CS5Reference.postCorpProductID = {
      return new CS5Reference.postCorpProductID(path_ref + "/ref_postpaid_corp_productid/REF_POSTPAID_CORP_PRODUCTID*" , sc)
   }
   
   /**
    * Foss reference
    */
   def getFossDF(sqlContext: SQLContext,str:String) : DataFrame = {
       val fossDF = broadcast(sqlContext.read
                      .format("com.databricks.spark.csv")
                      .option("header", "true") // Use first line of all files as header
                      .option("delimiter","|")
                      .schema(ReferenceSchema.fossSchema)
                      .load(path_ref + "/" +  str))
     return fossDF
  
   }

  def getGroupOfServicesDF(sqlContext: SQLContext, str: String) : DataFrame = {
    return broadcast(sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .schema(ReferenceSchema.refGroupOfServices)
      .load(str)
      .cache())
  }

  def getCustomerGroupDF(sqlContext: SQLContext, str: String) : DataFrame = {
    return broadcast(sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .schema(ReferenceSchema.refCustomerGroup)
      .load(str)
      .cache())
  }
}