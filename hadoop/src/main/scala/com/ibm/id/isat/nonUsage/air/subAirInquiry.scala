package com.ibm.id.isat.nonUsage.air

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object subAirInquiry {
  
  def getAirAdjustmentInquiry(sqlContext: SQLContext) : DataFrame = {
      
    val airAdjInqDF = sqlContext.sql(
           """SELECT recordID as KEY,
                accountNumber MSISDN, 
                timeStamp TRANSACTIONTIMESTAMP,
                case
                  when substr(currentServiceclass,1,1) in ('4','7') then 'POSTPAID'
                  else 'PREPAID'
                end as SUBSCRIBERTYPE,
                (transactionAmount/100) as AMOUNT,
                NULL as DA_DETAIL,
                currentServiceclass as CURRENTSERVICECLASS,
                NULL as ACTIVATIONDATE,
                currentServiceclass as SERVICECLASSID,
                svcClassName as SERVICECLASSNAME,
                promo_package_name as PROMOPACKAGENAME,
                brand_name as BRANDNAME ,            
                NULL as NEWSERVICECLASS ,  
                NULL as NEWSERVICECLASSNAME ,  
                NULL as NEWPROMOPACKAGENAME ,  
                NULL as NEWBRANDNAME,
                originNodeType ORIGINNODETYPE,
                originNodeID ORIGINHOSTNAME,
                externalData1 as EXTERNALDATA1,
                null as PROGRAMNAME    ,
                null as PROGRAMOWNER,   
                null as PROGRAMCATEGORY,
                null as BANKNAME,
                null as BANKDETAIL,
                null as MAINACCOUNTBALANCEBEFORE,
                null as MAINACCOUNTBALANCEAFTER,
                null as LAC,
                null as CELLID,
                'Discount' as TRANSACTIONTYPE,
                realFilename FILENAMEINPUT,
                null as VOURCHER_TYPE,
                null as RELOAD_TYPE,
                null as JOBID               
               from ha1_air_adj          
           """)
      
       return airAdjInqDF
  }
 
  
  def getAirRefillInquiry(sqlContext: SQLContext) : DataFrame = {
      
    val airRefillInqDF = sqlContext.sql(
           """SELECT recordID as KEY,
                accountNumber MSISDN, 
                originTimeStamp TRANSACTIONTIMESTAMP,
                case
                  when substr(currentServiceclass,1,1) in ('4','7') then 'POSTPAID'
                  else 'PREPAID'
                end as SUBSCRIBERTYPE,
                (transactionAmount/100) as AMOUNT,
                NULL as DA_DETAIL,
                serviceclassid as CURRENTSERVICECLASS,
                activationDate as ACTIVATIONDATE,
                serviceclassid as SERVICECLASSID,
                svcClassName as SERVICECLASSNAME,
                promo_package_name as PROMOPACKAGENAME,
                brand_name as BRANDNAME ,            
                NULL as NEWSERVICECLASS ,  
                NULL as NEWSERVICECLASSNAME ,  
                NULL as NEWPROMOPACKAGENAME ,  
                NULL as NEWBRANDNAME,
                originNodeType ORIGINNODETYPE,
                originNodeID ORIGINHOSTNAME,
                externalData1 as EXTERNALDATA1,
                null as PROGRAMNAME    ,
                null as PROGRAMOWNER,   
                null as PROGRAMCATEGORY,
                ref.column4 as BANKNAME,
                ref_detail.column2 as BANKDETAIL,
                concat(accountInformationBeforeRefill,tupple1,'#',tupple5) as MAINACCOUNTBALANCEBEFORE,
                concat(accountInformationAfterRefill,tupple1,'#',tupple5) as MAINACCOUNTBALANCEAFTER,
                null as LAC,
                null as CELLID,
                'Refill' as TRANSACTIONTYPE,
                realFilename FILENAMEINPUT,
                case reload_Type.reloadType 
                  when 'IVDB' then 'REGULAR' 
                  when 'IGATE' then 'REGULAR' 
                else 
                  voucher_Type.column9 as VOURCHER_TYPE,
                case reload_Type.reloadType 
                  when 'IVDB' then 'VOUCHER PHYSIC' 
                  when 'IGATE' then 'PARTNER'
                  when 'SEV' then 'ELETRIC' 
                else 
                  case reload_Type.originHostName 
                  when 'ssppadpsvr1' then 'Transfer Pulsa'
               else
                  '' as RELOAD_TYPE,
                null as JOBID               
               from 
               ( select * ha1_sor_air_refill_union ) air  
               left join BankName ref on substr(air.externalData1,7,3) = ref.column1 
               left join BankDetail ref_detail on substr(air.externalData1,10,2) = ref_detail.column1
               left join VoucherType voucher_Type on refillType = column2 and refillProfileID = column3
               left join reloadTypeSchema reload_Type on air.originNodeType = reload_Type.originNodeType and air.originNodeID = reload_Type.originHostName                           
           """)
      
       return airRefillInqDF
  }
 
  
}