package com.ibm.id.isat.IPCNRecon

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.functions._
import com.ibm.id.isat.utils._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date._
import org.apache.spark.sql.types
import org.apache.commons.lang3.time.DateUtils

object SchemaIPCN {
  
   def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "testing spark ", new SparkConf());
   }
   
   val IPCNSchema = StructType(Array(
    StructField("TRIGGER_TIME", StringType, true),
    StructField("TRIGGER_TIME_NORM", StringType, true),
    StructField("SERVED_MSISDN", StringType, true),
    StructField("NODE_NM", StringType, true),
    StructField("ORIGIN_REALM", StringType, true),
     StructField("ORIGIN_HOST", StringType, true),
    StructField("SSP_TRANSACTION_ID", StringType, true),
    StructField("SERVICE_CLASS_ID", StringType, true),
    StructField("AREA_NUM", StringType, true),
    StructField("SERVICE_ID", StringType, true), 
    StructField("A_NUM_CHARGING", StringType, true),
    StructField("OFFER_ID_1)", StringType, true),
    StructField("OFFER_ID_2", StringType, true),
    StructField("OFFER_ID_3", StringType, true),
    StructField("OFFER_ID_4", StringType, true),
     StructField("OFFER_ID_5", StringType, true),
    StructField("ACCOUNT_VALUE_DEDUCTED", StringType, true),
    StructField("DA_ID_1", StringType, true),
    StructField("DA_VALUE_CHANGE_1", StringType, true),
    StructField("DA_UOM_1", StringType, true),
    StructField("DA_ID_2", StringType, true),
    StructField("DA_VALUE_CHANGE_2", StringType, true),
    StructField("DA_UOM_2", StringType, true),
    StructField("DA_ID_3", StringType, true),
    StructField("DA_VALUE_CHANGE_3", StringType, true),
    StructField("DA_UOM_3", StringType, true),
    StructField("DA_ID_4", StringType, true),
    StructField("DA_VALUE_CHANGE_4", StringType, true),
    StructField("DA_UOM_4", StringType, true),
    StructField("DA_ID_5", StringType, true),
    StructField("DA_VALUE_CHANGE_5", StringType, true),
    StructField("DA_UOM_5", StringType, true)
    
  ));
    
    val str2 ="2001||617032000094307000|ccnjkt7|SCAP_V.2.0@ericsson.com|SCAP_V.2.0@ericsson.com|aaa://vasm.ssppadapt4.dmn1.indosat.com:1811;transport=tcp;628578112166820160723000049122;0|de7f3162|||0#6285781121668|##aaa://vasm.ssppadapt4.dmn1.indosat.com:1811;transport=tcp;628578112166820160723000049122;0#0|vasm.ssppadapt4.dmn1.indosat.com#vasm.ssppadapt4.dmn1.indosat.com|6#0******1#20160723000049+0700#20160723000049+0700#2##0#5*20160701T145042000-10.4.72.131-3947843-1465204886, ver: 0*0]Prepaid-SMSMT*20160722T154144000-10.4.72.96-7488715-907173160, ver: 0*0#54~8700.00\6\360~7500.00\6\360~1200.00\6\360~~~20160609~~~~~~~~1~~~~^20~46025.00\6\360~45175.00\6\360~850.00\6\360~~~20160609~~~~~~~~1~~~~#16778218*12345]16778219*123039]16777416*12300001014195]16777616*]16777228*1#AREANAME*JBA10]DWSCallClassCategory*-1####0~0~0~0~1*0~0~0~0~0*0~0~0~0~0######"
    val str1 = "2001||617032000094307000|ccnjkt7|SCAP_V.2.0@ericsson.com|SCAP_V.2.0@ericsson.com|aaa://vasm.ssppadapt4.dmn1.indosat.com:1811;transport=tcp;628578112166820160723000049122;0|de7f3162|||0#6285781121668|##aaa://vasm.ssppadapt4.dmn1.indosat.com:1811;transport=tcp;628578112166820160723000049122;0#0|vasm.ssppadapt4.dmn1.indosat.com#vasm.ssppadapt4.dmn1.indosat.com|6#0******1#20160723000049+0700#20160723000049+0700#2##0#5*20160701T145042000-10.4.72.131-3947843-1465204886, ver: 0*0]Prepaid-SMSMT*20160722T154144000-10.4.72.96-7488715-907173160, ver: 0*0#6285781121668*550*0*2000.00~6~360*250.00~6~360*~~~~62123039*test~test1~test2^test1^test2*test^test1*62123039*4097*1750.00~6~360**1750.00~6~360***0**0**200000100[20000022[200003333[20000400[20000500***#16778218*12345]16778219*123039]16777416*12300001014195]16777616*]16777228*1#AREANAME*JBA10]DWSCallClassCategory*-1####0~0~0~0~1*0~0~0~0~0*0~0~0~0~0######"
    val str = "2001||610101412062307000|OCCSBY10|6.32251@3gpp.org|Ericsson_OCS_V1_0.0.0.7.32251@3gpp.org|23372:-:3|0438d3|0|true|0#628563476312[1#510013460607673|##g1i2.ggsby02hw.gy.ggsn.epc.indosat.id;3685071722;364;122647#|epc.indosat.id#g1i2.ggsby02hw.gy.ggsn.epc.indosat.id|23372#0***0***#20161010140226+0700#20161010140226+0700#2##0#6285781121668*550*0*2000.00~6~360*250.00~6~360*~~~~62123039*test~test1~test2^test1^test2*test^test1*62123039*4097*1750.00~6~360**1750.00~6~360***0**0*200000100*0000500*200000100*200000100*#16778249*51001]16778233*51001]16778232*51001]16778235*indosat.gprs]16778250*07DD]16778234*5]16778237*0]16778252*2679]16778239*51001]16778254*23372]16778238*0800]16778241*0]16778240*1]16778227*0]16778243*8633650232836478]16778226*2760576597]16778229*CA98A9C7]16778245*0]16778228*0A01BC49]16778244*3]16778231*0A04081C]16778247*0]16778230*72000AD3#AREANAME*JTI14]DWSCallClassCategory*1019]DWSChargingCategory*3]DWSRoamingCategory*0]DWSTariffCategory*419####0~0~0~0~0*0~0~0~0~0*0~0~0~0~0######"
    val str3 ="2001||617032000094307000|ccnjkt7|SCAP_V.2.0@ericsson.com|SCAP_V.2.0@ericsson.com|aaa://vasm.ssppadapt4.dmn1.indosat.com:1811;transport=tcp;628578112166820160723000049122;0|de7f3162|||0#6285781121668|##aaa://vasm.ssppadapt4.dmn1.indosat.com:1811;transport=tcp;628578112166820160723000049122;0#0|vasm.ssppadapt4.dmn1.indosat.com#vasm.ssppadapt4.dmn1.indosat.com|6#0******1#20160723000049+0700#20160723000049+0700#2##0#5*20160701T145042000-10.4.72.131-3947843-1465204886, ver: 0*0]Prepaid-SMSMT*20160722T154144000-10.4.72.96-7488715-907173160, ver: 0*0#6285781121668*550*0*2000.00~6~360*250.00~6~360*~~~~62123039*test~test1~test2^test1^test2*54~8700.00\6\360~7500.00\6\360~1200.00\6\360~~~20160609~~~~~~~~1~~~~^20~46025.00\6\360~45175.00\6\360~850.00\6\360~~~20160609~~~~~~~~1~~~~*62123039*4097*1750.00~6~360**1750.00~6~360***0**0**200000100]20000022]200003333]20000400]20000500***#16778218*12345]16778219*123039]16777416*12300001014195]16777616*]16777228*1#AREANAME*JBA10]DWSCallClassCategory*-1####0~0~0~0~1*0~0~0~0~0*0~0~0~0~0######"
    val str4 = "2001||610101412062307000|OCCSBY10|6.32251@3gpp.org|Ericsson_OCS_V1_0.0.0.7.32251@3gpp.org|23372:-:3|0438d3|0|true|0#628563476312[1#510013460607673|##g1i2.ggsby02hw.gy.ggsn.epc.indosat.id;3685071722;364;122647#|epc.indosat.id#g1i2.ggsby02hw.gy.ggsn.epc.indosat.id|23372#0***0***#20161010140226+0700#20161010140226+2200#2##0#6285781121668*550*0*2000.00~6~360*250.00~6~360*~~~~62123039*test~test1~test2^test1^test2*test^test1*62123039*4097*1750.00~6~360**1750.00~6~360***0**0*200000100*0000500*200000100*200000100*#16778249*51001]16778233*51001]16778232*51001]16778235*indosat.gprs]16778250*07DD]16778234*5]16778237*0]16778252*2679]16778239*51001]16778254*23372]16778238*0800]16778241*0]16778240*1]16778227*0]16778243*8633650232836478]16778226*2760576597]16778229*CA98A9C7]16778245*0]16778228*0A01BC49]16778244*1[2[3[4[5*0A04081C]16778247*0]16778230*72000AD3#AREANAME*JTI14]DWSCallClassCategory*1019]DWSChargingCategory*3]DWSRoamingCategory*0]DWSTariffCategory*419####0~0~0~0~0*0~0~0~0~0*0~0~0~0~0######"
 // println(SelectIPCN.getDAID1(str3))
 // println(SelectIPCN.getDAValueChange1(str3))
  //println(SelectIPCN.getDAUOM1(str3))
 // println(SelectIPCN.getDAID2(str3))
 // println(SelectIPCN.getDAValueChange1(str3))
 // println(SelectIPCN.getDAUOM2(str3))
    
   val str4test = str4.replaceAll("\\[", "\\]")
   
   //println (str4test)
    println(SelectIPCN.getOfferId1(str4));
    println(SelectIPCN.getOfferId2(str4))
   println(SelectIPCN.getOfferId3(str4));
   println(SelectIPCN.getOfferId4(str4));
   println(SelectIPCN.getOfferId5(str4));
    
   
}