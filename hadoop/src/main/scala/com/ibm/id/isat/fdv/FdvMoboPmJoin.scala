package com.ibm.id.isat.fdv

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import com.ibm.id.isat.utils.Common
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

object FdvMoboPmJoin {
  def main(args: Array[String]): Unit = {
    val (prcDt, jobId, configDir, env, inputDir, targetDir) = try {
      (args(0), args(1), args(2), args(3), args(4), args(5))
    } catch {
      case e: ArrayIndexOutOfBoundsException => println("Usage: <process month> <config directory>  <LOCAL|PRODUCTION>")
      return
    };
    
    // Initialize Spark Context
    val sc = env match {
      case "PRODUCTION" => new SparkContext(new SparkConf())
      case "LOCAL" => new SparkContext("local[*]", "local spark", new SparkConf())
      case _ => return
    };
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false"); //Disable metadata parquet
 
 
    // Initialize Spark SQL
    val sqlContext = new HiveContext(sc)
    
    // Initialize Logging
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.INFO)
    
    //input
    val saldomobo_voucher_injection  = Common.getConfigDirectory(sc, configDir, "FDV.INPUT.SALDOMOBO_VOUCHER_INJECTION")
    val paymobile_vo_used            = Common.getConfigDirectory(sc, configDir, "FDV.INPUT.PAYMOBILE_VO_USED")
    val paymobile_phyvodump          = Common.getConfigDirectory(sc, configDir, "FDV.INPUT.PAYMOBILE_PHYVODUMP")
    //output
    val fdvMoboPmJoin                = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_MOBO_PM_JOIN")
    val fdvInjection                 = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_INJECTION")
    val fdvVoUsed                    = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_VO_USED")
    val fdvPhyvodump                 = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_PHYVODUMP")
    val fdvDetailData                = Common.getConfigDirectory(sc, configDir, "FDV.OUTPUT.FDV_DETAIL_DATA")
 
     
     
     sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter","|")
      .option("header","true")
      .schema(StructType(Array(
             StructField("SaldomoboTransactionID",StringType,true),
             StructField("TransactionDateTime",StringType,true),
             StructField("VoucherSerialNumber",StringType,true),
             StructField("DealerID",StringType,true),
             StructField("DealerName",StringType,true),
             StructField("OutletID",StringType,true),
             StructField("OuletName",StringType,true),
             StructField("OutletArea",StringType,true),
             StructField("VoucherExpiredDate",StringType,true),
             StructField("InjectedStatus",StringType,true),
             StructField("InjectedPackageID",StringType,true),
             StructField("InjectedPackageName",StringType,true),
             StructField("InjectedSalesPriceAmount",StringType,true),
             StructField("InjectedMainPrice",StringType,true),
             StructField("Discount",StringType,true)
        )))
      .load(saldomobo_voucher_injection).registerTempTable("SALDOMOBO_VOUCHER_INJECTION")
      
      val fdvInjectionDf = sqlContext.sql("""select *,"""+prcDt+""" PRC_DT  from SALDOMOBO_VOUCHER_INJECTION""")  
      Common.cleanDirectory(sc, fdvInjection + "/PRC_DT=" + prcDt)
      fdvInjectionDf.repartition(10)
      .write
      .mode("append")
      .save(fdvInjection + "/PRC_DT=" + prcDt)
         
      sqlContext.read.format("com.databricks.spark.csv")  
        .option("delimiter",";")
        .option("header","true")
        .schema(StructType(Array(
             StructField("SerialNo",StringType,true),
             StructField("RechargeValue",StringType,true),
             StructField("RechargePeriod",StringType,true),
             StructField("ExpirationDate",StringType,true),
             StructField("VoucherTypeID",StringType,true),
             StructField("PackageID",StringType,true),
             StructField("DealerID",StringType,true),
             StructField("VoucherState",StringType,true),
             StructField("StateChangeUser",StringType,true),
             StructField("MSISDN",StringType,true),
             StructField("ProviderID",StringType,true),
             StructField("VoucherProviderID",StringType,true),
             StructField("DisabledDate",StringType,true),
             StructField("DeliveredDate",StringType,true),
             StructField("EnabledDate",StringType,true),
             StructField("UsageReqDate",StringType,true),
             StructField("UsedDate",StringType,true),
             StructField("BlockedDate",StringType,true),
             StructField("DeletedDate",StringType,true)
        )))
        .load(paymobile_vo_used).registerTempTable("PAYMOBILE_VO_USED")
        
      val fdvVoUsedDf = sqlContext.sql("""select *,"""+prcDt+""" PRC_DT from PAYMOBILE_VO_USED""") 
      Common.cleanDirectory(sc, fdvVoUsed + "/PRC_DT=" + prcDt)
      fdvVoUsedDf.repartition(10)
      .write.partitionBy("PRC_DT")
      .mode("append")
      .save(fdvVoUsed + "/PRC_DT=" + prcDt)  
      
     
      sqlContext.read.format("com.databricks.spark.csv")
      .option("header","false")
      .option("delimiter",",")
      .schema(StructType(Array(
             StructField("SNE",StringType,true),
             StructField("voucherProviderId",StringType,true),
             StructField("dealerId",StringType,true),
             StructField("voucherTypeId",StringType,true),
             StructField("rechargeValue",StringType,true),
             StructField("disabledDate",StringType,true),
             StructField("enabledDate",StringType,true),
             StructField("expirationDate",StringType,true),
             StructField("voucherState",StringType,true),
             StructField("UsedDate",StringType,true)
       )))
       .load(paymobile_phyvodump).registerTempTable("PAYMOBILE_PHYVODUMP")
       
      val fdvPhyvodumpDf = sqlContext.sql("""select *,"""+prcDt+""" PRC_DT from PAYMOBILE_PHYVODUMP""") 
      Common.cleanDirectory(sc, fdvPhyvodump + "/PRC_DT=" + prcDt)
      fdvPhyvodumpDf.repartition(10)
      .write.partitionBy("PRC_DT")
      .mode("append")
      .save(fdvPhyvodump + "/PRC_DT=" + prcDt)  
      
      sqlContext.sql(""" SELECT * FROM (
                        SELECT 
                              SerialNo PM_VOUCHER_SN, 
                              RechargeValue PM_RECHARGE_VAL,
                              date_format(concat(substr(ExpirationDate,0,4),'-',substr(ExpirationDate,6,2),'-',substr(ExpirationDate,9,2)),'yyyy-MM-dd') PM_VOUCHER_EXP_DTTM,
                              VoucherTypeID PM_VOUCHER_GROUP_ID, 
                              VoucherState PM_VOUCHER_STATUS, 
                              MSISDN PM_RED_MSISDN, 
                              date_format(concat(substr(UsedDate,0,4),'-',substr(UsedDate,6,2),'-',substr(UsedDate,9,2)),'yyyy-MM-dd') PM_VOUCHER_RED_DTTM
                        FROM PAYMOBILE_VO_USED where VoucherTypeID like '99%' and VoucherState = 'U'
                        UNION ALL
                        SELECT SNE PM_VOUCHER_SN, 
                              RechargeValue PM_RECHARGE_VAL,
                              date_format(concat(substr(ExpirationDate,0,4),'-',substr(ExpirationDate,6,2),'-',substr(ExpirationDate,9,2)),'yyyy-MM-dd') PM_VOUCHER_EXP_DTTM,
                              VoucherTypeID PM_VOUCHER_GROUP_ID, 
                              VoucherState PM_VOUCHER_STATUS, 
                              null PM_RED_MSISDN, 
                              date_format(concat(substr(UsedDate,0,4),'-',substr(UsedDate,6,2),'-',substr(UsedDate,9,2)),'yyyy-MM-dd')
                       FROM PAYMOBILE_PHYVODUMP where VoucherTypeID like '99%' and VoucherState in ('E','X')) AS a ORDER BY PM_VOUCHER_STATUS,PM_VOUCHER_RED_DTTM ASC
      """).registerTempTable("PAYMOBILE_UNION_STATUS")
     
      val fdvMoboPmJoinDf = sqlContext.sql(""" 
                                              SELECT a.*,b.*,
                                              """+prcDt+""" PRC_DT 
                                              FROM PAYMOBILE_UNION_STATUS a
                                              FULL OUTER JOIN
                                              (SELECT 
                                              SaldomoboTransactionID MOBO_TRX_ID,
                                              date_format(substr(TransactionDateTime,0,10),'yyyy-MM-dd') MOBO_TRX_DTTM,
                                              VoucherSerialNumber MOBO_VOUCHER_SN,
                                              DealerID MOBO_INJ_DEALER_ID,
                                              DealerName MOBO_INJ_DEALER_NAME,
                                              OutletID MOBO_INJ_OUTLET_ID,
                                              OuletName MOBO_INJ_OUTLET_NAME,
                                              OutletArea MOBO_INJ_OUTLET_AREA,
                                              date_format(substr(VoucherExpiredDate,0,8),'yyyy-MM-dd') MOBO_VOUCHER_EXP_DT,
                                              InjectedStatus MOBO_INJ_STATUS,
                                              InjectedPackageID MOBO_INJ_PACK_ID,
                                              InjectedPackageName MOBO_INJ_PACK_NAME,
                                              cast(InjectedSalesPriceAmount/100 as bigint) MOBO_INJ_SALES_PRICE,
                                              cast(InjectedMainPrice/100 as bigint) MOBO_INJ_MAIN_PRICE,
                                              cast(Discount/100 as bigint) MOBO_INJ_DISC_PRICE
                                              FROM SALDOMOBO_VOUCHER_INJECTION) b on a.PM_VOUCHER_SN = b.MOBO_VOUCHER_SN 
                                          """)
     
      Common.cleanDirectory(sc, fdvMoboPmJoin + "/PRC_DT=" + prcDt)
      fdvMoboPmJoinDf.repartition(10)  
      .write.partitionBy("PRC_DT")
      .mode("append")
      .save(fdvMoboPmJoin)
    
      sqlContext.read.parquet(fdvMoboPmJoin+"/PRC_DT="+prcDt).registerTempTable("FDV_MOBO_PM_JOIN")
      sqlContext.sql("""select a.*,"""+prcDt+""" PRC_DT  from FDV_MOBO_PM_JOIN a""")
      .repartition(1)
      .write.mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("delimiter","|")
      .save(fdvDetailData+"/PRC_DT="+prcDt)
    
    
  }
}