package com.ibm.id.isat.utils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._

object ReferenceQuery {
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext("local", "testing spark ", new SparkConf());
   // val inputRDD = sc.textFile("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/sample_conv_output_CS3.txt");
    
    println("Reference");
  }
  val queryGprsMoSmy ="""
    SELECT 
            a_party_no msisdn
           ,COALESCE(lac,0) lac
           ,COALESCE(ci,0) ci 
           ,sum(COALESCE(g_total_chg,0)) g_total_chg
           ,sum(COALESCE(ppu_datavol,0)) ppu_datavol
           ,sum(COALESCE(addon_datavol,0)) addon_datavol
           ,sum(COALESCE(total_datavol,0)) total_datavol
           ,sum(case when rat_tp_id = 6 then g_total_chg else 0 end) g_4g_total_chg
           ,sum(case when rat_tp_id = 1 then g_total_chg else 0 end) g_3g_total_chg
           ,sum(case when rat_tp_id = 2 then g_total_chg else 0 end) g_2g_total_chg
           ,sum(case when rat_tp_id = 6 then ppu_datavol else 0 end) g_4g_ppu_datavol
           ,sum(case when rat_tp_id = 1 then ppu_datavol else 0 end) g_3g_ppu_datavol
           ,sum(case when rat_tp_id = 2 then ppu_datavol else 0 end) g_2g_ppu_datavol
           ,sum(case when rat_tp_id = 6 then addon_datavol else 0 end) g_4g_addon_datavol
           ,sum(case when rat_tp_id = 1 then addon_datavol else 0 end) g_3g_addon_datavol
           ,sum(case when rat_tp_id = 2 then addon_datavol else 0 end) g_2g_addon_datavol
           ,sum(COALESCE(g_total_chg_00,0) + COALESCE(g_total_chg_01,0) + COALESCE(g_total_chg_02,0) + COALESCE(g_total_chg_03,0) + COALESCE(g_total_chg_04,0) + COALESCE(g_total_chg_05,0) + COALESCE(g_total_chg_06,0) + COALESCE(g_total_chg_07,0) + COALESCE(g_total_chg_08,0) + COALESCE(g_total_chg_09,0) + COALESCE(g_total_chg_10,0) + COALESCE(g_total_chg_11,0) + COALESCE(g_total_chg_12,0) + COALESCE(g_total_chg_13,0) + COALESCE(g_total_chg_14,0) + COALESCE(g_total_chg_15,0) + COALESCE(g_total_chg_16,0)) g_offpeak_chg
           ,sum(COALESCE(g_total_chg_17,0) + COALESCE(g_total_chg_18,0) + COALESCE(g_total_chg_19,0) + COALESCE(g_total_chg_20,0) + COALESCE(g_total_chg_21,0) + COALESCE(g_total_chg_22,0) + COALESCE(g_total_chg_23,0)) g_peak_chg
           ,sum(COALESCE(ppu_datavol_00,0) + COALESCE(ppu_datavol_01,0) + COALESCE(ppu_datavol_02,0) + COALESCE(ppu_datavol_03,0) + COALESCE(ppu_datavol_04,0) + COALESCE(ppu_datavol_05,0) + COALESCE(ppu_datavol_06,0) + COALESCE(ppu_datavol_07,0) + COALESCE(ppu_datavol_08,0) + COALESCE(ppu_datavol_09,0) + COALESCE(ppu_datavol_10,0) + COALESCE(ppu_datavol_11,0) + COALESCE(ppu_datavol_12,0) + COALESCE(ppu_datavol_13,0) + COALESCE(ppu_datavol_14,0) + COALESCE(ppu_datavol_15,0) + COALESCE(ppu_datavol_16,0)) ppu_offpeak_datavol
           ,sum(COALESCE(ppu_datavol_17,0) + COALESCE(ppu_datavol_18,0) + COALESCE(ppu_datavol_19,0) + COALESCE(ppu_datavol_20,0) + COALESCE(ppu_datavol_21,0) + COALESCE(ppu_datavol_22,0) + COALESCE(ppu_datavol_23,0)) ppu_peak_datavol
           ,sum(COALESCE(addon_datavol_00,0) + COALESCE(addon_datavol_01,0) + COALESCE(addon_datavol_02,0) + COALESCE(addon_datavol_03,0) + COALESCE(addon_datavol_04,0) + COALESCE(addon_datavol_05,0) + COALESCE(addon_datavol_06,0) + COALESCE(addon_datavol_07,0) + COALESCE(addon_datavol_08,0) + COALESCE(addon_datavol_09,0) + COALESCE(addon_datavol_10,0) + COALESCE(addon_datavol_11,0) + COALESCE(addon_datavol_12,0) + COALESCE(addon_datavol_13,0) + COALESCE(addon_datavol_14,0) + COALESCE(addon_datavol_15,0) + COALESCE(addon_datavol_16,0)) addon_offpeak_datavol
           ,sum(COALESCE(addon_datavol_17,0) + COALESCE(addon_datavol_18,0) + COALESCE(addon_datavol_19,0) + COALESCE(addon_datavol_20,0) + COALESCE(addon_datavol_21,0) + COALESCE(addon_datavol_22,0) + COALESCE(addon_datavol_23,0)) addon_peak_datavol
           ,sum(COALESCE(total_datavol_00,0) + COALESCE(total_datavol_01,0) + COALESCE(total_datavol_02,0) + COALESCE(total_datavol_03,0) + COALESCE(total_datavol_04,0) + COALESCE(total_datavol_05,0) + COALESCE(total_datavol_06,0) + COALESCE(total_datavol_07,0) + COALESCE(total_datavol_08,0) + COALESCE(total_datavol_09,0) + COALESCE(total_datavol_10,0) + COALESCE(total_datavol_11,0) + COALESCE(total_datavol_12,0) + COALESCE(total_datavol_13,0) + COALESCE(total_datavol_14,0) + COALESCE(total_datavol_15,0) + COALESCE(total_datavol_16,0)) total_offpeak_datavol
           ,sum(COALESCE(total_datavol_17,0) + COALESCE(total_datavol_18,0) + COALESCE(total_datavol_19,0) + COALESCE(total_datavol_20,0) + COALESCE(total_datavol_21,0) + COALESCE(total_datavol_22,0) + COALESCE(total_datavol_23,0)) total_peak_datavol
           ,%1$s MONTH_ID
    FROM 
       USG_GPRS_DLY_LACCI
    WHERE LEVEL_7='DATA'
    GROUP BY 
        a_party_no
        , COALESCE(lac, 0), COALESCE(ci, 0)

"""
  val queryVoiceMoSmy ="""
    SELECT 
      a_party_no msisdn,
      COALESCE(lac,0) lac,
      COALESCE(ci,0) ci,
      SUM(COALESCE(v_total_chg,0)) v_total_chg,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(v_total_chg,0) ELSE 0 END) v_offnet_chg,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(v_total_chg,0) ELSE 0 END) v_onnet_chg,
      SUM(COALESCE(billed_dur,0)) billed_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(billed_dur,0) ELSE 0 END) billed_offnet_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(billed_dur,0) ELSE 0 END) billed_onnet_dur,
      SUM(COALESCE(billed_promo_dur,0)) promo_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(billed_promo_dur,0) ELSE 0 END) promo_offnet_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(billed_promo_dur,0) ELSE 0 END) promo_onnet_dur,
      SUM(COALESCE(tot_dur,0)) tot_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(tot_dur,0) ELSE 0 END) v_offnet_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(tot_dur,0) ELSE 0 END) v_onnet_dur,
      SUM(COALESCE(v_total_chg_00,0) + COALESCE(v_total_chg_01,0) + COALESCE(v_total_chg_02,0) + COALESCE(v_total_chg_03,0) + COALESCE(v_total_chg_04,0) + COALESCE(v_total_chg_05,0) + COALESCE(v_total_chg_06,0) + COALESCE(v_total_chg_07,0) + COALESCE(v_total_chg_08,0) + COALESCE(v_total_chg_09,0) + COALESCE(v_total_chg_10,0) + COALESCE(v_total_chg_11,0) + COALESCE(v_total_chg_12,0) + COALESCE(v_total_chg_13,0) + COALESCE(v_total_chg_14,0) + COALESCE(v_total_chg_15,0) + COALESCE(v_total_chg_16,0)) v_offpeak_chg,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(v_total_chg_00,0) + COALESCE(v_total_chg_01,0) + COALESCE(v_total_chg_02,0) + COALESCE(v_total_chg_03,0) + COALESCE(v_total_chg_04,0) + COALESCE(v_total_chg_05,0) + COALESCE(v_total_chg_06,0) + COALESCE(v_total_chg_07,0) + COALESCE(v_total_chg_08,0) + COALESCE(v_total_chg_09,0) + COALESCE(v_total_chg_10,0) + COALESCE(v_total_chg_11,0) + COALESCE(v_total_chg_12,0) + COALESCE(v_total_chg_13,0) + COALESCE(v_total_chg_14,0) + COALESCE(v_total_chg_15,0) + COALESCE(v_total_chg_16,0) ELSE 0 END) v_offnet_offpeak_chg,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(v_total_chg_00,0) + COALESCE(v_total_chg_01,0) + COALESCE(v_total_chg_02,0) + COALESCE(v_total_chg_03,0) + COALESCE(v_total_chg_04,0) + COALESCE(v_total_chg_05,0) + COALESCE(v_total_chg_06,0) + COALESCE(v_total_chg_07,0) + COALESCE(v_total_chg_08,0) + COALESCE(v_total_chg_09,0) + COALESCE(v_total_chg_10,0) + COALESCE(v_total_chg_11,0) + COALESCE(v_total_chg_12,0) + COALESCE(v_total_chg_13,0) + COALESCE(v_total_chg_14,0) + COALESCE(v_total_chg_15,0) + COALESCE(v_total_chg_16,0) ELSE 0 END) v_onnet_offpeak_chg,
      SUM(COALESCE(billed_dur_00,0) + COALESCE(billed_dur_01,0) + COALESCE(billed_dur_02,0) + COALESCE(billed_dur_03,0) + COALESCE(billed_dur_04,0) + COALESCE(billed_dur_05,0) + COALESCE(billed_dur_06,0) + COALESCE(billed_dur_07,0) + COALESCE(billed_dur_08,0) + COALESCE(billed_dur_09,0) + COALESCE(billed_dur_10,0) + COALESCE(billed_dur_11,0) + COALESCE(billed_dur_12,0) + COALESCE(billed_dur_13,0) + COALESCE(billed_dur_14,0) + COALESCE(billed_dur_15,0) + COALESCE(billed_dur_16,0)) billed_offpeak_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(billed_dur_00,0) + COALESCE(billed_dur_01,0) + COALESCE(billed_dur_02,0) + COALESCE(billed_dur_03,0) + COALESCE(billed_dur_04,0) + COALESCE(billed_dur_05,0) + COALESCE(billed_dur_06,0) + COALESCE(billed_dur_07,0) + COALESCE(billed_dur_08,0) + COALESCE(billed_dur_09,0) + COALESCE(billed_dur_10,0) + COALESCE(billed_dur_11,0) + COALESCE(billed_dur_12,0) + COALESCE(billed_dur_13,0) + COALESCE(billed_dur_14,0) + COALESCE(billed_dur_15,0) + COALESCE(billed_dur_16,0) ELSE 0 END) billed_offnet_offpeak_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(billed_dur_00,0) + COALESCE(billed_dur_01,0) + COALESCE(billed_dur_02,0) + COALESCE(billed_dur_03,0) + COALESCE(billed_dur_04,0) + COALESCE(billed_dur_05,0) + COALESCE(billed_dur_06,0) + COALESCE(billed_dur_07,0) + COALESCE(billed_dur_08,0) + COALESCE(billed_dur_09,0) + COALESCE(billed_dur_10,0) + COALESCE(billed_dur_11,0) + COALESCE(billed_dur_12,0) + COALESCE(billed_dur_13,0) + COALESCE(billed_dur_14,0) + COALESCE(billed_dur_15,0) + COALESCE(billed_dur_16,0) ELSE 0 END) billed_onnet_offpeak_dur,
      SUM(COALESCE(billed_promo_dur_00,0) + COALESCE(billed_promo_dur_01,0) + COALESCE(billed_promo_dur_02,0) + COALESCE(billed_promo_dur_03,0) + COALESCE(billed_promo_dur_04,0) + COALESCE(billed_promo_dur_05,0) + COALESCE(billed_promo_dur_06,0) + COALESCE(billed_promo_dur_07,0) + COALESCE(billed_promo_dur_08,0) + COALESCE(billed_promo_dur_09,0) + COALESCE(billed_promo_dur_10,0) + COALESCE(billed_promo_dur_11,0) + COALESCE(billed_promo_dur_12,0) + COALESCE(billed_promo_dur_13,0) + COALESCE(billed_promo_dur_14,0) + COALESCE(billed_promo_dur_15,0) + COALESCE(billed_promo_dur_16,0)) promo_offpeak_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(billed_promo_dur_00,0) + COALESCE(billed_promo_dur_01,0) + COALESCE(billed_promo_dur_02,0) + COALESCE(billed_promo_dur_03,0) + COALESCE(billed_promo_dur_04,0) + COALESCE(billed_promo_dur_05,0) + COALESCE(billed_promo_dur_06,0) + COALESCE(billed_promo_dur_07,0) + COALESCE(billed_promo_dur_08,0) + COALESCE(billed_promo_dur_09,0) + COALESCE(billed_promo_dur_10,0) + COALESCE(billed_promo_dur_11,0) + COALESCE(billed_promo_dur_12,0) + COALESCE(billed_promo_dur_13,0) + COALESCE(billed_promo_dur_14,0) + COALESCE(billed_promo_dur_15,0) + COALESCE(billed_promo_dur_16,0) ELSE 0 END) promo_offnet_offpeak_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(billed_promo_dur_00,0) + COALESCE(billed_promo_dur_01,0) + COALESCE(billed_promo_dur_02,0) + COALESCE(billed_promo_dur_03,0) + COALESCE(billed_promo_dur_04,0) + COALESCE(billed_promo_dur_05,0) + COALESCE(billed_promo_dur_06,0) + COALESCE(billed_promo_dur_07,0) + COALESCE(billed_promo_dur_08,0) + COALESCE(billed_promo_dur_09,0) + COALESCE(billed_promo_dur_10,0) + COALESCE(billed_promo_dur_11,0) + COALESCE(billed_promo_dur_12,0) + COALESCE(billed_promo_dur_13,0) + COALESCE(billed_promo_dur_14,0) + COALESCE(billed_promo_dur_15,0) + COALESCE(billed_promo_dur_16,0) ELSE 0 END) promo_onnet_offpeak_dur,
      SUM(COALESCE(tot_dur_00,0) + COALESCE(tot_dur_01,0) + COALESCE(tot_dur_02,0) + COALESCE(tot_dur_03,0) + COALESCE(tot_dur_04,0) + COALESCE(tot_dur_05,0) + COALESCE(tot_dur_06,0) + COALESCE(tot_dur_07,0) + COALESCE(tot_dur_08,0) + COALESCE(tot_dur_09,0) + COALESCE(tot_dur_10,0) + COALESCE(tot_dur_11,0) + COALESCE(tot_dur_12,0) + COALESCE(tot_dur_13,0) + COALESCE(tot_dur_14,0) + COALESCE(tot_dur_15,0) + COALESCE(tot_dur_16,0)) total_offpeak_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(tot_dur_00,0) + COALESCE(tot_dur_01,0) + COALESCE(tot_dur_02,0) + COALESCE(tot_dur_03,0) + COALESCE(tot_dur_04,0) + COALESCE(tot_dur_05,0) + COALESCE(tot_dur_06,0) + COALESCE(tot_dur_07,0) + COALESCE(tot_dur_08,0) + COALESCE(tot_dur_09,0) + COALESCE(tot_dur_10,0) + COALESCE(tot_dur_11,0) + COALESCE(tot_dur_12,0) + COALESCE(tot_dur_13,0) + COALESCE(tot_dur_14,0) + COALESCE(tot_dur_15,0) + COALESCE(tot_dur_16,0) ELSE 0 END) v_offnet_offpeak_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(tot_dur_00,0) + COALESCE(tot_dur_01,0) + COALESCE(tot_dur_02,0) + COALESCE(tot_dur_03,0) + COALESCE(tot_dur_04,0) + COALESCE(tot_dur_05,0) + COALESCE(tot_dur_06,0) + COALESCE(tot_dur_07,0) + COALESCE(tot_dur_08,0) + COALESCE(tot_dur_09,0) + COALESCE(tot_dur_10,0) + COALESCE(tot_dur_11,0) + COALESCE(tot_dur_12,0) + COALESCE(tot_dur_13,0) + COALESCE(tot_dur_14,0) + COALESCE(tot_dur_15,0) + COALESCE(tot_dur_16,0) ELSE 0 END) v_onnet_offpeak_dur,
      SUM(COALESCE(v_total_chg_17,0) + COALESCE(v_total_chg_18,0) + COALESCE(v_total_chg_19,0) + COALESCE(v_total_chg_20,0) + COALESCE(v_total_chg_21,0) + COALESCE(v_total_chg_22,0) + COALESCE(v_total_chg_23,0)) v_peak_chg,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(v_total_chg_17,0) + COALESCE(v_total_chg_18,0) + COALESCE(v_total_chg_19,0) + COALESCE(v_total_chg_20,0) + COALESCE(v_total_chg_21,0) + COALESCE(v_total_chg_22,0) + COALESCE(v_total_chg_23,0) ELSE 0 END) v_offnet_peak_chg,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(v_total_chg_17,0) + COALESCE(v_total_chg_18,0) + COALESCE(v_total_chg_19,0) + COALESCE(v_total_chg_20,0) + COALESCE(v_total_chg_21,0) + COALESCE(v_total_chg_22,0) + COALESCE(v_total_chg_23,0) ELSE 0 END) v_onnet_peak_chg,
      SUM(COALESCE(billed_dur_17,0) + COALESCE(billed_dur_18,0) + COALESCE(billed_dur_19,0) + COALESCE(billed_dur_20,0) + COALESCE(billed_dur_21,0) + COALESCE(billed_dur_22,0) + COALESCE(billed_dur_23,0)) billed_peak_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(billed_dur_17,0) + COALESCE(billed_dur_18,0) + COALESCE(billed_dur_19,0) + COALESCE(billed_dur_20,0) + COALESCE(billed_dur_21,0) + COALESCE(billed_dur_22,0) + COALESCE(billed_dur_23,0) ELSE 0 END) billed_offnet_peak_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(billed_dur_17,0) + COALESCE(billed_dur_18,0) + COALESCE(billed_dur_19,0) + COALESCE(billed_dur_20,0) + COALESCE(billed_dur_21,0) + COALESCE(billed_dur_22,0) + COALESCE(billed_dur_23,0) ELSE 0 END) billed_onnet_peak_dur,
      SUM(COALESCE(billed_promo_dur_17,0) + COALESCE(billed_promo_dur_18,0) + COALESCE(billed_promo_dur_19,0) + COALESCE(billed_promo_dur_20,0) + COALESCE(billed_promo_dur_21,0) + COALESCE(billed_promo_dur_22,0) + COALESCE(billed_promo_dur_23,0)) promo_peak_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(billed_promo_dur_17,0) + COALESCE(billed_promo_dur_18,0) + COALESCE(billed_promo_dur_19,0) + COALESCE(billed_promo_dur_20,0) + COALESCE(billed_promo_dur_21,0) + COALESCE(billed_promo_dur_22,0) + COALESCE(billed_promo_dur_23,0) ELSE 0 END) promo_offnet_peak_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(billed_promo_dur_17,0) + COALESCE(billed_promo_dur_18,0) + COALESCE(billed_promo_dur_19,0) + COALESCE(billed_promo_dur_20,0) + COALESCE(billed_promo_dur_21,0) + COALESCE(billed_promo_dur_22,0) + COALESCE(billed_promo_dur_23,0) ELSE 0 END) promo_onnet_peak_dur,
      SUM(COALESCE(tot_dur_17,0) + COALESCE(tot_dur_18,0) + COALESCE(tot_dur_19,0) + COALESCE(tot_dur_20,0) + COALESCE(tot_dur_21,0) + COALESCE(tot_dur_22,0) + COALESCE(tot_dur_23,0)) total_peak_dur,
      SUM(CASE WHEN lower(typ) = 'offnet' THEN COALESCE(tot_dur_17,0) + COALESCE(tot_dur_18,0) + COALESCE(tot_dur_19,0) + COALESCE(tot_dur_20,0) + COALESCE(tot_dur_21,0) + COALESCE(tot_dur_22,0) + COALESCE(tot_dur_23,0) ELSE 0 END) v_offnet_peak_dur,
      SUM(CASE WHEN lower(typ) = 'onnet' THEN COALESCE(tot_dur_17,0) + COALESCE(tot_dur_18,0) + COALESCE(tot_dur_19,0) + COALESCE(tot_dur_20,0) + COALESCE(tot_dur_21,0) + COALESCE(tot_dur_22,0) + COALESCE(tot_dur_23,0) ELSE 0 END) v_onnet_peak_dur
      , %1$s MONTH_ID
    FROM USG_VOICE_DLY_LACCI
    WHERE LEVEL_7='VOICE'
    GROUP BY 
      a_party_no
      , COALESCE(lac, 0), COALESCE(ci, 0)
"""
  val querySmsMoSmy ="""
    SELECT 
        a_party_no msisdn,
        COALESCE(lac,0) lac,
        COALESCE(ci,0) ci,
        sum(COALESCE(s_total_chg,0)) s_total_chg,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(s_total_chg,0) else 0 end) s_OFFNET_chg,
        sum(case when upper(typ) = 'ONNET' then COALESCE(s_total_chg,0) else 0 end) s_ONNET_chg,
        sum(COALESCE(billed_sms,0)) billed_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_sms,0) else 0 end) billed_OFFNET_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_sms,0) else 0 end) billed_ONNET_sms,
        sum(COALESCE(billed_promo_sms,0)) promo_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_promo_sms,0) else 0 end) promo_OFFNET_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_promo_sms,0) else 0 end) promo_ONNET_sms,
        sum(COALESCE(s_total_chg_00,0) + COALESCE(s_total_chg_01,0) + COALESCE(s_total_chg_02,0) + COALESCE(s_total_chg_03,0) + COALESCE(s_total_chg_04,0) + COALESCE(s_total_chg_05,0) + COALESCE(s_total_chg_06,0) + COALESCE(s_total_chg_07,0) + COALESCE(s_total_chg_08,0) + COALESCE(s_total_chg_09,0) + COALESCE(s_total_chg_10,0) + COALESCE(s_total_chg_11,0) + COALESCE(s_total_chg_12,0) + COALESCE(s_total_chg_13,0) + COALESCE(s_total_chg_14,0) + COALESCE(s_total_chg_15,0) + COALESCE(s_total_chg_16,0)) s_offpeak_chg,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(s_total_chg_00,0) + COALESCE(s_total_chg_01,0) + COALESCE(s_total_chg_02,0) + COALESCE(s_total_chg_03,0) + COALESCE(s_total_chg_04,0) + COALESCE(s_total_chg_05,0) + COALESCE(s_total_chg_06,0) + COALESCE(s_total_chg_07,0) + COALESCE(s_total_chg_08,0) + COALESCE(s_total_chg_09,0) + COALESCE(s_total_chg_10,0) + COALESCE(s_total_chg_11,0) + COALESCE(s_total_chg_12,0) + COALESCE(s_total_chg_13,0) + COALESCE(s_total_chg_14,0) + COALESCE(s_total_chg_15,0) + COALESCE(s_total_chg_16,0) else 0 end) s_OFFNET_offpeak_chg,
        sum(case when upper(typ) = 'ONNET' then COALESCE(s_total_chg_00,0) + COALESCE(s_total_chg_01,0) + COALESCE(s_total_chg_02,0) + COALESCE(s_total_chg_03,0) + COALESCE(s_total_chg_04,0) + COALESCE(s_total_chg_05,0) + COALESCE(s_total_chg_06,0) + COALESCE(s_total_chg_07,0) + COALESCE(s_total_chg_08,0) + COALESCE(s_total_chg_09,0) + COALESCE(s_total_chg_10,0) + COALESCE(s_total_chg_11,0) + COALESCE(s_total_chg_12,0) + COALESCE(s_total_chg_13,0) + COALESCE(s_total_chg_14,0) + COALESCE(s_total_chg_15,0) + COALESCE(s_total_chg_16,0) else 0 end) s_ONNET_offpeak_chg,
        sum(COALESCE(billed_sms_00,0) + COALESCE(billed_sms_01,0) + COALESCE(billed_sms_02,0) + COALESCE(billed_sms_03,0) + COALESCE(billed_sms_04,0) + COALESCE(billed_sms_05,0) + COALESCE(billed_sms_06,0) + COALESCE(billed_sms_07,0) + COALESCE(billed_sms_08,0) + COALESCE(billed_sms_09,0) + COALESCE(billed_sms_10,0) + COALESCE(billed_sms_11,0) + COALESCE(billed_sms_12,0) + COALESCE(billed_sms_13,0) + COALESCE(billed_sms_14,0) + COALESCE(billed_sms_15,0) + COALESCE(billed_sms_16,0)) billed_offpeak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_sms_00,0) + COALESCE(billed_sms_01,0) + COALESCE(billed_sms_02,0) + COALESCE(billed_sms_03,0) + COALESCE(billed_sms_04,0) + COALESCE(billed_sms_05,0) + COALESCE(billed_sms_06,0) + COALESCE(billed_sms_07,0) + COALESCE(billed_sms_08,0) + COALESCE(billed_sms_09,0) + COALESCE(billed_sms_10,0) + COALESCE(billed_sms_11,0) + COALESCE(billed_sms_12,0) + COALESCE(billed_sms_13,0) + COALESCE(billed_sms_14,0) + COALESCE(billed_sms_15,0) + COALESCE(billed_sms_16,0) else 0 end) billed_OFFNET_offpeak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_sms_00,0) + COALESCE(billed_sms_01,0) + COALESCE(billed_sms_02,0) + COALESCE(billed_sms_03,0) + COALESCE(billed_sms_04,0) + COALESCE(billed_sms_05,0) + COALESCE(billed_sms_06,0) + COALESCE(billed_sms_07,0) + COALESCE(billed_sms_08,0) + COALESCE(billed_sms_09,0) + COALESCE(billed_sms_10,0) + COALESCE(billed_sms_11,0) + COALESCE(billed_sms_12,0) + COALESCE(billed_sms_13,0) + COALESCE(billed_sms_14,0) + COALESCE(billed_sms_15,0) + COALESCE(billed_sms_16,0) else 0 end) billed_ONNET_offpeak_sms,
        sum(COALESCE(billed_promo_sms_00,0) + COALESCE(billed_promo_sms_01,0) + COALESCE(billed_promo_sms_02,0) + COALESCE(billed_promo_sms_03,0) + COALESCE(billed_promo_sms_04,0) + COALESCE(billed_promo_sms_05,0) + COALESCE(billed_promo_sms_06,0) + COALESCE(billed_promo_sms_07,0) + COALESCE(billed_promo_sms_08,0) + COALESCE(billed_promo_sms_09,0) + COALESCE(billed_promo_sms_10,0) + COALESCE(billed_promo_sms_11,0) + COALESCE(billed_promo_sms_12,0) + COALESCE(billed_promo_sms_13,0) + COALESCE(billed_promo_sms_14,0) + COALESCE(billed_promo_sms_15,0) + COALESCE(billed_promo_sms_16,0)) promo_offpeak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_promo_sms_00,0) + COALESCE(billed_promo_sms_01,0) + COALESCE(billed_promo_sms_02,0) + COALESCE(billed_promo_sms_03,0) + COALESCE(billed_promo_sms_04,0) + COALESCE(billed_promo_sms_05,0) + COALESCE(billed_promo_sms_06,0) + COALESCE(billed_promo_sms_07,0) + COALESCE(billed_promo_sms_08,0) + COALESCE(billed_promo_sms_09,0) + COALESCE(billed_promo_sms_10,0) + COALESCE(billed_promo_sms_11,0) + COALESCE(billed_promo_sms_12,0) + COALESCE(billed_promo_sms_13,0) + COALESCE(billed_promo_sms_14,0) + COALESCE(billed_promo_sms_15,0) + COALESCE(billed_promo_sms_16,0) else 0 end) promo_OFFNET_offpeak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_promo_sms_00,0) + COALESCE(billed_promo_sms_01,0) + COALESCE(billed_promo_sms_02,0) + COALESCE(billed_promo_sms_03,0) + COALESCE(billed_promo_sms_04,0) + COALESCE(billed_promo_sms_05,0) + COALESCE(billed_promo_sms_06,0) + COALESCE(billed_promo_sms_07,0) + COALESCE(billed_promo_sms_08,0) + COALESCE(billed_promo_sms_09,0) + COALESCE(billed_promo_sms_10,0) + COALESCE(billed_promo_sms_11,0) + COALESCE(billed_promo_sms_12,0) + COALESCE(billed_promo_sms_13,0) + COALESCE(billed_promo_sms_14,0) + COALESCE(billed_promo_sms_15,0) + COALESCE(billed_promo_sms_16,0) else 0 end) promo_ONNET_offpeak_sms,
        sum(COALESCE(s_total_chg_17,0) + COALESCE(s_total_chg_18,0) + COALESCE(s_total_chg_19,0) + COALESCE(s_total_chg_20,0) + COALESCE(s_total_chg_21,0) + COALESCE(s_total_chg_22,0) + COALESCE(s_total_chg_23,0)) s_peak_chg,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(s_total_chg_17,0) + COALESCE(s_total_chg_18,0) + COALESCE(s_total_chg_19,0) + COALESCE(s_total_chg_20,0) + COALESCE(s_total_chg_21,0) + COALESCE(s_total_chg_22,0) + COALESCE(s_total_chg_23,0) else 0 end) s_OFFNET_peak_chg,
        sum(case when upper(typ) = 'ONNET' then COALESCE(s_total_chg_17,0) + COALESCE(s_total_chg_18,0) + COALESCE(s_total_chg_19,0) + COALESCE(s_total_chg_20,0) + COALESCE(s_total_chg_21,0) + COALESCE(s_total_chg_22,0) + COALESCE(s_total_chg_23,0) else 0 end) s_ONNET_peak_chg,
        sum(COALESCE(billed_sms_17,0) + COALESCE(billed_sms_18,0) + COALESCE(billed_sms_19,0) + COALESCE(billed_sms_20,0) + COALESCE(billed_sms_21,0) + COALESCE(billed_sms_22,0) + COALESCE(billed_sms_23,0)) billed_peak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_sms_17,0) + COALESCE(billed_sms_18,0) + COALESCE(billed_sms_19,0) + COALESCE(billed_sms_20,0) + COALESCE(billed_sms_21,0) + COALESCE(billed_sms_22,0) + COALESCE(billed_sms_23,0) else 0 end) billed_OFFNET_peak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_sms_17,0) + COALESCE(billed_sms_18,0) + COALESCE(billed_sms_19,0) + COALESCE(billed_sms_20,0) + COALESCE(billed_sms_21,0) + COALESCE(billed_sms_22,0) + COALESCE(billed_sms_23,0) else 0 end) billed_ONNET_peak_sms,
        sum(COALESCE(billed_promo_sms_17,0) + COALESCE(billed_promo_sms_18,0) + COALESCE(billed_promo_sms_19,0) + COALESCE(billed_promo_sms_20,0) + COALESCE(billed_promo_sms_21,0) + COALESCE(billed_promo_sms_22,0) + COALESCE(billed_promo_sms_23,0)) promo_peak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_promo_sms_17,0) + COALESCE(billed_promo_sms_18,0) + COALESCE(billed_promo_sms_19,0) + COALESCE(billed_promo_sms_20,0) + COALESCE(billed_promo_sms_21,0) + COALESCE(billed_promo_sms_22,0) + COALESCE(billed_promo_sms_23,0) else 0 end) promo_OFFNET_peak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_promo_sms_17,0) + COALESCE(billed_promo_sms_18,0) + COALESCE(billed_promo_sms_19,0) + COALESCE(billed_promo_sms_20,0) + COALESCE(billed_promo_sms_21,0) + COALESCE(billed_promo_sms_22,0) + COALESCE(billed_promo_sms_23,0) else 0 end) promo_ONNET_peak_sms
        , %1$s MONTH_ID
    FROM USG_SMS_DLY_LACCI
    WHERE LEVEL_7='SMS'
    GROUP BY
        a_party_no,
        COALESCE(lac, 0), COALESCE(ci, 0)
"""
  
  val queryDataTrafficSmy="""
    SELECT 
       MSISDN
      ,COALESCE(lac,0) lac
      ,COALESCE(ci,0) ci 
      ,sum(case when acs_pnt_nm_ni_id = 146 then COALESCE(uplink,0) else 0 end) bb_uplink_tr
      ,sum(case when acs_pnt_nm_ni_id = 146 then COALESCE(downlink,0) else 0 end) bb_downlink_tr
      ,sum(case when acs_pnt_nm_ni_id = 146 then 0 else COALESCE(uplink,0) end) non_bb_uplink_tr
      ,sum(case when acs_pnt_nm_ni_id = 146 then 0 else COALESCE(downlink,0) end) non_bb_downlink_tr
      ,sum(case when rat_tp = 6 then COALESCE(uplink,0) else 0 end) total_4g_uplink_tr
      ,sum(case when rat_tp = 6 then COALESCE(downlink,0) else 0 end) total_4g_downlink_tr
      ,sum(case when rat_tp = 1 then COALESCE(uplink,0) else 0 end) total_3g_uplink_tr
      ,sum(case when rat_tp = 1 then COALESCE(downlink,0) else 0 end) total_3g_downlink_tr
      ,sum(case when rat_tp = 2 then COALESCE(uplink,0) else 0 end) total_2g_uplink_tr
      ,sum(case when rat_tp = 2 then COALESCE(downlink,0) else 0 end) total_2g_downlink_tr
      ,sum(case when tb = '00.00-16.59' then COALESCE(downlink,0) else 0 end) offpeak_data_downlink_tr
      ,sum(case when tb = '00.00-16.59' then COALESCE(uplink,0) else 0 end) offpeak_data_uplink_tr
      ,sum(case when tb = '17.00-23.59' then COALESCE(downlink,0) else 0 end) peak_data_downlink_tr
      ,sum(case when tb = '17.00-23.59' then COALESCE(uplink,0) else 0 end) peak_data_uplink_tr
      ,count(distinct daydate) num_days_data
      , %1$s MONTH_ID
    FROM 
      mbs_tmp_data
    GROUP BY 
      msisdn
    , COALESCE(lac, 0), COALESCE(ci, 0)
"""
  val querySmsTrafficSmy="""
    select 
       msisdn,
       lac,
       ci, 
       sum(cnt) sms_hits,
       sum(case when upper(typ) = 'onnet' then cnt else 0 end) onnet_sms_hits,
       sum(case when upper(typ) = 'offnet' then cnt else 0 end) offnet_sms_hits,
       count(distinct daydate) num_days_sms
       , %1$s MONTH_ID
    from TRAFFIC_SMS_DLY_LACCI
       group by msisdn,
       lac,
       ci
""" 
  
val queryGprsMoSmyOld ="""
    SELECT 
            a_party_no msisdn
           ,COALESCE(lac,0) lac
           ,COALESCE(ci,0) ci 
           ,0 v_total_chg
           ,0 v_offnet_chg
           ,0 v_onnet_chg
           ,0 billed_dur
           ,0 billed_offnet_dur
           ,0 billed_onnet_dur
           ,0 promo_dur
           ,0 promo_offnet_dur
           ,0 promo_onnet_dur
           ,0 tot_dur
           ,0 v_offnet_dur
           ,0 v_onnet_dur
           ,0 v_offpeak_chg
           ,0 v_offnet_offpeak_chg
           ,0 v_onnet_offpeak_chg
           ,0 billed_offpeak_dur
           ,0 billed_offnet_offpeak_dur
           ,0 billed_onnet_offpeak_dur
           ,0 promo_offpeak_dur
           ,0 promo_offnet_offpeak_dur
           ,0 promo_onnet_offpeak_dur
           ,0 total_offpeak_dur
           ,0 v_offnet_offpeak_dur
           ,0 v_onnet_offpeak_dur
           ,0 v_peak_chg
           ,0 v_offnet_peak_chg
           ,0 v_onnet_peak_chg
           ,0 billed_peak_dur
           ,0 billed_offnet_peak_dur
           ,0 billed_onnet_peak_dur
           ,0 promo_peak_dur
           ,0 promo_offnet_peak_dur
           ,0 promo_onnet_peak_dur
           ,0 total_peak_dur
           ,0 v_offnet_peak_dur
           ,0 v_onnet_peak_dur
           ,0 num_days_voice
           ,sum(COALESCE(g_total_chg,0)) g_total_chg
           ,sum(COALESCE(ppu_datavol,0)) ppu_datavol
           ,sum(COALESCE(addon_datavol,0)) addon_datavol
           ,sum(COALESCE(total_datavol,0)) total_datavol
           ,sum(case when rat_tp_id = 6 then g_total_chg else 0 end) g_4g_total_chg
           ,sum(case when rat_tp_id = 1 then g_total_chg else 0 end) g_3g_total_chg
           ,sum(case when rat_tp_id = 2 then g_total_chg else 0 end) g_2g_total_chg
           ,sum(case when rat_tp_id = 6 then ppu_datavol else 0 end) g_4g_ppu_datavol
           ,sum(case when rat_tp_id = 1 then ppu_datavol else 0 end) g_3g_ppu_datavol
           ,sum(case when rat_tp_id = 2 then ppu_datavol else 0 end) g_2g_ppu_datavol
           ,sum(case when rat_tp_id = 6 then addon_datavol else 0 end) g_4g_addon_datavol
           ,sum(case when rat_tp_id = 1 then addon_datavol else 0 end) g_3g_addon_datavol
           ,sum(case when rat_tp_id = 2 then addon_datavol else 0 end) g_2g_addon_datavol
           ,sum(COALESCE(g_total_chg_00,0) + COALESCE(g_total_chg_01,0) + COALESCE(g_total_chg_02,0) + COALESCE(g_total_chg_03,0) + COALESCE(g_total_chg_04,0) + COALESCE(g_total_chg_05,0) + COALESCE(g_total_chg_06,0) + COALESCE(g_total_chg_07,0) + COALESCE(g_total_chg_08,0) + COALESCE(g_total_chg_09,0) + COALESCE(g_total_chg_10,0) + COALESCE(g_total_chg_11,0) + COALESCE(g_total_chg_12,0) + COALESCE(g_total_chg_13,0) + COALESCE(g_total_chg_14,0) + COALESCE(g_total_chg_15,0) + COALESCE(g_total_chg_16,0)) g_offpeak_chg
           ,sum(COALESCE(g_total_chg_17,0) + COALESCE(g_total_chg_18,0) + COALESCE(g_total_chg_19,0) + COALESCE(g_total_chg_20,0) + COALESCE(g_total_chg_21,0) + COALESCE(g_total_chg_22,0) + COALESCE(g_total_chg_23,0)) g_peak_chg
           ,sum(COALESCE(ppu_datavol_00,0) + COALESCE(ppu_datavol_01,0) + COALESCE(ppu_datavol_02,0) + COALESCE(ppu_datavol_03,0) + COALESCE(ppu_datavol_04,0) + COALESCE(ppu_datavol_05,0) + COALESCE(ppu_datavol_06,0) + COALESCE(ppu_datavol_07,0) + COALESCE(ppu_datavol_08,0) + COALESCE(ppu_datavol_09,0) + COALESCE(ppu_datavol_10,0) + COALESCE(ppu_datavol_11,0) + COALESCE(ppu_datavol_12,0) + COALESCE(ppu_datavol_13,0) + COALESCE(ppu_datavol_14,0) + COALESCE(ppu_datavol_15,0) + COALESCE(ppu_datavol_16,0)) ppu_offpeak_datavol
           ,sum(COALESCE(ppu_datavol_17,0) + COALESCE(ppu_datavol_18,0) + COALESCE(ppu_datavol_19,0) + COALESCE(ppu_datavol_20,0) + COALESCE(ppu_datavol_21,0) + COALESCE(ppu_datavol_22,0) + COALESCE(ppu_datavol_23,0)) ppu_peak_datavol
           ,sum(COALESCE(addon_datavol_00,0) + COALESCE(addon_datavol_01,0) + COALESCE(addon_datavol_02,0) + COALESCE(addon_datavol_03,0) + COALESCE(addon_datavol_04,0) + COALESCE(addon_datavol_05,0) + COALESCE(addon_datavol_06,0) + COALESCE(addon_datavol_07,0) + COALESCE(addon_datavol_08,0) + COALESCE(addon_datavol_09,0) + COALESCE(addon_datavol_10,0) + COALESCE(addon_datavol_11,0) + COALESCE(addon_datavol_12,0) + COALESCE(addon_datavol_13,0) + COALESCE(addon_datavol_14,0) + COALESCE(addon_datavol_15,0) + COALESCE(addon_datavol_16,0)) addon_offpeak_datavol
           ,sum(COALESCE(addon_datavol_17,0) + COALESCE(addon_datavol_18,0) + COALESCE(addon_datavol_19,0) + COALESCE(addon_datavol_20,0) + COALESCE(addon_datavol_21,0) + COALESCE(addon_datavol_22,0) + COALESCE(addon_datavol_23,0)) addon_peak_datavol
           ,sum(COALESCE(total_datavol_00,0) + COALESCE(total_datavol_01,0) + COALESCE(total_datavol_02,0) + COALESCE(total_datavol_03,0) + COALESCE(total_datavol_04,0) + COALESCE(total_datavol_05,0) + COALESCE(total_datavol_06,0) + COALESCE(total_datavol_07,0) + COALESCE(total_datavol_08,0) + COALESCE(total_datavol_09,0) + COALESCE(total_datavol_10,0) + COALESCE(total_datavol_11,0) + COALESCE(total_datavol_12,0) + COALESCE(total_datavol_13,0) + COALESCE(total_datavol_14,0) + COALESCE(total_datavol_15,0) + COALESCE(total_datavol_16,0)) total_offpeak_datavol
           ,sum(COALESCE(total_datavol_17,0) + COALESCE(total_datavol_18,0) + COALESCE(total_datavol_19,0) + COALESCE(total_datavol_20,0) + COALESCE(total_datavol_21,0) + COALESCE(total_datavol_22,0) + COALESCE(total_datavol_23,0)) total_peak_datavol
           ,0 s_total_chg
           ,0 s_offnet_chg
           ,0 s_onnet_chg
           ,0 billed_sms
           ,0 billed_offnet_sms
           ,0 billed_onnet_sms
           ,0 promo_sms
           ,0 promo_offnet_sms
           ,0 promo_onnet_sms
           ,0 s_offpeak_chg
           ,0 s_offnet_offpeak_chg
           ,0 s_onnet_offpeak_chg
           ,0 billed_offpeak_sms
           ,0 billed_offnet_offpeak_sms
           ,0 billed_onnet_offpeak_sms
           ,0 promo_offpeak_sms
           ,0 promo_offnet_offpeak_sms
           ,0 promo_onnet_offpeak_sms
           ,0 s_peak_chg
           ,0 s_offnet_peak_chg
           ,0 s_onnet_peak_chg
           ,0 billed_peak_sms
           ,0 billed_offnet_peak_sms
           ,0 billed_onnet_peak_sms
           ,0 promo_peak_sms
           ,0 promo_offnet_peak_sms
           ,0 promo_onnet_peak_sms
           ,0 bb_uplink_tr
           ,0 bb_downlink_tr
           ,0 non_bb_uplink_tr
           ,0 non_bb_downlink_tr
           ,0 total_4g_uplink_tr
           ,0 total_4g_downlink_tr
           ,0 total_3g_uplink_tr
           ,0 total_3g_downlink_tr
           ,0 total_2g_uplink_tr
           ,0 total_2g_downlink_tr
           ,0 offpeak_data_downlink_tr
           ,0 offpeak_data_uplink_tr
           ,0 peak_data_downlink_tr
           ,0 peak_data_uplink_tr
           ,0 num_days_data
           ,0 sms_hits
           ,0 onnet_sms_hits
           ,0 offnet_sms_hits
           ,0 num_days_sms
    FROM 
       USG_GPRS_DLY_LACCI
    GROUP BY 
        a_party_no
        , COALESCE(lac, 0), COALESCE(ci, 0)

"""
  val queryVoiceMoSmyOld ="""
    SELECT 
      a_party_no msisdn,
      COALESCE(lac,0) lac,
      COALESCE(ci,0) ci,
      SUM(COALESCE(v_total_chg,0)) v_total_chg,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(v_total_chg,0) ELSE 0 END) v_offnet_chg,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(v_total_chg,0) ELSE 0 END) v_onnet_chg,
      SUM(COALESCE(billed_dur,0)) billed_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(billed_dur,0) ELSE 0 END) billed_offnet_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(billed_dur,0) ELSE 0 END) billed_onnet_dur,
      SUM(COALESCE(billed_promo_dur,0)) promo_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(billed_promo_dur,0) ELSE 0 END) promo_offnet_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(billed_promo_dur,0) ELSE 0 END) promo_onnet_dur,
      SUM(COALESCE(tot_dur,0)) tot_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(tot_dur,0) ELSE 0 END) v_offnet_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(tot_dur,0) ELSE 0 END) v_onnet_dur,
      SUM(COALESCE(v_total_chg_00,0) + COALESCE(v_total_chg_01,0) + COALESCE(v_total_chg_02,0) + COALESCE(v_total_chg_03,0) + COALESCE(v_total_chg_04,0) + COALESCE(v_total_chg_05,0) + COALESCE(v_total_chg_06,0) + COALESCE(v_total_chg_07,0) + COALESCE(v_total_chg_08,0) + COALESCE(v_total_chg_09,0) + COALESCE(v_total_chg_10,0) + COALESCE(v_total_chg_11,0) + COALESCE(v_total_chg_12,0) + COALESCE(v_total_chg_13,0) + COALESCE(v_total_chg_14,0) + COALESCE(v_total_chg_15,0) + COALESCE(v_total_chg_16,0)) v_offpeak_chg,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(v_total_chg_00,0) + COALESCE(v_total_chg_01,0) + COALESCE(v_total_chg_02,0) + COALESCE(v_total_chg_03,0) + COALESCE(v_total_chg_04,0) + COALESCE(v_total_chg_05,0) + COALESCE(v_total_chg_06,0) + COALESCE(v_total_chg_07,0) + COALESCE(v_total_chg_08,0) + COALESCE(v_total_chg_09,0) + COALESCE(v_total_chg_10,0) + COALESCE(v_total_chg_11,0) + COALESCE(v_total_chg_12,0) + COALESCE(v_total_chg_13,0) + COALESCE(v_total_chg_14,0) + COALESCE(v_total_chg_15,0) + COALESCE(v_total_chg_16,0) ELSE 0 END) v_offnet_offpeak_chg,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(v_total_chg_00,0) + COALESCE(v_total_chg_01,0) + COALESCE(v_total_chg_02,0) + COALESCE(v_total_chg_03,0) + COALESCE(v_total_chg_04,0) + COALESCE(v_total_chg_05,0) + COALESCE(v_total_chg_06,0) + COALESCE(v_total_chg_07,0) + COALESCE(v_total_chg_08,0) + COALESCE(v_total_chg_09,0) + COALESCE(v_total_chg_10,0) + COALESCE(v_total_chg_11,0) + COALESCE(v_total_chg_12,0) + COALESCE(v_total_chg_13,0) + COALESCE(v_total_chg_14,0) + COALESCE(v_total_chg_15,0) + COALESCE(v_total_chg_16,0) ELSE 0 END) v_onnet_offpeak_chg,
      SUM(COALESCE(billed_dur_00,0) + COALESCE(billed_dur_01,0) + COALESCE(billed_dur_02,0) + COALESCE(billed_dur_03,0) + COALESCE(billed_dur_04,0) + COALESCE(billed_dur_05,0) + COALESCE(billed_dur_06,0) + COALESCE(billed_dur_07,0) + COALESCE(billed_dur_08,0) + COALESCE(billed_dur_09,0) + COALESCE(billed_dur_10,0) + COALESCE(billed_dur_11,0) + COALESCE(billed_dur_12,0) + COALESCE(billed_dur_13,0) + COALESCE(billed_dur_14,0) + COALESCE(billed_dur_15,0) + COALESCE(billed_dur_16,0)) billed_offpeak_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(billed_dur_00,0) + COALESCE(billed_dur_01,0) + COALESCE(billed_dur_02,0) + COALESCE(billed_dur_03,0) + COALESCE(billed_dur_04,0) + COALESCE(billed_dur_05,0) + COALESCE(billed_dur_06,0) + COALESCE(billed_dur_07,0) + COALESCE(billed_dur_08,0) + COALESCE(billed_dur_09,0) + COALESCE(billed_dur_10,0) + COALESCE(billed_dur_11,0) + COALESCE(billed_dur_12,0) + COALESCE(billed_dur_13,0) + COALESCE(billed_dur_14,0) + COALESCE(billed_dur_15,0) + COALESCE(billed_dur_16,0) ELSE 0 END) billed_offnet_offpeak_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(billed_dur_00,0) + COALESCE(billed_dur_01,0) + COALESCE(billed_dur_02,0) + COALESCE(billed_dur_03,0) + COALESCE(billed_dur_04,0) + COALESCE(billed_dur_05,0) + COALESCE(billed_dur_06,0) + COALESCE(billed_dur_07,0) + COALESCE(billed_dur_08,0) + COALESCE(billed_dur_09,0) + COALESCE(billed_dur_10,0) + COALESCE(billed_dur_11,0) + COALESCE(billed_dur_12,0) + COALESCE(billed_dur_13,0) + COALESCE(billed_dur_14,0) + COALESCE(billed_dur_15,0) + COALESCE(billed_dur_16,0) ELSE 0 END) billed_onnet_offpeak_dur,
      SUM(COALESCE(billed_promo_dur_00,0) + COALESCE(billed_promo_dur_01,0) + COALESCE(billed_promo_dur_02,0) + COALESCE(billed_promo_dur_03,0) + COALESCE(billed_promo_dur_04,0) + COALESCE(billed_promo_dur_05,0) + COALESCE(billed_promo_dur_06,0) + COALESCE(billed_promo_dur_07,0) + COALESCE(billed_promo_dur_08,0) + COALESCE(billed_promo_dur_09,0) + COALESCE(billed_promo_dur_10,0) + COALESCE(billed_promo_dur_11,0) + COALESCE(billed_promo_dur_12,0) + COALESCE(billed_promo_dur_13,0) + COALESCE(billed_promo_dur_14,0) + COALESCE(billed_promo_dur_15,0) + COALESCE(billed_promo_dur_16,0)) promo_offpeak_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(billed_promo_dur_00,0) + COALESCE(billed_promo_dur_01,0) + COALESCE(billed_promo_dur_02,0) + COALESCE(billed_promo_dur_03,0) + COALESCE(billed_promo_dur_04,0) + COALESCE(billed_promo_dur_05,0) + COALESCE(billed_promo_dur_06,0) + COALESCE(billed_promo_dur_07,0) + COALESCE(billed_promo_dur_08,0) + COALESCE(billed_promo_dur_09,0) + COALESCE(billed_promo_dur_10,0) + COALESCE(billed_promo_dur_11,0) + COALESCE(billed_promo_dur_12,0) + COALESCE(billed_promo_dur_13,0) + COALESCE(billed_promo_dur_14,0) + COALESCE(billed_promo_dur_15,0) + COALESCE(billed_promo_dur_16,0) ELSE 0 END) promo_offnet_offpeak_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(billed_promo_dur_00,0) + COALESCE(billed_promo_dur_01,0) + COALESCE(billed_promo_dur_02,0) + COALESCE(billed_promo_dur_03,0) + COALESCE(billed_promo_dur_04,0) + COALESCE(billed_promo_dur_05,0) + COALESCE(billed_promo_dur_06,0) + COALESCE(billed_promo_dur_07,0) + COALESCE(billed_promo_dur_08,0) + COALESCE(billed_promo_dur_09,0) + COALESCE(billed_promo_dur_10,0) + COALESCE(billed_promo_dur_11,0) + COALESCE(billed_promo_dur_12,0) + COALESCE(billed_promo_dur_13,0) + COALESCE(billed_promo_dur_14,0) + COALESCE(billed_promo_dur_15,0) + COALESCE(billed_promo_dur_16,0) ELSE 0 END) promo_onnet_offpeak_dur,
      SUM(COALESCE(tot_dur_00,0) + COALESCE(tot_dur_01,0) + COALESCE(tot_dur_02,0) + COALESCE(tot_dur_03,0) + COALESCE(tot_dur_04,0) + COALESCE(tot_dur_05,0) + COALESCE(tot_dur_06,0) + COALESCE(tot_dur_07,0) + COALESCE(tot_dur_08,0) + COALESCE(tot_dur_09,0) + COALESCE(tot_dur_10,0) + COALESCE(tot_dur_11,0) + COALESCE(tot_dur_12,0) + COALESCE(tot_dur_13,0) + COALESCE(tot_dur_14,0) + COALESCE(tot_dur_15,0) + COALESCE(tot_dur_16,0)) total_offpeak_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(tot_dur_00,0) + COALESCE(tot_dur_01,0) + COALESCE(tot_dur_02,0) + COALESCE(tot_dur_03,0) + COALESCE(tot_dur_04,0) + COALESCE(tot_dur_05,0) + COALESCE(tot_dur_06,0) + COALESCE(tot_dur_07,0) + COALESCE(tot_dur_08,0) + COALESCE(tot_dur_09,0) + COALESCE(tot_dur_10,0) + COALESCE(tot_dur_11,0) + COALESCE(tot_dur_12,0) + COALESCE(tot_dur_13,0) + COALESCE(tot_dur_14,0) + COALESCE(tot_dur_15,0) + COALESCE(tot_dur_16,0) ELSE 0 END) v_offnet_offpeak_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(tot_dur_00,0) + COALESCE(tot_dur_01,0) + COALESCE(tot_dur_02,0) + COALESCE(tot_dur_03,0) + COALESCE(tot_dur_04,0) + COALESCE(tot_dur_05,0) + COALESCE(tot_dur_06,0) + COALESCE(tot_dur_07,0) + COALESCE(tot_dur_08,0) + COALESCE(tot_dur_09,0) + COALESCE(tot_dur_10,0) + COALESCE(tot_dur_11,0) + COALESCE(tot_dur_12,0) + COALESCE(tot_dur_13,0) + COALESCE(tot_dur_14,0) + COALESCE(tot_dur_15,0) + COALESCE(tot_dur_16,0) ELSE 0 END) v_onnet_offpeak_dur,
      SUM(COALESCE(v_total_chg_17,0) + COALESCE(v_total_chg_18,0) + COALESCE(v_total_chg_19,0) + COALESCE(v_total_chg_20,0) + COALESCE(v_total_chg_21,0) + COALESCE(v_total_chg_22,0) + COALESCE(v_total_chg_23,0)) v_peak_chg,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(v_total_chg_17,0) + COALESCE(v_total_chg_18,0) + COALESCE(v_total_chg_19,0) + COALESCE(v_total_chg_20,0) + COALESCE(v_total_chg_21,0) + COALESCE(v_total_chg_22,0) + COALESCE(v_total_chg_23,0) ELSE 0 END) v_offnet_peak_chg,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(v_total_chg_17,0) + COALESCE(v_total_chg_18,0) + COALESCE(v_total_chg_19,0) + COALESCE(v_total_chg_20,0) + COALESCE(v_total_chg_21,0) + COALESCE(v_total_chg_22,0) + COALESCE(v_total_chg_23,0) ELSE 0 END) v_onnet_peak_chg,
      SUM(COALESCE(billed_dur_17,0) + COALESCE(billed_dur_18,0) + COALESCE(billed_dur_19,0) + COALESCE(billed_dur_20,0) + COALESCE(billed_dur_21,0) + COALESCE(billed_dur_22,0) + COALESCE(billed_dur_23,0)) billed_peak_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(billed_dur_17,0) + COALESCE(billed_dur_18,0) + COALESCE(billed_dur_19,0) + COALESCE(billed_dur_20,0) + COALESCE(billed_dur_21,0) + COALESCE(billed_dur_22,0) + COALESCE(billed_dur_23,0) ELSE 0 END) billed_offnet_peak_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(billed_dur_17,0) + COALESCE(billed_dur_18,0) + COALESCE(billed_dur_19,0) + COALESCE(billed_dur_20,0) + COALESCE(billed_dur_21,0) + COALESCE(billed_dur_22,0) + COALESCE(billed_dur_23,0) ELSE 0 END) billed_onnet_peak_dur,
      SUM(COALESCE(billed_promo_dur_17,0) + COALESCE(billed_promo_dur_18,0) + COALESCE(billed_promo_dur_19,0) + COALESCE(billed_promo_dur_20,0) + COALESCE(billed_promo_dur_21,0) + COALESCE(billed_promo_dur_22,0) + COALESCE(billed_promo_dur_23,0)) promo_peak_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(billed_promo_dur_17,0) + COALESCE(billed_promo_dur_18,0) + COALESCE(billed_promo_dur_19,0) + COALESCE(billed_promo_dur_20,0) + COALESCE(billed_promo_dur_21,0) + COALESCE(billed_promo_dur_22,0) + COALESCE(billed_promo_dur_23,0) ELSE 0 END) promo_offnet_peak_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(billed_promo_dur_17,0) + COALESCE(billed_promo_dur_18,0) + COALESCE(billed_promo_dur_19,0) + COALESCE(billed_promo_dur_20,0) + COALESCE(billed_promo_dur_21,0) + COALESCE(billed_promo_dur_22,0) + COALESCE(billed_promo_dur_23,0) ELSE 0 END) promo_onnet_peak_dur,
      SUM(COALESCE(tot_dur_17,0) + COALESCE(tot_dur_18,0) + COALESCE(tot_dur_19,0) + COALESCE(tot_dur_20,0) + COALESCE(tot_dur_21,0) + COALESCE(tot_dur_22,0) + COALESCE(tot_dur_23,0)) total_peak_dur,
      SUM(CASE WHEN upper(typ) = 'offnet' THEN COALESCE(tot_dur_17,0) + COALESCE(tot_dur_18,0) + COALESCE(tot_dur_19,0) + COALESCE(tot_dur_20,0) + COALESCE(tot_dur_21,0) + COALESCE(tot_dur_22,0) + COALESCE(tot_dur_23,0) ELSE 0 END) v_offnet_peak_dur,
      SUM(CASE WHEN upper(typ) = 'onnet' THEN COALESCE(tot_dur_17,0) + COALESCE(tot_dur_18,0) + COALESCE(tot_dur_19,0) + COALESCE(tot_dur_20,0) + COALESCE(tot_dur_21,0) + COALESCE(tot_dur_22,0) + COALESCE(tot_dur_23,0) ELSE 0 END) v_onnet_peak_dur,
      count(distinct dt) num_days_voice,
      0 g_total_chg,
      0 ppu_datavol,
      0 addon_datavol,
      0 total_datavol,
      0 g_4g_total_chg,
      0 g_3g_total_chg,
      0 g_2g_total_chg,
      0 g_4g_ppu_datavol,
      0 g_3g_ppu_datavol,
      0 g_2g_ppu_datavol,
      0 g_4g_addon_datavol,
      0 g_3g_addon_datavol,
      0 g_2g_addon_datavol,
      0 g_offpeak_chg,
      0 g_peak_chg,
      0 ppu_offpeak_datavol,
      0 ppu_peak_datavol,
      0 addon_offpeak_datavol,
      0 addon_peak_datavol,
      0 total_offpeak_datavol,
      0 total_peak_datavol,
      0 s_total_chg,
      0 s_offnet_chg,
      0 s_onnet_chg,
      0 billed_sms,
      0 billed_offnet_sms,
      0 billed_onnet_sms,
      0 promo_sms,
      0 promo_offnet_sms,
      0 promo_onnet_sms,
      0 s_offpeak_chg,
      0 s_offnet_offpeak_chg,
      0 s_onnet_offpeak_chg,
      0 billed_offpeak_sms,
      0 billed_offnet_offpeak_sms,
      0 billed_onnet_offpeak_sms,
      0 promo_offpeak_sms,
      0 promo_offnet_offpeak_sms,
      0 promo_onnet_offpeak_sms,
      0 s_peak_chg,
      0 s_offnet_peak_chg,
      0 s_onnet_peak_chg,
      0 billed_peak_sms,
      0 billed_offnet_peak_sms,
      0 billed_onnet_peak_sms,
      0 promo_peak_sms,
      0 promo_offnet_peak_sms,
      0 promo_onnet_peak_sms,
      0 bb_uplink_tr,
      0 bb_downlink_tr,
      0 non_bb_uplink_tr,
      0 non_bb_downlink_tr,
      0 total_4g_uplink_tr,
      0 total_4g_downlink_tr,
      0 total_3g_uplink_tr,
      0 total_3g_downlink_tr,
      0 total_2g_uplink_tr,
      0 total_2g_downlink_tr,
      0 offpeak_data_downlink_tr,
      0 offpeak_data_uplink_tr,
      0 peak_data_downlink_tr,
      0 peak_data_uplink_tr,
      0 num_days_data,
      0 sms_hits,
      0 onnet_sms_hits,
      0 offnet_sms_hits
      ,0 num_days_sms
    FROM USG_VOICE_DLY_LACCI
    GROUP BY 
      a_party_no
      , COALESCE(lac, 0), COALESCE(ci, 0)
"""
  val querySmsMoSmyOld ="""
    SELECT 
        a_party_no msisdn,
        COALESCE(lac,0) lac,
        COALESCE(ci,0) ci,
        0 v_total_chg,
        0 v_offnet_chg,
        0 v_onnet_chg,
        0 billed_dur,
        0 billed_offnet_dur,
        0 billed_onnet_dur,
        0 promo_dur,
        0 promo_offnet_dur,
        0 promo_onnet_dur,
        0 tot_dur,
        0 v_offnet_dur,
        0 v_onnet_dur,
        0 v_offpeak_chg,
        0 v_offnet_offpeak_chg,
        0 v_onnet_offpeak_chg,
        0 billed_offpeak_dur,
        0 billed_offnet_offpeak_dur,
        0 billed_onnet_offpeak_dur,
        0 promo_offpeak_dur,
        0 promo_offnet_offpeak_dur,
        0 promo_onnet_offpeak_dur,
        0 total_offpeak_dur,
        0 v_offnet_offpeak_dur,
        0 v_onnet_offpeak_dur,
        0 v_peak_chg,
        0 v_offnet_peak_chg,
        0 v_onnet_peak_chg,
        0 billed_peak_dur,
        0 billed_offnet_peak_dur,
        0 billed_onnet_peak_dur,
        0 promo_peak_dur,
        0 promo_offnet_peak_dur,
        0 promo_onnet_peak_dur,
        0 total_peak_dur,
        0 v_offnet_peak_dur,
        0 v_onnet_peak_dur,
        0 num_days_voice,
        0 g_total_chg,
        0 ppu_datavol,
        0 addon_datavol,
        0 total_datavol,
        0 g_4g_total_chg,
        0 g_3g_total_chg,
        0 g_2g_total_chg,
        0 g_4g_ppu_datavol,
        0 g_3g_ppu_datavol,
        0 g_2g_ppu_datavol,
        0 g_4g_addon_datavol,
        0 g_3g_addon_datavol,
        0 g_2g_addon_datavol,
        0 g_offpeak_chg,
        0 g_peak_chg,
        0 ppu_offpeak_datavol,
        0 ppu_peak_datavol,
        0 addon_offpeak_datavol,
        0 addon_peak_datavol,
        0 total_offpeak_datavol,
        0 total_peak_datavol,
        sum(COALESCE(s_total_chg,0)) s_total_chg,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(s_total_chg,0) else 0 end) s_OFFNET_chg,
        sum(case when upper(typ) = 'ONNET' then COALESCE(s_total_chg,0) else 0 end) s_ONNET_chg,
        sum(COALESCE(billed_sms,0)) billed_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_sms,0) else 0 end) billed_OFFNET_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_sms,0) else 0 end) billed_ONNET_sms,
        sum(COALESCE(billed_promo_sms,0)) promo_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_promo_sms,0) else 0 end) promo_OFFNET_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_promo_sms,0) else 0 end) promo_ONNET_sms,
        sum(COALESCE(s_total_chg_00,0) + COALESCE(s_total_chg_01,0) + COALESCE(s_total_chg_02,0) + COALESCE(s_total_chg_03,0) + COALESCE(s_total_chg_04,0) + COALESCE(s_total_chg_05,0) + COALESCE(s_total_chg_06,0) + COALESCE(s_total_chg_07,0) + COALESCE(s_total_chg_08,0) + COALESCE(s_total_chg_09,0) + COALESCE(s_total_chg_10,0) + COALESCE(s_total_chg_11,0) + COALESCE(s_total_chg_12,0) + COALESCE(s_total_chg_13,0) + COALESCE(s_total_chg_14,0) + COALESCE(s_total_chg_15,0) + COALESCE(s_total_chg_16,0)) s_offpeak_chg,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(s_total_chg_00,0) + COALESCE(s_total_chg_01,0) + COALESCE(s_total_chg_02,0) + COALESCE(s_total_chg_03,0) + COALESCE(s_total_chg_04,0) + COALESCE(s_total_chg_05,0) + COALESCE(s_total_chg_06,0) + COALESCE(s_total_chg_07,0) + COALESCE(s_total_chg_08,0) + COALESCE(s_total_chg_09,0) + COALESCE(s_total_chg_10,0) + COALESCE(s_total_chg_11,0) + COALESCE(s_total_chg_12,0) + COALESCE(s_total_chg_13,0) + COALESCE(s_total_chg_14,0) + COALESCE(s_total_chg_15,0) + COALESCE(s_total_chg_16,0) else 0 end) s_OFFNET_offpeak_chg,
        sum(case when upper(typ) = 'ONNET' then COALESCE(s_total_chg_00,0) + COALESCE(s_total_chg_01,0) + COALESCE(s_total_chg_02,0) + COALESCE(s_total_chg_03,0) + COALESCE(s_total_chg_04,0) + COALESCE(s_total_chg_05,0) + COALESCE(s_total_chg_06,0) + COALESCE(s_total_chg_07,0) + COALESCE(s_total_chg_08,0) + COALESCE(s_total_chg_09,0) + COALESCE(s_total_chg_10,0) + COALESCE(s_total_chg_11,0) + COALESCE(s_total_chg_12,0) + COALESCE(s_total_chg_13,0) + COALESCE(s_total_chg_14,0) + COALESCE(s_total_chg_15,0) + COALESCE(s_total_chg_16,0) else 0 end) s_ONNET_offpeak_chg,
        sum(COALESCE(billed_sms_00,0) + COALESCE(billed_sms_01,0) + COALESCE(billed_sms_02,0) + COALESCE(billed_sms_03,0) + COALESCE(billed_sms_04,0) + COALESCE(billed_sms_05,0) + COALESCE(billed_sms_06,0) + COALESCE(billed_sms_07,0) + COALESCE(billed_sms_08,0) + COALESCE(billed_sms_09,0) + COALESCE(billed_sms_10,0) + COALESCE(billed_sms_11,0) + COALESCE(billed_sms_12,0) + COALESCE(billed_sms_13,0) + COALESCE(billed_sms_14,0) + COALESCE(billed_sms_15,0) + COALESCE(billed_sms_16,0)) billed_offpeak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_sms_00,0) + COALESCE(billed_sms_01,0) + COALESCE(billed_sms_02,0) + COALESCE(billed_sms_03,0) + COALESCE(billed_sms_04,0) + COALESCE(billed_sms_05,0) + COALESCE(billed_sms_06,0) + COALESCE(billed_sms_07,0) + COALESCE(billed_sms_08,0) + COALESCE(billed_sms_09,0) + COALESCE(billed_sms_10,0) + COALESCE(billed_sms_11,0) + COALESCE(billed_sms_12,0) + COALESCE(billed_sms_13,0) + COALESCE(billed_sms_14,0) + COALESCE(billed_sms_15,0) + COALESCE(billed_sms_16,0) else 0 end) billed_OFFNET_offpeak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_sms_00,0) + COALESCE(billed_sms_01,0) + COALESCE(billed_sms_02,0) + COALESCE(billed_sms_03,0) + COALESCE(billed_sms_04,0) + COALESCE(billed_sms_05,0) + COALESCE(billed_sms_06,0) + COALESCE(billed_sms_07,0) + COALESCE(billed_sms_08,0) + COALESCE(billed_sms_09,0) + COALESCE(billed_sms_10,0) + COALESCE(billed_sms_11,0) + COALESCE(billed_sms_12,0) + COALESCE(billed_sms_13,0) + COALESCE(billed_sms_14,0) + COALESCE(billed_sms_15,0) + COALESCE(billed_sms_16,0) else 0 end) billed_ONNET_offpeak_sms,
        sum(COALESCE(billed_promo_sms_00,0) + COALESCE(billed_promo_sms_01,0) + COALESCE(billed_promo_sms_02,0) + COALESCE(billed_promo_sms_03,0) + COALESCE(billed_promo_sms_04,0) + COALESCE(billed_promo_sms_05,0) + COALESCE(billed_promo_sms_06,0) + COALESCE(billed_promo_sms_07,0) + COALESCE(billed_promo_sms_08,0) + COALESCE(billed_promo_sms_09,0) + COALESCE(billed_promo_sms_10,0) + COALESCE(billed_promo_sms_11,0) + COALESCE(billed_promo_sms_12,0) + COALESCE(billed_promo_sms_13,0) + COALESCE(billed_promo_sms_14,0) + COALESCE(billed_promo_sms_15,0) + COALESCE(billed_promo_sms_16,0)) promo_offpeak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_promo_sms_00,0) + COALESCE(billed_promo_sms_01,0) + COALESCE(billed_promo_sms_02,0) + COALESCE(billed_promo_sms_03,0) + COALESCE(billed_promo_sms_04,0) + COALESCE(billed_promo_sms_05,0) + COALESCE(billed_promo_sms_06,0) + COALESCE(billed_promo_sms_07,0) + COALESCE(billed_promo_sms_08,0) + COALESCE(billed_promo_sms_09,0) + COALESCE(billed_promo_sms_10,0) + COALESCE(billed_promo_sms_11,0) + COALESCE(billed_promo_sms_12,0) + COALESCE(billed_promo_sms_13,0) + COALESCE(billed_promo_sms_14,0) + COALESCE(billed_promo_sms_15,0) + COALESCE(billed_promo_sms_16,0) else 0 end) promo_OFFNET_offpeak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_promo_sms_00,0) + COALESCE(billed_promo_sms_01,0) + COALESCE(billed_promo_sms_02,0) + COALESCE(billed_promo_sms_03,0) + COALESCE(billed_promo_sms_04,0) + COALESCE(billed_promo_sms_05,0) + COALESCE(billed_promo_sms_06,0) + COALESCE(billed_promo_sms_07,0) + COALESCE(billed_promo_sms_08,0) + COALESCE(billed_promo_sms_09,0) + COALESCE(billed_promo_sms_10,0) + COALESCE(billed_promo_sms_11,0) + COALESCE(billed_promo_sms_12,0) + COALESCE(billed_promo_sms_13,0) + COALESCE(billed_promo_sms_14,0) + COALESCE(billed_promo_sms_15,0) + COALESCE(billed_promo_sms_16,0) else 0 end) promo_ONNET_offpeak_sms,
        sum(COALESCE(s_total_chg_17,0) + COALESCE(s_total_chg_18,0) + COALESCE(s_total_chg_19,0) + COALESCE(s_total_chg_20,0) + COALESCE(s_total_chg_21,0) + COALESCE(s_total_chg_22,0) + COALESCE(s_total_chg_23,0)) s_peak_chg,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(s_total_chg_17,0) + COALESCE(s_total_chg_18,0) + COALESCE(s_total_chg_19,0) + COALESCE(s_total_chg_20,0) + COALESCE(s_total_chg_21,0) + COALESCE(s_total_chg_22,0) + COALESCE(s_total_chg_23,0) else 0 end) s_OFFNET_peak_chg,
        sum(case when upper(typ) = 'ONNET' then COALESCE(s_total_chg_17,0) + COALESCE(s_total_chg_18,0) + COALESCE(s_total_chg_19,0) + COALESCE(s_total_chg_20,0) + COALESCE(s_total_chg_21,0) + COALESCE(s_total_chg_22,0) + COALESCE(s_total_chg_23,0) else 0 end) s_ONNET_peak_chg,
        sum(COALESCE(billed_sms_17,0) + COALESCE(billed_sms_18,0) + COALESCE(billed_sms_19,0) + COALESCE(billed_sms_20,0) + COALESCE(billed_sms_21,0) + COALESCE(billed_sms_22,0) + COALESCE(billed_sms_23,0)) billed_peak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_sms_17,0) + COALESCE(billed_sms_18,0) + COALESCE(billed_sms_19,0) + COALESCE(billed_sms_20,0) + COALESCE(billed_sms_21,0) + COALESCE(billed_sms_22,0) + COALESCE(billed_sms_23,0) else 0 end) billed_OFFNET_peak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_sms_17,0) + COALESCE(billed_sms_18,0) + COALESCE(billed_sms_19,0) + COALESCE(billed_sms_20,0) + COALESCE(billed_sms_21,0) + COALESCE(billed_sms_22,0) + COALESCE(billed_sms_23,0) else 0 end) billed_ONNET_peak_sms,
        sum(COALESCE(billed_promo_sms_17,0) + COALESCE(billed_promo_sms_18,0) + COALESCE(billed_promo_sms_19,0) + COALESCE(billed_promo_sms_20,0) + COALESCE(billed_promo_sms_21,0) + COALESCE(billed_promo_sms_22,0) + COALESCE(billed_promo_sms_23,0)) promo_peak_sms,
        sum(case when upper(typ) = 'OFFNET' then COALESCE(billed_promo_sms_17,0) + COALESCE(billed_promo_sms_18,0) + COALESCE(billed_promo_sms_19,0) + COALESCE(billed_promo_sms_20,0) + COALESCE(billed_promo_sms_21,0) + COALESCE(billed_promo_sms_22,0) + COALESCE(billed_promo_sms_23,0) else 0 end) promo_OFFNET_peak_sms,
        sum(case when upper(typ) = 'ONNET' then COALESCE(billed_promo_sms_17,0) + COALESCE(billed_promo_sms_18,0) + COALESCE(billed_promo_sms_19,0) + COALESCE(billed_promo_sms_20,0) + COALESCE(billed_promo_sms_21,0) + COALESCE(billed_promo_sms_22,0) + COALESCE(billed_promo_sms_23,0) else 0 end) promo_ONNET_peak_sms,
        0 bb_uplink_tr,
        0 bb_downlink_tr,
        0 non_bb_uplink_tr,
        0 non_bb_downlink_tr,
        0 total_4g_uplink_tr,
        0 total_4g_downlink_tr,
        0 total_3g_uplink_tr,
        0 total_3g_downlink_tr,
        0 total_2g_uplink_tr,
        0 total_2g_downlink_tr,
        0 offpeak_data_downlink_tr,
        0 offpeak_data_uplink_tr,
        0 peak_data_downlink_tr,
        0 peak_data_uplink_tr,
        0 num_days_data,
        0 sms_hits,
        0 onnet_sms_hits,
        0 offnet_sms_hits
        ,0 num_days_sms
    FROM USG_SMS_DLY_LACCI
    GROUP BY
        a_party_no,
        COALESCE(lac, 0), COALESCE(ci, 0)
"""
  
  val queryDataTrafficSmyOld="""
    SELECT 
       MSISDN
      ,COALESCE(lac,0) lac
      ,COALESCE(ci,0) ci 
      ,0 v_total_chg
      ,0 v_offnet_chg
      ,0 v_onnet_chg
      ,0 billed_dur
      ,0 billed_offnet_dur
      ,0 billed_onnet_dur
      ,0 promo_dur
      ,0 promo_offnet_dur
      ,0 promo_onnet_dur
      ,0 tot_dur
      ,0 v_offnet_dur
      ,0 v_onnet_dur
      ,0 v_offpeak_chg
      ,0 v_offnet_offpeak_chg
      ,0 v_onnet_offpeak_chg
      ,0 billed_offpeak_dur
      ,0 billed_offnet_offpeak_dur
      ,0 billed_onnet_offpeak_dur
      ,0 promo_offpeak_dur
      ,0 promo_offnet_offpeak_dur
      ,0 promo_onnet_offpeak_dur
      ,0 total_offpeak_dur
      ,0 v_offnet_offpeak_dur
      ,0 v_onnet_offpeak_dur
      ,0 v_peak_chg
      ,0 v_offnet_peak_chg
      ,0 v_onnet_peak_chg
      ,0 billed_peak_dur
      ,0 billed_offnet_peak_dur
      ,0 billed_onnet_peak_dur
      ,0 promo_peak_dur
      ,0 promo_offnet_peak_dur
      ,0 promo_onnet_peak_dur
      ,0 total_peak_dur
      ,0 v_offnet_peak_dur
      ,0 v_onnet_peak_dur
      ,0 num_days_voice
      ,0 g_total_chg
      ,0 ppu_datavol
      ,0 addon_datavol
      ,0 total_datavol
      ,0 g_4g_total_chg
      ,0 g_3g_total_chg
      ,0 g_2g_total_chg
      ,0 g_4g_ppu_datavol
      ,0 g_3g_ppu_datavol
      ,0 g_2g_ppu_datavol
      ,0 g_4g_addon_datavol
      ,0 g_3g_addon_datavol
      ,0 g_2g_addon_datavol
      ,0 g_offpeak_chg
      ,0 g_peak_chg
      ,0 ppu_offpeak_datavol
      ,0 ppu_peak_datavol
      ,0 addon_offpeak_datavol
      ,0 addon_peak_datavol
      ,0 total_offpeak_datavol
      ,0 total_peak_datavol
      ,0 s_total_chg
      ,0 s_offnet_chg
      ,0 s_onnet_chg
      ,0 billed_sms
      ,0 billed_offnet_sms
      ,0 billed_onnet_sms
      ,0 promo_sms
      ,0 promo_offnet_sms
      ,0 promo_onnet_sms
      ,0 s_offpeak_chg
      ,0 s_offnet_offpeak_chg
      ,0 s_onnet_offpeak_chg
      ,0 billed_offpeak_sms
      ,0 billed_offnet_offpeak_sms
      ,0 billed_onnet_offpeak_sms
      ,0 promo_offpeak_sms
      ,0 promo_offnet_offpeak_sms
      ,0 promo_onnet_offpeak_sms
      ,0 s_peak_chg
      ,0 s_offnet_peak_chg
      ,0 s_onnet_peak_chg
      ,0 billed_peak_sms
      ,0 billed_offnet_peak_sms
      ,0 billed_onnet_peak_sms
      ,0 promo_peak_sms
      ,0 promo_offnet_peak_sms
      ,0 promo_onnet_peak_sms
      ,sum(case when acs_pnt_nm_ni_id = 146 then COALESCE(uplink,0) else 0 end) bb_uplink_tr
      ,sum(case when acs_pnt_nm_ni_id = 146 then COALESCE(downlink,0) else 0 end) bb_downlink_tr
      ,sum(case when acs_pnt_nm_ni_id = 146 then 0 else COALESCE(uplink,0) end) non_bb_uplink_tr
      ,sum(case when acs_pnt_nm_ni_id = 146 then 0 else COALESCE(downlink,0) end) non_bb_downlink_tr
      ,sum(case when rat_tp = 6 then COALESCE(uplink,0) else 0 end) total_4g_uplink_tr
      ,sum(case when rat_tp = 6 then COALESCE(downlink,0) else 0 end) total_4g_downlink_tr
      ,sum(case when rat_tp = 1 then COALESCE(uplink,0) else 0 end) total_3g_uplink_tr
      ,sum(case when rat_tp = 1 then COALESCE(downlink,0) else 0 end) total_3g_downlink_tr
      ,sum(case when rat_tp = 2 then COALESCE(uplink,0) else 0 end) total_2g_uplink_tr
      ,sum(case when rat_tp = 2 then COALESCE(downlink,0) else 0 end) total_2g_downlink_tr
      ,sum(case when tb = '00.00-16.59' then COALESCE(downlink,0) else 0 end) offpeak_data_downlink_tr
      ,sum(case when tb = '00.00-16.59' then COALESCE(uplink,0) else 0 end) offpeak_data_uplink_tr
      ,sum(case when tb = '17.00-23.59' then COALESCE(downlink,0) else 0 end) peak_data_downlink_tr
      ,sum(case when tb = '17.00-23.59' then COALESCE(uplink,0) else 0 end) peak_data_uplink_tr
      ,count(distinct DAYDATE) num_days_data
      ,0 sms_hits
      ,0 onnet_sms_hits
      ,0 offnet_sms_hits
      ,0 num_days_sms
    FROM 
      mbs_tmp_data
    GROUP BY 
      msisdn
    , COALESCE(lac, 0), COALESCE(ci, 0)
"""
  val querySmsTrafficSmyOld="""
    select 
       msisdn,
       lac,
       ci, 
       0 v_total_chg,
       0 v_offnet_chg,
       0 v_onnet_chg,
       0 billed_dur,
       0 billed_offnet_dur,
       0 billed_onnet_dur,
       0 promo_dur,
       0 promo_offnet_dur,
       0 promo_onnet_dur,
       0 tot_dur,
       0 v_offnet_dur,
       0 v_onnet_dur,
       0 v_offpeak_chg,
       0 v_offnet_offpeak_chg,
       0 v_onnet_offpeak_chg,
       0 billed_offpeak_dur,
       0 billed_offnet_offpeak_dur,
       0 billed_onnet_offpeak_dur,
       0 promo_offpeak_dur,
       0 promo_offnet_offpeak_dur,
       0 promo_onnet_offpeak_dur,
       0 total_offpeak_dur,
       0 v_offnet_offpeak_dur,
       0 v_onnet_offpeak_dur,
       0 v_peak_chg,
       0 v_offnet_peak_chg,
       0 v_onnet_peak_chg,
       0 billed_peak_dur,
       0 billed_offnet_peak_dur,
       0 billed_onnet_peak_dur,
       0 promo_peak_dur,
       0 promo_offnet_peak_dur,
       0 promo_onnet_peak_dur,
       0 total_peak_dur,
       0 v_offnet_peak_dur,
       0 v_onnet_peak_dur,
       0 num_days_voice,
       0 g_total_chg,
       0 ppu_datavol,
       0 addon_datavol,
       0 total_datavol,
       0 g_4g_total_chg,
       0 g_3g_total_chg,
       0 g_2g_total_chg,
       0 g_4g_ppu_datavol,
       0 g_3g_ppu_datavol,
       0 g_2g_ppu_datavol,
       0 g_4g_addon_datavol,
       0 g_3g_addon_datavol,
       0 g_2g_addon_datavol,
       0 g_offpeak_chg,
       0 g_peak_chg,
       0 ppu_offpeak_datavol,
       0 ppu_peak_datavol,
       0 addon_offpeak_datavol,
       0 addon_peak_datavol,
       0 total_offpeak_datavol,
       0 total_peak_datavol,
       0 s_total_chg,
       0 s_offnet_chg,
       0 s_onnet_chg,
       0 billed_sms,
       0 billed_offnet_sms,
       0 billed_onnet_sms,
       0 promo_sms,
       0 promo_offnet_sms,
       0 promo_onnet_sms,
       0 s_offpeak_chg,
       0 s_offnet_offpeak_chg,
       0 s_onnet_offpeak_chg,
       0 billed_offpeak_sms,
       0 billed_offnet_offpeak_sms,
       0 billed_onnet_offpeak_sms,
       0 promo_offpeak_sms,
       0 promo_offnet_offpeak_sms,
       0 promo_onnet_offpeak_sms,
       0 s_peak_chg,
       0 s_offnet_peak_chg,
       0 s_onnet_peak_chg,
       0 billed_peak_sms,
       0 billed_offnet_peak_sms,
       0 billed_onnet_peak_sms,
       0 promo_peak_sms,
       0 promo_offnet_peak_sms,
       0 promo_onnet_peak_sms,
       0 bb_uplink_tr,
       0 bb_downlink_tr,
       0 non_bb_uplink_tr,
       0 non_bb_downlink_tr,
       0 total_4g_uplink_tr,
       0 total_4g_downlink_tr,
       0 total_3g_uplink_tr,
       0 total_3g_downlink_tr,
       0 total_2g_uplink_tr,
       0 total_2g_downlink_tr,
       0 offpeak_data_downlink_tr,
       0 offpeak_data_uplink_tr,
       0 peak_data_downlink_tr,
       0 peak_data_uplink_tr,
       0 num_days_data,
       sum(cnt) sms_hits,
       sum(case when upper(typ) = 'onnet' then cnt else 0 end) onnet_sms_hits,
       sum(case when upper(typ) = 'offnet' then cnt else 0 end) offnet_sms_hits
       ,count(distinct daydate) num_days_sms
    from TRAFFIC_SMS_DLY_LACCI
       group by msisdn,
       lac,
       ci
"""
  val queryUsgAllSvcLacciOld="""
    select 
        msisdn
       ,lac
       ,ci
       ,sum(v_total_chg) v_total_chg
       ,sum(v_offnet_chg) v_offnet_chg
       ,sum(v_onnet_chg) v_onnet_chg
       ,sum(billed_dur) billed_dur
       ,sum(billed_offnet_dur) billed_offnet_dur
       ,sum(billed_onnet_dur) billed_onnet_dur
       ,sum(promo_dur) promo_dur
       ,sum(promo_offnet_dur) promo_offnet_dur
       ,sum(promo_onnet_dur) promo_onnet_dur
       ,sum(tot_dur) tot_dur
       ,sum(v_offnet_dur) v_offnet_dur
       ,sum(v_onnet_dur) v_onnet_dur
       ,sum(v_offpeak_chg) v_offpeak_chg
       ,sum(v_offnet_offpeak_chg) v_offnet_offpeak_chg
       ,sum(v_onnet_offpeak_chg) v_onnet_offpeak_chg
       ,sum(billed_offpeak_dur) billed_offpeak_dur
       ,sum(billed_offnet_offpeak_dur) billed_offnet_offpeak_dur
       ,sum(billed_onnet_offpeak_dur) billed_onnet_offpeak_dur
       ,sum(promo_offpeak_dur) promo_offpeak_dur
       ,sum(promo_offnet_offpeak_dur) promo_offnet_offpeak_dur
       ,sum(promo_onnet_offpeak_dur) promo_onnet_offpeak_dur
       ,sum(total_offpeak_dur) total_offpeak_dur
       ,sum(v_offnet_offpeak_dur) v_offnet_offpeak_dur
       ,sum(v_onnet_offpeak_dur) v_onnet_offpeak_dur
       ,sum(v_peak_chg) v_peak_chg
       ,sum(v_offnet_peak_chg) v_offnet_peak_chg
       ,sum(v_onnet_peak_chg) v_onnet_peak_chg
       ,sum(billed_peak_dur) billed_peak_dur
       ,sum(billed_offnet_peak_dur) billed_offnet_peak_dur
       ,sum(billed_onnet_peak_dur) billed_onnet_peak_dur
       ,sum(promo_peak_dur) promo_peak_dur
       ,sum(promo_offnet_peak_dur) promo_offnet_peak_dur
       ,sum(promo_onnet_peak_dur) promo_onnet_peak_dur
       ,sum(total_peak_dur) total_peak_dur
       ,sum(v_offnet_peak_dur) v_offnet_peak_dur
       ,sum(v_onnet_peak_dur) v_onnet_peak_dur
       ,sum(num_days_voice) num_days_voice
       ,sum(g_total_chg) g_total_chg
       ,sum(ppu_datavol) ppu_datavol
       ,sum(addon_datavol) addon_datavol
       ,sum(total_datavol) total_datavol
       ,sum(g_4g_total_chg) g_4g_total_chg
       ,sum(g_3g_total_chg) g_3g_total_chg
       ,sum(g_2g_total_chg) g_2g_total_chg
       ,sum(g_4g_ppu_datavol) g_4g_ppu_datavol
       ,sum(g_3g_ppu_datavol) g_3g_ppu_datavol
       ,sum(g_2g_ppu_datavol) g_2g_ppu_datavol
       ,sum(g_4g_addon_datavol) g_4g_addon_datavol
       ,sum(g_3g_addon_datavol) g_3g_addon_datavol
       ,sum(g_2g_addon_datavol) g_2g_addon_datavol
       ,sum(g_offpeak_chg) g_offpeak_chg
       ,sum(g_peak_chg) g_peak_chg
       ,sum(ppu_offpeak_datavol) ppu_offpeak_datavol
       ,sum(ppu_peak_datavol) ppu_peak_datavol
       ,sum(addon_offpeak_datavol) addon_offpeak_datavol
       ,sum(addon_peak_datavol) addon_peak_datavol
       ,sum(total_offpeak_datavol) total_offpeak_datavol
       ,sum(total_peak_datavol) total_peak_datavol
       ,sum(s_total_chg) s_total_chg
       ,sum(s_offnet_chg) s_offnet_chg
       ,sum(s_onnet_chg) s_onnet_chg
       ,sum(billed_sms) billed_sms
       ,sum(billed_offnet_sms) billed_offnet_sms
       ,sum(billed_onnet_sms) billed_onnet_sms
       ,sum(promo_sms) promo_sms
       ,sum(promo_offnet_sms) promo_offnet_sms
       ,sum(promo_onnet_sms) promo_onnet_sms
       ,sum(s_offpeak_chg) s_offpeak_chg
       ,sum(s_offnet_offpeak_chg) s_offnet_offpeak_chg
       ,sum(s_onnet_offpeak_chg) s_onnet_offpeak_chg
       ,sum(billed_offpeak_sms) billed_offpeak_sms
       ,sum(billed_offnet_offpeak_sms) billed_offnet_offpeak_sms
       ,sum(billed_onnet_offpeak_sms) billed_onnet_offpeak_sms
       ,sum(promo_offpeak_sms) promo_offpeak_sms
       ,sum(promo_offnet_offpeak_sms) promo_offnet_offpeak_sms
       ,sum(promo_onnet_offpeak_sms) promo_onnet_offpeak_sms
       ,sum(s_peak_chg) s_peak_chg
       ,sum(s_offnet_peak_chg) s_offnet_peak_chg
       ,sum(s_onnet_peak_chg) s_onnet_peak_chg
       ,sum(billed_peak_sms) billed_peak_sms
       ,sum(billed_offnet_peak_sms) billed_offnet_peak_sms
       ,sum(billed_onnet_peak_sms) billed_onnet_peak_sms
       ,sum(promo_peak_sms) promo_peak_sms
       ,sum(promo_offnet_peak_sms) promo_offnet_peak_sms
       ,sum(promo_onnet_peak_sms) promo_onnet_peak_sms
       ,sum(bb_uplink_tr) bb_uplink_tr
       ,sum(bb_downlink_tr) bb_downlink_tr
       ,sum(non_bb_uplink_tr) non_bb_uplink_tr
       ,sum(non_bb_downlink_tr) non_bb_downlink_tr
       ,sum(total_4g_uplink_tr) total_4g_uplink_tr
       ,sum(total_4g_downlink_tr) total_4g_downlink_tr
       ,sum(total_3g_uplink_tr) total_3g_uplink_tr
       ,sum(total_3g_downlink_tr) total_3g_downlink_tr
       ,sum(total_2g_uplink_tr) total_2g_uplink_tr
       ,sum(total_2g_downlink_tr) total_2g_downlink_tr
       ,sum(offpeak_data_downlink_tr) offpeak_data_downlink_tr
       ,sum(offpeak_data_uplink_tr) offpeak_data_uplink_tr
       ,sum(peak_data_downlink_tr) peak_data_downlink_tr
       ,sum(peak_data_uplink_tr) peak_data_uplink_tr
       ,sum(num_days_data) num_days_data
       ,sum(sms_hits) sms_hits
       ,sum(onnet_sms_hits) onnet_sms_hits
       ,sum(offnet_sms_hits) offnet_sms_hits
       ,sum(num_days_sms) num_days_sms
      from MERGE_USG_TRAFFIC
      group by msisdn
        ,lac
        ,ci
"""
   val queryUsgAllSvcLacci="""
    select 
        msisdn
       ,lac
       ,ci
       ,sum(v_total_chg) v_total_chg
       ,sum(v_offnet_chg) v_offnet_chg
       ,sum(v_onnet_chg) v_onnet_chg
       ,sum(billed_dur) billed_dur
       ,sum(billed_offnet_dur) billed_offnet_dur
       ,sum(billed_onnet_dur) billed_onnet_dur
       ,sum(promo_dur) promo_dur
       ,sum(promo_offnet_dur) promo_offnet_dur
       ,sum(promo_onnet_dur) promo_onnet_dur
       ,sum(tot_dur) tot_dur
       ,sum(v_offnet_dur) v_offnet_dur
       ,sum(v_onnet_dur) v_onnet_dur
       ,sum(v_offpeak_chg) v_offpeak_chg
       ,sum(v_offnet_offpeak_chg) v_offnet_offpeak_chg
       ,sum(v_onnet_offpeak_chg) v_onnet_offpeak_chg
       ,sum(billed_offpeak_dur) billed_offpeak_dur
       ,sum(billed_offnet_offpeak_dur) billed_offnet_offpeak_dur
       ,sum(billed_onnet_offpeak_dur) billed_onnet_offpeak_dur
       ,sum(promo_offpeak_dur) promo_offpeak_dur
       ,sum(promo_offnet_offpeak_dur) promo_offnet_offpeak_dur
       ,sum(promo_onnet_offpeak_dur) promo_onnet_offpeak_dur
       ,sum(total_offpeak_dur) total_offpeak_dur
       ,sum(v_offnet_offpeak_dur) v_offnet_offpeak_dur
       ,sum(v_onnet_offpeak_dur) v_onnet_offpeak_dur
       ,sum(v_peak_chg) v_peak_chg
       ,sum(v_offnet_peak_chg) v_offnet_peak_chg
       ,sum(v_onnet_peak_chg) v_onnet_peak_chg
       ,sum(billed_peak_dur) billed_peak_dur
       ,sum(billed_offnet_peak_dur) billed_offnet_peak_dur
       ,sum(billed_onnet_peak_dur) billed_onnet_peak_dur
       ,sum(promo_peak_dur) promo_peak_dur
       ,sum(promo_offnet_peak_dur) promo_offnet_peak_dur
       ,sum(promo_onnet_peak_dur) promo_onnet_peak_dur
       ,sum(total_peak_dur) total_peak_dur
       ,sum(v_offnet_peak_dur) v_offnet_peak_dur
       ,sum(v_onnet_peak_dur) v_onnet_peak_dur
       ,sum(num_days_voice) num_days_voice
       ,sum(g_total_chg) g_total_chg
       ,sum(ppu_datavol) ppu_datavol
       ,sum(addon_datavol) addon_datavol
       ,sum(total_datavol) total_datavol
       ,sum(g_4g_total_chg) g_4g_total_chg
       ,sum(g_3g_total_chg) g_3g_total_chg
       ,sum(g_2g_total_chg) g_2g_total_chg
       ,sum(g_4g_ppu_datavol) g_4g_ppu_datavol
       ,sum(g_3g_ppu_datavol) g_3g_ppu_datavol
       ,sum(g_2g_ppu_datavol) g_2g_ppu_datavol
       ,sum(g_4g_addon_datavol) g_4g_addon_datavol
       ,sum(g_3g_addon_datavol) g_3g_addon_datavol
       ,sum(g_2g_addon_datavol) g_2g_addon_datavol
       ,sum(g_offpeak_chg) g_offpeak_chg
       ,sum(g_peak_chg) g_peak_chg
       ,sum(ppu_offpeak_datavol) ppu_offpeak_datavol
       ,sum(ppu_peak_datavol) ppu_peak_datavol
       ,sum(addon_offpeak_datavol) addon_offpeak_datavol
       ,sum(addon_peak_datavol) addon_peak_datavol
       ,sum(total_offpeak_datavol) total_offpeak_datavol
       ,sum(total_peak_datavol) total_peak_datavol
       ,sum(s_total_chg) s_total_chg
       ,sum(s_offnet_chg) s_offnet_chg
       ,sum(s_onnet_chg) s_onnet_chg
       ,sum(billed_sms) billed_sms
       ,sum(billed_offnet_sms) billed_offnet_sms
       ,sum(billed_onnet_sms) billed_onnet_sms
       ,sum(promo_sms) promo_sms
       ,sum(promo_offnet_sms) promo_offnet_sms
       ,sum(promo_onnet_sms) promo_onnet_sms
       ,sum(s_offpeak_chg) s_offpeak_chg
       ,sum(s_offnet_offpeak_chg) s_offnet_offpeak_chg
       ,sum(s_onnet_offpeak_chg) s_onnet_offpeak_chg
       ,sum(billed_offpeak_sms) billed_offpeak_sms
       ,sum(billed_offnet_offpeak_sms) billed_offnet_offpeak_sms
       ,sum(billed_onnet_offpeak_sms) billed_onnet_offpeak_sms
       ,sum(promo_offpeak_sms) promo_offpeak_sms
       ,sum(promo_offnet_offpeak_sms) promo_offnet_offpeak_sms
       ,sum(promo_onnet_offpeak_sms) promo_onnet_offpeak_sms
       ,sum(s_peak_chg) s_peak_chg
       ,sum(s_offnet_peak_chg) s_offnet_peak_chg
       ,sum(s_onnet_peak_chg) s_onnet_peak_chg
       ,sum(billed_peak_sms) billed_peak_sms
       ,sum(billed_offnet_peak_sms) billed_offnet_peak_sms
       ,sum(billed_onnet_peak_sms) billed_onnet_peak_sms
       ,sum(promo_peak_sms) promo_peak_sms
       ,sum(promo_offnet_peak_sms) promo_offnet_peak_sms
       ,sum(promo_onnet_peak_sms) promo_onnet_peak_sms
       ,sum(bb_uplink_tr) bb_uplink_tr
       ,sum(bb_downlink_tr) bb_downlink_tr
       ,sum(non_bb_uplink_tr) non_bb_uplink_tr
       ,sum(non_bb_downlink_tr) non_bb_downlink_tr
       ,sum(total_4g_uplink_tr) total_4g_uplink_tr
       ,sum(total_4g_downlink_tr) total_4g_downlink_tr
       ,sum(total_3g_uplink_tr) total_3g_uplink_tr
       ,sum(total_3g_downlink_tr) total_3g_downlink_tr
       ,sum(total_2g_uplink_tr) total_2g_uplink_tr
       ,sum(total_2g_downlink_tr) total_2g_downlink_tr
       ,sum(offpeak_data_downlink_tr) offpeak_data_downlink_tr
       ,sum(offpeak_data_uplink_tr) offpeak_data_uplink_tr
       ,sum(peak_data_downlink_tr) peak_data_downlink_tr
       ,sum(peak_data_uplink_tr) peak_data_uplink_tr
       ,sum(num_days_data) num_days_data
       ,sum(sms_hits) sms_hits
       ,sum(onnet_sms_hits) onnet_sms_hits
       ,sum(offnet_sms_hits) offnet_sms_hits
       ,sum(num_days_sms) num_days_sms
       , '201709' month_id
      from MERGE_USG_TRAFFIC
      group by msisdn
        ,lac
        ,ci
"""
   val queryAllSvcLacciSubAlloc="""
    select 
       a.*
      , b.national_dur
      , b.national_bb_tr
      , b.national_non_bb_tr
      , b.national_sms_hits
      ,case when national_dur = 0 then 0 else tot_dur/national_dur end v_alloc_pct
      ,case when national_sms_hits = 0 then 0 else sms_hits/national_sms_hits end s_alloc_pct
      ,case when national_bb_tr = 0 then 0 else (nvl(bb_uplink_tr,0)+nvl(bb_downlink_tr,0))/national_bb_tr end bb_alloc_pct
      ,case when national_non_bb_tr = 0 then 0 else (nvl(non_bb_uplink_tr,0)+nvl(non_bb_downlink_tr,0))/national_non_bb_tr end non_bb_alloc_pct
      ,'201709' MONTH_ID
   from USG_ALL_SVC_LACCI_MO_SMY a join
     (select msisdn
               ,sum(nvl(tot_dur,0)) national_dur
               ,sum(nvl(bb_downlink_tr,0)) + sum(nvl(bb_uplink_tr,0)) national_bb_tr
               ,sum(nvl(non_bb_downlink_tr,0)) + sum(nvl(non_bb_uplink_tr,0)) national_non_bb_tr
               ,sum(nvl(sms_hits,0)) national_sms_hits
         from USG_ALL_SVC_LACCI_MO_SMY
         group by msisdn) b
    on a.msisdn = b.msisdn
"""
   val querySummary="""
    SELECT
       subscriber,
       SUM(v_total_chg) v_total_chg,
       SUM(g_total_chg) g_total_chg,
       SUM(s_total_chg) s_total_chg,
       SUM(va_voice_chg) va_voice_chg,
       SUM(va_sms_chg) va_sms_chg,
       SUM(COALESCE(va_lscr_chg,0)) + SUM(COALESCE(va_smscr_chg,0)) va_data_chg,
       SUM(va_bb_chg) va_bb_chg,
       SUM(va_total_chg) va_total_chg,
       SUM(trg_total_tr) trg_total_tr,
       SUM(trs_total_tr) trs_total_tr,
       SUM(v_total_dur) v_total_dur
    FROM 
       summary_daily_cs5
    GROUP BY 
       subscriber
"""
    val queryCustValueScore="""
    SELECT 
       MSISDN,
       CASE
          WHEN COALESCE(TOTAL_CHG,0) > 100000 THEN 'A) > 100.000 Rp'
          WHEN COALESCE(TOTAL_CHG,0) > 50000 THEN  'B) 50.000-100.000 Rp'
          WHEN COALESCE(TOTAL_CHG,0) > 20000 THEN  'C) 20.000-50.000 Rp'
          WHEN COALESCE(TOTAL_CHG,0) > 0 THEN 'D) 1-20.000 Rp'
          ELSE 'E) = 0 Rp' end as TOTAL_CHG_GRP,
       CASE 
          WHEN COALESCE(DATA_CHG,0) > 50000 THEN 'A) > 50.000 Rp'
          WHEN COALESCE(DATA_CHG,0) > 20000 THEN 'B) 20.000-50.000 Rp'
          WHEN COALESCE(DATA_CHG,0) > 0 THEN 'C) 1-20.000 Rp'
          ELSE 'D) = 0 Rp' end as DATA_CHG_GRP,
       TOTAL_CHG, DATA_CHG,
       VA_VOICE_CHG,
       VA_SMS_CHG,
       VA_BB_CHG,
       VA_DATA_CHG,
       VA_TOTAL_CHG,
       TYP
    FROM
       (
         SELECT 
            SUBSCRIBER MSISDN,
            COALESCE(VA_VOICE_CHG,0) VA_VOICE_CHG,
            COALESCE(VA_SMS_CHG,0) VA_SMS_CHG,
            COALESCE(VA_BB_CHG,0) VA_BB_CHG,
            COALESCE(VA_DATA_CHG,0) VA_DATA_CHG,
            COALESCE(VA_TOTAL_CHG,0) VA_TOTAL_CHG,
            COALESCE(V_TOTAL_CHG,0) + COALESCE(S_TOTAL_CHG,0) + COALESCE(G_TOTAL_CHG,0) + COALESCE(VA_VOICE_CHG,0) + COALESCE(VA_SMS_CHG,0) + COALESCE(VA_DATA_CHG,0) + COALESCE(VA_BB_CHG,0) + COALESCE(VA_TOTAL_CHG,0) TOTAL_CHG,
            COALESCE(G_TOTAL_CHG,0) + COALESCE(VA_BB_CHG,0) + COALESCE(VA_DATA_CHG,0) DATA_CHG,
            CASE WHEN (COALESCE(V_TOTAL_DUR,0) > 0 or COALESCE(V_TOTAL_CHG,0) > 0)
               and COALESCE(TRG_TOTAL_TR,0) = 0 and COALESCE(G_TOTAL_CHG,0) = 0 and COALESCE(VA_DATA_CHG,0) = 0 and COALESCE(VA_BB_CHG,0) = 0 and COALESCE(VA_VOICE_CHG,0) = 0 and COALESCE(VA_SMS_CHG,0) = 0 and COALESCE(VA_TOTAL_CHG,0) = 0
                            and COALESCE(TRS_TOTAL_TR,0) = 0 and COALESCE(S_TOTAL_CHG,0) = 0
                       THEN 'VOICE ONLY'
                       WHEN COALESCE(V_TOTAL_DUR,0) = 0 and COALESCE(V_TOTAL_CHG,0) = 0
                            and (COALESCE(TRG_TOTAL_TR,0) > 0 or COALESCE(G_TOTAL_CHG,0) > 0 or COALESCE(VA_BB_CHG,0) > 0 or COALESCE(VA_DATA_CHG,0) > 0 or COALESCE(VA_VOICE_CHG,0) > 0 or COALESCE(VA_SMS_CHG,0) > 0 or COALESCE(VA_TOTAL_CHG,0) > 0)
                            and COALESCE(TRS_TOTAL_TR,0) = 0 and COALESCE(S_TOTAL_CHG,0) = 0
                       THEN 'DATA/VAS ONLY'
                       WHEN COALESCE(V_TOTAL_DUR,0) = 0 and COALESCE(V_TOTAL_CHG,0) = 0
                            and COALESCE(TRG_TOTAL_TR,0) = 0 and COALESCE(G_TOTAL_CHG,0) = 0 and COALESCE(VA_BB_CHG,0) = 0 and COALESCE(VA_DATA_CHG,0) = 0 and COALESCE(VA_VOICE_CHG,0) = 0 and COALESCE(VA_SMS_CHG,0) = 0 and COALESCE(VA_TOTAL_CHG,0) = 0
                            and (COALESCE(TRS_TOTAL_TR,0) > 0 or COALESCE(S_TOTAL_CHG,0) > 0)
                       THEN 'SMS ONLY'
                       WHEN ((COALESCE(V_TOTAL_DUR,0) > 0 or COALESCE(V_TOTAL_CHG,0) > 0) and (COALESCE(TRG_TOTAL_TR,0) > 0 or COALESCE(G_TOTAL_CHG,0) > 0 or COALESCE(VA_BB_CHG,0) > 0 or COALESCE(VA_DATA_CHG,0) > 0 or COALESCE(VA_VOICE_CHG,0) > 0 or COALESCE(VA_SMS_CHG,0) > 0 or COALESCE(VA_TOTAL_CHG,0) > 0))
                            and COALESCE(TRS_TOTAL_TR,0) = 0 and COALESCE(S_TOTAL_CHG,0) = 0
                       THEN 'VOICE + DATA/VAS ONLY'
                       WHEN ((COALESCE(V_TOTAL_DUR,0) > 0 or COALESCE(V_TOTAL_CHG,0) > 0) and (COALESCE(TRS_TOTAL_TR,0) > 0 or COALESCE(S_TOTAL_CHG,0) > 0))
                            and COALESCE(TRG_TOTAL_TR,0) = 0 and COALESCE(G_TOTAL_CHG,0) = 0 and COALESCE(VA_BB_CHG,0) = 0 and COALESCE(VA_DATA_CHG,0) = 0 and COALESCE(VA_VOICE_CHG,0) = 0 and COALESCE(VA_SMS_CHG,0) = 0 and COALESCE(VA_TOTAL_CHG,0) = 0
                       THEN 'VOICE + SMS ONLY'
                       WHEN COALESCE(V_TOTAL_DUR,0) = 0 and COALESCE(V_TOTAL_CHG,0) = 0
                            and ((COALESCE(TRG_TOTAL_TR,0) > 0 or COALESCE(G_TOTAL_CHG,0) > 0 or COALESCE(VA_BB_CHG,0) > 0 or COALESCE(VA_DATA_CHG,0) > 0 or COALESCE(VA_VOICE_CHG,0) > 0 or COALESCE(VA_SMS_CHG,0) > 0 or COALESCE(VA_TOTAL_CHG,0) > 0) and (COALESCE(TRS_TOTAL_TR,0) > 0 or COALESCE(S_TOTAL_CHG,0) > 0))
                       THEN 'SMS + DATA/VAS ONLY'
                       WHEN ((COALESCE(V_TOTAL_DUR,0) > 0 or COALESCE(V_TOTAL_CHG,0) > 0) and (COALESCE(TRG_TOTAL_TR,0) > 0 or COALESCE(G_TOTAL_CHG,0) > 0 or COALESCE(VA_BB_CHG,0) > 0 or COALESCE(VA_DATA_CHG,0) > 0 or COALESCE(VA_SMS_CHG,0) > 0 or COALESCE(VA_VOICE_CHG,0) > 0 or COALESCE(VA_TOTAL_CHG,0) > 0) and (COALESCE(TRS_TOTAL_TR,0) > 0 or COALESCE(S_TOTAL_CHG,0) > 0))
                       THEN 'VOICE + SMS + DATA/VAS ONLY'
                       ELSE 'OTHERS' end TYP
         FROM SUMMARY ) a
"""
    val queryPostPaidRev="""
    SELECT 
        EVENT_SOURCE MSISDN,
        EVENT_DTM DT, 
        CASE WHEN EVENT_TYPE_ID = 51 OR (EVENT_TYPE_ID = 54 AND EVENT_ATTR_17 not like '%MT%') THEN
          CASE WHEN length(regexp_replace(EVENT_ATTR_1,'\\+','')) < 5 THEN 'Internal'
          ELSE
            CASE WHEN regexp_replace(EVENT_ATTR_1,'\\+','') like '62%' THEN
              CASE WHEN substr(regexp_replace(EVENT_ATTR_1,'\\+',''),3,3) in ('814', '815', '816', '855', '856', '857', '858') OR
      substr(regexp_replace(EVENT_ATTR_1,'\\+',''),3,3) in ('213', '223', '251', '316', '341', '343', '355', '361', '542', '613', '778') THEN 'Onnet' ELSE 'Offnet' 
      				END
              ELSE 'Foreign'
  			    END
    		  END
        ELSE 'NA'
        END TYP,
    	  SUM(CASE WHEN EVENT_TYPE_ID in (40, 51) THEN EVENT_COST_MNY/10 ELSE 0 END) V_TOTAL_CHG,
        SUM(CASE WHEN EVENT_TYPE_ID in (41, 55) THEN EVENT_COST_MNY/10 ELSE 0 END) G_TOTAL_CHG,
        SUM(CASE WHEN EVENT_TYPE_ID = 54 AND EVENT_ATTR_17 not like '%MT%' THEN EVENT_COST_MNY/10 ELSE 0 END) S_TOTAL_CHG,
        SUM(CASE WHEN EVENT_TYPE_ID = 56 OR (EVENT_TYPE_ID = 54 AND EVENT_ATTR_17 like '%MT%') THEN EVENT_COST_MNY/10 ELSE 0 END) VA_TOTAL_CHG
    FROM 
        STG_RBM_COSTED_EVENT a
    WHERE 
       EVENT_TYPE_ID in (40, 41, 51, 54, 55, 56)
    GROUP BY 
       EVENT_SOURCE,
       EVENT_DTM,
       CASE WHEN EVENT_TYPE_ID = 51 OR (EVENT_TYPE_ID = 54 AND EVENT_ATTR_17 not like '%MT%') THEN
          CASE WHEN length(regexp_replace(EVENT_ATTR_1,'\\+','')) < 5 THEN 'Internal'
          ELSE
            CASE WHEN regexp_replace(EVENT_ATTR_1,'\\+','') like '62%' THEN
              CASE WHEN substr(regexp_replace(EVENT_ATTR_1,'\\+',''),3,3) in ('814', '815', '816', '855', '856', '857', '858') OR
      substr(regexp_replace(EVENT_ATTR_1,'\\+',''),3,3) in ('213', '223', '251', '316', '341', '343', '355', '361', '542', '613', '778') THEN 'Onnet' ELSE 'Offnet' 
      				END
              ELSE 'Foreign'
  			    END
    		  END
        ELSE 'NA'
        END   
    """
    val queryPostpaidCsttp="""
    SELECT 
       a.MSISDN
        , a.DT 
    	, a.TYP 
    	, a.V_TOTAL_CHG 
    	, a.G_TOTAL_CHG 
    	, a.S_TOTAL_CHG 
    	, a.VA_TOTAL_CHG 
      , b.CUSTOMER_TYPE_MOD
    FROM 
       PSTPAID_REV a 
       INNER JOIN 
       (
           SELECT DISTINCT 
              EVENT_SOURCE,
              CASE 
                 WHEN CUSTOMER_TYPE IN ('Individual') THEN 'Individual'
                 WHEN CUSTOMER_TYPE IN ('VVIP') THEN 'VVIP'
                 ELSE 'Corporate' 
              END CUSTOMER_TYPE_MOD
           FROM 
              POSTPAID_SUBS
           WHERE
    	CUSTOMER_TYPE IS NOT NULL AND CUSTOMER_TYPE not in ('Opers', 'Roam')
       ) b
    ON
       a.MSISDN = b.EVENT_SOURCE
    """
    val queryPostpaidCsttpTp="""
    SELECT
       MSISDN,
       CUSTOMER_TYPE_MOD,
       SUM(CASE WHEN TYP = 'Onnet' THEN V_TOTAL_CHG ELSE 0 END) V_ONNET_CHG,
       SUM(CASE WHEN TYP = 'Offnet' THEN V_TOTAL_CHG ELSE 0 END) V_OFFNET_CHG,
       SUM(CASE WHEN TYP in ('Onnet', 'Offnet') THEN 0 ELSE V_TOTAL_CHG END) V_OTHERS_CHG,
       SUM(CASE WHEN TYP = 'Onnet' THEN S_TOTAL_CHG ELSE 0 END) S_ONNET_CHG,
       SUM(CASE WHEN TYP = 'Offnet' THEN S_TOTAL_CHG ELSE 0 END) S_OFFNET_CHG,
       SUM(CASE WHEN TYP in ('Onnet', 'Offnet') THEN 0 ELSE S_TOTAL_CHG END) S_OTHERS_CHG,
       SUM(G_TOTAL_CHG) G_TOTAL_CHG,
       SUM(VA_TOTAL_CHG) VA_TOTAL_CHG
    FROM
       POSTPAID_REV_CUSTTYPE
    GROUP BY 
       MSISDN,
       CUSTOMER_TYPE_MOD
    """
    val queryAllSvcLacciSub="""
      select 
         a.*
         , nvl(TOTAL_CHG,0) TOTAL_CHG
         , nvl(DATA_CHG,0) DATA_CHG
         , case when nvl(TOTAL_CHG,0) > 100000 then 'Platinum'
                  when nvl(TOTAL_CHG,0) > 50000 then 'Gold'
                    when nvl(TOTAL_CHG,0) > 20000 then 'Regular'
                      when nvl(TOTAL_CHG,0) > 0 then 'Mass'
                        else 'No Usage' end SUB_CAT
         , case when nvl(DATA_CHG,0) > 50000 then 'Platinum'
                  when nvl(DATA_CHG,0) > 20000 then 'Gold'
                    when nvl(DATA_CHG,0) > 0 then 'Regular'
                      else 'Non Data Users' end SUB_DATA_CAT
         , b.TYP
         , nvl(b.VA_VOICE_CHG,0) * a.V_ALLOC_PCT VA_VOICE_CHG
         , nvl(b.VA_SMS_CHG,0) * a.S_ALLOC_PCT VA_SMS_CHG
         , nvl(b.VA_BB_CHG,0) * a.BB_ALLOC_PCT VA_BB_CHG
         , nvl(VA_DATA_CHG,0) * a.NON_BB_ALLOC_PCT VA_DATA_CHG
         , nvl(VA_TOTAL_CHG,0) * a.S_ALLOC_PCT VA_TOTAL_CHG
      from ALL_SVC_LACCI_SUB_ALLOC a
      left join
      CUSTOMER_VALUE_SCORE b
      on a.MSISDN = b.MSISDN
"""
    val queryPostpaidSmsTrMo="""
      select 
        a.*
      from TR_SMS_LACCI_MO_SMY a
        ,
        (
          select distinct EVENT_SOURCE MSISDN
          from POSTPAID_SUBS
        ) b
        where a.MSISDN = b.MSISDN
"""
    val queryPostpaidDataTrMo="""
      select 
      	A.MSISDN, 
      	A.LAC, 
      	A.CI,
      	A.G_TOTAL_CHG,
      	A.PPU_DATAVOL,
      	A.ADDON_DATAVOL,
      	A.TOTAL_DATAVOL,
      	A.G_4G_TOTAL_CHG,
      	A.G_3G_TOTAL_CHG,
      	A.G_2G_TOTAL_CHG,
      	A.G_4G_PPU_DATAVOL,
      	A.G_3G_PPU_DATAVOL,
      	A.G_2G_PPU_DATAVOL,
      	A.G_4G_ADDON_DATAVOL,
      	A.G_3G_ADDON_DATAVOL,
      	A.G_2G_ADDON_DATAVOL,
      	A.G_OFFPEAK_CHG,
      	A.G_PEAK_CHG,
      	A.PPU_OFFPEAK_DATAVOL,
      	A.PPU_PEAK_DATAVOL,
      	A.ADDON_OFFPEAK_DATAVOL,
      	A.ADDON_PEAK_DATAVOL,
      	A.TOTAL_OFFPEAK_DATAVOL,
      	A.TOTAL_PEAK_DATAVOL,
      	A.BB_UPLINK_TR,
      	A.BB_DOWNLINK_TR,
      	A.NON_BB_UPLINK_TR,
      	A.NON_BB_DOWNLINK_TR,
      	A.TOTAL_4G_UPLINK_TR,
      	A.TOTAL_4G_DOWNLINK_TR,
      	A.TOTAL_3G_UPLINK_TR,
      	A.TOTAL_3G_DOWNLINK_TR,
      	A.TOTAL_2G_UPLINK_TR,
      	A.TOTAL_2G_DOWNLINK_TR,
      	A.OFFPEAK_DATA_DOWNLINK_TR,
      	A.OFFPEAK_DATA_UPLINK_TR,
      	A.PEAK_DATA_DOWNLINK_TR,
      	A.PEAK_DATA_UPLINK_TR,
      	A.NUM_DAYS_DATA
      from ALL_SVC_LACCI_SUB_SMY A, POSTPAID_SUBS B
          where A.MSISDN = B.EVENT_SOURCE
"""
    val queryPostpaidVoiceTrMo="""
      select 
          a.MSISDN
         ,LAC
         ,CI 
         , sum(DUR) TOTAL_DUR
         , sum(case when lower(TYP) = 'onnet' then DUR else 0 end) ONNET_DUR
         , sum(case when lower(TYP) = 'offnet' then DUR else 0 end) OFFNET_DUR
         , count(distinct DAYDATE) NUM_DAYS_VOICE
      from TRAFFIC_VOICE_DLY_LACCI a, POSTPAID_SUBS b
      where a.MSISDN = b.EVENT_SOURCE
      group by a.MSISDN
      ,a.LAC
      ,a.CI
"""
    val queryPostpaidVSTrMo="""
      select 
         coalesce(a.msisdn,b.msisdn) msisdn
         , coalesce(a.lac,b.lac) lac
         , coalesce(a.ci,b.ci) ci
         , a.total_dur, a.onnet_dur, a.offnet_dur, a.num_days_voice
         , b.sms_hits, b.onnet_sms_hits, b.offnet_sms_hits, b.num_days_sms
      from POSTPAID_VOICE_TR a
      full outer join
      POSTPAID_SMS_TR b
      on a.msisdn = b.msisdn
      and a.lac = b.lac
      and a.ci = b.ci
"""
    val queryPostpaidVSGTrMo="""
      SELECT
         COALESCE(a.msisdn,b.msisdn) msisdn,
         COALESCE(a.lac,b.lac) lac, 
         COALESCE(a.ci,b.ci_new) ci,
         a.total_dur,
         a.onnet_dur,
         a.offnet_dur,
         a.num_days_voice,
         a.sms_hits,
         a.onnet_sms_hits,
         a.offnet_sms_hits,
         a.num_days_sms,
         b.bb_uplink_tr,
         b.bb_downlink_tr,
         b.non_bb_uplink_tr,
         b.non_bb_downlink_tr,
         b.total_3g_uplink_tr,
         b.total_3g_downlink_tr,
         b.total_2g_uplink_tr,
         b.total_2g_downlink_tr,
         b.offpeak_data_downlink_tr,
         b.offpeak_data_uplink_tr,
         b.peak_data_downlink_tr,
         b.peak_data_uplink_tr,
         b.num_days_data,
         G_4G_TOTAL_CHG,
         G_4G_PPU_DATAVOL, 
         G_4G_ADDON_DATAVOL,
         TOTAL_4G_UPLINK_TR,
         TOTAL_4G_DOWNLINK_TR
      FROM 
         POSTPAID_V_S_TR a FULL OUTER JOIN
         ( SELECT  
               b.*,
               CAST(NVL(regexp_replace(ci,'[^[^0-9]-]',''),0) as int) ci_new
            FROM 
               POSTPAID_DATA_TR b
          ) b ON a.msisdn = b.msisdn
      WHERE
         trim(a.lac) = trim(b.lac)
         AND trim(a.ci)= trim(b.ci)
"""
    val queryPostpaidAllTrAlloc="""
      select
           a.*
         , b.national_dur, b.national_bb_tr, b.national_non_bb_tr
         , b.national_sms_hits
         , case when national_dur = 0 then 0 else total_dur/national_dur end v_alloc_pct
         , case when national_onnet_dur = 0 then 0 else onnet_dur/national_onnet_dur end v_onnet_alloc_pct
         , case when national_offnet_dur = 0 then 0 else offnet_dur/national_offnet_dur end v_offnet_alloc_pct
         , case when national_sms_hits is null or national_sms_hits = 0 then 0 else sms_hits/national_sms_hits end s_alloc_pct
         , case when national_sms_onnet_hits is null or national_sms_onnet_hits = 0 then 0 else onnet_sms_hits/national_sms_onnet_hits end s_onnet_alloc_pct
         , case when national_sms_offnet_hits is null or national_sms_offnet_hits = 0 then 0 else offnet_sms_hits/national_sms_offnet_hits end s_offnet_alloc_pct
         , case when national_bb_tr is null or national_bb_tr = 0 then 0 else (nvl(bb_uplink_tr,0)+nvl(bb_downlink_tr,0))/national_bb_tr end bb_alloc_pct
         , case when national_non_bb_tr is null or national_non_bb_tr = 0 then 0 else (nvl(non_bb_uplink_tr,0)+nvl(non_bb_downlink_tr,0))/national_non_bb_tr end non_bb_alloc_pct
      from POSTPAID_V_S_G_TR a
       , (
           select msisdn
                  , sum(nvl(total_dur,0)) national_dur
                  , sum(nvl(onnet_dur,0)) national_onnet_dur
                  , sum(nvl(offnet_dur,0)) national_offnet_dur
                  , sum(nvl(bb_downlink_tr,0)) + sum(nvl(bb_uplink_tr,0)) national_bb_tr
                  , sum(nvl(non_bb_downlink_tr,0)) + sum(nvl(non_bb_uplink_tr,0)) national_non_bb_tr
                  , sum(nvl(sms_hits,0)) national_sms_hits
                  , sum(nvl(onnet_sms_hits,0)) national_sms_onnet_hits
                  , sum(nvl(offnet_sms_hits,0)) national_sms_offnet_hits
           from POSTPAID_V_S_G_TR
           group by msisdn
         ) b
      where a.msisdn = b.msisdn

"""
    val queryPostpaidAllTrRev="""
      SELECT a.MSISDN 
         , a.LAC 
         , a.CI 
         , a.TOTAL_DUR 
         , a.ONNET_DUR 
         , a.OFFNET_DUR 
         , a.NUM_DAYS_VOICE 
         , a.SMS_HITS 
         , a.ONNET_SMS_HITS 
         , a.OFFNET_SMS_HITS 
         , a.NUM_DAYS_SMS 
         , a.BB_UPLINK_TR 
         , a.BB_DOWNLINK_TR 
         , a.NON_BB_UPLINK_TR 
         , a.NON_BB_DOWNLINK_TR 
         , a.TOTAL_3G_UPLINK_TR 
         , a.TOTAL_3G_DOWNLINK_TR 
         , a.TOTAL_2G_UPLINK_TR 
         , a.TOTAL_2G_DOWNLINK_TR 
         , a.OFFPEAK_DATA_DOWNLINK_TR 
         , a.OFFPEAK_DATA_UPLINK_TR 
         , a.PEAK_DATA_DOWNLINK_TR 
         , a.PEAK_DATA_UPLINK_TR 
         , a.NUM_DAYS_DATA 
         , a.G_4G_TOTAL_CHG 
         , a.G_4G_PPU_DATAVOL 
         , a.G_4G_ADDON_DATAVOL 
         , a.TOTAL_4G_UPLINK_TR 
         , a.TOTAL_4G_DOWNLINK_TR 
         , a.NATIONAL_DUR 
         , a.NATIONAL_BB_TR 
         , a.NATIONAL_NON_BB_TR 
         , a.NATIONAL_SMS_HITS 
         , a.V_ALLOC_PCT 
         , a.V_ONNET_ALLOC_PCT 
         , a.V_OFFNET_ALLOC_PCT 
         , a.S_ALLOC_PCT 
         , a.S_ONNET_ALLOC_PCT 
         , a.S_OFFNET_ALLOC_PCT 
         , a.BB_ALLOC_PCT 
         , a.NON_BB_ALLOC_PCT ,
           b.customer_type_mod,
           (nvl(b.v_onnet_chg,0)+nvl(b.v_offnet_chg,0)+nvl(b.v_others_chg,0)) * a.v_alloc_pct v_total_chg ,
           nvl(b.v_onnet_chg,0) * a.v_onnet_alloc_pct v_onnet_chg , nvl(b.v_offnet_chg,0) * a.v_offnet_alloc_pct v_offnet_chg,
           (nvl(b.s_onnet_chg,0)+nvl(b.s_offnet_chg,0)+nvl(b.s_others_chg,0)) * a.s_alloc_pct s_total_chg , nvl(b.s_onnet_chg,0) * a.s_onnet_alloc_pct s_onnet_chg,
           nvl(b.s_offnet_chg,0) * a.s_offnet_alloc_pct s_offnet_chg,
           nvl(b.g_total_chg,0) * a.non_bb_alloc_pct g_total_chg,
           nvl(b.va_total_chg,0) * a.s_alloc_pct va_total_chg
      FROM POSTPAID_ALL_TR_ALLOC a LEFT JOIN POSTPAID_CUSTTYPE_TP b ON a.msisdn = b.msisdn

"""
    val queryPreAllSvcLacci="""
      select * from ALL_SVC_LACCI_SUB_SMY where MSISDN not in (%1$s)
"""
    val queryPrePostpaidLacciSmy="""
      select 
           coalesce(a.msisdn,b.msisdn) msisdn
         , coalesce(a.lac,b.lac) lac
         , coalesce(a.ci,b.ci) ci
         , nvl(a.v_total_chg,b.v_total_chg) v_total_chg
         , nvl(a.v_offnet_chg,b.v_offnet_chg) v_offnet_chg
         , nvl(a.v_onnet_chg,b.v_onnet_chg) v_onnet_chg
         , a.billed_dur, a.billed_offnet_dur, a.billed_onnet_dur, a.promo_dur, a.promo_offnet_dur, a.promo_onnet_dur
         , nvl(a.tot_dur,b.total_dur) tot_dur
         , nvl(a.v_offnet_dur, b.offnet_dur) v_offnet_dur
         , nvl(a.v_onnet_dur,b.onnet_dur) v_onnet_dur
         , a.v_offpeak_chg, a.v_offnet_offpeak_chg, a.v_onnet_offpeak_chg, a.billed_offpeak_dur, a.billed_offnet_offpeak_dur
         , a.billed_onnet_offpeak_dur, a.promo_offpeak_dur, a.promo_offnet_offpeak_dur, a.promo_onnet_offpeak_dur
         , a.total_offpeak_dur, a.v_offnet_offpeak_dur, a.v_onnet_offpeak_dur, a.v_peak_chg, a.v_offnet_peak_chg
         , a.v_onnet_peak_chg, a.billed_peak_dur, a.billed_offnet_peak_dur, a.billed_onnet_peak_dur, a.promo_peak_dur
         , a.promo_offnet_peak_dur, a.promo_onnet_peak_dur, a.total_peak_dur, a.v_offnet_peak_dur, a.v_onnet_peak_dur
         , nvl(a.num_days_voice,b.num_days_voice) num_days_voice
         , nvl(a.g_total_chg,b.g_total_chg) g_total_chg
         , a.ppu_datavol, a.addon_datavol, a.total_datavol, a.g_3g_total_chg, a.g_2g_total_chg, a.g_3g_ppu_datavol
         , a.g_2g_ppu_datavol, a.g_3g_addon_datavol, a.g_2g_addon_datavol, a.g_offpeak_chg, a.g_peak_chg
         , a.ppu_offpeak_datavol, a.ppu_peak_datavol, a.addon_offpeak_datavol, a.addon_peak_datavol, a.total_offpeak_datavol
         , a.total_peak_datavol
         , a.G_4G_TOTAL_CHG, a.G_4G_PPU_DATAVOL, a.G_4G_ADDON_DATAVOL
         , nvl(a.bb_uplink_tr,b.bb_uplink_tr) bb_uplink_tr
         , nvl(a.bb_downlink_tr,b.bb_downlink_tr) bb_downlink_tr
         , nvl(a.non_bb_uplink_tr,b.non_bb_uplink_tr) non_bb_uplink_tr
         , nvl(a.non_bb_downlink_tr,b.non_bb_downlink_tr) non_bb_downlink_tr
         , nvl(a.TOTAL_4G_UPLINK_TR, b.TOTAL_4G_UPLINK_TR) TOTAL_4G_UPLINK_TR
         , nvl(a.TOTAL_4G_DOWNLINK_TR, b.TOTAL_4G_DOWNLINK_TR) TOTAL_4G_DOWNLINK_TR
         , nvl(a.total_3g_uplink_tr,b.total_3g_uplink_tr) total_3g_uplink_tr
         , nvl(a.total_3g_downlink_tr,b.total_3g_downlink_tr) total_3g_downlink_tr
         , nvl(a.total_2g_uplink_tr,b.total_2g_uplink_tr) total_2g_uplink_tr
         , nvl(a.total_2g_downlink_tr,b.total_2g_downlink_tr) total_2g_downlink_tr
         , nvl(a.offpeak_data_downlink_tr,b.offpeak_data_downlink_tr) offpeak_data_downlink_tr
         , nvl(a.offpeak_data_uplink_tr,b.offpeak_data_uplink_tr) offpeak_data_uplink_tr
         , nvl(a.peak_data_downlink_tr,b.peak_data_downlink_tr) peak_data_downlink_tr
         , nvl(a.peak_data_uplink_tr,b.peak_data_uplink_tr) peak_data_uplink_tr
         , nvl(a.num_days_data,b.num_days_data) num_days_data
         , nvl(a.s_total_chg,b.s_total_chg) s_total_chg
         , nvl(a.s_offnet_chg,b.s_offnet_chg) s_offnet_chg
         , nvl(a.s_onnet_chg,b.s_onnet_chg) s_onnet_chg
         , a.billed_sms, a.billed_offnet_sms, a.billed_onnet_sms, a.promo_sms, a.promo_offnet_sms, a.promo_onnet_sms
         , a.s_offpeak_chg, a.s_offnet_offpeak_chg, a.s_onnet_offpeak_chg, a.billed_offpeak_sms, a.billed_offnet_offpeak_sms
         , a.billed_onnet_offpeak_sms, a.promo_offpeak_sms, a.promo_offnet_offpeak_sms, a.promo_onnet_offpeak_sms
         , a.s_peak_chg, a.s_offnet_peak_chg, a.s_onnet_peak_chg, a.billed_peak_sms, a.billed_offnet_peak_sms
         , a.billed_onnet_peak_sms, a.promo_peak_sms, a.promo_offnet_peak_sms, a.promo_onnet_peak_sms
         , nvl(a.sms_hits,b.sms_hits) sms_hits
         , nvl(a.onnet_sms_hits,b.onnet_sms_hits) onnet_sms_hits
         , nvl(a.offnet_sms_hits,b.offnet_sms_hits) offnet_sms_hits
         , nvl(a.num_days_sms,b.num_days_sms) num_days_sms
         , nvl(a.v_alloc_pct,b.v_alloc_pct) v_alloc_pct
         , b.v_onnet_alloc_pct, b.v_offnet_alloc_pct
         , nvl(a.s_alloc_pct,b.s_alloc_pct) s_alloc_pct
         , b.s_onnet_alloc_pct, b.s_offnet_alloc_pct
         , nvl(a.bb_alloc_pct,b.bb_alloc_pct) bb_alloc_pct
         , nvl(a.non_bb_alloc_pct,b.non_bb_alloc_pct) non_bb_alloc_pct
         , a.va_voice_chg, a.va_sms_chg, a.va_data_chg, a.va_bb_chg
         , b.va_total_chg
         , a.sub_cat, a.sub_data_cat, a.typ
         , case when b.customer_type_mod is null then 'Prepaid' else b.customer_type_mod end customer_type_mod
         , '201709' MONTH_ID
      from PRE_ALL_SVC_LACCI a
          full outer join
      POSTPAID_ALL_TR_REV b
        on a.msisdn = b.msisdn
        and a.lac = b.lac and a.ci = b.ci
    
"""
    val queryPrePostpaidLacciSite="""
      select 
         a.MSISDN, a.LAC, a.CI
       , b.SITE_ID, b.LONGITUDE, b.LATITUDE, cast(NULL as String) as ID_KECA, b.PROVINCE as PROP, b.MUNICIPAL as KAB_KOTA, b.DISTRICT as KECA
       , b.AREA as AREA_CHANNELS, b.SALES_AREA as SALES_AREA_CHANNELS
       , a.V_TOTAL_CHG, a.V_OFFNET_CHG, a.V_ONNET_CHG
       , a.BILLED_DUR, a.BILLED_OFFNET_DUR, a.BILLED_ONNET_DUR, a.PROMO_DUR, a.PROMO_OFFNET_DUR, a.PROMO_ONNET_DUR
       , a.TOT_DUR, a.V_OFFNET_DUR, a.V_ONNET_DUR
       , a.V_OFFPEAK_CHG, a.V_OFFNET_OFFPEAK_CHG, a.V_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_DUR, a.BILLED_OFFNET_OFFPEAK_DUR
       , a.BILLED_ONNET_OFFPEAK_DUR, a.PROMO_OFFPEAK_DUR, a.PROMO_OFFNET_OFFPEAK_DUR, a.PROMO_ONNET_OFFPEAK_DUR
       , a.TOTAL_OFFPEAK_DUR, a.V_OFFNET_OFFPEAK_DUR, a.V_ONNET_OFFPEAK_DUR, a.V_PEAK_CHG, a.V_OFFNET_PEAK_CHG
       , a.V_ONNET_PEAK_CHG, a.BILLED_PEAK_DUR, a.BILLED_OFFNET_PEAK_DUR, a.BILLED_ONNET_PEAK_DUR, a.PROMO_PEAK_DUR
       , a.PROMO_OFFNET_PEAK_DUR, a.PROMO_ONNET_PEAK_DUR, a.TOTAL_PEAK_DUR, a.V_OFFNET_PEAK_DUR, a.V_ONNET_PEAK_DUR
       , a.NUM_DAYS_VOICE
       , a.G_TOTAL_CHG, a.PPU_DATAVOL, a.ADDON_DATAVOL, a.TOTAL_DATAVOL, a.G_3G_TOTAL_CHG, a.G_2G_TOTAL_CHG, a.G_3G_PPU_DATAVOL
       , a.G_2G_PPU_DATAVOL, a.G_3G_ADDON_DATAVOL, a.G_2G_ADDON_DATAVOL, a.G_OFFPEAK_CHG, a.G_PEAK_CHG
       , a.PPU_OFFPEAK_DATAVOL, a.PPU_PEAK_DATAVOL, a.ADDON_OFFPEAK_DATAVOL, a.ADDON_PEAK_DATAVOL, a.TOTAL_OFFPEAK_DATAVOL
       , a.TOTAL_PEAK_DATAVOL
       , a.BB_UPLINK_TR, a.BB_DOWNLINK_TR, a.NON_BB_UPLINK_TR, a.NON_BB_DOWNLINK_TR, a.TOTAL_3G_UPLINK_TR
       , a.TOTAL_3G_DOWNLINK_TR, a.TOTAL_2G_UPLINK_TR, a.TOTAL_2G_DOWNLINK_TR, a.OFFPEAK_DATA_DOWNLINK_TR
       , a.OFFPEAK_DATA_UPLINK_TR, a.PEAK_DATA_DOWNLINK_TR, a.PEAK_DATA_UPLINK_TR, a.NUM_DAYS_DATA
       , a.S_TOTAL_CHG, a.S_OFFNET_CHG, a.S_ONNET_CHG
       , a.BILLED_SMS, a.BILLED_OFFNET_SMS, a.BILLED_ONNET_SMS, a.PROMO_SMS, a.PROMO_OFFNET_SMS, a.PROMO_ONNET_SMS
       , a.S_OFFPEAK_CHG, a.S_OFFNET_OFFPEAK_CHG, a.S_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_SMS, a.BILLED_OFFNET_OFFPEAK_SMS
       , a.BILLED_ONNET_OFFPEAK_SMS, a.PROMO_OFFPEAK_SMS, a.PROMO_OFFNET_OFFPEAK_SMS, a.PROMO_ONNET_OFFPEAK_SMS
       , a.S_PEAK_CHG, a.S_OFFNET_PEAK_CHG, a.S_ONNET_PEAK_CHG, a.BILLED_PEAK_SMS, a.BILLED_OFFNET_PEAK_SMS
       , a.BILLED_ONNET_PEAK_SMS, a.PROMO_PEAK_SMS, a.PROMO_OFFNET_PEAK_SMS, a.PROMO_ONNET_PEAK_SMS
       , a.SMS_HITS, a.ONNET_SMS_HITS, a.OFFNET_SMS_HITS, a.NUM_DAYS_SMS, a.V_ALLOC_PCT, a.V_ONNET_ALLOC_PCT
       , a.V_OFFNET_ALLOC_PCT, a.S_ALLOC_PCT, a.S_ONNET_ALLOC_PCT, a.S_OFFNET_ALLOC_PCT, a.BB_ALLOC_PCT, a.NON_BB_ALLOC_PCT
       , a.VA_VOICE_CHG, a.VA_SMS_CHG, a.VA_DATA_CHG, a.VA_BB_CHG, a.VA_TOTAL_CHG
       , case when a.SUB_CAT is null then
                  case when a.V_TOTAL_CHG + a.G_TOTAL_CHG + a.S_TOTAL_CHG + a.VA_TOTAL_CHG >= 400000 then 'Platinum'
                         when a.V_TOTAL_CHG + a.G_TOTAL_CHG + a.S_TOTAL_CHG + a.VA_TOTAL_CHG >= 200000 then 'Gold'
                           when a.V_TOTAL_CHG + a.G_TOTAL_CHG + a.S_TOTAL_CHG + a.VA_TOTAL_CHG >= 100000 then 'Regular'
                             else 'Mass'
                  end
              else a.SUB_CAT
         end SUB_CAT
       , case when a.SUB_DATA_CAT is null then
                  case when a.G_TOTAL_CHG >= 50000 then 'Platinum'
                         when a.G_TOTAL_CHG >= 20000 then 'Gold'
                           when a.G_TOTAL_CHG > 0 then 'Regular'
                             else 'Non Data Users'
                  end
              else a.SUB_DATA_CAT
         end SUB_DATA_CAT
       , a.TYP, a.CUSTOMER_TYPE_MOD
       ,G_4G_TOTAL_CHG, G_4G_PPU_DATAVOL, G_4G_ADDON_DATAVOL, TOTAL_4G_UPLINK_TR,TOTAL_4G_DOWNLINK_TR
    from PRE_POSTPAID_LACCI_SMY a
    left join
    ISAT_CONSOL_NDB b
    on a.LAC = b.LAC and a.CI = b.CI
"""
    val queryPrePostpaidLacciHst="""
      select 
        a.*, b.flag_tag device_fl, b.smartphone_flag
           , case when b.network_flag='4G' then 1 else 0 end flag_4g
           , case when b.network_flag in ('3G','3G/U900') and b.network_flag<>'4G' then 1 else 0 end flag_3g       
           , case when b.network_flag='2G' and b.network_flag not in ('4G','3G','3G/U900') then 1 else 0 end flag_2g       
           , case when b.u900_flag='YES' then 1 else 0 end u900_fl
           , '201709' MONTH_ID
      from PRE_POSTPAID_LACCI_SITE a
      left join
      handset_msisdn b
      on a.msisdn = b.msisdn
"""
    val querySnapshot="""
      select  a.msisdn, a.tnr,a.ar_prim_pymt_tp_id, svc_clss_id
             , case when b.promo_package_name is null then 'UNKNOWN'
                      else b.promo_package_name
               end promo_package_name
             , case when a.cust_type = 'PREPAID' and b.cust_type ='' then 'B2C PREPAID' 
                    when a.cust_type = 'PREPAID' and b.cust_type <>'' then b.cust_type  
                    when a.cust_type = 'PREPAID' then 'B2C PREPAID'
                    else a.cust_type end customer_type_mod
      from (
          select msisdn,tnr,svc_clss_id,ar_prim_pymt_tp_id,offer_id_sc,
              case when ar_prim_pymt_tp_id='3003' then 'PREPAID'
                   when upper(cst_tp) in ('INDIVIDUAL','VVIP') then 'B2C POSTPAID'
                   else 'B2B POSTPAID' end cust_type
          from AR_CST_DLY_SMY 
          where ar_prim_pymt_tp_id in ('2003','3003')
      )a
      left join
      REF_SVC_CLASS_OFR b
      on a.svc_clss_id = b.SVC_CLASS_ID
      and a.offer_id_sc = b.offer_id
      --where b.promo_package_name is not null
"""
    val queryPrePostpaidLacciTnr="""
        select a.*
               , b.tnr
               , b.promo_package_name
        from PRE_POSTPAID_LACCI_HST a
        left join
        snapshot b
        on a.msisdn = b.msisdn
"""
    val queryUsgLacciSmyAlloc="""
      select a.*
         , case when (nvl(TOT_V_TOTAL_CHG,0)+nvl(TOT_G_TOTAL_CHG,0)+nvl(TOT_S_TOTAL_CHG,0)+nvl(TOT_VA_VOICE_CHG,0)+nvl(TOT_VA_SMS_CHG,0)+nvl(TOT_VA_BB_CHG,0)+nvl(TOT_VA_DATA_CHG,0)+nvl(MOBOSP_REVENUE,0)) = 0 then 0
                 else (nvl(V_TOTAL_CHG,0)+nvl(G_TOTAL_CHG,0)+nvl(S_TOTAL_CHG,0)+nvl(VA_VOICE_CHG,0)+nvl(VA_SMS_CHG,0)+nvl(VA_BB_CHG,0)+nvl(VA_DATA_CHG,0)+nvl(MOBOSP_REVENUE,0))/(nvl(TOT_V_TOTAL_CHG,0)+nvl(TOT_G_TOTAL_CHG,0)+nvl(TOT_S_TOTAL_CHG,0)+nvl(TOT_VA_VOICE_CHG,0)+nvl(TOT_VA_SMS_CHG,0)+nvl(TOT_VA_BB_CHG,0)+nvl(TOT_VA_DATA_CHG,0)+nvl(TOT_MOBOSP,0))
           end Sub_Alloc
      from USG_LACCI_SMY_MOBO a
       , (
           select msisdn
                  , sum(nvl(V_TOTAL_CHG,0)) TOT_V_TOTAL_CHG
                  , sum(nvl(G_TOTAL_CHG,0)) TOT_G_TOTAL_CHG
                  , sum(nvl(S_TOTAL_CHG,0)) TOT_S_TOTAL_CHG
                  , sum(nvl(VA_VOICE_CHG,0)) TOT_VA_VOICE_CHG
                  , sum(nvl(VA_SMS_CHG,0)) TOT_VA_SMS_CHG
                  , sum(nvl(VA_BB_CHG,0)) TOT_VA_BB_CHG
                  , sum(nvl(VA_DATA_CHG,0)) TOT_VA_DATA_CHG
                  , sum(nvl(MOBOSP_REVENUE,0)) TOT_MOBOSP
           from USG_LACCI_SMY_MOBO
           group by msisdn
         ) b
      where a.msisdn = b.msisdn
"""
    val queryLacciSmyUsg="""
      select  LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA 
           , sum(V_TOTAL_CHG) V_TOTAL_CHG, sum(V_OFFNET_CHG) V_OFFNET_CHG, sum(V_ONNET_CHG) V_ONNET_CHG, sum(MOBOSP_REVENUE) MOBOSP_REVENUE
           , sum(BILLED_DUR) BILLED_DUR, sum(BILLED_OFFNET_DUR) BILLED_OFFNET_DUR, sum(BILLED_ONNET_DUR) BILLED_ONNET_DUR
           , sum(PROMO_DUR) PROMO_DUR, sum(PROMO_OFFNET_DUR) PROMO_OFFNET_DUR, sum(PROMO_ONNET_DUR) PROMO_ONNET_DUR
           , sum(TOT_DUR) TOT_DUR, sum(V_OFFNET_DUR) V_OFFNET_DUR, sum(V_ONNET_DUR) V_ONNET_DUR, sum(V_OFFPEAK_CHG) V_OFFPEAK_CHG
           , sum(V_OFFNET_OFFPEAK_CHG) V_OFFNET_OFFPEAK_CHG, sum(V_ONNET_OFFPEAK_CHG) V_ONNET_OFFPEAK_CHG
           , sum(BILLED_OFFPEAK_DUR) BILLED_OFFPEAK_DUR, sum(BILLED_OFFNET_OFFPEAK_DUR) BILLED_OFFNET_OFFPEAK_DUR
           , sum(BILLED_ONNET_OFFPEAK_DUR) BILLED_ONNET_OFFPEAK_DUR, sum(PROMO_OFFPEAK_DUR) PROMO_OFFPEAK_DUR
           , sum(PROMO_OFFNET_OFFPEAK_DUR) PROMO_OFFNET_OFFPEAK_DUR, sum(PROMO_ONNET_OFFPEAK_DUR) PROMO_ONNET_OFFPEAK_DUR
           , sum(TOTAL_OFFPEAK_DUR) TOTAL_OFFPEAK_DUR, sum(V_OFFNET_OFFPEAK_DUR) V_OFFNET_OFFPEAK_DUR
           , sum(V_ONNET_OFFPEAK_DUR) V_ONNET_OFFPEAK_DUR, sum(V_PEAK_CHG) V_PEAK_CHG, sum(V_OFFNET_PEAK_CHG) V_OFFNET_PEAK_CHG
           , sum(V_ONNET_PEAK_CHG) V_ONNET_PEAK_CHG, sum(BILLED_PEAK_DUR) BILLED_PEAK_DUR
           , sum(BILLED_OFFNET_PEAK_DUR) BILLED_OFFNET_PEAK_DUR, sum(BILLED_ONNET_PEAK_DUR) BILLED_ONNET_PEAK_DUR
           , sum(PROMO_PEAK_DUR) PROMO_PEAK_DUR, sum(PROMO_OFFNET_PEAK_DUR) PROMO_OFFNET_PEAK_DUR
           , sum(PROMO_ONNET_PEAK_DUR) PROMO_ONNET_PEAK_DUR, sum(TOTAL_PEAK_DUR) TOTAL_PEAK_DUR
           , sum(V_OFFNET_PEAK_DUR) V_OFFNET_PEAK_DUR, sum(V_ONNET_PEAK_DUR) V_ONNET_PEAK_DUR
           , sum(G_TOTAL_CHG) G_TOTAL_CHG, sum(PPU_DATAVOL) PPU_DATAVOL, sum(ADDON_DATAVOL) ADDON_DATAVOL
           , sum(G_OFFPEAK_CHG) G_OFFPEAK_CHG, sum(G_PEAK_CHG) G_PEAK_CHG, sum(PPU_OFFPEAK_DATAVOL) PPU_OFFPEAK_DATAVOL
           , sum(PPU_PEAK_DATAVOL) PPU_PEAK_DATAVOL, sum(ADDON_OFFPEAK_DATAVOL) ADDON_OFFPEAK_DATAVOL
           , sum(ADDON_PEAK_DATAVOL) ADDON_PEAK_DATAVOL, sum(BB_UPLINK_TR) BB_UPLINK_TR, sum(BB_DOWNLINK_TR) BB_DOWNLINK_TR
           , sum(NON_BB_UPLINK_TR) NON_BB_UPLINK_TR, sum(NON_BB_DOWNLINK_TR) NON_BB_DOWNLINK_TR
           , sum(nvl(TOTAL_4G_UPLINK_TR,0)) + sum(nvl(TOTAL_4G_DOWNLINK_TR,0)) TOTAL_4G_TR
           , sum(nvl(TOTAL_3G_UPLINK_TR,0)) + sum(nvl(TOTAL_3G_DOWNLINK_TR,0)) TOTAL_3G_TR
           , sum(nvl(TOTAL_2G_UPLINK_TR,0)) + sum(nvl(TOTAL_2G_DOWNLINK_TR,0)) TOTAL_2G_TR
           , sum(nvl(OFFPEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(OFFPEAK_DATA_UPLINK_TR,0)) OFFPEAK_DATA_TR
           , sum(nvl(PEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(PEAK_DATA_UPLINK_TR,0)) PEAK_DATA_TR
           , sum(S_TOTAL_CHG) S_TOTAL_CHG, sum(S_OFFNET_CHG) S_OFFNET_CHG, sum(S_ONNET_CHG) S_ONNET_CHG
           , sum(BILLED_SMS) BILLED_SMS, sum(BILLED_OFFNET_SMS) BILLED_OFFNET_SMS
           , sum(BILLED_ONNET_SMS) BILLED_ONNET_SMS, sum(PROMO_SMS) PROMO_SMS
           , sum(PROMO_OFFNET_SMS) PROMO_OFFNET_SMS, sum(PROMO_ONNET_SMS) PROMO_ONNET_SMS
           , sum(S_OFFPEAK_CHG) S_OFFPEAK_CHG, sum(S_OFFNET_OFFPEAK_CHG) S_OFFNET_OFFPEAK_CHG
           , sum(S_ONNET_OFFPEAK_CHG) S_ONNET_OFFPEAK_CHG, sum(BILLED_OFFPEAK_SMS) BILLED_OFFPEAK_SMS
           , sum(BILLED_OFFNET_OFFPEAK_SMS) BILLED_OFFNET_OFFPEAK_SMS
           , sum(BILLED_ONNET_OFFPEAK_SMS) BILLED_ONNET_OFFPEAK_SMS, sum(PROMO_OFFPEAK_SMS) PROMO_OFFPEAK_SMS
           , sum(PROMO_OFFNET_OFFPEAK_SMS) PROMO_OFFNET_OFFPEAK_SMS, sum(PROMO_ONNET_OFFPEAK_SMS) PROMO_ONNET_OFFPEAK_SMS
           , sum(S_PEAK_CHG) S_PEAK_CHG, sum(S_OFFNET_PEAK_CHG) S_OFFNET_PEAK_CHG
           , sum(S_ONNET_PEAK_CHG) S_ONNET_PEAK_CHG, sum(BILLED_PEAK_SMS) BILLED_PEAK_SMS
           , sum(BILLED_OFFNET_PEAK_SMS) BILLED_OFFNET_PEAK_SMS, sum(BILLED_ONNET_PEAK_SMS) BILLED_ONNET_PEAK_SMS
           , sum(PROMO_PEAK_SMS) PROMO_PEAK_SMS, sum(PROMO_OFFNET_PEAK_SMS) PROMO_OFFNET_PEAK_SMS
           , sum(PROMO_ONNET_PEAK_SMS) PROMO_ONNET_PEAK_SMS, sum(SMS_HITS) SMS_HITS, sum(ONNET_SMS_HITS) ONNET_SMS_HITS
           , sum(OFFNET_SMS_HITS) OFFNET_SMS_HITS
           , sum(VA_VOICE_CHG) VA_VOICE_CHG, sum(VA_BB_CHG) VA_BB_CHG, sum(VA_SMS_CHG) VA_SMS_CHG, sum(VA_DATA_CHG) VA_DATA_CHG
        from USG_LACCI_SMY_ALLOC
        group by LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA
"""
    val queryLacciSmyUsg2="""
      select  LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA 
           , sum(V_TOTAL_CHG) V_TOTAL_CHG, sum(V_OFFNET_CHG) V_OFFNET_CHG, sum(V_ONNET_CHG) V_ONNET_CHG--, sum(MOBOSP_REVENUE) MOBOSP_REVENUE
           , sum(BILLED_DUR) BILLED_DUR, sum(BILLED_OFFNET_DUR) BILLED_OFFNET_DUR, sum(BILLED_ONNET_DUR) BILLED_ONNET_DUR
           , sum(PROMO_DUR) PROMO_DUR, sum(PROMO_OFFNET_DUR) PROMO_OFFNET_DUR, sum(PROMO_ONNET_DUR) PROMO_ONNET_DUR
           , sum(TOT_DUR) TOT_DUR, sum(V_OFFNET_DUR) V_OFFNET_DUR, sum(V_ONNET_DUR) V_ONNET_DUR, sum(V_OFFPEAK_CHG) V_OFFPEAK_CHG
           , sum(V_OFFNET_OFFPEAK_CHG) V_OFFNET_OFFPEAK_CHG, sum(V_ONNET_OFFPEAK_CHG) V_ONNET_OFFPEAK_CHG
           , sum(BILLED_OFFPEAK_DUR) BILLED_OFFPEAK_DUR, sum(BILLED_OFFNET_OFFPEAK_DUR) BILLED_OFFNET_OFFPEAK_DUR
           , sum(BILLED_ONNET_OFFPEAK_DUR) BILLED_ONNET_OFFPEAK_DUR, sum(PROMO_OFFPEAK_DUR) PROMO_OFFPEAK_DUR
           , sum(PROMO_OFFNET_OFFPEAK_DUR) PROMO_OFFNET_OFFPEAK_DUR, sum(PROMO_ONNET_OFFPEAK_DUR) PROMO_ONNET_OFFPEAK_DUR
           , sum(TOTAL_OFFPEAK_DUR) TOTAL_OFFPEAK_DUR, sum(V_OFFNET_OFFPEAK_DUR) V_OFFNET_OFFPEAK_DUR
           , sum(V_ONNET_OFFPEAK_DUR) V_ONNET_OFFPEAK_DUR, sum(V_PEAK_CHG) V_PEAK_CHG, sum(V_OFFNET_PEAK_CHG) V_OFFNET_PEAK_CHG
           , sum(V_ONNET_PEAK_CHG) V_ONNET_PEAK_CHG, sum(BILLED_PEAK_DUR) BILLED_PEAK_DUR
           , sum(BILLED_OFFNET_PEAK_DUR) BILLED_OFFNET_PEAK_DUR, sum(BILLED_ONNET_PEAK_DUR) BILLED_ONNET_PEAK_DUR
           , sum(PROMO_PEAK_DUR) PROMO_PEAK_DUR, sum(PROMO_OFFNET_PEAK_DUR) PROMO_OFFNET_PEAK_DUR
           , sum(PROMO_ONNET_PEAK_DUR) PROMO_ONNET_PEAK_DUR, sum(TOTAL_PEAK_DUR) TOTAL_PEAK_DUR
           , sum(V_OFFNET_PEAK_DUR) V_OFFNET_PEAK_DUR, sum(V_ONNET_PEAK_DUR) V_ONNET_PEAK_DUR
           , sum(G_TOTAL_CHG) G_TOTAL_CHG, sum(PPU_DATAVOL) PPU_DATAVOL, sum(ADDON_DATAVOL) ADDON_DATAVOL
           , sum(G_OFFPEAK_CHG) G_OFFPEAK_CHG, sum(G_PEAK_CHG) G_PEAK_CHG, sum(PPU_OFFPEAK_DATAVOL) PPU_OFFPEAK_DATAVOL
           , sum(PPU_PEAK_DATAVOL) PPU_PEAK_DATAVOL, sum(ADDON_OFFPEAK_DATAVOL) ADDON_OFFPEAK_DATAVOL
           , sum(ADDON_PEAK_DATAVOL) ADDON_PEAK_DATAVOL, sum(BB_UPLINK_TR) BB_UPLINK_TR, sum(BB_DOWNLINK_TR) BB_DOWNLINK_TR
           , sum(NON_BB_UPLINK_TR) NON_BB_UPLINK_TR, sum(NON_BB_DOWNLINK_TR) NON_BB_DOWNLINK_TR
           , sum(nvl(TOTAL_4G_UPLINK_TR,0)) + sum(nvl(TOTAL_4G_DOWNLINK_TR,0)) TOTAL_4G_TR
           , sum(nvl(TOTAL_3G_UPLINK_TR,0)) + sum(nvl(TOTAL_3G_DOWNLINK_TR,0)) TOTAL_3G_TR
           , sum(nvl(TOTAL_2G_UPLINK_TR,0)) + sum(nvl(TOTAL_2G_DOWNLINK_TR,0)) TOTAL_2G_TR
           , sum(nvl(OFFPEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(OFFPEAK_DATA_UPLINK_TR,0)) OFFPEAK_DATA_TR
           , sum(nvl(PEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(PEAK_DATA_UPLINK_TR,0)) PEAK_DATA_TR
           , sum(S_TOTAL_CHG) S_TOTAL_CHG, sum(S_OFFNET_CHG) S_OFFNET_CHG, sum(S_ONNET_CHG) S_ONNET_CHG
           , sum(BILLED_SMS) BILLED_SMS, sum(BILLED_OFFNET_SMS) BILLED_OFFNET_SMS
           , sum(BILLED_ONNET_SMS) BILLED_ONNET_SMS, sum(PROMO_SMS) PROMO_SMS
           , sum(PROMO_OFFNET_SMS) PROMO_OFFNET_SMS, sum(PROMO_ONNET_SMS) PROMO_ONNET_SMS
           , sum(S_OFFPEAK_CHG) S_OFFPEAK_CHG, sum(S_OFFNET_OFFPEAK_CHG) S_OFFNET_OFFPEAK_CHG
           , sum(S_ONNET_OFFPEAK_CHG) S_ONNET_OFFPEAK_CHG, sum(BILLED_OFFPEAK_SMS) BILLED_OFFPEAK_SMS
           , sum(BILLED_OFFNET_OFFPEAK_SMS) BILLED_OFFNET_OFFPEAK_SMS
           , sum(BILLED_ONNET_OFFPEAK_SMS) BILLED_ONNET_OFFPEAK_SMS, sum(PROMO_OFFPEAK_SMS) PROMO_OFFPEAK_SMS
           , sum(PROMO_OFFNET_OFFPEAK_SMS) PROMO_OFFNET_OFFPEAK_SMS, sum(PROMO_ONNET_OFFPEAK_SMS) PROMO_ONNET_OFFPEAK_SMS
           , sum(S_PEAK_CHG) S_PEAK_CHG, sum(S_OFFNET_PEAK_CHG) S_OFFNET_PEAK_CHG
           , sum(S_ONNET_PEAK_CHG) S_ONNET_PEAK_CHG, sum(BILLED_PEAK_SMS) BILLED_PEAK_SMS
           , sum(BILLED_OFFNET_PEAK_SMS) BILLED_OFFNET_PEAK_SMS, sum(BILLED_ONNET_PEAK_SMS) BILLED_ONNET_PEAK_SMS
           , sum(PROMO_PEAK_SMS) PROMO_PEAK_SMS, sum(PROMO_OFFNET_PEAK_SMS) PROMO_OFFNET_PEAK_SMS
           , sum(PROMO_ONNET_PEAK_SMS) PROMO_ONNET_PEAK_SMS, sum(SMS_HITS) SMS_HITS, sum(ONNET_SMS_HITS) ONNET_SMS_HITS
           , sum(OFFNET_SMS_HITS) OFFNET_SMS_HITS
           , sum(VA_VOICE_CHG) VA_VOICE_CHG, sum(VA_BB_CHG) VA_BB_CHG, sum(VA_SMS_CHG) VA_SMS_CHG, sum(VA_DATA_CHG) VA_DATA_CHG
        from USG_LACCI_SMY_ALLOC
        group by LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA
"""
    val queryLacciSmy="""
      select LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA
           , sum(nvl(V_TOTAL_CHG,0)) + sum(nvl(VA_VOICE_CHG,0)) V_TOTAL_CHG
           , sum(nvl(TOT_DUR,0)) TOT_DUR
           , sum(nvl(G_TOTAL_CHG,0)) G_TOTAL_CHG, sum(nvl(VA_DATA_CHG,0)) VA_DATA_CHG
           , sum(nvl(PPU_DATAVOL,0)) PPU_DATAVOL, sum(nvl(ADDON_DATAVOL,0)) ADDON_DATAVOL
           , sum(nvl(BB_UPLINK_TR,0)) + sum(nvl(BB_DOWNLINK_TR,0)) TOTAL_BB_TR
           , sum(nvl(NON_BB_UPLINK_TR,0)) + sum(nvl(NON_BB_DOWNLINK_TR,0)) TOTAL_NON_BB_TR
           , sum(nvl(TOTAL_4G_UPLINK_TR,0)) + sum(nvl(TOTAL_4G_DOWNLINK_TR,0)) TOTAL_4G_TR
           , sum(nvl(TOTAL_3G_UPLINK_TR,0)) + sum(nvl(TOTAL_3G_DOWNLINK_TR,0)) TOTAL_3G_TR
           , sum(nvl(TOTAL_2G_UPLINK_TR,0)) + sum(nvl(TOTAL_2G_DOWNLINK_TR,0)) TOTAL_2G_TR
           , sum(nvl(S_TOTAL_CHG,0)) + sum(nvl(VA_SMS_CHG,0)) S_TOTAL_CHG
           , sum(nvl(SMS_HITS,0)) SMS_HITS
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then SUB_ALLOC else 0 end) SUB_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_4G = 1 then SUB_ALLOC else 0 end) SUB_4G_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_3G = 1 then SUB_ALLOC else 0 end) SUB_3G_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_2G = 1 then SUB_ALLOC else 0 end) SUB_2G_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then SUB_ALLOC else 0 end) SUB_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_4G = 1 then SUB_ALLOC else 0 end) SUB_4G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_3G = 1 then SUB_ALLOC else 0 end) SUB_3G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_2G = 1 then SUB_ALLOC else 0 end) SUB_2G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_2G = 0 and FLAG_3G = 0 and FLAG_4G = 0 then SUB_ALLOC else 0 end) SUB_NON_DATA_FP
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then V_TOTAL_CHG else 0 end) V_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then G_TOTAL_CHG else 0 end) G_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then S_TOTAL_CHG else 0 end) S_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_VOICE_CHG else 0 end) VA_VOICE_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_SMS_CHG else 0 end) VA_SMS_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_DATA_CHG else 0 end) VA_DATA_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_BB_CHG else 0 end) VA_BB_U900
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then V_TOTAL_CHG else 0 end) V_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then G_TOTAL_CHG else 0 end) G_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then S_TOTAL_CHG else 0 end) S_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_VOICE_CHG else 0 end) VA_VOICE_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_SMS_CHG else 0 end) VA_SMS_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_DATA_CHG else 0 end) VA_DATA_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_BB_CHG else 0 end) VA_BB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then V_TOTAL_CHG else 0 end) V_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then G_TOTAL_CHG else 0 end) G_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then S_TOTAL_CHG else 0 end) S_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_VOICE_CHG else 0 end) VA_VOICE_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_SMS_CHG else 0 end) VA_SMS_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_DATA_CHG else 0 end) VA_DATA_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_BB_CHG else 0 end) VA_BB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(TOT_DUR,0) else 0 end) VOICE_DUR_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) + nvl(NON_BB_UPLINK_TR,0) + nvl(NON_BB_DOWNLINK_TR,0) else 0 end) DATA_TR_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(SMS_HITS,0) else 0 end) SMS_HITS_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(TOT_DUR,0) else 0 end) VOICE_DUR_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) + nvl(NON_BB_UPLINK_TR,0) + nvl(NON_BB_DOWNLINK_TR,0) else 0 end) DATA_TR_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(SMS_HITS,0) else 0 end) SMS_HITS_FP
           , sum(case when SUB_CAT_MHVC='MHVC' then V_TOTAL_CHG else 0 end) SUB_MHVC_V_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_VOICE_CHG else 0 end) SUB_MHVC_VA_VOICE_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then G_TOTAL_CHG else 0 end) SUB_MHVC_G_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_DATA_CHG else 0 end) SUB_MHVC_VA_DATA_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then S_TOTAL_CHG else 0 end) SUB_MHVC_SMS_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_SMS_CHG else 0 end) SUB_MHVC_VA_SMS_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_BB_CHG else 0 end) SUB_MHVC_VA_BB_CHG
           , sum(case when a.SUB_CAT='Platinum' then V_TOTAL_CHG else 0 end) SUB_PLATINUM_V_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_VOICE_CHG else 0 end) SUB_PLATINUM_VA_VOICE_CHG
           , sum(case when a.SUB_CAT='Platinum' then G_TOTAL_CHG else 0 end) SUB_PLATINUM_G_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_DATA_CHG else 0 end) SUB_PLATINUM_VA_DATA_CHG
           , sum(case when a.SUB_CAT='Platinum' then S_TOTAL_CHG else 0 end) SUB_PLATINUM_SMS_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_SMS_CHG else 0 end) SUB_PLATINUM_VA_SMS_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_BB_CHG else 0 end) SUB_PLATINUM_VA_BB_CHG
           , sum(case when a.SUB_CAT='Gold' then V_TOTAL_CHG else 0 end) SUB_GOLD_V_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_VOICE_CHG else 0 end) SUB_GOLD_VA_VOICE_CHG
           , sum(case when a.SUB_CAT='Gold' then G_TOTAL_CHG else 0 end) SUB_GOLD_G_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_DATA_CHG else 0 end) SUB_GOLD_VA_DATA_CHG
           , sum(case when a.SUB_CAT='Gold' then S_TOTAL_CHG else 0 end) SUB_GOLD_SMS_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_SMS_CHG else 0 end) SUB_GOLD_VA_SMS_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_BB_CHG else 0 end) SUB_GOLD_VA_BB_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then SUB_ALLOC else 0 end) SUB_MHVC
           , sum(case when SUB_CAT_MHVC='NON MHVC' then SUB_ALLOC else 0 end) SUB_NON_MHVC
           , sum(case when a.SUB_CAT='Platinum' then SUB_ALLOC else 0 end) SUB_PLATINUM
           , sum(case when a.SUB_CAT='Gold' then SUB_ALLOC else 0 end) SUB_GOLD
           , sum(case when a.SUB_CAT='Regular' then SUB_ALLOC else 0 end) SUB_REGULAR
           , sum(case when a.SUB_CAT='Mass' then SUB_ALLOC else 0 end) SUB_MASS
           , sum(case when SUB_DATA_CAT='Platinum' then SUB_ALLOC else 0 end) DATA_PLATINUM
           , sum(case when SUB_DATA_CAT='Gold' then SUB_ALLOC else 0 end) DATA_GOLD
           , sum(case when SUB_DATA_CAT='Regular' then SUB_ALLOC else 0 end) DATA_REGULAR
           , sum(case when SUB_DATA_CAT='Non Data Users' then SUB_ALLOC else 0 end) NON_DATA_REGULAR
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('VOICE ONLY', 'VOICE + DATA/VAS ONLY', 'VOICE + SMS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) VOICE_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('VOICE ONLY', 'VOICE + DATA/VAS ONLY', 'VOICE + SMS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) VOICE_SUB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('DATA/VAS ONLY', 'VOICE + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) DATA_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('DATA/VAS ONLY', 'VOICE + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) DATA_SUB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('SMS ONLY', 'SMS + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) SMS_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('SMS ONLY', 'SMS + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) SMS_SUB_FP
           , sum(case when TYP='VOICE ONLY' then SUB_ALLOC else 0 end) V_ONLY_SUBS
           , sum(case when TYP='DATA/VAS ONLY' then SUB_ALLOC else 0 end) D_ONLY_SUBS
           , sum(case when TYP='SMS ONLY' then SUB_ALLOC else 0 end) S_ONLY_SUBS
           , sum(case when TYP='VOICE + DATA/VAS ONLY' then SUB_ALLOC else 0 end) V_D_ONLY_SUBS
           , sum(case when TYP='VOICE + SMS ONLY' then SUB_ALLOC else 0 end) V_S_ONLY_SUBS
           , sum(case when TYP='SMS + DATA/VAS ONLY' then SUB_ALLOC else 0 end) S_D_ONLY_SUBS
           , sum(case when TYP='VOICE + SMS + DATA/VAS ONLY' then SUB_ALLOC else 0 end) V_S_D_ONLY_SUBS
           , sum(SUB_ALLOC) SUB_ALLOC
           , sum(case when nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) > 0 then SUB_ALLOC else 0 end) BB_ALLOC_SUB
           , sum(case when CUSTOMER_TYPE_MOD = 'B2B PREPAID' then SUB_ALLOC else 0 end) B2B_PREPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2C PREPAID' then SUB_ALLOC else 0 end) B2C_PREPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2B POSTPAID' then SUB_ALLOC else 0 end) B2B_POSTPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2C POSTPAID' then SUB_ALLOC else 0 end) B2C_POSTPAID_SUB_ALLOC
        from USG_LACCI_SMY_ALLOC a
        group by LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA
"""
    val queryLacciTotalSmy2="""
      select  LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA 
           , sum(V_TOTAL_CHG) V_TOTAL_CHG, sum(V_OFFNET_CHG) V_OFFNET_CHG, sum(V_ONNET_CHG) V_ONNET_CHG, sum(MOBOSP_REVENUE) MOBOSP_REVENUE
           , sum(BILLED_DUR) BILLED_DUR, sum(BILLED_OFFNET_DUR) BILLED_OFFNET_DUR, sum(BILLED_ONNET_DUR) BILLED_ONNET_DUR
           , sum(PROMO_DUR) PROMO_DUR, sum(PROMO_OFFNET_DUR) PROMO_OFFNET_DUR, sum(PROMO_ONNET_DUR) PROMO_ONNET_DUR
           , sum(TOT_DUR) TOT_DUR, sum(V_OFFNET_DUR) V_OFFNET_DUR, sum(V_ONNET_DUR) V_ONNET_DUR, sum(V_OFFPEAK_CHG) V_OFFPEAK_CHG
           , sum(V_OFFNET_OFFPEAK_CHG) V_OFFNET_OFFPEAK_CHG, sum(V_ONNET_OFFPEAK_CHG) V_ONNET_OFFPEAK_CHG
           , sum(BILLED_OFFPEAK_DUR) BILLED_OFFPEAK_DUR, sum(BILLED_OFFNET_OFFPEAK_DUR) BILLED_OFFNET_OFFPEAK_DUR
           , sum(BILLED_ONNET_OFFPEAK_DUR) BILLED_ONNET_OFFPEAK_DUR, sum(PROMO_OFFPEAK_DUR) PROMO_OFFPEAK_DUR
           , sum(PROMO_OFFNET_OFFPEAK_DUR) PROMO_OFFNET_OFFPEAK_DUR, sum(PROMO_ONNET_OFFPEAK_DUR) PROMO_ONNET_OFFPEAK_DUR
           , sum(TOTAL_OFFPEAK_DUR) TOTAL_OFFPEAK_DUR, sum(V_OFFNET_OFFPEAK_DUR) V_OFFNET_OFFPEAK_DUR
           , sum(V_ONNET_OFFPEAK_DUR) V_ONNET_OFFPEAK_DUR, sum(V_PEAK_CHG) V_PEAK_CHG, sum(V_OFFNET_PEAK_CHG) V_OFFNET_PEAK_CHG
           , sum(V_ONNET_PEAK_CHG) V_ONNET_PEAK_CHG, sum(BILLED_PEAK_DUR) BILLED_PEAK_DUR
           , sum(BILLED_OFFNET_PEAK_DUR) BILLED_OFFNET_PEAK_DUR, sum(BILLED_ONNET_PEAK_DUR) BILLED_ONNET_PEAK_DUR
           , sum(PROMO_PEAK_DUR) PROMO_PEAK_DUR, sum(PROMO_OFFNET_PEAK_DUR) PROMO_OFFNET_PEAK_DUR
           , sum(PROMO_ONNET_PEAK_DUR) PROMO_ONNET_PEAK_DUR, sum(TOTAL_PEAK_DUR) TOTAL_PEAK_DUR
           , sum(V_OFFNET_PEAK_DUR) V_OFFNET_PEAK_DUR, sum(V_ONNET_PEAK_DUR) V_ONNET_PEAK_DUR
           , sum(G_TOTAL_CHG) G_TOTAL_CHG, sum(PPU_DATAVOL) PPU_DATAVOL, sum(ADDON_DATAVOL) ADDON_DATAVOL
           , sum(G_OFFPEAK_CHG) G_OFFPEAK_CHG, sum(G_PEAK_CHG) G_PEAK_CHG, sum(PPU_OFFPEAK_DATAVOL) PPU_OFFPEAK_DATAVOL
           , sum(PPU_PEAK_DATAVOL) PPU_PEAK_DATAVOL, sum(ADDON_OFFPEAK_DATAVOL) ADDON_OFFPEAK_DATAVOL
           , sum(ADDON_PEAK_DATAVOL) ADDON_PEAK_DATAVOL, sum(BB_UPLINK_TR) BB_UPLINK_TR, sum(BB_DOWNLINK_TR) BB_DOWNLINK_TR
           , sum(NON_BB_UPLINK_TR) NON_BB_UPLINK_TR, sum(NON_BB_DOWNLINK_TR) NON_BB_DOWNLINK_TR
           , sum(nvl(TOTAL_4G_UPLINK_TR,0)) + sum(nvl(TOTAL_4G_DOWNLINK_TR,0)) TOTAL_4G_TR
           , sum(nvl(TOTAL_3G_UPLINK_TR,0)) + sum(nvl(TOTAL_3G_DOWNLINK_TR,0)) TOTAL_3G_TR
           , sum(nvl(TOTAL_2G_UPLINK_TR,0)) + sum(nvl(TOTAL_2G_DOWNLINK_TR,0)) TOTAL_2G_TR
           , sum(nvl(OFFPEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(OFFPEAK_DATA_UPLINK_TR,0)) OFFPEAK_DATA_TR
           , sum(nvl(PEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(PEAK_DATA_UPLINK_TR,0)) PEAK_DATA_TR
           , sum(S_TOTAL_CHG) S_TOTAL_CHG, sum(S_OFFNET_CHG) S_OFFNET_CHG, sum(S_ONNET_CHG) S_ONNET_CHG
           , sum(BILLED_SMS) BILLED_SMS, sum(BILLED_OFFNET_SMS) BILLED_OFFNET_SMS
           , sum(BILLED_ONNET_SMS) BILLED_ONNET_SMS, sum(PROMO_SMS) PROMO_SMS
           , sum(PROMO_OFFNET_SMS) PROMO_OFFNET_SMS, sum(PROMO_ONNET_SMS) PROMO_ONNET_SMS
           , sum(S_OFFPEAK_CHG) S_OFFPEAK_CHG, sum(S_OFFNET_OFFPEAK_CHG) S_OFFNET_OFFPEAK_CHG
           , sum(S_ONNET_OFFPEAK_CHG) S_ONNET_OFFPEAK_CHG, sum(BILLED_OFFPEAK_SMS) BILLED_OFFPEAK_SMS
           , sum(BILLED_OFFNET_OFFPEAK_SMS) BILLED_OFFNET_OFFPEAK_SMS
           , sum(BILLED_ONNET_OFFPEAK_SMS) BILLED_ONNET_OFFPEAK_SMS, sum(PROMO_OFFPEAK_SMS) PROMO_OFFPEAK_SMS
           , sum(PROMO_OFFNET_OFFPEAK_SMS) PROMO_OFFNET_OFFPEAK_SMS, sum(PROMO_ONNET_OFFPEAK_SMS) PROMO_ONNET_OFFPEAK_SMS
           , sum(S_PEAK_CHG) S_PEAK_CHG, sum(S_OFFNET_PEAK_CHG) S_OFFNET_PEAK_CHG
           , sum(S_ONNET_PEAK_CHG) S_ONNET_PEAK_CHG, sum(BILLED_PEAK_SMS) BILLED_PEAK_SMS
           , sum(BILLED_OFFNET_PEAK_SMS) BILLED_OFFNET_PEAK_SMS, sum(BILLED_ONNET_PEAK_SMS) BILLED_ONNET_PEAK_SMS
           , sum(PROMO_PEAK_SMS) PROMO_PEAK_SMS, sum(PROMO_OFFNET_PEAK_SMS) PROMO_OFFNET_PEAK_SMS
           , sum(PROMO_ONNET_PEAK_SMS) PROMO_ONNET_PEAK_SMS, sum(SMS_HITS) SMS_HITS, sum(ONNET_SMS_HITS) ONNET_SMS_HITS
           , sum(OFFNET_SMS_HITS) OFFNET_SMS_HITS
           , sum(VA_VOICE_CHG) VA_VOICE_CHG, sum(VA_BB_CHG) VA_BB_CHG, sum(VA_SMS_CHG) VA_SMS_CHG, sum(VA_DATA_CHG) VA_DATA_CHG
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then SUB_ALLOC else 0 end) SUB_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_4G = 1 then SUB_ALLOC else 0 end) SUB_4G_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_3G = 1 then SUB_ALLOC else 0 end) SUB_3G_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_2G = 1 then SUB_ALLOC else 0 end) SUB_2G_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then SUB_ALLOC else 0 end) SUB_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_4G = 1 then SUB_ALLOC else 0 end) SUB_4G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_3G = 1 then SUB_ALLOC else 0 end) SUB_3G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_2G = 1 then SUB_ALLOC else 0 end) SUB_2G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_2G = 0 and FLAG_3G = 0 and FLAG_4G = 0 then SUB_ALLOC else 0 end) SUB_NON_DATA_FP
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then V_TOTAL_CHG else 0 end) V_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then G_TOTAL_CHG else 0 end) G_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then S_TOTAL_CHG else 0 end) S_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_VOICE_CHG else 0 end) VA_VOICE_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_SMS_CHG else 0 end) VA_SMS_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_DATA_CHG else 0 end) VA_DATA_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_BB_CHG else 0 end) VA_BB_U900
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then V_TOTAL_CHG else 0 end) V_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then G_TOTAL_CHG else 0 end) G_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then S_TOTAL_CHG else 0 end) S_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_VOICE_CHG else 0 end) VA_VOICE_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_SMS_CHG else 0 end) VA_SMS_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_DATA_CHG else 0 end) VA_DATA_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_BB_CHG else 0 end) VA_BB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then V_TOTAL_CHG else 0 end) V_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then G_TOTAL_CHG else 0 end) G_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then S_TOTAL_CHG else 0 end) S_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_VOICE_CHG else 0 end) VA_VOICE_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_SMS_CHG else 0 end) VA_SMS_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_DATA_CHG else 0 end) VA_DATA_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_BB_CHG else 0 end) VA_BB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(TOT_DUR,0) else 0 end) VOICE_DUR_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) + nvl(NON_BB_UPLINK_TR,0) + nvl(NON_BB_DOWNLINK_TR,0) else 0 end) DATA_TR_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(SMS_HITS,0) else 0 end) SMS_HITS_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(TOT_DUR,0) else 0 end) VOICE_DUR_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) + nvl(NON_BB_UPLINK_TR,0) + nvl(NON_BB_DOWNLINK_TR,0) else 0 end) DATA_TR_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(SMS_HITS,0) else 0 end) SMS_HITS_FP
           , sum(case when SUB_CAT_MHVC='MHVC' then V_TOTAL_CHG else 0 end) SUB_MHVC_V_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_VOICE_CHG else 0 end) SUB_MHVC_VA_VOICE_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then G_TOTAL_CHG else 0 end) SUB_MHVC_G_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_DATA_CHG else 0 end) SUB_MHVC_VA_DATA_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then S_TOTAL_CHG else 0 end) SUB_MHVC_SMS_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_SMS_CHG else 0 end) SUB_MHVC_VA_SMS_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_BB_CHG else 0 end) SUB_MHVC_VA_BB_CHG
           , sum(case when SUB_CAT='Platinum' then V_TOTAL_CHG else 0 end) SUB_PLATINUM_V_CHG
           , sum(case when SUB_CAT='Platinum' then VA_VOICE_CHG else 0 end) SUB_PLATINUM_VA_VOICE_CHG
           , sum(case when SUB_CAT='Platinum' then G_TOTAL_CHG else 0 end) SUB_PLATINUM_G_CHG
           , sum(case when SUB_CAT='Platinum' then VA_DATA_CHG else 0 end) SUB_PLATINUM_VA_DATA_CHG
           , sum(case when SUB_CAT='Platinum' then S_TOTAL_CHG else 0 end) SUB_PLATINUM_SMS_CHG
           , sum(case when SUB_CAT='Platinum' then VA_SMS_CHG else 0 end) SUB_PLATINUM_VA_SMS_CHG
           , sum(case when SUB_CAT='Platinum' then VA_BB_CHG else 0 end) SUB_PLATINUM_VA_BB_CHG
           , sum(case when SUB_CAT='Gold' then V_TOTAL_CHG else 0 end) SUB_GOLD_V_CHG
           , sum(case when SUB_CAT='Gold' then VA_VOICE_CHG else 0 end) SUB_GOLD_VA_VOICE_CHG
           , sum(case when SUB_CAT='Gold' then G_TOTAL_CHG else 0 end) SUB_GOLD_G_CHG
           , sum(case when SUB_CAT='Gold' then VA_DATA_CHG else 0 end) SUB_GOLD_VA_DATA_CHG
           , sum(case when SUB_CAT='Gold' then S_TOTAL_CHG else 0 end) SUB_GOLD_SMS_CHG
           , sum(case when SUB_CAT='Gold' then VA_SMS_CHG else 0 end) SUB_GOLD_VA_SMS_CHG
           , sum(case when SUB_CAT='Gold' then VA_BB_CHG else 0 end) SUB_GOLD_VA_BB_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then SUB_ALLOC else 0 end) SUB_MHVC
           , sum(case when SUB_CAT_MHVC='NON MHVC' then SUB_ALLOC else 0 end) SUB_NON_MHVC
           , sum(case when SUB_CAT='Platinum' then SUB_ALLOC else 0 end) SUB_PLATINUM
           , sum(case when SUB_CAT='Gold' then SUB_ALLOC else 0 end) SUB_GOLD
           , sum(case when SUB_CAT='Regular' then SUB_ALLOC else 0 end) SUB_REGULAR
           , sum(case when SUB_CAT='Mass' then SUB_ALLOC else 0 end) SUB_MASS
           , sum(case when SUB_DATA_CAT='Platinum' then SUB_ALLOC else 0 end) DATA_PLATINUM
           , sum(case when SUB_DATA_CAT='Gold' then SUB_ALLOC else 0 end) DATA_GOLD
           , sum(case when SUB_DATA_CAT='Regular' then SUB_ALLOC else 0 end) DATA_REGULAR
           , sum(case when SUB_DATA_CAT='Non Data Users' then SUB_ALLOC else 0 end) NON_DATA_REGULAR
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('VOICE ONLY', 'VOICE + DATA/VAS ONLY', 'VOICE + SMS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) VOICE_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('VOICE ONLY', 'VOICE + DATA/VAS ONLY', 'VOICE + SMS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) VOICE_SUB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('DATA/VAS ONLY', 'VOICE + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) DATA_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('DATA/VAS ONLY', 'VOICE + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) DATA_SUB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('SMS ONLY', 'SMS + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) SMS_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('SMS ONLY', 'SMS + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) SMS_SUB_FP
           , sum(case when TYP='VOICE ONLY' then SUB_ALLOC else 0 end) V_ONLY_SUBS
           , sum(case when TYP='DATA/VAS ONLY' then SUB_ALLOC else 0 end) D_ONLY_SUBS
           , sum(case when TYP='SMS ONLY' then SUB_ALLOC else 0 end) S_ONLY_SUBS
           , sum(case when TYP='VOICE + DATA/VAS ONLY' then SUB_ALLOC else 0 end) V_D_ONLY_SUBS
           , sum(case when TYP='VOICE + SMS ONLY' then SUB_ALLOC else 0 end) V_S_ONLY_SUBS
           , sum(case when TYP='SMS + DATA/VAS ONLY' then SUB_ALLOC else 0 end) S_D_ONLY_SUBS
           , sum(case when TYP='VOICE + SMS + DATA/VAS ONLY' then SUB_ALLOC else 0 end) V_S_D_ONLY_SUBS
           , sum(SUB_ALLOC) SUB_ALLOC
           , sum(case when nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) > 0 then SUB_ALLOC else 0 end) BB_ALLOC_SUB
           , sum(case when CUSTOMER_TYPE_MOD = 'B2B PREPAID' then SUB_ALLOC else 0 end) B2B_PREPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2C PREPAID' then SUB_ALLOC else 0 end) B2C_PREPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2B POSTPAID' then SUB_ALLOC else 0 end) B2B_POSTPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2C POSTPAID' then SUB_ALLOC else 0 end) B2C_POSTPAID_SUB_ALLOC
        from USG_LACCI_SMY_ALLOC
        group by LAC, CI--,SITE_ID, CELL_NAME, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR, AREA
"""
    val queryLacciTotalSmy="""
      select a.LAC, a.CI,a.SITE_ID, a.CELL_NAME, a.CELL_TECHNOLOGY_TYPE, a.CELL_COVERAGE_TYPE, a.CELL_BAND, a.SECTOR, a.AREA,
        a.V_TOTAL_CHG, a.V_OFFNET_CHG, a.V_ONNET_CHG, a.BILLED_DUR, a.BILLED_OFFNET_DUR, a.MOBOSP_REVENUE,
        a.BILLED_ONNET_DUR, a.PROMO_DUR, a.PROMO_OFFNET_DUR, a.PROMO_ONNET_DUR, a.TOT_DUR, a.V_OFFNET_DUR, a.V_ONNET_DUR, a.V_OFFPEAK_CHG, a.V_OFFNET_OFFPEAK_CHG,
        a.V_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_DUR, a.BILLED_OFFNET_OFFPEAK_DUR, a.BILLED_ONNET_OFFPEAK_DUR, a.PROMO_OFFPEAK_DUR, a.PROMO_OFFNET_OFFPEAK_DUR,
        a.PROMO_ONNET_OFFPEAK_DUR, a.TOTAL_OFFPEAK_DUR, a.V_OFFNET_OFFPEAK_DUR, a.V_ONNET_OFFPEAK_DUR, a.V_PEAK_CHG, a.V_OFFNET_PEAK_CHG, a.V_ONNET_PEAK_CHG,
        a.BILLED_PEAK_DUR, a.BILLED_OFFNET_PEAK_DUR, a.BILLED_ONNET_PEAK_DUR, a.PROMO_PEAK_DUR, a.PROMO_OFFNET_PEAK_DUR, a.PROMO_ONNET_PEAK_DUR, a.TOTAL_PEAK_DUR,
        a.V_OFFNET_PEAK_DUR, a.V_ONNET_PEAK_DUR, a.G_TOTAL_CHG, a.PPU_DATAVOL, a.ADDON_DATAVOL, a.G_OFFPEAK_CHG, a.G_PEAK_CHG, a.PPU_OFFPEAK_DATAVOL,
        a.PPU_PEAK_DATAVOL, a.ADDON_OFFPEAK_DATAVOL, a.ADDON_PEAK_DATAVOL, a.BB_UPLINK_TR, a.BB_DOWNLINK_TR, a.NON_BB_UPLINK_TR, a.NON_BB_DOWNLINK_TR,
        a.TOTAL_4G_TR, a.TOTAL_3G_TR, a.TOTAL_2G_TR, a.OFFPEAK_DATA_TR, a.PEAK_DATA_TR, a.S_TOTAL_CHG, a.S_OFFNET_CHG, a.S_ONNET_CHG, a.BILLED_SMS,
        a.BILLED_OFFNET_SMS, a.BILLED_ONNET_SMS, a.PROMO_SMS, a.PROMO_OFFNET_SMS, a.PROMO_ONNET_SMS, a.S_OFFPEAK_CHG, a.S_OFFNET_OFFPEAK_CHG,
        a.S_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_SMS, a.BILLED_OFFNET_OFFPEAK_SMS, a.BILLED_ONNET_OFFPEAK_SMS, a.PROMO_OFFPEAK_SMS, a.PROMO_OFFNET_OFFPEAK_SMS,
        a.PROMO_ONNET_OFFPEAK_SMS, a.S_PEAK_CHG, a.S_OFFNET_PEAK_CHG, a.S_ONNET_PEAK_CHG, a.BILLED_PEAK_SMS, a.BILLED_OFFNET_PEAK_SMS, a.BILLED_ONNET_PEAK_SMS,
        a.PROMO_PEAK_SMS, a.PROMO_OFFNET_PEAK_SMS, a.PROMO_ONNET_PEAK_SMS, a.SMS_HITS, a.ONNET_SMS_HITS, a.OFFNET_SMS_HITS, a.VA_VOICE_CHG, a.VA_SMS_CHG,
        a.VA_DATA_CHG, a.VA_BB_CHG, b.SUB_SP, b.SUB_4G_SP, b.SUB_3G_SP, b.SUB_2G_SP, b.SUB_FP, b.SUB_4G_FP, b.SUB_3G_FP, b.SUB_2G_FP,
        b.SUB_NON_DATA_FP, b.V_CHG_U900, b.G_CHG_U900, b.S_CHG_U900, b.VA_VOICE_U900, b.VA_SMS_U900, b.VA_DATA_U900, b.VA_BB_U900,
        b.V_CHG_SP, b.G_CHG_SP, b.S_CHG_SP, b.VA_VOICE_SP, b.VA_SMS_SP, b.VA_DATA_SP, b.VA_BB_SP, b.V_CHG_FP, b.G_CHG_FP, b.S_CHG_FP,
        b.VA_VOICE_FP, b.VA_SMS_FP, b.VA_DATA_FP, b.VA_BB_FP, b.VOICE_DUR_SP, b.DATA_TR_SP, b.SMS_HITS_SP, b.VOICE_DUR_FP,
        b.DATA_TR_FP, b.SMS_HITS_FP, b.SUB_MHVC_V_CHG, b.SUB_MHVC_VA_VOICE_CHG, b.SUB_MHVC_G_CHG, b.SUB_MHVC_VA_DATA_CHG,b.SUB_MHVC_VA_BB_CHG,
        b.SUB_PLATINUM_V_CHG, b.SUB_PLATINUM_VA_VOICE_CHG, b.SUB_PLATINUM_G_CHG, b.SUB_PLATINUM_VA_DATA_CHG,b.SUB_PLATINUM_VA_BB_CHG,
        b.SUB_PLATINUM_SMS_CHG, b.SUB_PLATINUM_VA_SMS_CHG, b.SUB_GOLD_V_CHG, b.SUB_GOLD_VA_VOICE_CHG,
        b.SUB_GOLD_G_CHG, b.SUB_GOLD_VA_DATA_CHG, b.SUB_GOLD_SMS_CHG, b.SUB_GOLD_VA_SMS_CHG, b.SUB_GOLD_VA_BB_CHG, b.SUB_MHVC, b.SUB_NON_MHVC, b.SUB_PLATINUM,
        b.SUB_GOLD, b.SUB_REGULAR, b.SUB_MASS, b.DATA_PLATINUM, b.DATA_GOLD, b.DATA_REGULAR, b.NON_DATA_REGULAR, b.VOICE_SUB_SP, b.VOICE_SUB_FP,
        b.DATA_SUB_SP, b.DATA_SUB_FP, b.SMS_SUB_SP, b.SMS_SUB_FP, b.V_ONLY_SUBS, b.D_ONLY_SUBS, b.S_ONLY_SUBS, b.V_D_ONLY_SUBS, b.V_S_ONLY_SUBS,
        b.S_D_ONLY_SUBS, b.V_S_D_ONLY_SUBS, b.SUB_ALLOC, b.BB_ALLOC_SUB, b.B2B_PREPAID_SUB_ALLOC, b.B2C_PREPAID_SUB_ALLOC, b.B2B_POSTPAID_SUB_ALLOC, b.B2C_POSTPAID_SUB_ALLOC
      from LACCI_SMY_USG a
      left join
      LACCI_SMY b
      on a.lac = b.lac
         and a.ci = b.ci
"""
    val queryLacciTotalSmy3="""
      select a.LAC, a.CI,--a.SITE_ID, a.CELL_NAME, a.CELL_TECHNOLOGY_TYPE, a.CELL_COVERAGE_TYPE, a.CELL_BAND, a.SECTOR, a.AREA,
        a.V_TOTAL_CHG, a.V_OFFNET_CHG, a.V_ONNET_CHG, a.BILLED_DUR, a.BILLED_OFFNET_DUR, --a.MOBOSP_REVENUE,
        a.BILLED_ONNET_DUR, a.PROMO_DUR, a.PROMO_OFFNET_DUR, a.PROMO_ONNET_DUR, a.TOT_DUR, a.V_OFFNET_DUR, a.V_ONNET_DUR, a.V_OFFPEAK_CHG, a.V_OFFNET_OFFPEAK_CHG,
        a.V_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_DUR, a.BILLED_OFFNET_OFFPEAK_DUR, a.BILLED_ONNET_OFFPEAK_DUR, a.PROMO_OFFPEAK_DUR, a.PROMO_OFFNET_OFFPEAK_DUR,
        a.PROMO_ONNET_OFFPEAK_DUR, a.TOTAL_OFFPEAK_DUR, a.V_OFFNET_OFFPEAK_DUR, a.V_ONNET_OFFPEAK_DUR, a.V_PEAK_CHG, a.V_OFFNET_PEAK_CHG, a.V_ONNET_PEAK_CHG,
        a.BILLED_PEAK_DUR, a.BILLED_OFFNET_PEAK_DUR, a.BILLED_ONNET_PEAK_DUR, a.PROMO_PEAK_DUR, a.PROMO_OFFNET_PEAK_DUR, a.PROMO_ONNET_PEAK_DUR, a.TOTAL_PEAK_DUR,
        a.V_OFFNET_PEAK_DUR, a.V_ONNET_PEAK_DUR, a.G_TOTAL_CHG, a.PPU_DATAVOL, a.ADDON_DATAVOL, a.G_OFFPEAK_CHG, a.G_PEAK_CHG, a.PPU_OFFPEAK_DATAVOL,
        a.PPU_PEAK_DATAVOL, a.ADDON_OFFPEAK_DATAVOL, a.ADDON_PEAK_DATAVOL, a.BB_UPLINK_TR, a.BB_DOWNLINK_TR, a.NON_BB_UPLINK_TR, a.NON_BB_DOWNLINK_TR,
        a.TOTAL_4G_TR, a.TOTAL_3G_TR, a.TOTAL_2G_TR, a.OFFPEAK_DATA_TR, a.PEAK_DATA_TR, a.S_TOTAL_CHG, a.S_OFFNET_CHG, a.S_ONNET_CHG, a.BILLED_SMS,
        a.BILLED_OFFNET_SMS, a.BILLED_ONNET_SMS, a.PROMO_SMS, a.PROMO_OFFNET_SMS, a.PROMO_ONNET_SMS, a.S_OFFPEAK_CHG, a.S_OFFNET_OFFPEAK_CHG,
        a.S_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_SMS, a.BILLED_OFFNET_OFFPEAK_SMS, a.BILLED_ONNET_OFFPEAK_SMS, a.PROMO_OFFPEAK_SMS, a.PROMO_OFFNET_OFFPEAK_SMS,
        a.PROMO_ONNET_OFFPEAK_SMS, a.S_PEAK_CHG, a.S_OFFNET_PEAK_CHG, a.S_ONNET_PEAK_CHG, a.BILLED_PEAK_SMS, a.BILLED_OFFNET_PEAK_SMS, a.BILLED_ONNET_PEAK_SMS,
        a.PROMO_PEAK_SMS, a.PROMO_OFFNET_PEAK_SMS, a.PROMO_ONNET_PEAK_SMS, a.SMS_HITS, a.ONNET_SMS_HITS, a.OFFNET_SMS_HITS, a.VA_VOICE_CHG, a.VA_SMS_CHG,
        a.VA_DATA_CHG, a.VA_BB_CHG, b.SUB_SP, b.SUB_4G_SP, b.SUB_3G_SP, b.SUB_2G_SP, b.SUB_FP, b.SUB_4G_FP, b.SUB_3G_FP, b.SUB_2G_FP,
        b.SUB_NON_DATA_FP, b.V_CHG_U900, b.G_CHG_U900, b.S_CHG_U900, b.VA_VOICE_U900, b.VA_SMS_U900, b.VA_DATA_U900, b.VA_BB_U900,
        b.V_CHG_SP, b.G_CHG_SP, b.S_CHG_SP, b.VA_VOICE_SP, b.VA_SMS_SP, b.VA_DATA_SP, b.VA_BB_SP, b.V_CHG_FP, b.G_CHG_FP, b.S_CHG_FP,
        b.VA_VOICE_FP, b.VA_SMS_FP, b.VA_DATA_FP, b.VA_BB_FP, b.VOICE_DUR_SP, b.DATA_TR_SP, b.SMS_HITS_SP, b.VOICE_DUR_FP,
        b.DATA_TR_FP, b.SMS_HITS_FP, b.SUB_MHVC_V_CHG, b.SUB_MHVC_VA_VOICE_CHG, b.SUB_MHVC_G_CHG, b.SUB_MHVC_VA_DATA_CHG,b.SUB_MHVC_VA_BB_CHG,
        b.SUB_PLATINUM_V_CHG, b.SUB_PLATINUM_VA_VOICE_CHG, b.SUB_PLATINUM_G_CHG, b.SUB_PLATINUM_VA_DATA_CHG,b.SUB_PLATINUM_VA_BB_CHG,
        b.SUB_PLATINUM_SMS_CHG, b.SUB_PLATINUM_VA_SMS_CHG, b.SUB_GOLD_V_CHG, b.SUB_GOLD_VA_VOICE_CHG,
        b.SUB_GOLD_G_CHG, b.SUB_GOLD_VA_DATA_CHG, b.SUB_GOLD_SMS_CHG, b.SUB_GOLD_VA_SMS_CHG, b.SUB_GOLD_VA_BB_CHG, b.SUB_MHVC, b.SUB_NON_MHVC, b.SUB_PLATINUM,
        b.SUB_GOLD, b.SUB_REGULAR, b.SUB_MASS, b.DATA_PLATINUM, b.DATA_GOLD, b.DATA_REGULAR, b.NON_DATA_REGULAR, b.VOICE_SUB_SP, b.VOICE_SUB_FP,
        b.DATA_SUB_SP, b.DATA_SUB_FP, b.SMS_SUB_SP, b.SMS_SUB_FP, b.V_ONLY_SUBS, b.D_ONLY_SUBS, b.S_ONLY_SUBS, b.V_D_ONLY_SUBS, b.V_S_ONLY_SUBS,
        b.S_D_ONLY_SUBS, b.V_S_D_ONLY_SUBS, b.SUB_ALLOC, b.BB_ALLOC_SUB, b.B2B_PREPAID_SUB_ALLOC, b.B2C_PREPAID_SUB_ALLOC, b.B2B_POSTPAID_SUB_ALLOC, b.B2C_POSTPAID_SUB_ALLOC
      from LACCI_SMY_USG a
      left join
      LACCI_SMY b
      on a.lac = b.lac
         and a.ci = b.ci
"""
    val queryReload="""
      select c.LAC, c.CI
              ,sum(c.RELOAD) RELOAD
              ,sum(case when SMARTPHONE_FLAG = 'Smartphone' then RELOAD else 0 end) RELOAD_SP
              ,sum(case when lower(SMARTPHONE_FLAG) in ('feature phone', 'feature phone data capable') then RELOAD else 0 end) RELOAD_FP
              ,sum(case when U900_FLAG = 'YES' then RELOAD else 0 end) RELOAD_U900
      from VBT_MAPGW_RELOAD c
      left join
      HANDSET_MSISDN d
      on c.MSISDN = d.MSISDN
      group by c.LAC, c.CI
"""
    val queryMapgwReload="""
    SELECT concat('62',MSISDN) MSISDN, LAC_ID LAC, CELL_ID CI, SUM(RLD_AMT)/100 RELOAD
    FROM MAPGW_RELOAD
    WHERE RELOAD_CHANNEL <> 'SSP'
    GROUP BY MSISDN, LAC_ID, CELL_ID
"""
    val queryUsgAllSvcLacciNew="""
      select coalesce(coalesce(coalesce(coalesce(a.msisdn,b.msisdn),c.msisdn),f.msisdn),e.msisdn) MSISDN,
      	coalesce(coalesce(coalesce(coalesce(a.lac,b.lac),c.lac),f.lac),e.lac) LAC,
      	coalesce(coalesce(coalesce(coalesce(a.ci,b.ci),c.ci),f.ci),e.ci) CI,
      	nvl(a.v_total_chg,0) v_total_chg,
      	nvl(a.v_offnet_chg,0) v_offnet_chg,
      	nvl(a.v_onnet_chg,0) v_onnet_chg,
      	nvl(a.billed_dur,0) billed_dur,
      	nvl(a.billed_offnet_dur,0) billed_offnet_dur,
      	nvl(a.billed_onnet_dur,0) billed_onnet_dur,
      	nvl(a.promo_dur,0) promo_dur,
      	nvl(a.promo_offnet_dur,0) promo_offnet_dur,
      	nvl(a.promo_onnet_dur,0) promo_onnet_dur,
      	nvl(a.tot_dur,0) tot_dur,
      	nvl(a.v_offnet_dur,0) v_offnet_dur,
      	nvl(a.v_onnet_dur,0) v_onnet_dur,
      	nvl(a.v_offpeak_chg,0) v_offpeak_chg,
      	nvl(a.v_offnet_offpeak_chg,0) v_offnet_offpeak_chg,
      	nvl(a.v_onnet_offpeak_chg,0) v_onnet_offpeak_chg,
      	nvl(a.billed_offpeak_dur,0) billed_offpeak_dur,
      	nvl(a.billed_offnet_offpeak_dur,0) billed_offnet_offpeak_dur,
      	nvl(a.billed_onnet_offpeak_dur,0) billed_onnet_offpeak_dur,
      	nvl(a.promo_offpeak_dur,0) promo_offpeak_dur,
      	nvl(a.promo_offnet_offpeak_dur,0) promo_offnet_offpeak_dur,
      	nvl(a.promo_onnet_offpeak_dur,0) promo_onnet_offpeak_dur,
      	nvl(a.total_offpeak_dur,0) total_offpeak_dur,
      	nvl(a.v_offnet_offpeak_dur,0) v_offnet_offpeak_dur,
      	nvl(a.v_onnet_offpeak_dur,0) v_onnet_offpeak_dur,
      	nvl(a.v_peak_chg,0) v_peak_chg,
      	nvl(a.v_offnet_peak_chg,0) v_offnet_peak_chg,
      	nvl(a.v_onnet_peak_chg,0) v_onnet_peak_chg,
      	nvl(a.billed_peak_dur,0) billed_peak_dur,
      	nvl(a.billed_offnet_peak_dur,0) billed_offnet_peak_dur,
      	nvl(a.billed_onnet_peak_dur,0) billed_onnet_peak_dur,
      	nvl(a.promo_peak_dur,0) promo_peak_dur,
      	nvl(a.promo_offnet_peak_dur,0) promo_offnet_peak_dur,
      	nvl(a.promo_onnet_peak_dur,0) promo_onnet_peak_dur,
      	nvl(a.total_peak_dur,0) total_peak_dur,
      	nvl(a.v_offnet_peak_dur,0) v_offnet_peak_dur,
      	nvl(a.v_onnet_peak_dur,0) v_onnet_peak_dur,
      	
      	nvl(b.g_total_chg,0) g_total_chg,
      	nvl(b.ppu_datavol,0) ppu_datavol,
      	nvl(b.addon_datavol,0) addon_datavol,
      	nvl(b.total_datavol,0) total_datavol,
      	nvl(b.g_4g_total_chg,0) g_4g_total_chg,
      	nvl(b.g_3g_total_chg,0) g_3g_total_chg,
      	nvl(b.g_2g_total_chg,0) g_2g_total_chg,
      	nvl(b.g_4g_ppu_datavol,0) g_4g_ppu_datavol,
      	nvl(b.g_3g_ppu_datavol,0) g_3g_ppu_datavol,
      	nvl(b.g_2g_ppu_datavol,0) g_2g_ppu_datavol,
      	nvl(b.g_4g_addon_datavol,0) g_4g_addon_datavol,
      	nvl(b.g_3g_addon_datavol,0) g_3g_addon_datavol,
      	nvl(b.g_2g_addon_datavol,0) g_2g_addon_datavol,
      	nvl(b.g_offpeak_chg,0) g_offpeak_chg,
      	nvl(b.g_peak_chg,0) g_peak_chg,
      	nvl(b.ppu_offpeak_datavol,0) ppu_offpeak_datavol,
      	nvl(b.ppu_peak_datavol,0) ppu_peak_datavol,
      	nvl(b.addon_offpeak_datavol,0) addon_offpeak_datavol,
      	nvl(b.addon_peak_datavol,0) addon_peak_datavol,
      	nvl(b.total_offpeak_datavol,0) total_offpeak_datavol,
      	nvl(b.total_peak_datavol,0) total_peak_datavol,
      	
      	nvl(c.s_total_chg,0) s_total_chg,
      	nvl(c.s_offnet_chg,0) s_offnet_chg,
      	nvl(c.s_onnet_chg,0) s_onnet_chg,
      	nvl(c.billed_sms,0) billed_sms,
      	nvl(c.billed_offnet_sms,0) billed_offnet_sms,
      	nvl(c.billed_onnet_sms,0) billed_onnet_sms,
      	nvl(c.promo_sms,0) promo_sms,
      	nvl(c.promo_offnet_sms,0) promo_offnet_sms,
      	nvl(c.promo_onnet_sms,0) promo_onnet_sms,
      	nvl(c.s_offpeak_chg,0) s_offpeak_chg,
      	nvl(c.s_offnet_offpeak_chg,0) s_offnet_offpeak_chg,
      	nvl(c.s_onnet_offpeak_chg,0) s_onnet_offpeak_chg,
      	nvl(c.billed_offpeak_sms,0) billed_offpeak_sms,
      	nvl(c.billed_offnet_offpeak_sms,0) billed_offnet_offpeak_sms,
      	nvl(c.billed_onnet_offpeak_sms,0) billed_onnet_offpeak_sms,
      	nvl(c.promo_offpeak_sms,0) promo_offpeak_sms,
      	nvl(c.promo_offnet_offpeak_sms,0) promo_offnet_offpeak_sms,
      	nvl(c.promo_onnet_offpeak_sms,0) promo_onnet_offpeak_sms,
      	nvl(c.s_peak_chg,0) s_peak_chg,
      	nvl(c.s_offnet_peak_chg,0) s_offnet_peak_chg,
      	nvl(c.s_onnet_peak_chg,0) s_onnet_peak_chg,
      	nvl(c.billed_peak_sms,0) billed_peak_sms,
      	nvl(c.billed_offnet_peak_sms,0) billed_offnet_peak_sms,
      	nvl(c.billed_onnet_peak_sms,0) billed_onnet_peak_sms,
      	nvl(c.promo_peak_sms,0) promo_peak_sms,
      	nvl(c.promo_offnet_peak_sms,0) promo_offnet_peak_sms,
      	nvl(c.promo_onnet_peak_sms,0) promo_onnet_peak_sms,
      	
      	nvl(f.bb_uplink_tr,0) bb_uplink_tr,
      	nvl(f.bb_downlink_tr,0) bb_downlink_tr,
      	nvl(f.non_bb_uplink_tr,0) non_bb_uplink_tr,
      	nvl(f.non_bb_downlink_tr,0) non_bb_downlink_tr,
      	nvl(f.total_4g_uplink_tr,0) total_4g_uplink_tr,
      	nvl(f.total_4g_downlink_tr,0) total_4g_downlink_tr,
      	nvl(f.total_3g_uplink_tr,0) total_3g_uplink_tr,
      	nvl(f.total_3g_downlink_tr,0) total_3g_downlink_tr,
      	nvl(f.total_2g_uplink_tr,0) total_2g_uplink_tr,
      	nvl(f.total_2g_downlink_tr,0) total_2g_downlink_tr,
      	nvl(f.offpeak_data_downlink_tr,0) offpeak_data_downlink_tr,
      	nvl(f.offpeak_data_uplink_tr,0) offpeak_data_uplink_tr,
      	nvl(f.peak_data_downlink_tr,0) peak_data_downlink_tr,
      	nvl(f.peak_data_uplink_tr,0) peak_data_uplink_tr,
      	
      	nvl(e.sms_hits,0) sms_hits,
      	nvl(e.onnet_sms_hits,0) onnet_sms_hits,
      	nvl(e.offnet_sms_hits,0) offnet_sms_hits
      from
      	USG_VOICE_LACCI_MO_SMY a full outer join USG_GPRS_LACCI_MO_SMY b on (a.MSISDN=b.MSISDN and a.CI=b.CI and a.LAC=b.LAC) 
      	full outer join USG_SMS_LACCI_MO_SMY c on (c.MSISDN=b.MSISDN and c.CI=b.CI and c.LAC=b.LAC)
      	full outer join TR_GGSN_LACCI_MO_SMY f on (c.MSISDN=f.MSISDN and c.CI=f.CI and c.LAC=f.LAC)
      	full outer join TR_SMS_LACCI_MO_SMY e on (f.MSISDN=e.MSISDN and f.CI=e.CI and f.LAC=e.LAC)
"""
    val queryAllSvcLacciSubAllocNew="""
    select 
       a.*
      , b.national_dur
      , b.national_bb_tr
      , b.national_non_bb_tr
      , b.national_sms_hits
      ,case when national_dur = 0 then 0 else tot_dur/national_dur end v_alloc_pct
      ,case when national_sms_hits = 0 then 0 else sms_hits/national_sms_hits end s_alloc_pct
      ,case when national_bb_tr = 0 then 0 else (nvl(bb_uplink_tr,0)+nvl(bb_downlink_tr,0))/national_bb_tr end bb_alloc_pct
      ,case when national_non_bb_tr = 0 then 0 else (nvl(non_bb_uplink_tr,0)+nvl(non_bb_downlink_tr,0))/national_non_bb_tr end non_bb_alloc_pct
      ,%1$s MONTH_ID
   from USG_ALL_SVC_LACCI_MO_SMY a join
     (select msisdn
               ,sum(nvl(tot_dur,0)) national_dur
               ,sum(nvl(bb_downlink_tr,0)) + sum(nvl(bb_uplink_tr,0)) national_bb_tr
               ,sum(nvl(non_bb_downlink_tr,0)) + sum(nvl(non_bb_uplink_tr,0)) national_non_bb_tr
               ,sum(nvl(sms_hits,0)) national_sms_hits
         from USG_ALL_SVC_LACCI_MO_SMY
         group by msisdn) b
    on a.msisdn = b.msisdn
    --where a.msisdn not in (select msisdn in POSTPAID_SUBS)
"""
    val queryMsisdnRef="""
        SELECT 
        	coalesce(coalesce(a.MSISDN,b.MSISDN),c.MSISDN) MSISDN,
        	a.TNR,
        	a.PROMO_PACKAGE_NAME,
          a.customer_type_mod,
          b.FLAG_TAG device_fl, 
          b.smartphone_flag, 
          case when b.network_flag='4G' then 1 else 0 end flag_4g,
          case when b.network_flag in ('3G','3G/U900') and b.network_flag<>'4G' then 1 else 0 end flag_3g,      
          case when b.network_flag='2G' and b.network_flag not in ('4G','3G','3G/U900') then 1 else 0 end flag_2g,    
          case when b.u900_flag='YES' then 1 else 0 end u900_fl,
        	CASE
            WHEN c.TOTAL_CHG > 100000 THEN 'A) > 100.000 Rp'
            WHEN c.TOTAL_CHG > 50000 THEN  'B) 50.000-100.000 Rp'
            WHEN c.TOTAL_CHG > 20000 THEN  'C) 20.000-50.000 Rp'
            WHEN c.TOTAL_CHG > 0 THEN 'D) 1-20.000 Rp'
            ELSE 'E) = 0 Rp' end as TOTAL_CHG_GRP,
         CASE 
            WHEN c.DATA_CHG > 50000 THEN 'A) > 50.000 Rp'
            WHEN c.DATA_CHG > 20000 THEN 'B) 20.000-50.000 Rp'
            WHEN c.DATA_CHG > 0 THEN 'C) 1-20.000 Rp'
            ELSE 'D) = 0 Rp' end as DATA_CHG_GRP,
        	c.TOTAL_CHG,
        	c.DATA_CHG,
          c.VA_VOICE_CHG,
          c.VA_DATA_CHG,
          c.VA_BB_CHG,
          c.VA_SMS_CHG,
        	c.TYP,
          case when c.TOTAL_CHG > 100000 then 'Platinum'
              when c.TOTAL_CHG > 50000 then 'Gold'
              when c.TOTAL_CHG > 20000 then 'Regular'
              when c.TOTAL_CHG > 0 then 'Mass'
              else 'No Usage' end SUB_CAT,
          case when c.TOTAL_CHG > 50000 or a.ar_prim_pymt_tp_id = '2003' then 'MHVC'
              else 'NON MHVC' end SUB_CAT_MHVC,
          case when c.DATA_CHG > 50000 then 'Platinum'
                when c.DATA_CHG > 20000 then 'Gold'
                when c.DATA_CHG > 0 then 'Regular'
                else 'Non Data Users' end SUB_DATA_CAT,
          %1$s MONTH_ID
        FROM 
        	SNAPSHOT a FULL OUTER JOIN HANDSET_MSISDN b on (a.MSISDN=b.MSISDN)
        	FULL OUTER JOIN UMR_SEGMENT_ADDON c on (c.MSISDN=b.MSISDN)
"""
    val queryMsisdnRefAddon="""
        SELECT
        
        FROM MSISDN_REF a LEFT OUTER JOIN ADDON b 
"""
    val queryAlllSvcLacciAllocEnrich="""
        select a.*,b.TNR,
            b.PROMO_PACKAGE_NAME,
            b.DEVICE_FL,
            b.smartphone_flag,
            b.FLAG_4G,
            b.FLAG_3G,
            b.FLAG_2G,
            b.U900_FL,
            b.TOTAL_CHG_GRP,
            b.DATA_CHG_GRP,
            b.TOTAL_CHG,
            b.DATA_CHG,
            b.TYP,
            case when nvl(b.TOTAL_CHG,0) > 100000 or (nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.VA_TOTAL_CHG,0)) >= 400000 then 'Platinum'
                  when nvl(b.TOTAL_CHG,0) > 50000 or (nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.VA_TOTAL_CHG,0)) >= 200000 then 'Gold'
                    when nvl(b.TOTAL_CHG,0) > 20000 or (nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.VA_TOTAL_CHG,0)) >= 100000 then 'Regular'
                      when nvl(b.TOTAL_CHG,0) > 0 then 'Mass'
                        else 'No Usage' end SUB_CAT,
            case when nvl(b.DATA_CHG,0) > 50000 or nvl(a.G_TOTAL_CHG,0) >= 50000 then 'Platinum'
                    when nvl(b.DATA_CHG,0) > 20000 or nvl(a.G_TOTAL_CHG,0) >= 20000 then 'Gold'
                      when nvl(b.DATA_CHG,0) > 0 or nvl(a.G_TOTAL_CHG,0) > 0 then 'Regular'
                        else 'Non Data Users' end SUB_DATA_CAT,
            nvl(b.VA_VOICE_CHG,0) * a.V_ALLOC_PCT VA_VOICE_CHG,
            nvl(b.VA_SMS_CHG,0) * a.S_ALLOC_PCT VA_SMS_CHG,
            nvl(b.VA_BB_CHG,0) * a.BB_ALLOC_PCT VA_BB_CHG,
            nvl(b.VA_DATA_CHG,0) * a.NON_BB_ALLOC_PCT VA_DATA_CHG,
            nvl(b.VA_TOTAL_CHG,0) * a.S_ALLOC_PCT VA_TOTAL_CHG
            
        from ALL_SVC_LACCI_SUB_ALLOC a left outer join MSISDN_REF b on(a.MSISDN=b.MSISDN)
"""
    val queryPostpaidSmsTrMoNew="""
      select 
        A.*
      from TR_SMS_LACCI_MO_SMY A join POSTPAID_SUBS B
      on A.MSISDN = B.MSISDN
"""
    val queryPostpaidDataTrMoNew="""
      select 
      	A.MSISDN, 
      	A.LAC, 
      	A.CI,
      	A.BB_UPLINK_TR,
      	A.BB_DOWNLINK_TR,
      	A.NON_BB_UPLINK_TR,
      	A.NON_BB_DOWNLINK_TR,
      	A.TOTAL_4G_UPLINK_TR,
      	A.TOTAL_4G_DOWNLINK_TR,
      	A.TOTAL_3G_UPLINK_TR,
      	A.TOTAL_3G_DOWNLINK_TR,
      	A.TOTAL_2G_UPLINK_TR,
      	A.TOTAL_2G_DOWNLINK_TR,
      	A.OFFPEAK_DATA_DOWNLINK_TR,
      	A.OFFPEAK_DATA_UPLINK_TR,
      	A.PEAK_DATA_DOWNLINK_TR,
      	A.PEAK_DATA_UPLINK_TR,
        A.NUM_DAYS_DATA
      from TR_GGSN_LACCI_MO_SMY A join POSTPAID_SUBS B
      on A.MSISDN = B.MSISDN
"""
    val queryPostpaidVoiceTrMoNew="""
      select 
          a.MSISDN
         ,a.LAC
         ,a.CI 
         , sum(a.DUR) TOTAL_DUR
         , sum(case when lower(a.TYP) = 'onnet' then a.DUR else 0 end) ONNET_DUR
         , sum(case when lower(a.TYP) = 'offnet' then a.DUR else 0 end) OFFNET_DUR
         , count(distinct DAYDATE) NUM_DAYS_VOICE
      from TRAFFIC_VOICE_DLY_LACCI a join POSTPAID_SUBS b
      on a.MSISDN = b.MSISDN
      group by a.MSISDN
      ,a.LAC
      ,a.CI
"""
    val queryPostpaidVSGTrMoNew="""
      SELECT
         COALESCE(a.msisdn,b.msisdn,c.msisdn) msisdn,
         COALESCE(a.lac,b.lac,c.lac) lac, 
         COALESCE(a.ci,b.ci,c.ci) ci,
         a.total_dur,
         a.onnet_dur,
         a.offnet_dur,
         b.sms_hits,
         b.onnet_sms_hits,
         b.offnet_sms_hits,
         c.bb_uplink_tr,
         c.bb_downlink_tr,
         c.non_bb_uplink_tr,
         c.non_bb_downlink_tr,
         c.total_3g_uplink_tr,
         c.total_3g_downlink_tr,
         c.total_2g_uplink_tr,
         c.total_2g_downlink_tr,
         c.offpeak_data_downlink_tr,
         c.offpeak_data_uplink_tr,
         c.peak_data_downlink_tr,
         c.peak_data_uplink_tr,
         c.TOTAL_4G_UPLINK_TR,
         c.TOTAL_4G_DOWNLINK_TR,
         a.NUM_DAYS_VOICE,
         b.NUM_DAYS_SMS,
         c.NUM_DAYS_DATA
      FROM 
         POSTPAID_VOICE_TR a full outer join POSTPAID_SMS_TR b on (a.msisdn = b.msisdn and trim(a.lac) = trim(b.lac) AND trim(a.ci)= trim(b.ci))
		     FULL OUTER JOIN
         POSTPAID_DATA_TR c ON (c.msisdn = b.msisdn and trim(c.lac) = trim(b.lac) AND trim(c.ci)= trim(b.ci))
"""
    val queryPostpaidAllTrRevNew="""
      SELECT a.MSISDN 
         , a.LAC 
         , a.CI 
         , a.TOTAL_DUR 
         , a.ONNET_DUR 
         , a.OFFNET_DUR 
         , a.SMS_HITS 
         , a.ONNET_SMS_HITS 
         , a.OFFNET_SMS_HITS 
         , a.BB_UPLINK_TR 
         , a.BB_DOWNLINK_TR 
         , a.NON_BB_UPLINK_TR 
         , a.NON_BB_DOWNLINK_TR 
         , a.TOTAL_3G_UPLINK_TR 
         , a.TOTAL_3G_DOWNLINK_TR 
         , a.TOTAL_2G_UPLINK_TR 
         , a.TOTAL_2G_DOWNLINK_TR 
         , a.OFFPEAK_DATA_DOWNLINK_TR 
         , a.OFFPEAK_DATA_UPLINK_TR 
         , a.PEAK_DATA_DOWNLINK_TR 
         , a.PEAK_DATA_UPLINK_TR 
         , a.TOTAL_4G_UPLINK_TR 
         , a.TOTAL_4G_DOWNLINK_TR 
         , a.NATIONAL_DUR 
         , a.NATIONAL_BB_TR 
         , a.NATIONAL_NON_BB_TR 
         , a.NATIONAL_SMS_HITS 
         , a.V_ALLOC_PCT 
         , a.V_ONNET_ALLOC_PCT 
         , a.V_OFFNET_ALLOC_PCT 
         , a.S_ALLOC_PCT 
         , a.S_ONNET_ALLOC_PCT 
         , a.S_OFFNET_ALLOC_PCT 
         , a.BB_ALLOC_PCT 
         , a.NON_BB_ALLOC_PCT , 
           b.customer_type_mod, 
           (nvl(b.v_onnet_chg,0)+nvl(b.v_offnet_chg,0)+nvl(b.v_others_chg,0)) * a.v_alloc_pct v_total_chg ,
           nvl(b.v_onnet_chg,0) * a.v_onnet_alloc_pct v_onnet_chg , nvl(b.v_offnet_chg,0) * a.v_offnet_alloc_pct v_offnet_chg,
           (nvl(b.s_onnet_chg,0)+nvl(b.s_offnet_chg,0)+nvl(b.s_others_chg,0)) * a.s_alloc_pct s_total_chg , nvl(b.s_onnet_chg,0) * a.s_onnet_alloc_pct s_onnet_chg,
           nvl(b.s_offnet_chg,0) * a.s_offnet_alloc_pct s_offnet_chg,
           nvl(b.g_total_chg,0) * a.non_bb_alloc_pct g_total_chg,
           nvl(b.va_total_chg,0) * a.s_alloc_pct va_total_chg, %1$s MONTH_ID
      FROM POSTPAID_ALL_TR_ALLOC a LEFT JOIN POSTPAID_CUSTTYPE_TP b ON a.msisdn = b.msisdn
"""
    val queryPostpaidAllTrRevNew1="""
      SELECT COALESCE(a.MSISDN,b.MSISDN) MSISDN 
         , COALESCE(a.LAC,b.LAC) LAC 
         , COALESCE(a.CI,b.CI) CI 
         , a.TOTAL_DUR 
         , a.ONNET_DUR 
         , a.OFFNET_DUR 
         , a.SMS_HITS 
         , a.ONNET_SMS_HITS 
         , a.OFFNET_SMS_HITS 
         , a.BB_UPLINK_TR 
         , a.BB_DOWNLINK_TR 
         , a.NON_BB_UPLINK_TR 
         , a.NON_BB_DOWNLINK_TR 
         , a.TOTAL_3G_UPLINK_TR 
         , a.TOTAL_3G_DOWNLINK_TR 
         , a.TOTAL_2G_UPLINK_TR 
         , a.TOTAL_2G_DOWNLINK_TR 
         , a.OFFPEAK_DATA_DOWNLINK_TR 
         , a.OFFPEAK_DATA_UPLINK_TR 
         , a.PEAK_DATA_DOWNLINK_TR 
         , a.PEAK_DATA_UPLINK_TR 
         , a.TOTAL_4G_UPLINK_TR 
         , a.TOTAL_4G_DOWNLINK_TR 
         , b.V_TOTAL_CHG
         , b.V_OFFNET_CHG
         , b.V_ONNET_CHG
         , b.S_TOTAL_CHG 
         , b.S_ONNET_CHG
         , b.S_OFFNET_CHG
         , b.G_TOTAL_CHG
         , b.VA_TOTAL_CHG
         , a.NUM_DAYS_VOICE
         , a.NUM_DAYS_SMS
         , a.NUM_DAYS_DATA 
         , %1$s MONTH_ID
      FROM POSTPAID_V_S_G_TR a 
      FULL OUTER JOIN POSTPAID_CUSTTYPE_TP b ON a.MSISDN = b.MSISDN and a.LAC=b.LAC and a.CI=b.CI
"""
    val queryPrePostpaidLacciSmyNew="""
        select 
           coalesce(a.msisdn,b.msisdn) msisdn
         , coalesce(a.lac,b.lac) lac
         , coalesce(a.ci,b.ci) ci
         , nvl(a.v_total_chg,b.v_total_chg) v_total_chg
         , nvl(a.v_offnet_chg,b.v_offnet_chg) v_offnet_chg
         , nvl(a.v_onnet_chg,b.v_onnet_chg) v_onnet_chg
         , a.billed_dur, a.billed_offnet_dur, a.billed_onnet_dur, a.promo_dur, a.promo_offnet_dur, a.promo_onnet_dur
         , nvl(a.tot_dur,b.total_dur) tot_dur
         , nvl(a.v_offnet_dur, b.offnet_dur) v_offnet_dur
         , nvl(a.v_onnet_dur,b.onnet_dur) v_onnet_dur
         , a.v_offpeak_chg, a.v_offnet_offpeak_chg, a.v_onnet_offpeak_chg, a.billed_offpeak_dur, a.billed_offnet_offpeak_dur
         , a.billed_onnet_offpeak_dur, a.promo_offpeak_dur, a.promo_offnet_offpeak_dur, a.promo_onnet_offpeak_dur
         , a.total_offpeak_dur, a.v_offnet_offpeak_dur, a.v_onnet_offpeak_dur, a.v_peak_chg, a.v_offnet_peak_chg
         , a.v_onnet_peak_chg, a.billed_peak_dur, a.billed_offnet_peak_dur, a.billed_onnet_peak_dur, a.promo_peak_dur
         , a.promo_offnet_peak_dur, a.promo_onnet_peak_dur, a.total_peak_dur, a.v_offnet_peak_dur, a.v_onnet_peak_dur
         , nvl(a.g_total_chg,b.g_total_chg) g_total_chg
         , a.ppu_datavol, a.addon_datavol, a.total_datavol, a.g_3g_total_chg, a.g_2g_total_chg, a.g_3g_ppu_datavol
         , a.g_2g_ppu_datavol, a.g_3g_addon_datavol, a.g_2g_addon_datavol, a.g_offpeak_chg, a.g_peak_chg
         , a.ppu_offpeak_datavol, a.ppu_peak_datavol, a.addon_offpeak_datavol, a.addon_peak_datavol, a.total_offpeak_datavol
         , a.total_peak_datavol
         , a.G_4G_TOTAL_CHG, a.G_4G_PPU_DATAVOL, a.G_4G_ADDON_DATAVOL
         , nvl(a.bb_uplink_tr,b.bb_uplink_tr) bb_uplink_tr
         , nvl(a.bb_downlink_tr,b.bb_downlink_tr) bb_downlink_tr
         , nvl(a.non_bb_uplink_tr,b.non_bb_uplink_tr) non_bb_uplink_tr
         , nvl(a.non_bb_downlink_tr,b.non_bb_downlink_tr) non_bb_downlink_tr
         , nvl(a.TOTAL_4G_UPLINK_TR, b.TOTAL_4G_UPLINK_TR) TOTAL_4G_UPLINK_TR
         , nvl(a.TOTAL_4G_DOWNLINK_TR, b.TOTAL_4G_DOWNLINK_TR) TOTAL_4G_DOWNLINK_TR
         , nvl(a.total_3g_uplink_tr,b.total_3g_uplink_tr) total_3g_uplink_tr
         , nvl(a.total_3g_downlink_tr,b.total_3g_downlink_tr) total_3g_downlink_tr
         , nvl(a.total_2g_uplink_tr,b.total_2g_uplink_tr) total_2g_uplink_tr
         , nvl(a.total_2g_downlink_tr,b.total_2g_downlink_tr) total_2g_downlink_tr
         , nvl(a.offpeak_data_downlink_tr,b.offpeak_data_downlink_tr) offpeak_data_downlink_tr
         , nvl(a.offpeak_data_uplink_tr,b.offpeak_data_uplink_tr) offpeak_data_uplink_tr
         , nvl(a.peak_data_downlink_tr,b.peak_data_downlink_tr) peak_data_downlink_tr
         , nvl(a.peak_data_uplink_tr,b.peak_data_uplink_tr) peak_data_uplink_tr
         , nvl(a.s_total_chg,b.s_total_chg) s_total_chg
         , nvl(a.s_offnet_chg,b.s_offnet_chg) s_offnet_chg
         , nvl(a.s_onnet_chg,b.s_onnet_chg) s_onnet_chg
         , a.billed_sms, a.billed_offnet_sms, a.billed_onnet_sms, a.promo_sms, a.promo_offnet_sms, a.promo_onnet_sms
         , a.s_offpeak_chg, a.s_offnet_offpeak_chg, a.s_onnet_offpeak_chg, a.billed_offpeak_sms, a.billed_offnet_offpeak_sms
         , a.billed_onnet_offpeak_sms, a.promo_offpeak_sms, a.promo_offnet_offpeak_sms, a.promo_onnet_offpeak_sms
         , a.s_peak_chg, a.s_offnet_peak_chg, a.s_onnet_peak_chg, a.billed_peak_sms, a.billed_offnet_peak_sms
         , a.billed_onnet_peak_sms, a.promo_peak_sms, a.promo_offnet_peak_sms, a.promo_onnet_peak_sms
         , nvl(a.sms_hits,b.sms_hits) sms_hits
         , nvl(a.onnet_sms_hits,b.onnet_sms_hits) onnet_sms_hits
         , nvl(a.offnet_sms_hits,b.offnet_sms_hits) offnet_sms_hits
         , a.v_alloc_pct
         , a.s_alloc_pct
         , a.bb_alloc_pct
         , a.non_bb_alloc_pct
         , nvl(a.num_days_data,b.num_days_data) num_days_data
         , nvl(a.num_days_voice,b.num_days_voice) num_days_voice
         , nvl(a.num_days_sms,b.num_days_sms) num_days_sms
      from ALL_SVC_LACCI_SUB_ALLOC a
          full outer join
      POSTPAID_ALL_TR_REV b
        on a.msisdn = b.msisdn
        --and a.lac = b.lac and a.ci = b.ci
"""
    val queryLacciSmyUsgNew="""
      select  regexp_replace(LAC,'[^-0-9]','') LAC, regexp_replace(CI,'[^-0-9]','') CI, 
           case when TNR is null then '05. Unknown'
                    when TNR <= 63 then '01. <= 63 Days'
                      when TNR <= 180 then '02. 64 Days - 6 Months'
                        when TNR <= 360 then '03. 6 - 12 Months'
                          else '04. > 12 Months'
             end TENURE
           , PROMO_PACKAGE_NAME
           , sum(V_TOTAL_CHG) V_TOTAL_CHG, sum(V_OFFNET_CHG) V_OFFNET_CHG, sum(V_ONNET_CHG) V_ONNET_CHG
           , sum(BILLED_DUR) BILLED_DUR, sum(BILLED_OFFNET_DUR) BILLED_OFFNET_DUR, sum(BILLED_ONNET_DUR) BILLED_ONNET_DUR
           , sum(PROMO_DUR) PROMO_DUR, sum(PROMO_OFFNET_DUR) PROMO_OFFNET_DUR, sum(PROMO_ONNET_DUR) PROMO_ONNET_DUR
           , sum(TOT_DUR) TOT_DUR, sum(V_OFFNET_DUR) V_OFFNET_DUR, sum(V_ONNET_DUR) V_ONNET_DUR, sum(V_OFFPEAK_CHG) V_OFFPEAK_CHG
           , sum(V_OFFNET_OFFPEAK_CHG) V_OFFNET_OFFPEAK_CHG, sum(V_ONNET_OFFPEAK_CHG) V_ONNET_OFFPEAK_CHG
           , sum(BILLED_OFFPEAK_DUR) BILLED_OFFPEAK_DUR, sum(BILLED_OFFNET_OFFPEAK_DUR) BILLED_OFFNET_OFFPEAK_DUR
           , sum(BILLED_ONNET_OFFPEAK_DUR) BILLED_ONNET_OFFPEAK_DUR, sum(PROMO_OFFPEAK_DUR) PROMO_OFFPEAK_DUR
           , sum(PROMO_OFFNET_OFFPEAK_DUR) PROMO_OFFNET_OFFPEAK_DUR, sum(PROMO_ONNET_OFFPEAK_DUR) PROMO_ONNET_OFFPEAK_DUR
           , sum(TOTAL_OFFPEAK_DUR) TOTAL_OFFPEAK_DUR, sum(V_OFFNET_OFFPEAK_DUR) V_OFFNET_OFFPEAK_DUR
           , sum(V_ONNET_OFFPEAK_DUR) V_ONNET_OFFPEAK_DUR, sum(V_PEAK_CHG) V_PEAK_CHG, sum(V_OFFNET_PEAK_CHG) V_OFFNET_PEAK_CHG
           , sum(V_ONNET_PEAK_CHG) V_ONNET_PEAK_CHG, sum(BILLED_PEAK_DUR) BILLED_PEAK_DUR
           , sum(BILLED_OFFNET_PEAK_DUR) BILLED_OFFNET_PEAK_DUR, sum(BILLED_ONNET_PEAK_DUR) BILLED_ONNET_PEAK_DUR
           , sum(PROMO_PEAK_DUR) PROMO_PEAK_DUR, sum(PROMO_OFFNET_PEAK_DUR) PROMO_OFFNET_PEAK_DUR
           , sum(PROMO_ONNET_PEAK_DUR) PROMO_ONNET_PEAK_DUR, sum(TOTAL_PEAK_DUR) TOTAL_PEAK_DUR
           , sum(V_OFFNET_PEAK_DUR) V_OFFNET_PEAK_DUR, sum(V_ONNET_PEAK_DUR) V_ONNET_PEAK_DUR
           , sum(G_TOTAL_CHG) G_TOTAL_CHG, sum(PPU_DATAVOL) PPU_DATAVOL, sum(ADDON_DATAVOL) ADDON_DATAVOL
           , sum(G_OFFPEAK_CHG) G_OFFPEAK_CHG, sum(G_PEAK_CHG) G_PEAK_CHG, sum(PPU_OFFPEAK_DATAVOL) PPU_OFFPEAK_DATAVOL
           , sum(PPU_PEAK_DATAVOL) PPU_PEAK_DATAVOL, sum(ADDON_OFFPEAK_DATAVOL) ADDON_OFFPEAK_DATAVOL
           , sum(ADDON_PEAK_DATAVOL) ADDON_PEAK_DATAVOL, sum(BB_UPLINK_TR) BB_UPLINK_TR, sum(BB_DOWNLINK_TR) BB_DOWNLINK_TR
           , sum(NON_BB_UPLINK_TR) NON_BB_UPLINK_TR, sum(NON_BB_DOWNLINK_TR) NON_BB_DOWNLINK_TR
           , sum(nvl(TOTAL_4G_UPLINK_TR,0)) + sum(nvl(TOTAL_4G_DOWNLINK_TR,0)) TOTAL_4G_TR
           , sum(nvl(TOTAL_3G_UPLINK_TR,0)) + sum(nvl(TOTAL_3G_DOWNLINK_TR,0)) TOTAL_3G_TR
           , sum(nvl(TOTAL_2G_UPLINK_TR,0)) + sum(nvl(TOTAL_2G_DOWNLINK_TR,0)) TOTAL_2G_TR
           , sum(nvl(OFFPEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(OFFPEAK_DATA_UPLINK_TR,0)) OFFPEAK_DATA_TR
           , sum(nvl(PEAK_DATA_DOWNLINK_TR,0)) + sum(nvl(PEAK_DATA_UPLINK_TR,0)) PEAK_DATA_TR
           , sum(S_TOTAL_CHG) S_TOTAL_CHG, sum(S_OFFNET_CHG) S_OFFNET_CHG, sum(S_ONNET_CHG) S_ONNET_CHG
           , sum(BILLED_SMS) BILLED_SMS, sum(BILLED_OFFNET_SMS) BILLED_OFFNET_SMS
           , sum(BILLED_ONNET_SMS) BILLED_ONNET_SMS, sum(PROMO_SMS) PROMO_SMS
           , sum(PROMO_OFFNET_SMS) PROMO_OFFNET_SMS, sum(PROMO_ONNET_SMS) PROMO_ONNET_SMS
           , sum(S_OFFPEAK_CHG) S_OFFPEAK_CHG, sum(S_OFFNET_OFFPEAK_CHG) S_OFFNET_OFFPEAK_CHG
           , sum(S_ONNET_OFFPEAK_CHG) S_ONNET_OFFPEAK_CHG, sum(BILLED_OFFPEAK_SMS) BILLED_OFFPEAK_SMS
           , sum(BILLED_OFFNET_OFFPEAK_SMS) BILLED_OFFNET_OFFPEAK_SMS
           , sum(BILLED_ONNET_OFFPEAK_SMS) BILLED_ONNET_OFFPEAK_SMS, sum(PROMO_OFFPEAK_SMS) PROMO_OFFPEAK_SMS
           , sum(PROMO_OFFNET_OFFPEAK_SMS) PROMO_OFFNET_OFFPEAK_SMS, sum(PROMO_ONNET_OFFPEAK_SMS) PROMO_ONNET_OFFPEAK_SMS
           , sum(S_PEAK_CHG) S_PEAK_CHG, sum(S_OFFNET_PEAK_CHG) S_OFFNET_PEAK_CHG
           , sum(S_ONNET_PEAK_CHG) S_ONNET_PEAK_CHG, sum(BILLED_PEAK_SMS) BILLED_PEAK_SMS
           , sum(BILLED_OFFNET_PEAK_SMS) BILLED_OFFNET_PEAK_SMS, sum(BILLED_ONNET_PEAK_SMS) BILLED_ONNET_PEAK_SMS
           , sum(PROMO_PEAK_SMS) PROMO_PEAK_SMS, sum(PROMO_OFFNET_PEAK_SMS) PROMO_OFFNET_PEAK_SMS
           , sum(PROMO_ONNET_PEAK_SMS) PROMO_ONNET_PEAK_SMS, sum(SMS_HITS) SMS_HITS, sum(ONNET_SMS_HITS) ONNET_SMS_HITS
           , sum(OFFNET_SMS_HITS) OFFNET_SMS_HITS
           , sum(VA_VOICE_CHG) VA_VOICE_CHG, sum(VA_BB_CHG) VA_BB_CHG, sum(VA_SMS_CHG) VA_SMS_CHG, sum(VA_DATA_CHG) VA_DATA_CHG
        from USG_LACCI_SMY_ALLOC
        group by regexp_replace(LAC,'[^-0-9]',''), regexp_replace(CI,'[^-0-9]',''), 
            case when TNR is null then '05. Unknown'
                    when TNR <= 63 then '01. <= 63 Days'
                      when TNR <= 180 then '02. 64 Days - 6 Months'
                        when TNR <= 360 then '03. 6 - 12 Months'
                          else '04. > 12 Months'
             end
           , PROMO_PACKAGE_NAME

"""
    val queryPostpaidCsttpNew="""
    SELECT 
       a.MSISDN
        , a.DT 
    	, a.TYP 
    	, a.V_TOTAL_CHG 
    	, a.G_TOTAL_CHG 
    	, a.S_TOTAL_CHG 
    	, a.VA_TOTAL_CHG 
      , b.CUSTOMER_TYPE_MOD
    FROM 
       PSTPAID_REV a 
       INNER JOIN 
         POSTPAID_SUBS b ON
       a.MSISDN = b.MSISDN
    """
    val queryPostpaidCsttpTpNew="""
    SELECT
       a.MSISDN,
       b.CUSTOMER_TYPE_MOD,
       SUM(CASE WHEN a.TYP = 'Onnet' THEN a.V_TOTAL_CHG ELSE 0 END) V_ONNET_CHG,
       SUM(CASE WHEN a.TYP = 'Offnet' THEN a.V_TOTAL_CHG ELSE 0 END) V_OFFNET_CHG,
       SUM(CASE WHEN a.TYP in ('Onnet', 'Offnet') THEN 0 ELSE V_TOTAL_CHG END) V_OTHERS_CHG,
       SUM(CASE WHEN a.TYP = 'Onnet' THEN a.S_TOTAL_CHG ELSE 0 END) S_ONNET_CHG,
       SUM(CASE WHEN a.TYP = 'Offnet' THEN a.S_TOTAL_CHG ELSE 0 END) S_OFFNET_CHG,
       SUM(CASE WHEN a.TYP in ('Onnet', 'Offnet') THEN 0 ELSE S_TOTAL_CHG END) S_OTHERS_CHG,
       SUM(a.G_TOTAL_CHG) G_TOTAL_CHG,
       SUM(a.VA_TOTAL_CHG) VA_TOTAL_CHG
    FROM 
       PSTPAID_REV a 
       INNER JOIN 
         POSTPAID_SUBS b ON
       a.MSISDN = b.MSISDN
    GROUP BY 
       a.MSISDN,
       b.CUSTOMER_TYPE_MOD
    """
    val queryPostpaidCsttpTpNew1="""
    SELECT
       a.MSISDN,
       a.LAC,a.CI,
       b.CUSTOMER_TYPE_MOD,
       a.V_ONNET_CHG,
       a.V_OFFNET_CHG,
       a.V_OTHERS_CHG,
       a.S_ONNET_CHG,
       a.S_OFFNET_CHG,
       a.S_OTHERS_CHG,
       a.G_TOTAL_CHG,
       a.VA_TOTAL_CHG
    FROM 
       PSTPAID_REV a 
       LEFT JOIN 
         POSTPAID_SUBS b ON
       a.MSISDN = b.MSISDN
    """
    val queryUmrSegment="""
      select MSISDN,VOICE_REV VA_VOICE_CHG,SMS_REV VA_SMS_CHG,DATA_REV VA_DATA_CHG,
        (VOICE_REV+SMS_REV+VAS_REV+OTH_REV) TOTAL_CHG, (DATA_REV+VAS_REV) DATA_CHG,
        case when nvl(VOICE_REV+SMS_REV+VAS_REV+DATA_REV+OTH_REV,0) > 100000 then 'Platinum'
                          when nvl(VOICE_REV+SMS_REV+VAS_REV+DATA_REV+OTH_REV,0) > 50000 then 'Gold'
                            when nvl(VOICE_REV+SMS_REV+VAS_REV+DATA_REV+OTH_REV,0) > 20000 then 'Regular'
                              when nvl(VOICE_REV+SMS_REV+VAS_REV+DATA_REV+OTH_REV,0) > 0 then 'Mass'
                                else 'No Usage' end SUB_CAT,
                  case when nvl(DATA_REV,0) > 50000 then 'Platinum'
                          when nvl(DATA_REV,0) > 20000 then 'Gold'
                            when nvl(DATA_REV,0) > 0 then 'Regular'
                              else 'Non Data Users' end SUB_DATA_CAT,
        case when VOICE_REV>0 and SMS_REV=0 and DATA_REV=0 and VAS_REV=0 then 'VOICE ONLY'
            when VOICE_REV=0 and SMS_REV>0 and DATA_REV=0 and VAS_REV=0 then 'SMS ONLY'
            when VOICE_REV=0 and SMS_REV=0 and (DATA_REV>0 or VAS_REV>0) then 'DATA/VAS ONLY'
            when VOICE_REV>0 and SMS_REV>0 and DATA_REV=0 and VAS_REV=0 then 'VOICE + SMS ONLY'
            when VOICE_REV>0 and SMS_REV=0 and (DATA_REV>0 or VAS_REV>0) then 'VOICE + DATA/VAS ONLY'
            when VOICE_REV=0 and SMS_REV>0 and (DATA_REV>0 or VAS_REV>0) then 'SMS + DATA/VAS ONLY'
            when VOICE_REV>0 and SMS_REV>0 and (DATA_REV>0 or VAS_REV>0) then 'VOICE + SMS + DATA/VAS ONLY'
            else 'OTHERS' end TYP,
       CASE
          WHEN (VOICE_REV+SMS_REV+VAS_REV+OTH_REV) > 100000 THEN 'A) > 100.000 Rp'
          WHEN (VOICE_REV+SMS_REV+VAS_REV+OTH_REV) > 50000 THEN  'B) 50.000-100.000 Rp'
          WHEN (VOICE_REV+SMS_REV+VAS_REV+OTH_REV) > 20000 THEN  'C) 20.000-50.000 Rp'
          WHEN (VOICE_REV+SMS_REV+VAS_REV+OTH_REV) > 0 THEN 'D) 1-20.000 Rp'
          ELSE 'E) = 0 Rp' end as TOTAL_CHG_GRP,
       CASE 
          WHEN DATA_REV+VAS_REV > 50000 THEN 'A) > 50.000 Rp'
          WHEN DATA_REV+VAS_REV > 20000 THEN 'B) 20.000-50.000 Rp'
          WHEN DATA_REV+VAS_REV > 0 THEN 'C) 1-20.000 Rp'
          ELSE 'D) = 0 Rp' end as DATA_CHG_GRP
      from UMR_SEGMENT
"""
    val queryUmrSegmentAddon="""
      select coalesce(a.MSISDN,b.MSISDN) MSISDN 
         ,nvl(b.VA_VOICE_CHG,0) VA_VOICE_CHG
         ,nvl(b.VA_SMS_CHG,0) VA_SMS_CHG
         ,nvl(b.VA_DATA_CHG,0) VA_DATA_CHG
         ,nvl(b.VA_BB_CHG,0) VA_BB_CHG
         ,(a.VOICE_REV+a.SMS_REV+a.OTH_REV+a.MENTARI_REV+a.MOBO_REV+a.VAS_REV) TOTAL_CHG
         , (a.DATA_REV+b.VA_DATA_CHG) DATA_CHG
         , case when VOICE_REV>0 and SMS_REV=0 and DATA_REV=0 and VAS_REV=0 then 'VOICE ONLY'
            when VOICE_REV=0 and SMS_REV>0 and DATA_REV=0 and VAS_REV=0 then 'SMS ONLY'
            when VOICE_REV=0 and SMS_REV=0 and (DATA_REV>0 or VAS_REV>0) then 'DATA/VAS ONLY'
            when VOICE_REV>0 and SMS_REV>0 and DATA_REV=0 and VAS_REV=0 then 'VOICE + SMS ONLY'
            when VOICE_REV>0 and SMS_REV=0 and (DATA_REV>0 or VAS_REV>0) then 'VOICE + DATA/VAS ONLY'
            when VOICE_REV=0 and SMS_REV>0 and (DATA_REV>0 or VAS_REV>0) then 'SMS + DATA/VAS ONLY'
            when VOICE_REV>0 and SMS_REV>0 and (DATA_REV>0 or VAS_REV>0) then 'VOICE + SMS + DATA/VAS ONLY'
            else 'OTHERS' end TYP
      from UMR_SEGMENT a FULL OUTER JOIN ADDON b on (a.MSISDN=b.MSISDN)
"""
        val queryUsgLacciSmyTmp="""
        select
        	a.MSISDN,
        	a.LAC,
        	a.CI,
        	a.V_TOTAL_CHG,
        	a.V_OFFNET_CHG,
        	a.V_ONNET_CHG,
        	a.BILLED_DUR,
        	a.BILLED_OFFNET_DUR,
        	a.BILLED_ONNET_DUR,
        	a.PROMO_DUR,
        	a.PROMO_OFFNET_DUR,
        	a.PROMO_ONNET_DUR,
        	a.TOT_DUR,
        	a.V_OFFNET_DUR,
        	a.V_ONNET_DUR,
        	a.V_OFFPEAK_CHG,
        	a.V_OFFNET_OFFPEAK_CHG,
        	a.V_ONNET_OFFPEAK_CHG,
        	a.BILLED_OFFPEAK_DUR,
        	a.BILLED_OFFNET_OFFPEAK_DUR,
        	a.BILLED_ONNET_OFFPEAK_DUR,
        	a.PROMO_OFFPEAK_DUR,
        	a.PROMO_OFFNET_OFFPEAK_DUR,
        	a.PROMO_ONNET_OFFPEAK_DUR,
        	a.TOTAL_OFFPEAK_DUR,
        	a.V_OFFNET_OFFPEAK_DUR,
        	a.V_ONNET_OFFPEAK_DUR,
        	a.V_PEAK_CHG,
        	a.V_OFFNET_PEAK_CHG,
        	a.V_ONNET_PEAK_CHG,
        	a.BILLED_PEAK_DUR,
        	a.BILLED_OFFNET_PEAK_DUR,
        	a.BILLED_ONNET_PEAK_DUR,
        	a.PROMO_PEAK_DUR,
        	a.PROMO_OFFNET_PEAK_DUR,
        	a.PROMO_ONNET_PEAK_DUR,
        	a.TOTAL_PEAK_DUR,
        	a.V_OFFNET_PEAK_DUR,
        	a.V_ONNET_PEAK_DUR,
        	a.G_TOTAL_CHG,
        	a.PPU_DATAVOL,
        	a.ADDON_DATAVOL,
        	a.TOTAL_DATAVOL,
        	a.G_3G_TOTAL_CHG,
        	a.G_2G_TOTAL_CHG,
        	a.G_3G_PPU_DATAVOL,
        	a.G_2G_PPU_DATAVOL,
        	a.G_3G_ADDON_DATAVOL,
        	a.G_2G_ADDON_DATAVOL,
        	a.G_OFFPEAK_CHG,
        	a.G_PEAK_CHG,
        	a.PPU_OFFPEAK_DATAVOL,
        	a.PPU_PEAK_DATAVOL,
        	a.ADDON_OFFPEAK_DATAVOL,
        	a.ADDON_PEAK_DATAVOL,
        	a.TOTAL_OFFPEAK_DATAVOL,
        	a.TOTAL_PEAK_DATAVOL,
        	a.G_4G_TOTAL_CHG,
        	a.G_4G_PPU_DATAVOL,
        	a.G_4G_ADDON_DATAVOL,
        	a.BB_UPLINK_TR,
        	a.BB_DOWNLINK_TR,
        	a.NON_BB_UPLINK_TR,
        	a.NON_BB_DOWNLINK_TR,
        	a.TOTAL_4G_UPLINK_TR,
        	a.TOTAL_4G_DOWNLINK_TR,
        	a.TOTAL_3G_UPLINK_TR,
        	a.TOTAL_3G_DOWNLINK_TR,
        	a.TOTAL_2G_UPLINK_TR,
        	a.TOTAL_2G_DOWNLINK_TR,
        	a.OFFPEAK_DATA_DOWNLINK_TR,
        	a.OFFPEAK_DATA_UPLINK_TR,
        	a.PEAK_DATA_DOWNLINK_TR,
        	a.PEAK_DATA_UPLINK_TR,
        	a.S_TOTAL_CHG,
        	a.S_OFFNET_CHG,
        	a.S_ONNET_CHG,
        	a.BILLED_SMS,
        	a.BILLED_OFFNET_SMS,
        	a.BILLED_ONNET_SMS,
        	a.PROMO_SMS,
        	a.PROMO_OFFNET_SMS,
        	a.PROMO_ONNET_SMS,
        	a.S_OFFPEAK_CHG,
        	a.S_OFFNET_OFFPEAK_CHG,
        	a.S_ONNET_OFFPEAK_CHG,
        	a.BILLED_OFFPEAK_SMS,
        	a.BILLED_OFFNET_OFFPEAK_SMS,
        	a.BILLED_ONNET_OFFPEAK_SMS,
        	a.PROMO_OFFPEAK_SMS,
        	a.PROMO_OFFNET_OFFPEAK_SMS,
        	a.PROMO_ONNET_OFFPEAK_SMS,
        	a.S_PEAK_CHG,
        	a.S_OFFNET_PEAK_CHG,
        	a.S_ONNET_PEAK_CHG,
        	a.BILLED_PEAK_SMS,
        	a.BILLED_OFFNET_PEAK_SMS,
        	a.BILLED_ONNET_PEAK_SMS,
        	a.PROMO_PEAK_SMS,
        	a.PROMO_OFFNET_PEAK_SMS,
        	a.PROMO_ONNET_PEAK_SMS,
        	a.SMS_HITS,
        	a.ONNET_SMS_HITS,
        	a.OFFNET_SMS_HITS,
        	a.V_ALLOC_PCT,
        	a.S_ALLOC_PCT,
        	a.BB_ALLOC_PCT,
        	a.NON_BB_ALLOC_PCT,
          a.num_days_data,
          a.num_days_voice,
          a.num_days_sms,
        	b.TNR,
        	b.PROMO_PACKAGE_NAME,
        	b.device_fl, 
        	b.smartphone_flag, 
        	b.flag_4g,
        	b.flag_3g,      
        	b.flag_2g,    
        	b.u900_fl,
        	b.TOTAL_CHG_GRP,
        	b.DATA_CHG_GRP,
        	b.TOTAL_CHG,
        	b.DATA_CHG,
        	(a.V_ALLOC_PCT * b.VA_VOICE_CHG) VA_VOICE_CHG,
        	(a.S_ALLOC_PCT * b.VA_SMS_CHG) VA_SMS_CHG,
        	(a.NON_BB_ALLOC_PCT * b.VA_DATA_CHG) VA_DATA_CHG,
          (a.BB_ALLOC_PCT * b.VA_BB_CHG) VA_BB_CHG,
          b.CUSTOMER_TYPE_MOD,
        	b.TYP,
          case when b.sub_cat is null then
                  case when a.v_total_chg + a.g_total_chg + a.s_total_chg  >= 400000 then 'Platinum'
                         when a.v_total_chg + a.g_total_chg + a.s_total_chg  >= 200000 then 'Gold'
                           when a.v_total_chg + a.g_total_chg + a.s_total_chg  >= 100000 then 'Regular'
                             else 'Mass'
                  end
              else b.sub_cat
         end sub_cat,
         case when b.sub_cat_mhvc is null then
                  case when a.v_total_chg + a.g_total_chg + a.s_total_chg  >= 50000 then 'MHVC'
                         else 'NON MHVC'
                  end
              else b.sub_cat_mhvc
         end sub_cat_mhvc,
         case when b.sub_data_cat is null then
                  case when a.g_total_chg >= 50000 then 'Platinum'
                         when a.g_total_chg >= 20000 then 'Gold'
                           when a.g_total_chg > 0 then 'Regular'
                             else 'Non Data Users'
                  end
              else b.sub_data_cat
         end sub_data_cat
        , %1$s MONTH_ID
        FROM
        	PRE_POSTPAID_LACCI_SMY a
        	LEFT OUTER JOIN 
        	MSISDN_REF b ON (a.MSISDN=b.MSISDN)
"""
    val queryUsgLacciSmy="""
        select
        	a.MSISDN,
        	a.LAC,
        	a.CI,
        	a.V_TOTAL_CHG,
        	a.V_OFFNET_CHG,
        	a.V_ONNET_CHG,
        	a.BILLED_DUR,
        	a.BILLED_OFFNET_DUR,
        	a.BILLED_ONNET_DUR,
        	a.PROMO_DUR,
        	a.PROMO_OFFNET_DUR,
        	a.PROMO_ONNET_DUR,
        	a.TOT_DUR,
        	a.V_OFFNET_DUR,
        	a.V_ONNET_DUR,
        	a.V_OFFPEAK_CHG,
        	a.V_OFFNET_OFFPEAK_CHG,
        	a.V_ONNET_OFFPEAK_CHG,
        	a.BILLED_OFFPEAK_DUR,
        	a.BILLED_OFFNET_OFFPEAK_DUR,
        	a.BILLED_ONNET_OFFPEAK_DUR,
        	a.PROMO_OFFPEAK_DUR,
        	a.PROMO_OFFNET_OFFPEAK_DUR,
        	a.PROMO_ONNET_OFFPEAK_DUR,
        	a.TOTAL_OFFPEAK_DUR,
        	a.V_OFFNET_OFFPEAK_DUR,
        	a.V_ONNET_OFFPEAK_DUR,
        	a.V_PEAK_CHG,
        	a.V_OFFNET_PEAK_CHG,
        	a.V_ONNET_PEAK_CHG,
        	a.BILLED_PEAK_DUR,
        	a.BILLED_OFFNET_PEAK_DUR,
        	a.BILLED_ONNET_PEAK_DUR,
        	a.PROMO_PEAK_DUR,
        	a.PROMO_OFFNET_PEAK_DUR,
        	a.PROMO_ONNET_PEAK_DUR,
        	a.TOTAL_PEAK_DUR,
        	a.V_OFFNET_PEAK_DUR,
        	a.V_ONNET_PEAK_DUR,
        	a.G_TOTAL_CHG,
        	a.PPU_DATAVOL,
        	a.ADDON_DATAVOL,
        	a.TOTAL_DATAVOL,
        	a.G_3G_TOTAL_CHG,
        	a.G_2G_TOTAL_CHG,
        	a.G_3G_PPU_DATAVOL,
        	a.G_2G_PPU_DATAVOL,
        	a.G_3G_ADDON_DATAVOL,
        	a.G_2G_ADDON_DATAVOL,
        	a.G_OFFPEAK_CHG,
        	a.G_PEAK_CHG,
        	a.PPU_OFFPEAK_DATAVOL,
        	a.PPU_PEAK_DATAVOL,
        	a.ADDON_OFFPEAK_DATAVOL,
        	a.ADDON_PEAK_DATAVOL,
        	a.TOTAL_OFFPEAK_DATAVOL,
        	a.TOTAL_PEAK_DATAVOL,
        	a.G_4G_TOTAL_CHG,
        	a.G_4G_PPU_DATAVOL,
        	a.G_4G_ADDON_DATAVOL,
        	a.BB_UPLINK_TR,
        	a.BB_DOWNLINK_TR,
        	a.NON_BB_UPLINK_TR,
        	a.NON_BB_DOWNLINK_TR,
        	a.TOTAL_4G_UPLINK_TR,
        	a.TOTAL_4G_DOWNLINK_TR,
        	a.TOTAL_3G_UPLINK_TR,
        	a.TOTAL_3G_DOWNLINK_TR,
        	a.TOTAL_2G_UPLINK_TR,
        	a.TOTAL_2G_DOWNLINK_TR,
        	a.OFFPEAK_DATA_DOWNLINK_TR,
        	a.OFFPEAK_DATA_UPLINK_TR,
        	a.PEAK_DATA_DOWNLINK_TR,
        	a.PEAK_DATA_UPLINK_TR,
        	a.S_TOTAL_CHG,
        	a.S_OFFNET_CHG,
        	a.S_ONNET_CHG,
        	a.BILLED_SMS,
        	a.BILLED_OFFNET_SMS,
        	a.BILLED_ONNET_SMS,
        	a.PROMO_SMS,
        	a.PROMO_OFFNET_SMS,
        	a.PROMO_ONNET_SMS,
        	a.S_OFFPEAK_CHG,
        	a.S_OFFNET_OFFPEAK_CHG,
        	a.S_ONNET_OFFPEAK_CHG,
        	a.BILLED_OFFPEAK_SMS,
        	a.BILLED_OFFNET_OFFPEAK_SMS,
        	a.BILLED_ONNET_OFFPEAK_SMS,
        	a.PROMO_OFFPEAK_SMS,
        	a.PROMO_OFFNET_OFFPEAK_SMS,
        	a.PROMO_ONNET_OFFPEAK_SMS,
        	a.S_PEAK_CHG,
        	a.S_OFFNET_PEAK_CHG,
        	a.S_ONNET_PEAK_CHG,
        	a.BILLED_PEAK_SMS,
        	a.BILLED_OFFNET_PEAK_SMS,
        	a.BILLED_ONNET_PEAK_SMS,
        	a.PROMO_PEAK_SMS,
        	a.PROMO_OFFNET_PEAK_SMS,
        	a.PROMO_ONNET_PEAK_SMS,
        	a.SMS_HITS,
        	a.ONNET_SMS_HITS,
        	a.OFFNET_SMS_HITS,
        	a.V_ALLOC_PCT,
        	a.S_ALLOC_PCT,
        	a.BB_ALLOC_PCT,
        	a.NON_BB_ALLOC_PCT,
          a.NUM_DAYS_DATA,
          a.NUM_DAYS_VOICE,
          a.NUM_DAYS_SMS,
        	b.TNR,
        	b.PROMO_PACKAGE_NAME,
        	b.DEVICE_FL, 
        	b.SMARTPHONE_FLAG, 
        	b.FLAG_4G,
        	b.FLAG_3G,      
        	b.FLAG_2G,    
        	b.U900_FL,
          CASE
            WHEN nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) > 100000 THEN 'A) > 100.000 Rp'
            WHEN nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) > 50000 THEN  'B) 50.000-100.000 Rp'
            WHEN nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) > 20000 THEN  'C) 20.000-50.000 Rp'
            WHEN nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) > 0 THEN 'D) 1-20.000 Rp'
            ELSE 'E) = 0 Rp' 
          end as TOTAL_CHG_GRP,
          CASE 
            WHEN nvl(a.G_TOTAL_CHG,0) + nvl(b.DATA_CHG,0) > 50000 THEN 'A) > 50.000 Rp'
            WHEN nvl(a.G_TOTAL_CHG,0) + nvl(b.DATA_CHG,0) > 20000 THEN 'B) 20.000-50.000 Rp'
            WHEN nvl(a.G_TOTAL_CHG,0) + nvl(b.DATA_CHG,0) > 0 THEN 'C) 1-20.000 Rp'
            ELSE 'D) = 0 Rp' 
          end as DATA_CHG_GRP,
        	nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) TOTAL_CHG,
        	nvl(a.G_TOTAL_CHG,0) + nvl(b.DATA_CHG,0) DATA_CHG,
        	(a.V_ALLOC_PCT * b.VA_VOICE_CHG) VA_VOICE_CHG,
        	(a.S_ALLOC_PCT * b.VA_SMS_CHG) VA_SMS_CHG,
        	(a.NON_BB_ALLOC_PCT * b.VA_DATA_CHG) VA_DATA_CHG,
          (a.BB_ALLOC_PCT * b.VA_BB_CHG) VA_BB_CHG,
          b.CUSTOMER_TYPE_MOD,
        	b.TYP,
          case when nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) >= 400000 then 'Platinum'
                 when nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG)  >= 200000 then 'Gold'
                   when nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) >= 100000 then 'Regular'
                     else 'Mass'
         end SUB_CAT,
         case when b.SUB_CAT_MHVC = 'NON MHVC' or nvl(b.SUB_CAT_MHVC,'')='' then
                  case when nvl(a.V_TOTAL_CHG,0) + nvl(a.G_TOTAL_CHG,0) + nvl(a.S_TOTAL_CHG,0) + nvl(b.TOTAL_CHG) >= 50000 then 'MHVC'
                         else 'NON MHVC'
                  end
              else b.SUB_CAT_MHVC
         end SUB_CAT_MHVC,
         case when nvl(a.G_TOTAL_CHG,0) + nvl(b.DATA_CHG,0) >= 50000 then 'Platinum'
                 when nvl(a.G_TOTAL_CHG,0) + nvl(b.DATA_CHG,0) >= 20000 then 'Gold'
                   when nvl(a.G_TOTAL_CHG,0) + nvl(b.DATA_CHG,0)  > 0 then 'Regular'
                     else 'Non Data Users'
         end SUB_DATA_CAT
        , %1$s MONTH_ID
        FROM
        	PRE_POSTPAID_LACCI_SMY a
        	LEFT OUTER JOIN 
        	MSISDN_REF b ON (a.MSISDN=b.MSISDN)
"""
    val queryUsgLacciSmyAllocNew="""
      select a.*
         , case when (nvl(TOT_V_TOTAL_CHG,0)+nvl(TOT_G_TOTAL_CHG,0)+nvl(TOT_S_TOTAL_CHG,0)+nvl(TOT_VA_VOICE_CHG,0)+nvl(TOT_VA_SMS_CHG,0)+nvl(TOT_VA_BB_CHG,0)+nvl(TOT_VA_DATA_CHG,0)) = 0 then 0
                 else (nvl(V_TOTAL_CHG,0)+nvl(G_TOTAL_CHG,0)+nvl(S_TOTAL_CHG,0)+nvl(VA_VOICE_CHG,0)+nvl(VA_SMS_CHG,0)+nvl(VA_BB_CHG,0)+nvl(VA_DATA_CHG,0))/(nvl(TOT_V_TOTAL_CHG,0)+nvl(TOT_G_TOTAL_CHG,0)+nvl(TOT_S_TOTAL_CHG,0)+nvl(TOT_VA_VOICE_CHG,0)+nvl(TOT_VA_SMS_CHG,0)+nvl(TOT_VA_BB_CHG,0)+nvl(TOT_VA_DATA_CHG,0))
           end Sub_Alloc
      from USG_LACCI_SMY a
       , (
           select msisdn
                  , sum(nvl(V_TOTAL_CHG,0)) TOT_V_TOTAL_CHG
                  , sum(nvl(G_TOTAL_CHG,0)) TOT_G_TOTAL_CHG
                  , sum(nvl(S_TOTAL_CHG,0)) TOT_S_TOTAL_CHG
                  , sum(nvl(VA_VOICE_CHG,0)) TOT_VA_VOICE_CHG
                  , sum(nvl(VA_SMS_CHG,0)) TOT_VA_SMS_CHG
                  , sum(nvl(VA_BB_CHG,0)) TOT_VA_BB_CHG
                  , sum(nvl(VA_DATA_CHG,0)) TOT_VA_DATA_CHG
           from USG_LACCI_SMY
           group by msisdn
         ) b
      where a.msisdn = b.msisdn
"""
    val queryLacciSmyNew="""
      select regexp_replace(LAC,'[^-0-9]','') LAC, regexp_replace(CI,'[^-0-9]','') CI, 
          case when TNR is null then '05. Unknown'
                    when TNR <= 63 then '01. <= 63 Days'
                      when TNR <= 180 then '02. 64 Days - 6 Months'
                        when TNR <= 360 then '03. 6 - 12 Months'
                          else '04. > 12 Months'
             end TENURE
           , PROMO_PACKAGE_NAME
           , sum(nvl(V_TOTAL_CHG,0)) + sum(nvl(VA_VOICE_CHG,0)) V_TOTAL_CHG
           , sum(nvl(TOT_DUR,0)) TOT_DUR
           , sum(nvl(G_TOTAL_CHG,0)) G_TOTAL_CHG, sum(nvl(VA_DATA_CHG,0)) VA_DATA_CHG
           , sum(nvl(PPU_DATAVOL,0)) PPU_DATAVOL, sum(nvl(ADDON_DATAVOL,0)) ADDON_DATAVOL
           , sum(nvl(BB_UPLINK_TR,0)) + sum(nvl(BB_DOWNLINK_TR,0)) TOTAL_BB_TR
           , sum(nvl(NON_BB_UPLINK_TR,0)) + sum(nvl(NON_BB_DOWNLINK_TR,0)) TOTAL_NON_BB_TR
           , sum(nvl(TOTAL_4G_UPLINK_TR,0)) + sum(nvl(TOTAL_4G_DOWNLINK_TR,0)) TOTAL_4G_TR
           , sum(nvl(TOTAL_3G_UPLINK_TR,0)) + sum(nvl(TOTAL_3G_DOWNLINK_TR,0)) TOTAL_3G_TR
           , sum(nvl(TOTAL_2G_UPLINK_TR,0)) + sum(nvl(TOTAL_2G_DOWNLINK_TR,0)) TOTAL_2G_TR
           , sum(nvl(S_TOTAL_CHG,0)) + sum(nvl(VA_SMS_CHG,0)) S_TOTAL_CHG
           , sum(nvl(SMS_HITS,0)) SMS_HITS
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then SUB_ALLOC else 0 end) SUB_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_4G = 1 then SUB_ALLOC else 0 end) SUB_4G_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_3G = 1 then SUB_ALLOC else 0 end) SUB_3G_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and FLAG_2G = 1 then SUB_ALLOC else 0 end) SUB_2G_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then SUB_ALLOC else 0 end) SUB_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_4G = 1 then SUB_ALLOC else 0 end) SUB_4G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_3G = 1 then SUB_ALLOC else 0 end) SUB_3G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_2G = 1 then SUB_ALLOC else 0 end) SUB_2G_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and FLAG_2G = 0 and FLAG_3G = 0 and FLAG_4G = 0 then SUB_ALLOC else 0 end) SUB_NON_DATA_FP
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then V_TOTAL_CHG else 0 end) V_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then G_TOTAL_CHG else 0 end) G_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then S_TOTAL_CHG else 0 end) S_CHG_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_VOICE_CHG else 0 end) VA_VOICE_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_SMS_CHG else 0 end) VA_SMS_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_DATA_CHG else 0 end) VA_DATA_U900
           , sum(case when DEVICE_FL = 1 and U900_FL = 1 then VA_BB_CHG else 0 end) VA_BB_U900
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then V_TOTAL_CHG else 0 end) V_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then G_TOTAL_CHG else 0 end) G_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then S_TOTAL_CHG else 0 end) S_CHG_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_VOICE_CHG else 0 end) VA_VOICE_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_SMS_CHG else 0 end) VA_SMS_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_DATA_CHG else 0 end) VA_DATA_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then VA_BB_CHG else 0 end) VA_BB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then V_TOTAL_CHG else 0 end) V_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then G_TOTAL_CHG else 0 end) G_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then S_TOTAL_CHG else 0 end) S_CHG_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_VOICE_CHG else 0 end) VA_VOICE_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_SMS_CHG else 0 end) VA_SMS_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_DATA_CHG else 0 end) VA_DATA_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then VA_BB_CHG else 0 end) VA_BB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(TOT_DUR,0) else 0 end) VOICE_DUR_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) + nvl(NON_BB_UPLINK_TR,0) + nvl(NON_BB_DOWNLINK_TR,0) else 0 end) DATA_TR_SP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' then nvl(SMS_HITS,0) else 0 end) SMS_HITS_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(TOT_DUR,0) else 0 end) VOICE_DUR_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) + nvl(NON_BB_UPLINK_TR,0) + nvl(NON_BB_DOWNLINK_TR,0) else 0 end) DATA_TR_FP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) then nvl(SMS_HITS,0) else 0 end) SMS_HITS_FP
           , sum(case when SUB_CAT_MHVC='MHVC' then V_TOTAL_CHG else 0 end) SUB_MHVC_V_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_VOICE_CHG else 0 end) SUB_MHVC_VA_VOICE_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then G_TOTAL_CHG else 0 end) SUB_MHVC_G_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_DATA_CHG else 0 end) SUB_MHVC_VA_DATA_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then S_TOTAL_CHG else 0 end) SUB_MHVC_SMS_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_SMS_CHG else 0 end) SUB_MHVC_VA_SMS_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then VA_BB_CHG else 0 end) SUB_MHVC_VA_BB_CHG
           , sum(case when a.SUB_CAT='Platinum' then V_TOTAL_CHG else 0 end) SUB_PLATINUM_V_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_VOICE_CHG else 0 end) SUB_PLATINUM_VA_VOICE_CHG
           , sum(case when a.SUB_CAT='Platinum' then G_TOTAL_CHG else 0 end) SUB_PLATINUM_G_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_DATA_CHG else 0 end) SUB_PLATINUM_VA_DATA_CHG
           , sum(case when a.SUB_CAT='Platinum' then S_TOTAL_CHG else 0 end) SUB_PLATINUM_SMS_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_SMS_CHG else 0 end) SUB_PLATINUM_VA_SMS_CHG
           , sum(case when a.SUB_CAT='Platinum' then VA_BB_CHG else 0 end) SUB_PLATINUM_VA_BB_CHG
           , sum(case when a.SUB_CAT='Gold' then V_TOTAL_CHG else 0 end) SUB_GOLD_V_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_VOICE_CHG else 0 end) SUB_GOLD_VA_VOICE_CHG
           , sum(case when a.SUB_CAT='Gold' then G_TOTAL_CHG else 0 end) SUB_GOLD_G_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_DATA_CHG else 0 end) SUB_GOLD_VA_DATA_CHG
           , sum(case when a.SUB_CAT='Gold' then S_TOTAL_CHG else 0 end) SUB_GOLD_SMS_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_SMS_CHG else 0 end) SUB_GOLD_VA_SMS_CHG
           , sum(case when a.SUB_CAT='Gold' then VA_BB_CHG else 0 end) SUB_GOLD_VA_BB_CHG
           , sum(case when SUB_CAT_MHVC='MHVC' then SUB_ALLOC else 0 end) SUB_MHVC
           , sum(case when SUB_CAT_MHVC='NON MHVC' then SUB_ALLOC else 0 end) SUB_NON_MHVC
           , sum(case when a.SUB_CAT='Platinum' then SUB_ALLOC else 0 end) SUB_PLATINUM
           , sum(case when a.SUB_CAT='Gold' then SUB_ALLOC else 0 end) SUB_GOLD
           , sum(case when a.SUB_CAT='Regular' then SUB_ALLOC else 0 end) SUB_REGULAR
           , sum(case when a.SUB_CAT='Mass' then SUB_ALLOC else 0 end) SUB_MASS
           , sum(case when SUB_DATA_CAT='Platinum' then SUB_ALLOC else 0 end) DATA_PLATINUM
           , sum(case when SUB_DATA_CAT='Gold' then SUB_ALLOC else 0 end) DATA_GOLD
           , sum(case when SUB_DATA_CAT='Regular' then SUB_ALLOC else 0 end) DATA_REGULAR
           , sum(case when SUB_DATA_CAT='Non Data Users' then SUB_ALLOC else 0 end) NON_DATA_REGULAR
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('VOICE ONLY', 'VOICE + DATA/VAS ONLY', 'VOICE + SMS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) VOICE_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('VOICE ONLY', 'VOICE + DATA/VAS ONLY', 'VOICE + SMS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) VOICE_SUB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('DATA/VAS ONLY', 'VOICE + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) DATA_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('DATA/VAS ONLY', 'VOICE + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) DATA_SUB_FP
           , sum(case when DEVICE_FL = 1 and SMARTPHONE_FLAG = 'Smartphone' and TYP in ('SMS ONLY', 'SMS + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) SMS_SUB_SP
           , sum(case when DEVICE_FL = 1 and (SMARTPHONE_FLAG <>  'Smartphone' or SMARTPHONE_FLAG is null) and TYP in ('SMS ONLY', 'SMS + DATA/VAS ONLY', 'SMS + DATA/VAS ONLY', 'VOICE + SMS + DATA/VAS ONLY') then SUB_ALLOC else 0 end) SMS_SUB_FP
           , sum(case when TYP='VOICE ONLY' then SUB_ALLOC else 0 end) V_ONLY_SUBS
           , sum(case when TYP='DATA/VAS ONLY' then SUB_ALLOC else 0 end) D_ONLY_SUBS
           , sum(case when TYP='SMS ONLY' then SUB_ALLOC else 0 end) S_ONLY_SUBS
           , sum(case when TYP='VOICE + DATA/VAS ONLY' then SUB_ALLOC else 0 end) V_D_ONLY_SUBS
           , sum(case when TYP='VOICE + SMS ONLY' then SUB_ALLOC else 0 end) V_S_ONLY_SUBS
           , sum(case when TYP='SMS + DATA/VAS ONLY' then SUB_ALLOC else 0 end) S_D_ONLY_SUBS
           , sum(case when TYP='VOICE + SMS + DATA/VAS ONLY' then SUB_ALLOC else 0 end) V_S_D_ONLY_SUBS
           , sum(SUB_ALLOC) SUB_ALLOC
           , sum(case when nvl(BB_UPLINK_TR,0) + nvl(BB_DOWNLINK_TR,0) > 0 then SUB_ALLOC else 0 end) BB_ALLOC_SUB
           , sum(case when CUSTOMER_TYPE_MOD = 'B2B PREPAID' then SUB_ALLOC else 0 end) B2B_PREPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2C PREPAID' then SUB_ALLOC else 0 end) B2C_PREPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2B POSTPAID' then SUB_ALLOC else 0 end) B2B_POSTPAID_SUB_ALLOC
           , sum(case when CUSTOMER_TYPE_MOD = 'B2C POSTPAID' then SUB_ALLOC else 0 end) B2C_POSTPAID_SUB_ALLOC
        from USG_LACCI_SMY_ALLOC a
        group by regexp_replace(LAC,'[^-0-9]',''), regexp_replace(CI,'[^-0-9]',''), 
              case when TNR is null then '05. Unknown'
                      when TNR <= 63 then '01. <= 63 Days'
                        when TNR <= 180 then '02. 64 Days - 6 Months'
                          when TNR <= 360 then '03. 6 - 12 Months'
                            else '04. > 12 Months'
               end
             , PROMO_PACKAGE_NAME
"""
    val queryLacciTotalSmyNew="""
      select regexp_replace(a.LAC,'[^-0-9]','') LAC, regexp_replace(a.CI,'[^-0-9]','') CI, a.TENURE, a.PROMO_PACKAGE_NAME, a.V_TOTAL_CHG, a.V_OFFNET_CHG, a.V_ONNET_CHG, a.BILLED_DUR, a.BILLED_OFFNET_DUR,
        a.BILLED_ONNET_DUR, a.PROMO_DUR, a.PROMO_OFFNET_DUR, a.PROMO_ONNET_DUR, a.TOT_DUR, a.V_OFFNET_DUR, a.V_ONNET_DUR, a.V_OFFPEAK_CHG, a.V_OFFNET_OFFPEAK_CHG,
        a.V_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_DUR, a.BILLED_OFFNET_OFFPEAK_DUR, a.BILLED_ONNET_OFFPEAK_DUR, a.PROMO_OFFPEAK_DUR, a.PROMO_OFFNET_OFFPEAK_DUR,
        a.PROMO_ONNET_OFFPEAK_DUR, a.TOTAL_OFFPEAK_DUR, a.V_OFFNET_OFFPEAK_DUR, a.V_ONNET_OFFPEAK_DUR, a.V_PEAK_CHG, a.V_OFFNET_PEAK_CHG, a.V_ONNET_PEAK_CHG,
        a.BILLED_PEAK_DUR, a.BILLED_OFFNET_PEAK_DUR, a.BILLED_ONNET_PEAK_DUR, a.PROMO_PEAK_DUR, a.PROMO_OFFNET_PEAK_DUR, a.PROMO_ONNET_PEAK_DUR, a.TOTAL_PEAK_DUR,
        a.V_OFFNET_PEAK_DUR, a.V_ONNET_PEAK_DUR, a.G_TOTAL_CHG, a.PPU_DATAVOL, a.ADDON_DATAVOL, a.G_OFFPEAK_CHG, a.G_PEAK_CHG, a.PPU_OFFPEAK_DATAVOL,
        a.PPU_PEAK_DATAVOL, a.ADDON_OFFPEAK_DATAVOL, a.ADDON_PEAK_DATAVOL, a.BB_UPLINK_TR, a.BB_DOWNLINK_TR, a.NON_BB_UPLINK_TR, a.NON_BB_DOWNLINK_TR,
        a.TOTAL_4G_TR, a.TOTAL_3G_TR, a.TOTAL_2G_TR, a.OFFPEAK_DATA_TR, a.PEAK_DATA_TR, a.S_TOTAL_CHG, a.S_OFFNET_CHG, a.S_ONNET_CHG, a.BILLED_SMS,
        a.BILLED_OFFNET_SMS, a.BILLED_ONNET_SMS, a.PROMO_SMS, a.PROMO_OFFNET_SMS, a.PROMO_ONNET_SMS, a.S_OFFPEAK_CHG, a.S_OFFNET_OFFPEAK_CHG,
        a.S_ONNET_OFFPEAK_CHG, a.BILLED_OFFPEAK_SMS, a.BILLED_OFFNET_OFFPEAK_SMS, a.BILLED_ONNET_OFFPEAK_SMS, a.PROMO_OFFPEAK_SMS, a.PROMO_OFFNET_OFFPEAK_SMS,
        a.PROMO_ONNET_OFFPEAK_SMS, a.S_PEAK_CHG, a.S_OFFNET_PEAK_CHG, a.S_ONNET_PEAK_CHG, a.BILLED_PEAK_SMS, a.BILLED_OFFNET_PEAK_SMS, a.BILLED_ONNET_PEAK_SMS,
        a.PROMO_PEAK_SMS, a.PROMO_OFFNET_PEAK_SMS, a.PROMO_ONNET_PEAK_SMS, a.SMS_HITS, a.ONNET_SMS_HITS, a.OFFNET_SMS_HITS, a.VA_VOICE_CHG, a.VA_SMS_CHG,
        a.VA_DATA_CHG, b.SUB_SP, b.SUB_4G_SP, b.SUB_3G_SP, b.SUB_2G_SP, b.SUB_FP, b.SUB_4G_FP, b.SUB_3G_FP, b.SUB_2G_FP,
        b.SUB_NON_DATA_FP, b.V_CHG_U900, b.G_CHG_U900, b.S_CHG_U900, b.VA_VOICE_U900, b.VA_SMS_U900, b.VA_DATA_U900, b.VA_BB_U900,
        b.V_CHG_SP, b.G_CHG_SP, b.S_CHG_SP, b.VA_VOICE_SP, b.VA_SMS_SP, b.VA_DATA_SP, b.VA_BB_SP, b.V_CHG_FP, b.G_CHG_FP, b.S_CHG_FP,
        b.VA_VOICE_FP, b.VA_SMS_FP, b.VA_DATA_FP, b.VA_BB_FP, b.VOICE_DUR_SP, b.DATA_TR_SP, b.SMS_HITS_SP, b.VOICE_DUR_FP,
        b.DATA_TR_FP, b.SMS_HITS_FP, b.SUB_MHVC_V_CHG, b.SUB_MHVC_VA_VOICE_CHG, b.SUB_MHVC_G_CHG, b.SUB_MHVC_VA_DATA_CHG,b.SUB_MHVC_VA_BB_CHG,
        b.SUB_PLATINUM_V_CHG, b.SUB_PLATINUM_VA_VOICE_CHG, b.SUB_PLATINUM_G_CHG, b.SUB_PLATINUM_VA_DATA_CHG,b.SUB_PLATINUM_VA_BB_CHG,
        b.SUB_PLATINUM_SMS_CHG, b.SUB_PLATINUM_VA_SMS_CHG, b.SUB_GOLD_V_CHG, b.SUB_GOLD_VA_VOICE_CHG,
        b.SUB_GOLD_G_CHG, b.SUB_GOLD_VA_DATA_CHG, b.SUB_GOLD_SMS_CHG, b.SUB_GOLD_VA_SMS_CHG, b.SUB_GOLD_VA_BB_CHG, b.SUB_MHVC, b.SUB_NON_MHVC, b.SUB_PLATINUM,
        b.SUB_GOLD, b.SUB_REGULAR, b.SUB_MASS, b.DATA_PLATINUM, b.DATA_GOLD, b.DATA_REGULAR, b.NON_DATA_REGULAR, b.VOICE_SUB_SP, b.VOICE_SUB_FP,
        b.DATA_SUB_SP, b.DATA_SUB_FP, b.SMS_SUB_SP, b.SMS_SUB_FP, b.V_ONLY_SUBS, b.D_ONLY_SUBS, b.S_ONLY_SUBS, b.V_D_ONLY_SUBS, b.V_S_ONLY_SUBS,
        b.S_D_ONLY_SUBS, b.V_S_D_ONLY_SUBS, b.SUB_ALLOC, b.BB_ALLOC_SUB, b.B2B_PREPAID_SUB_ALLOC, b.B2C_PREPAID_SUB_ALLOC, b.B2B_POSTPAID_SUB_ALLOC, b.B2C_POSTPAID_SUB_ALLOC
      from LACCI_SMY_USG a
      left join
      LACCI_SMY b
      on regexp_replace(a.lac,'[^-0-9]','') = regexp_replace(b.lac,'[^-0-9]','')
         and regexp_replace(a.ci,'[^-0-9]','') = regexp_replace(b.ci,'[^-0-9]','')
         and a.tenure = b.tenure
         and a.promo_package_name = b.promo_package_name
"""
    val queryLacciSmyFinal="""
      select a.LAC,
      a.CI,
      b.SITE_ID,
      b.CELL_TECHNOLOGY_TYPE,
      b.CELL_COVERAGE_TYPE,
      b.CELL_BAND,
      b.SECTOR,
      a.TENURE,
      a.PROMO_PACKAGE_NAME,
      a.V_TOTAL_CHG,
      a.V_OFFNET_CHG,
      a.V_ONNET_CHG,
      a.BILLED_DUR,
      a.BILLED_OFFNET_DUR,
      a.BILLED_ONNET_DUR,
      a.PROMO_DUR,
      a.PROMO_OFFNET_DUR,
      a.PROMO_ONNET_DUR,
      a.TOT_DUR,
      a.V_OFFNET_DUR,
      a.V_ONNET_DUR,
      a.V_OFFPEAK_CHG,
      a.V_OFFNET_OFFPEAK_CHG,
      a.V_ONNET_OFFPEAK_CHG,
      a.BILLED_OFFPEAK_DUR,
      a.BILLED_OFFNET_OFFPEAK_DUR,
      a.BILLED_ONNET_OFFPEAK_DUR,
      a.PROMO_OFFPEAK_DUR,
      a.PROMO_OFFNET_OFFPEAK_DUR,
      a.PROMO_ONNET_OFFPEAK_DUR,
      a.TOTAL_OFFPEAK_DUR,
      a.V_OFFNET_OFFPEAK_DUR,
      a.V_ONNET_OFFPEAK_DUR,
      a.V_PEAK_CHG,
      a.V_OFFNET_PEAK_CHG,
      a.V_ONNET_PEAK_CHG,
      a.BILLED_PEAK_DUR,
      a.BILLED_OFFNET_PEAK_DUR,
      a.BILLED_ONNET_PEAK_DUR,
      a.PROMO_PEAK_DUR,
      a.PROMO_OFFNET_PEAK_DUR,
      a.PROMO_ONNET_PEAK_DUR,
      a.TOTAL_PEAK_DUR,
      a.V_OFFNET_PEAK_DUR,
      a.V_ONNET_PEAK_DUR,
      a.G_TOTAL_CHG,
      a.PPU_DATAVOL,
      a.ADDON_DATAVOL,
      a.G_OFFPEAK_CHG,
      a.G_PEAK_CHG,
      a.PPU_OFFPEAK_DATAVOL,
      a.PPU_PEAK_DATAVOL,
      a.ADDON_OFFPEAK_DATAVOL,
      a.ADDON_PEAK_DATAVOL,
      a.BB_UPLINK_TR,
      a.BB_DOWNLINK_TR,
      a.NON_BB_UPLINK_TR,
      a.NON_BB_DOWNLINK_TR,
      a.TOTAL_4G_TR,
      a.TOTAL_3G_TR,
      a.TOTAL_2G_TR,
      a.OFFPEAK_DATA_TR,
      a.PEAK_DATA_TR,
      a.S_TOTAL_CHG,
      a.S_OFFNET_CHG,
      a.S_ONNET_CHG,
      a.BILLED_SMS,
      a.BILLED_OFFNET_SMS,
      a.BILLED_ONNET_SMS,
      a.PROMO_SMS,
      a.PROMO_OFFNET_SMS,
      a.PROMO_ONNET_SMS,
      a.S_OFFPEAK_CHG,
      a.S_OFFNET_OFFPEAK_CHG,
      a.S_ONNET_OFFPEAK_CHG,
      a.BILLED_OFFPEAK_SMS,
      a.BILLED_OFFNET_OFFPEAK_SMS,
      a.BILLED_ONNET_OFFPEAK_SMS,
      a.PROMO_OFFPEAK_SMS,
      a.PROMO_OFFNET_OFFPEAK_SMS,
      a.PROMO_ONNET_OFFPEAK_SMS,
      a.S_PEAK_CHG,
      a.S_OFFNET_PEAK_CHG,
      a.S_ONNET_PEAK_CHG,
      a.BILLED_PEAK_SMS,
      a.BILLED_OFFNET_PEAK_SMS,
      a.BILLED_ONNET_PEAK_SMS,
      a.PROMO_PEAK_SMS,
      a.PROMO_OFFNET_PEAK_SMS,
      a.PROMO_ONNET_PEAK_SMS,
      a.SMS_HITS,
      a.ONNET_SMS_HITS,
      a.OFFNET_SMS_HITS,
      a.VA_VOICE_CHG,
      a.VA_SMS_CHG,
      a.VA_DATA_CHG,
      a.SUB_SP,
      a.SUB_4G_SP,
      a.SUB_3G_SP,
      a.SUB_2G_SP,
      a.SUB_FP,
      a.SUB_4G_FP,
      a.SUB_3G_FP,
      a.SUB_2G_FP,
      a.SUB_NON_DATA_FP,
      a.V_CHG_U900,
      a.G_CHG_U900,
      a.S_CHG_U900,
      a.VA_VOICE_U900,
      a.VA_SMS_U900,
      a.VA_DATA_U900,
      a.VA_BB_U900,
      a.V_CHG_SP,
      a.G_CHG_SP,
      a.S_CHG_SP,
      a.VA_VOICE_SP,
      a.VA_SMS_SP,
      a.VA_DATA_SP,
      a.VA_BB_SP,
      a.V_CHG_FP,
      a.G_CHG_FP,
      a.S_CHG_FP,
      a.VA_VOICE_FP,
      a.VA_SMS_FP,
      a.VA_DATA_FP,
      a.VA_BB_FP,
      a.VOICE_DUR_SP,
      a.DATA_TR_SP,
      a.SMS_HITS_SP,
      a.VOICE_DUR_FP,
      a.DATA_TR_FP,
      a.SMS_HITS_FP,
      a.SUB_MHVC_V_CHG,
      a.SUB_MHVC_VA_VOICE_CHG,
      a.SUB_MHVC_G_CHG,
      a.SUB_MHVC_VA_DATA_CHG,a.SUB_MHVC_VA_BB_CHG,
      a.SUB_PLATINUM_V_CHG,
      a.SUB_PLATINUM_VA_VOICE_CHG,
      a.SUB_PLATINUM_G_CHG,
      a.SUB_PLATINUM_VA_DATA_CHG,a.SUB_PLATINUM_VA_BB_CHG,
      a.SUB_PLATINUM_SMS_CHG,
      a.SUB_PLATINUM_VA_SMS_CHG,
      a.SUB_GOLD_V_CHG,
      a.SUB_GOLD_VA_VOICE_CHG,
      a.SUB_GOLD_G_CHG,
      a.SUB_GOLD_VA_DATA_CHG,
      a.SUB_GOLD_SMS_CHG,
      a.SUB_GOLD_VA_SMS_CHG,
      a.SUB_GOLD_VA_BB_CHG,
      a.SUB_MHVC,
      a.SUB_NON_MHVC,
      a.SUB_PLATINUM,
      a.SUB_GOLD,
      a.SUB_REGULAR,
      a.SUB_MASS,
      a.DATA_PLATINUM,
      a.DATA_GOLD,
      a.DATA_REGULAR,
      a.NON_DATA_REGULAR,
      a.VOICE_SUB_SP,
      a.VOICE_SUB_FP,
      a.DATA_SUB_SP,
      a.DATA_SUB_FP,
      a.SMS_SUB_SP,
      a.SMS_SUB_FP,
      a.V_ONLY_SUBS,
      a.D_ONLY_SUBS,
      a.S_ONLY_SUBS,
      a.V_D_ONLY_SUBS,
      a.V_S_ONLY_SUBS,
      a.S_D_ONLY_SUBS,
      a.V_S_D_ONLY_SUBS,
      a.SUB_ALLOC,
      a.BB_ALLOC_SUB,
      a.B2B_PREPAID_SUB_ALLOC,
      a.B2C_PREPAID_SUB_ALLOC,
      a.B2B_POSTPAID_SUB_ALLOC,
      a.B2C_POSTPAID_SUB_ALLOC, %1$s MONTH_ID
      from LACCI_TOTAL_SMY a
      left outer join ISAT_CONSOL_NDB b on a.lac=b.lac and b.ci=a.ci 

"""
    val queryAddonFreedom="""
      SELECT MSISDN
        , Sum(DATA_REVENUE) VA_DATA_FREEDOM
        , Sum(VOICE_REVENUE) VA_VOICE_FREEDOM
        , Sum(SMS_REVENUE) VA_SMS_FREEDOM
        , 0 VA_BB_FREEDOM
      FROM AUTO_FREEDOM
      GROUP BY MSISDN
"""
    val queryAddonNonFreedom="""
      SELECT MSISDN
        ,Sum(CASE WHEN SVC_USG_TP='DATA' AND (LEVEL_5 like '%Small Screen%' OR LEVEL_5 like '%Large Screen%') THEN REVENUE ELSE NULL END) VA_DATA_CHG
        ,Sum(CASE WHEN SVC_USG_TP='VOICE' THEN REVENUE ELSE NULL END) VA_VOICE_CHG
        ,Sum(CASE WHEN SVC_USG_TP='SMS' THEN REVENUE ELSE NULL END) VA_SMS_CHG
        ,Sum(CASE WHEN SVC_USG_TP='DATA' AND LEVEL_5 like '%Blackberry%' THEN REVENUE ELSE NULL END) VA_BB_CHG
      FROM AUTO_DLY_REG
      GROUP BY MSISDN
"""
    val queryAddonMerge="""
      select * FROM ADDON_NON_FREEDOM
      UNION all
      SELECT * FROM ADDON_FREEDOM
"""
    val queryAddon="""
      SELECT msisdn
      , Sum(va_voice_chg) va_voice_chg, Sum(va_sms_chg) va_sms_chg
      , Sum(va_data_chg) va_data_chg, Sum(va_bb_chg) va_bb_chg
      from
        ADDON_MERGE
      GROUP BY msisdn
"""
    val queryAutoBtsClusterTemp="""
      SELECT AREA_CHANNELS AREA, SUM(NVL(MOBO_REVENUE,0)+NVL(MENTARI_SP_DATA_REVENUE,0)) MOBOSP_REVENUE 
      FROM AUTO_BTS_CLUSTER
"""
    val queryUsgLacciSmyMoboAlloc="""
      SELECT ( nvl(TOTAL_4G_UPLINK_TR,0)+nvl(TOTAL_4G_DOWNLINK_TR,0)+nvl(TOTAL_3G_UPLINK_TR,0)+nvl(TOTAL_3G_DOWNLINK_TR,0)+nvl(TOTAL_2G_UPLINK_TR,0)+nvl(TOTAL_2G_DOWNLINK_TR,0 ))/B.TR_DATA MOBOSP_REVENUE_PCT, A.*
      FROM USG_LACCI_SMY_SITE A 
      LEFT JOIN (
          SELECT AREA,SUM(nvl(TOTAL_4G_UPLINK_TR,0)+nvl(TOTAL_4G_DOWNLINK_TR,0)+nvl(TOTAL_3G_UPLINK_TR,0)+nvl(TOTAL_3G_DOWNLINK_TR,0)+nvl(TOTAL_2G_UPLINK_TR,0)+nvl(TOTAL_2G_DOWNLINK_TR,0)) TR_DATA
          FROM USG_LACCI_SMY_SITE
          GROUP BY AREA
      ) B
      ON A.AREA=B.AREA
"""
    val queryUsgLacciSmyMoboAllocNew="""
      SELECT ( nvl(TOTAL_2G_TR,0)+nvl(TOTAL_3G_TR,0)+nvl(TOTAL_4G_TR,0))/B.TR_DATA MOBOSP_REVENUE_PCT, A.*
      FROM USG_LACCI_SMY_SITE A 
      LEFT JOIN (
          SELECT AREA,SUM( nvl(TOTAL_2G_TR,0)+nvl(TOTAL_3G_TR,0)+nvl(TOTAL_4G_TR,0)) TR_DATA
          FROM USG_LACCI_SMY_SITE
          GROUP BY AREA
      ) B
      ON A.AREA=B.AREA
"""
    val queryUsgLacciSmyMobo="""
    SELECT B.MOBOSP_REVENUE*A.MOBOSP_REVENUE_PCT MOBOSP_REVENUE, A.*
    FROM USG_LACCI_SMY_MOBO_ALLOC A LEFT JOIN
    AUTO_BTS_CLUSTER_AGG  B
    ON A.AREA=B.AREA
"""
    val queryUsgLacciSmySite="""
    SELECT CASE WHEN D.SITE_ID_2G3G IS NOT NULL THEN D.SITE_ID_2G3G
    			WHEN D.SITE_ID_4G IS NOT NULL THEN D.SITE_ID_4G
    			ELSE 'SITENULL' END SITE_ID,
    	CASE WHEN D.SITE_ID_2G3G IS NOT NULL THEN D.CELL_NAME_2G3G
    		WHEN D.SITE_ID_4G IS NOT NULL THEN D.CELL_NAME_4G
    		END CELL_NAME,
    	CASE WHEN D.SITE_ID_2G3G IS NOT NULL THEN D.CELL_COVERAGE_TYPE_2G3G
    		WHEN D.SITE_ID_4G IS NOT NULL THEN D.CELL_COVERAGE_TYPE_4G
    		END CELL_COVERAGE_TYPE,
    	CASE WHEN D.SITE_ID_2G3G IS NOT NULL THEN D.CELL_TECHNOLOGY_TYPE_2G3G
    		WHEN D.SITE_ID_4G IS NOT NULL THEN D.CELL_TECHNOLOGY_TYPE_4G
    		END CELL_TECHNOLOGY_TYPE,
    	CASE WHEN D.SITE_ID_2G3G IS NOT NULL THEN D.CELL_BAND_2G3G
    		WHEN D.SITE_ID_4G IS NOT NULL THEN D.CELL_BAND_4G
    		END CELL_BAND,
    	CASE WHEN D.SITE_ID_2G3G IS NOT NULL THEN D.SECTOR_2G3G
    		WHEN D.SITE_ID_4G IS NOT NULL THEN D.SECTOR_4G
    		END SECTOR,
    	CASE WHEN D.SITE_ID_2G3G IS NOT NULL THEN D.AREA_2G3G
    		WHEN D.SITE_ID_4G IS NOT NULL THEN D.AREA_4G
    		END AREA,
      D.MSISDN,
      D.LAC,
      D.CI,
      D.V_TOTAL_CHG,
      D.V_OFFNET_CHG,
      D.V_ONNET_CHG,
      D.BILLED_DUR,
      D.BILLED_OFFNET_DUR,
      D.BILLED_ONNET_DUR,
      D.PROMO_DUR,
      D.PROMO_OFFNET_DUR,
      D.PROMO_ONNET_DUR,
      D.TOT_DUR,
      D.V_OFFNET_DUR,
      D.V_ONNET_DUR,
      D.V_OFFPEAK_CHG,
      D.V_OFFNET_OFFPEAK_CHG,
      D.V_ONNET_OFFPEAK_CHG,
      D.BILLED_OFFPEAK_DUR,
      D.BILLED_OFFNET_OFFPEAK_DUR,
      D.BILLED_ONNET_OFFPEAK_DUR,
      D.PROMO_OFFPEAK_DUR,
      D.PROMO_OFFNET_OFFPEAK_DUR,
      D.PROMO_ONNET_OFFPEAK_DUR,
      D.TOTAL_OFFPEAK_DUR,
      D.V_OFFNET_OFFPEAK_DUR,
      D.V_ONNET_OFFPEAK_DUR,
      D.V_PEAK_CHG,
      D.V_OFFNET_PEAK_CHG,
      D.V_ONNET_PEAK_CHG,
      D.BILLED_PEAK_DUR,
      D.BILLED_OFFNET_PEAK_DUR,
      D.BILLED_ONNET_PEAK_DUR,
      D.PROMO_PEAK_DUR,
      D.PROMO_OFFNET_PEAK_DUR,
      D.PROMO_ONNET_PEAK_DUR,
      D.TOTAL_PEAK_DUR,
      D.V_OFFNET_PEAK_DUR,
      D.V_ONNET_PEAK_DUR,
      D.G_TOTAL_CHG,
      D.PPU_DATAVOL,
      D.ADDON_DATAVOL,
      D.TOTAL_DATAVOL,
      D.G_3G_TOTAL_CHG,
      D.G_2G_TOTAL_CHG,
      D.G_3G_PPU_DATAVOL,
      D.G_2G_PPU_DATAVOL,
      D.G_3G_ADDON_DATAVOL,
      D.G_2G_ADDON_DATAVOL,
      D.G_OFFPEAK_CHG,
      D.G_PEAK_CHG,
      D.PPU_OFFPEAK_DATAVOL,
      D.PPU_PEAK_DATAVOL,
      D.ADDON_OFFPEAK_DATAVOL,
      D.ADDON_PEAK_DATAVOL,
      D.TOTAL_OFFPEAK_DATAVOL,
      D.TOTAL_PEAK_DATAVOL,
      D.G_4G_TOTAL_CHG,
      D.G_4G_PPU_DATAVOL,
      D.G_4G_ADDON_DATAVOL,
      D.BB_UPLINK_TR,
      D.BB_DOWNLINK_TR,
      D.NON_BB_UPLINK_TR,
      D.NON_BB_DOWNLINK_TR,
      D.TOTAL_4G_UPLINK_TR,
      D.TOTAL_4G_DOWNLINK_TR,
      D.TOTAL_3G_UPLINK_TR,
      D.TOTAL_3G_DOWNLINK_TR,
      D.TOTAL_2G_UPLINK_TR,
      D.TOTAL_2G_DOWNLINK_TR,
      D.OFFPEAK_DATA_DOWNLINK_TR,
      D.OFFPEAK_DATA_UPLINK_TR,
      D.PEAK_DATA_DOWNLINK_TR,
      D.PEAK_DATA_UPLINK_TR,
      D.S_TOTAL_CHG,
      D.S_OFFNET_CHG,
      D.S_ONNET_CHG,
      D.BILLED_SMS,
      D.BILLED_OFFNET_SMS,
      D.BILLED_ONNET_SMS,
      D.PROMO_SMS,
      D.PROMO_OFFNET_SMS,
      D.PROMO_ONNET_SMS,
      D.S_OFFPEAK_CHG,
      D.S_OFFNET_OFFPEAK_CHG,
      D.S_ONNET_OFFPEAK_CHG,
      D.BILLED_OFFPEAK_SMS,
      D.BILLED_OFFNET_OFFPEAK_SMS,
      D.BILLED_ONNET_OFFPEAK_SMS,
      D.PROMO_OFFPEAK_SMS,
      D.PROMO_OFFNET_OFFPEAK_SMS,
      D.PROMO_ONNET_OFFPEAK_SMS,
      D.S_PEAK_CHG,
      D.S_OFFNET_PEAK_CHG,
      D.S_ONNET_PEAK_CHG,
      D.BILLED_PEAK_SMS,
      D.BILLED_OFFNET_PEAK_SMS,
      D.BILLED_ONNET_PEAK_SMS,
      D.PROMO_PEAK_SMS,
      D.PROMO_OFFNET_PEAK_SMS,
      D.PROMO_ONNET_PEAK_SMS,
      D.SMS_HITS,
      D.ONNET_SMS_HITS,
      D.OFFNET_SMS_HITS,
      D.V_ALLOC_PCT,
      D.S_ALLOC_PCT,
      D.BB_ALLOC_PCT,
      D.NON_BB_ALLOC_PCT,
      D.NUM_DAYS_DATA,
      D.NUM_DAYS_VOICE,
      D.NUM_DAYS_SMS,
      D.TNR,
      D.PROMO_PACKAGE_NAME,
      D.DEVICE_FL, 
      D.SMARTPHONE_FLAG, 
      D.FLAG_4G,
      D.FLAG_3G,      
      D.FLAG_2G,    
      D.U900_FL,
      D.TOTAL_CHG_GRP,
      D.DATA_CHG_GRP,
      D.TOTAL_CHG,
      D.DATA_CHG,
      D.VA_VOICE_CHG,
      D.VA_SMS_CHG,
      D.VA_DATA_CHG,
      D.VA_BB_CHG,
      D.CUSTOMER_TYPE_MOD,
      D.TYP,
      D.SUB_CAT,
      D.SUB_CAT_MHVC,
      D.SUB_DATA_CAT
    FROM
    (
    	SELECT A.*,
    			B.SITE_ID SITE_ID_2G3G,  
    			C.SITE_ID SITE_ID_4G,
    			B.CELL_NAME CELL_NAME_2G3G, 
    			B.CELL_COVERAGE_TYPE CELL_COVERAGE_TYPE_2G3G, 
    			B.CELL_TECHNOLOGY_TYPE CELL_TECHNOLOGY_TYPE_2G3G,
    			B.CELL_BAND CELL_BAND_2G3G, 
    			B.SECTOR SECTOR_2G3G, 
    			B.AREA AREA_2G3G,
    			C.CELL_NAME CELL_NAME_4G, 
    			C.CELL_COVERAGE_TYPE CELL_COVERAGE_TYPE_4G, 
    			C.CELL_TECHNOLOGY_TYPE CELL_TECHNOLOGY_TYPE_4G,
    			C.CELL_BAND CELL_BAND_4G, 
    			C.SECTOR SECTOR_4G, 
    			C.AREA AREA_4G
    	FROM USG_LACCI_SMY A
    	LEFT JOIN
    	( SELECT LAC, CI, SITE_ID, CELL_NAME, CELL_COVERAGE_TYPE, CELL_TECHNOLOGY_TYPE, CELL_BAND, SECTOR, AREA
    		FROM ISAT_CONSOL_NDB
    		WHERE CELL_TECHNOLOGY_TYPE<>'4G'
    	) B ON A.LAC=B.LAC AND A.CI=B.CI 
    	LEFT JOIN
    	(SELECT CI, SITE_ID, CELL_NAME, CELL_COVERAGE_TYPE, CELL_TECHNOLOGY_TYPE, CELL_BAND, SECTOR, AREA
    		FROM ISAT_CONSOL_NDB
    		WHERE CELL_TECHNOLOGY_TYPE='4G'
    	) C
    	ON A.CI=C.CI
    ) D

"""
    val queryAutoBtsClusterAgg="""
      SELECT AREA_CHANNELS AREA, SUM(NVL(MOBO_REVENUE,0)+NVL(MENTARI_SP_DATA_REVENUE,0)) MOBOSP_REVENUE 
      FROM AUTO_BTS_CLUSTER
      GROUP BY AREA_CHANNELS
"""
    val queryUsgLacciSmySiteNew="""
    SELECT A.*,
    		B.SITE_ID,
    		B.CELL_NAME,
        	B.CELL_COVERAGE_TYPE,
        	B.CELL_TECHNOLOGY_TYPE,
        	B.CELL_BAND,
        	B.SECTOR,
        	B.AREA
    FROM USG_LACCI_SMY A
    LEFT JOIN ISAT_CONSOL_NDB B ON A.LAC=B.LAC AND A.CI=B.CI 
"""
    val queryUsgLacciSmySite2="""
    SELECT A.*,
    		B.SITE_ID,
    		B.CELL_NAME,
        	B.CELL_COVERAGE_TYPE,
        	B.CELL_TECHNOLOGY_TYPE,
        	B.CELL_BAND,
        	B.SECTOR,
        	B.AREA
    FROM LACCI_TOTAL A
    LEFT JOIN ISAT_CONSOL_NDB B ON A.LAC=B.LAC AND A.CI=B.CI 
"""
    val queryUsgLacciSmySiteNew2="""
    SELECT A.*,
    		nvl(nvl(B.SITE_ID,C.SITE_ID),'SITENULL') SITE_ID,
    		CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_NAME
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_NAME
        		END CELL_NAME,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_COVERAGE_TYPE
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_COVERAGE_TYPE
        		END CELL_COVERAGE_TYPE,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_TECHNOLOGY_TYPE
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_TECHNOLOGY_TYPE
        		END CELL_TECHNOLOGY_TYPE,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_BAND
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_BAND
        		END CELL_BAND,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.SECTOR
        		WHEN C.SITE_ID IS NOT NULL THEN C.SECTOR
        		END SECTOR,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.AREA_2G3G
        		WHEN C.SITE_ID IS NOT NULL THEN C.AREA_4G
        		END AREA
    FROM USG_LACCI_SMY A
    LEFT JOIN NDB_2G3G B ON A.LAC=B.LAC AND A.CI=B.CI 
    LEFT JOIN NDB_4G C ON A.CI=C.CI
"""

    val queryNdb2g3g="""
    SELECT LAC, CI, SITE_ID, CELL_NAME, CELL_COVERAGE_TYPE, CELL_TECHNOLOGY_TYPE, CELL_BAND, SECTOR, AREA AREA_2G3G
    	FROM ISAT_CONSOL_NDB
    	WHERE CELL_TECHNOLOGY_TYPE<>'4G'
"""
    val queryNdb4g="""
    SELECT CI, SITE_ID, CELL_NAME, CELL_COVERAGE_TYPE, CELL_TECHNOLOGY_TYPE, CELL_BAND, SECTOR, AREA AREA_4G
    	FROM ISAT_CONSOL_NDB
    	WHERE CELL_TECHNOLOGY_TYPE='4G'
"""
    
    val queryLacciTotalSmyReload="""
    select a.*,b.RELOAD,b.RELOAD_SP
    from LACCI_TOTAL a left join
    RELOAD b on a.lac=b.lac and a.ci=b.ci
"""
    val queryLacciTotalSmyReloadNew="""
    select a.*,b.RELOAD,b.RELOAD_SP
    from USG_LACCI_SMY_MOBO a left join
    RELOAD b on a.lac=b.lac and a.ci=b.ci
"""
    val queryLacciTotalSmyNdb="""
    select A.LAC, A.CI,
        A.V_TOTAL_CHG, A.V_OFFNET_CHG, A.V_ONNET_CHG, A.BILLED_DUR, A.BILLED_OFFNET_DUR, A.MOBOSP_REVENUE,A.RELOAD,A.RELOAD_SP,
        A.BILLED_ONNET_DUR, A.PROMO_DUR, A.PROMO_OFFNET_DUR, A.PROMO_ONNET_DUR, A.TOT_DUR, A.V_OFFNET_DUR, A.V_ONNET_DUR, A.V_OFFPEAK_CHG, A.V_OFFNET_OFFPEAK_CHG,
        A.V_ONNET_OFFPEAK_CHG, A.BILLED_OFFPEAK_DUR, A.BILLED_OFFNET_OFFPEAK_DUR, A.BILLED_ONNET_OFFPEAK_DUR, A.PROMO_OFFPEAK_DUR, A.PROMO_OFFNET_OFFPEAK_DUR,
        A.PROMO_ONNET_OFFPEAK_DUR, A.TOTAL_OFFPEAK_DUR, A.V_OFFNET_OFFPEAK_DUR, A.V_ONNET_OFFPEAK_DUR, A.V_PEAK_CHG, A.V_OFFNET_PEAK_CHG, A.V_ONNET_PEAK_CHG,
        A.BILLED_PEAK_DUR, A.BILLED_OFFNET_PEAK_DUR, A.BILLED_ONNET_PEAK_DUR, A.PROMO_PEAK_DUR, A.PROMO_OFFNET_PEAK_DUR, A.PROMO_ONNET_PEAK_DUR, A.TOTAL_PEAK_DUR,
        A.V_OFFNET_PEAK_DUR, A.V_ONNET_PEAK_DUR, A.G_TOTAL_CHG, A.PPU_DATAVOL, A.ADDON_DATAVOL, A.G_OFFPEAK_CHG, A.G_PEAK_CHG, A.PPU_OFFPEAK_DATAVOL,
        A.PPU_PEAK_DATAVOL, A.ADDON_OFFPEAK_DATAVOL, A.ADDON_PEAK_DATAVOL, A.BB_UPLINK_TR, A.BB_DOWNLINK_TR, A.NON_BB_UPLINK_TR, A.NON_BB_DOWNLINK_TR,
        A.TOTAL_4G_TR, A.TOTAL_3G_TR, A.TOTAL_2G_TR, A.OFFPEAK_DATA_TR, A.PEAK_DATA_TR, A.S_TOTAL_CHG, A.S_OFFNET_CHG, A.S_ONNET_CHG, A.BILLED_SMS,
        A.BILLED_OFFNET_SMS, A.BILLED_ONNET_SMS, A.PROMO_SMS, A.PROMO_OFFNET_SMS, A.PROMO_ONNET_SMS, A.S_OFFPEAK_CHG, A.S_OFFNET_OFFPEAK_CHG,
        A.S_ONNET_OFFPEAK_CHG, A.BILLED_OFFPEAK_SMS, A.BILLED_OFFNET_OFFPEAK_SMS, A.BILLED_ONNET_OFFPEAK_SMS, A.PROMO_OFFPEAK_SMS, A.PROMO_OFFNET_OFFPEAK_SMS,
        A.PROMO_ONNET_OFFPEAK_SMS, A.S_PEAK_CHG, A.S_OFFNET_PEAK_CHG, A.S_ONNET_PEAK_CHG, A.BILLED_PEAK_SMS, A.BILLED_OFFNET_PEAK_SMS, A.BILLED_ONNET_PEAK_SMS,
        A.PROMO_PEAK_SMS, A.PROMO_OFFNET_PEAK_SMS, A.PROMO_ONNET_PEAK_SMS, A.SMS_HITS, A.ONNET_SMS_HITS, A.OFFNET_SMS_HITS, A.VA_VOICE_CHG, A.VA_SMS_CHG,
        A.VA_DATA_CHG, A.VA_BB_CHG, A.SUB_SP, A.SUB_4G_SP, A.SUB_3G_SP, A.SUB_2G_SP, A.SUB_FP, A.SUB_4G_FP, A.SUB_3G_FP, A.SUB_2G_FP,
        A.SUB_NON_DATA_FP, A.V_CHG_U900, A.G_CHG_U900, A.S_CHG_U900, A.VA_VOICE_U900, A.VA_SMS_U900, A.VA_DATA_U900, A.VA_BB_U900,
        A.V_CHG_SP, A.G_CHG_SP, A.S_CHG_SP, A.VA_VOICE_SP, A.VA_SMS_SP, A.VA_DATA_SP, A.VA_BB_SP, A.V_CHG_FP, A.G_CHG_FP, A.S_CHG_FP,
        A.VA_VOICE_FP, A.VA_SMS_FP, A.VA_DATA_FP, A.VA_BB_FP, A.VOICE_DUR_SP, A.DATA_TR_SP, A.SMS_HITS_SP, A.VOICE_DUR_FP,
        A.DATA_TR_FP, A.SMS_HITS_FP, A.SUB_MHVC_V_CHG, A.SUB_MHVC_VA_VOICE_CHG, A.SUB_MHVC_G_CHG, A.SUB_MHVC_VA_DATA_CHG,A.SUB_MHVC_VA_BB_CHG,
        A.SUB_PLATINUM_V_CHG, A.SUB_PLATINUM_VA_VOICE_CHG, A.SUB_PLATINUM_G_CHG, A.SUB_PLATINUM_VA_DATA_CHG,A.SUB_PLATINUM_VA_BB_CHG,
        A.SUB_PLATINUM_SMS_CHG, A.SUB_PLATINUM_VA_SMS_CHG, A.SUB_GOLD_V_CHG, A.SUB_GOLD_VA_VOICE_CHG,
        A.SUB_GOLD_G_CHG, A.SUB_GOLD_VA_DATA_CHG, A.SUB_GOLD_SMS_CHG, A.SUB_GOLD_VA_SMS_CHG, A.SUB_GOLD_VA_BB_CHG, A.SUB_MHVC, A.SUB_NON_MHVC, A.SUB_PLATINUM,
        A.SUB_GOLD, A.SUB_REGULAR, A.SUB_MASS, A.DATA_PLATINUM, A.DATA_GOLD, A.DATA_REGULAR, A.NON_DATA_REGULAR, A.VOICE_SUB_SP, A.VOICE_SUB_FP,
        A.DATA_SUB_SP, A.DATA_SUB_FP, A.SMS_SUB_SP, A.SMS_SUB_FP, A.V_ONLY_SUBS, A.D_ONLY_SUBS, A.S_ONLY_SUBS, A.V_D_ONLY_SUBS, A.V_S_ONLY_SUBS,
        A.S_D_ONLY_SUBS, A.V_S_D_ONLY_SUBS, A.SUB_ALLOC, A.BB_ALLOC_SUB, A.B2B_PREPAID_SUB_ALLOC, A.B2C_PREPAID_SUB_ALLOC, A.B2B_POSTPAID_SUB_ALLOC, A.B2C_POSTPAID_SUB_ALLOC,
    		nvl(nvl(B.SITE_ID,C.SITE_ID),'SITENULL') SITE_ID,
    		CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_NAME
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_NAME
        		END CELL_NAME,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_COVERAGE_TYPE
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_COVERAGE_TYPE
        		END CELL_COVERAGE_TYPE,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_TECHNOLOGY_TYPE
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_TECHNOLOGY_TYPE
        		END CELL_TECHNOLOGY_TYPE,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.CELL_BAND
        		WHEN C.SITE_ID IS NOT NULL THEN C.CELL_BAND
        		END CELL_BAND,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.SECTOR
        		WHEN C.SITE_ID IS NOT NULL THEN C.SECTOR
        		END SECTOR,
        	CASE WHEN B.SITE_ID IS NOT NULL THEN B.AREA_2G3G
        		WHEN C.SITE_ID IS NOT NULL THEN C.AREA_4G
        		END AREA
    FROM LACCI_TOTAL_RELOAD A
    LEFT JOIN NDB_2G3G B ON A.LAC=B.LAC AND A.CI=B.CI 
    LEFT JOIN NDB_4G C ON A.CI=C.CI
"""
    val queryLacciSmyFinalNew="""
    SELECT A.*, NVL(V_TOTAL_CHG,0) + NVL(VA_VOICE_CHG,0) REV_VOICE
    	, NVL(S_TOTAL_CHG,0) + NVL(VA_SMS_CHG,0) REV_SMS
    	, NVL(G_TOTAL_CHG,0) + NVL(VA_BB_CHG,0) + NVL(VA_DATA_CHG,0) + NVL(MOBOSP_REVENUE,0) REV_DATA
    	, NVL(V_TOTAL_CHG,0) + NVL(VA_VOICE_CHG,0) + NVL(S_TOTAL_CHG,0) + NVL(VA_SMS_CHG,0) + NVL(G_TOTAL_CHG,0) + NVL(VA_BB_CHG,0) + NVL(VA_DATA_CHG,0) + NVL(MOBOSP_REVENUE,0) TOTAL_REVENUE
    	, CASE WHEN CELL_TECHNOLOGY_TYPE='2G' OR  CELL_TECHNOLOGY_TYPE IS NULL
    		THEN NVL(V_TOTAL_CHG,0) + NVL(VA_VOICE_CHG,0) + NVL(S_TOTAL_CHG,0) + NVL(VA_SMS_CHG,0) + NVL(G_TOTAL_CHG,0) + NVL(VA_BB_CHG,0) + NVL(VA_DATA_CHG,0) + NVL(MOBOSP_REVENUE,0)
    		ELSE NULL END TOTAL_2G_REVENUE
    	, CASE WHEN CELL_TECHNOLOGY_TYPE='3G' THEN NVL(V_TOTAL_CHG,0) + NVL(VA_VOICE_CHG,0) + NVL(S_TOTAL_CHG,0) + NVL(VA_SMS_CHG,0) + NVL(G_TOTAL_CHG,0) + NVL(VA_BB_CHG,0) + NVL(VA_DATA_CHG,0) + NVL(MOBOSP_REVENUE,0) ELSE NULL END TOTAL_3G_REVENUE
    	, CASE WHEN CELL_TECHNOLOGY_TYPE='4G' THEN NVL(V_TOTAL_CHG,0) + NVL(VA_VOICE_CHG,0) + NVL(S_TOTAL_CHG,0) + NVL(VA_SMS_CHG,0) + NVL(G_TOTAL_CHG,0) + NVL(VA_BB_CHG,0) + NVL(VA_DATA_CHG,0) + NVL(MOBOSP_REVENUE,0) ELSE NULL END TOTAL_4G_REVENUE
    	, (NVL(A.TOTAL_2G_TR,0)+NVL(A.TOTAL_3G_TR,0)+NVL(A.TOTAL_4G_TR,0)) TR_DATA
      , %1$s MONTH_ID
    FROM LACCI_TOTAL_NDB A
"""
    
    val queryCommercialL1="""
    SELECT MONTH_ID,SITE_ID, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR,
      COUNT(DISTINCT concat(LAC,CI)) CNT_CELL,
      SUM(SUB_ALLOC) SUBS_TOTAL,
      SUM(CASE WHEN SUB_SP>0 THEN SUB_SP ELSE NULL END) SUBS_SMARTPHONE,
      SUM(CASE WHEN SUB_PLATINUM+SUB_GOLD>0 THEN SUB_PLATINUM+SUB_GOLD ELSE NULL END) SUBS_HVC,
      SUM(CASE WHEN DATA_PLATINUM+DATA_GOLD+DATA_REGULAR>0 THEN DATA_PLATINUM+DATA_GOLD+DATA_REGULAR ELSE NULL END) SUBS_DATA_USER,
      SUM(CASE WHEN SUB_2G_FP+SUB_2G_SP>0 THEN SUB_2G_FP+SUB_2G_SP ELSE NULL END) SUBS_HSET_2G,
      SUM(CASE WHEN SUB_3G_FP+SUB_3G_SP>0 THEN SUB_3G_FP+SUB_3G_SP ELSE NULL END) SUBS_HSET_3G,
      SUM(CASE WHEN SUB_4G_FP+SUB_4G_SP>0 THEN SUB_4G_FP+SUB_4G_SP ELSE NULL END) SUBS_HSET_4G,
      SUM(RELOAD) RELOAD,
      SUM(TOTAL_REVENUE) REVENUE_TOTAL,
      SUM(REV_VOICE) REVENUE_VOICE_TOTAL,
      SUM(V_TOTAL_CHG) REVENUE_VOICE_PPU,
      SUM(REV_SMS) REVENUE_SMS_TOTAL,
      SUM(S_TOTAL_CHG) REVENUE_SMS_PPU,
      SUM(REV_DATA) REVENUE_DATA_TOTAL,
      SUM(G_TOTAL_CHG) REVENUE_DATA_PPU,
      SUM(REV_DATA)-SUM(G_TOTAL_CHG)-SUM(MOBOSP_REVENUE) REVENUE_DATA_ADDON,
      SUM(MOBOSP_REVENUE) REVENUE_DATA_MOBOSP,
      0 REVENUE_VAS_TOTAL,
      SUM(TOTAL_2G_REVENUE) REVENUE_CELL_2G,
      SUM(TOTAL_3G_REVENUE) REVENUE_CELL_3G,
      SUM(TOTAL_4G_REVENUE) REVENUE_CELL_4G,
      SUM(TOT_DUR) TRAFFIC_VOICE_TOTAL,
      SUM(BILLED_DUR) TRAFFIC_VOICE_BILLED,
      SUM(SMS_HITS) TRAFFIC_SMS_TOTAL,
      SUM(BILLED_SMS) TRAFFIC_SMS_BILLED,
      SUM(TOTAL_2G_TR)+SUM(TOTAL_3G_TR)+SUM(TOTAL_4G_TR) TRAFFIC_DATA_TOTAL,
      SUM(PPU_DATAVOL) TRAFFIC_DATA_PPU,
      (SUM(TOTAL_2G_TR)+SUM(TOTAL_3G_TR)+SUM(TOTAL_4G_TR))-SUM(PPU_DATAVOL) TRAFFIC_DATA_ADDON,
      SUM(TOTAL_2G_TR) TRAFFIC_DATA_RAT_2G,
      SUM(TOTAL_3G_TR) TRAFFIC_DATA_RAT_3G,
      SUM(TOTAL_4G_TR) TRAFFIC_DATA_RAT_4G
    FROM LACCI_TOTAL_SMY
    GROUP BY MONTH_ID,SITE_ID, CELL_TECHNOLOGY_TYPE, CELL_COVERAGE_TYPE, CELL_BAND, SECTOR
"""
    val queryCommercialL2="""
    SELECT MONTH_ID,SITE_ID,
      COUNT(DISTINCT concat(LAC,CI)) CNT_CELL,
      SUM(SUB_ALLOC) SUBS_TOTAL,
      SUM(CASE WHEN SUB_SP>0 THEN SUB_SP ELSE NULL END) SUBS_SMARTPHONE,
      SUM(CASE WHEN SUB_PLATINUM+SUB_GOLD>0 THEN SUB_PLATINUM+SUB_GOLD ELSE NULL END) SUBS_HVC,
      SUM(CASE WHEN DATA_PLATINUM+DATA_GOLD+DATA_REGULAR>0 THEN DATA_PLATINUM+DATA_GOLD+DATA_REGULAR ELSE NULL END) SUBS_DATA_USER,
      SUM(CASE WHEN SUB_2G_FP+SUB_2G_SP>0 THEN SUB_2G_FP+SUB_2G_SP ELSE NULL END) SUBS_HSET_2G,
      SUM(CASE WHEN SUB_3G_FP+SUB_3G_SP>0 THEN SUB_3G_FP+SUB_3G_SP ELSE NULL END) SUBS_HSET_3G,
      SUM(CASE WHEN SUB_4G_FP+SUB_4G_SP>0 THEN SUB_4G_FP+SUB_4G_SP ELSE NULL END) SUBS_HSET_4G,
      SUM(RELOAD) RELOAD,
      SUM(TOTAL_REVENUE) REVENUE_TOTAL,
      SUM(REV_VOICE) REVENUE_VOICE_TOTAL,
      SUM(V_TOTAL_CHG) REVENUE_VOICE_PPU,
      SUM(REV_SMS) REVENUE_SMS_TOTAL,
      SUM(S_TOTAL_CHG) REVENUE_SMS_PPU,
      SUM(REV_DATA) REVENUE_DATA_TOTAL,
      SUM(G_TOTAL_CHG) REVENUE_DATA_PPU,
      SUM(REV_DATA)-SUM(G_TOTAL_CHG)-SUM(MOBOSP_REVENUE) REVENUE_DATA_ADDON,
      SUM(MOBOSP_REVENUE) REVENUE_DATA_MOBOSP,
      0 REVENUE_VAS_TOTAL,
      SUM(TOTAL_2G_REVENUE) REVENUE_CELL_2G,
      SUM(TOTAL_3G_REVENUE) REVENUE_CELL_3G,
      SUM(TOTAL_4G_REVENUE) REVENUE_CELL_4G,
      SUM(TOT_DUR) TRAFFIC_VOICE_TOTAL,
      SUM(BILLED_DUR) TRAFFIC_VOICE_BILLED,
      SUM(SMS_HITS) TRAFFIC_SMS_TOTAL,
      SUM(BILLED_SMS) TRAFFIC_SMS_BILLED,
      SUM(TOTAL_2G_TR)+SUM(TOTAL_3G_TR)+SUM(TOTAL_4G_TR) TRAFFIC_DATA_TOTAL,
      SUM(PPU_DATAVOL) TRAFFIC_DATA_PPU,
      (SUM(TOTAL_2G_TR)+SUM(TOTAL_3G_TR)+SUM(TOTAL_4G_TR))-SUM(PPU_DATAVOL) TRAFFIC_DATA_ADDON,
      SUM(TOTAL_2G_TR) TRAFFIC_DATA_RAT_2G,
      SUM(TOTAL_3G_TR) TRAFFIC_DATA_RAT_3G,
      SUM(TOTAL_4G_TR) TRAFFIC_DATA_RAT_4G
    FROM LACCI_TOTAL_SMY
    GROUP BY MONTH_ID,SITE_ID

"""    
}  

