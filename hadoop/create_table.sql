CREATE TABLE `stg.ifrsb2b_custgroupvalidation`(
  `customer_name` string,
  `customer_group` string,
  `ppm_dttm` timestamp)
PARTITIONED BY (
  `job_id` string,
  `dt_id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'



CREATE TABLE `stg.ifrsb2b_groupofservicesvalidation`(
  `business_area_name` string,
  `service_group` string,
  `ppm_dttm` timestamp)
PARTITIONED BY (
  `job_id` string,
  `dt_id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  
  
  
CREATE TABLE `stg.ifrsb2b_recon`(
  `total` double,
  `rec_type` string,
  `ppm_dttm` timestamp)
PARTITIONED BY (
  `job_id` string,
  `dt_id` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  
  
CREATE TABLE `stg.ifrsb2b_eventtype`(
  `service_id` string,
  `agreement_num` string,
  `po_id` string,
  `agreement_name` string)
PARTITIONED BY (
   `row_number` int
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  
  
CREATE TABLE `stg.ifrsb2b_dailyorder`(
  `order_line_item_id` string,
  `order_line_item_status` string,
  `order_id` string,
  `order_num` string,
  `order_type` string,
  `order_status` string,
  `order_created_date` string,
  `order_submission_date` string,
  `order_completion_date` string,
  `opportunity_num` string,
  `quote_num` string,
  `sr_num` string,
  `document_required` string,
  `priority` string,
  `service_id` string,
  `service_account_id` string,
  `billing_account_id` string,
  `customer_account_id` string,
  `corp_customer_id` string,
  `promise_to_pay_amt` string,
  `promise_to_pay_date` string,
  `nik` string,
  `created_by` string,
  `submitted_by` string,
  `dealer_id` string,
  `outlet` string,
  `channel` string,
  `price_list` string,
  `total` string,
  `assigned_to` string,
  `sales_person` string,
  `contract_signing_officer` string,
  `action` string,
  `product_id` string,
  `product` string,
  `parent_order_id` string,
  `product_type` string,
  `product_category` string,
  `block_status` string,
  `promo_code` string,
  `qty` string,
  `agreement_name` string,
  `quadrant` string,
  `reason` string,
  `start_price` string,
  `net_price` string,
  `extended_net_price` string,
  `otc_sub_total` string,
  `mrc_sub_total` string,
  `otc` string,
  `mrc` string,
  `entitlement` string,
  `activation_date` string,
  `project_name` string,
  `actual_rfs_date` string,
  `requested_rfs_date` string,
  `bandwith` string,
  `bandwith_unit_of_measure` string,
  `site_a_address_line_1` string,
  `site_a_address_line_2` string,
  `site_a_bulding_name` string,
  `site_a_city` string,
  `site_a_country` string,
  `site_a_zip_code` string,
  `site_b_address_line_1` string,
  `site_b_address_line_2` string,
  `site_b_building_name` string,
  `site_b_city` string,
  `site_b_country` string,
  `site_b_zip_code` string,
  `integration_status` string,
  `is_limitless` string,
  `old_is_limitless` string,
  `usage_limit` string,
  `old_usage_limit` string,
  `iccid` string,
  `old_iccid` string,
  `asset_name` string,
  `old_service_number` string,
  `local_exchange_port` string,
  `ip_address` string,
  `link_ref_number` string,
  `access_port` string,
  `last_update_date` string,
  `card_type` string,
  `number_of_e1` string,
  `order_comment` string,
  `contract_start_date` string,
  `contract_end_date` string,
  `approved_by` string,
  `midi_location` string,
  `product_line` string,
  `fab_signed_date` string,
  `fab_earlier_flag` string,
  `eve` string,
  `quote_created_date` string,
  `quote_submission_date` string,
  `quote_completion_date` string,
  `lead_time` string,
  `lead_time_in_weekdays` string,
  `lead_time_reason` string,
  `initial_rfs_date` string,
  `department_sd` string,
  `project_manager_name` string,
  `ass_project_manager_name` string,
  `outask` string,
  `interface_type` string,
  `transmission_type` string,
  `total_pending_estimation` string,
  `updated_rfs_date` string,
  `churn_reason` string,
  `churn_comment` string,
  `termination_dt` string,
  `product_seq` string,
  `billing_product_id` string,
  `subscription_ref` string,
  `agreement_number` string)
PARTITIONED BY (
  `file_date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    
  
LOAD DATA INPATH 'hdfs://nameservice1/user/hive/warehouse/sor.db/b2bpostpaid_dailyorder/file_date=20190114' INTO TABLE stg.b2bpostpaid_dailyorder PARTITION (file_date='20190114');


hdfs dfs -put "/apps/hadoop_spark/ifrs_b2b/eventtype/b2b_ifrs_event_type_temp_csv" "/user/hdp-rev_dev/ifrs_b2b/temp/"



alter table stg.ifrsb2b_dailyorder add partition (file_date='20190112');
