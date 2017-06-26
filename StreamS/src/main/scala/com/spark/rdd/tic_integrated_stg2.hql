set hive.auto.convert.join = true;

SET mapred.compress.map.output=true ;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

set parquet.compression=snappy;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=134217728;
set hive.merge.size.per.task=209715200;

use ${hv_db_efgifi_wrk};

set mapred.job.queue.name=root.bdh.med;

insert overwrite table wrk_tic_integrated_stg_2
select 
tic_stg.log_time,
tic_stg.log_logonid,
tic_stg.teller_key,
tic_stg.sqnbr_key,
tic_stg.log_trncode,
tic_stg.log_bknbr,
tic_stg.log_brnbr,
tic_stg.log_bkdate,
tic_stg.log_caldate,
tic_stg.log_wsid,
tic_stg.log_acctnbr,
tic_stg.log_online,
tic_stg.log_tamt,
tic_stg.log_cash,
tic_stg.log_ouamt,
tic_stg.log_sf,
tic_stg.log_reverse,
tic_stg.log_reversed,
tic_stg.log_ovrdcod,
tic_stg.log_frdaba,
tic_stg.log_dbcr,
tic_stg.log_custidct,
tic_stg.log_override_affil,
tic_stg.log_money_order_serial_no_1,
tic_stg.log_money_order_serial_no_2,
tic_stg.log_money_order_serial_no_3,
tic_stg.log_money_order_serial_no_4,
tic_stg.log_money_order_serial_no_5,
tic_stg.log_money_order_serial_no_6,
tic_stg.log_money_order_serial_no_7,
tic_stg.log_money_order_serial_no_8,
tic_stg.log_money_order_serial_no_9,
tic_stg.log_money_order_serial_no_10,
tic_stg.log_money_order_serial_no_11,
tic_stg.log_money_order_serial_no_12,
tic_stg.log_money_order_serial_no_13,
tic_stg.log_money_order_serial_no_14,
tic_stg.log_money_order_serial_no_15,
tic_stg.log_money_order_face_amt_1,
tic_stg.log_money_order_face_amt_2,
tic_stg.log_money_order_face_amt_3,
tic_stg.log_money_order_face_amt_4,
tic_stg.log_money_order_face_amt_5,
tic_stg.log_money_order_face_amt_6,
tic_stg.log_money_order_face_amt_7,
tic_stg.log_money_order_face_amt_8,
tic_stg.log_money_order_face_amt_9,
tic_stg.log_money_order_face_amt_10,
tic_stg.log_money_order_face_amt_11,
tic_stg.log_money_order_face_amt_12,
tic_stg.log_money_order_face_amt_13,
tic_stg.log_money_order_face_amt_14,
tic_stg.log_money_order_face_amt_15,
tic_stg.log_money_order_rec_acct,
tic_stg.log_cust_id_type_primid,
tic_stg.log_cust_id_number_primid,
tic_stg.log_cust_id_st_cntry_primid,
tic_stg.log_cust_id_type_secid,
tic_stg.log_cust_id_number_secid,
tic_stg.log_cust_id_st_cntry_secid,
tic_stg.log_check_number,
tic_stg.log_trans_fr_appl,
tic_stg.log_trans_fr_cust_name,
tic_stg.log_trans_fr_acct,
tic_stg.log_trans_fr_affil,
tic_stg.log_trans_fr_branch,
tic_stg.log_trans_to_cust_name,
tic_stg.log_trans_to_acct,
tic_stg.log_trans_to_appl,
tic_stg.log_trans_to_affil,
tic_stg.log_trans_to_branch,
tic_stg.log_feeamt,
tic_stg.log_micr_line,
tic_stg.log_wd_business_id,
tic_stg.log_wd_pnc_acct,
tic_stg.log_wd_transit,
tic_stg.log_cpcs_itemseq,
tic_stg.log_cash_back,
tic_stg.mem_session,
tic_stg.hr_empl_name,
tic_stg.hr_empl_ssn,
tic_stg.hr_empl_jobtitle,
tic_stg.hr_empl_jobcode,
tic_stg.hr_empl_dept_name,
tic_stg.hr_empl_orig_hire_dt,
tic_stg.hr_empl_rehire_dt,
tic_stg.hr_empl_termination_dt,
tic_stg.hr_empl_last_dt_worked,
tic_stg.hr_empl_leave_status,
tic_stg.hr_empl_job_status,
tic_stg.hr_empl_pid,
tic_stg.hr_empl_grade,
tic_stg.act2_type,
tic_stg.act2_legal_title_line_1,
tic_stg.act2_social_security_num,
tic_stg.act2_account_status,
tic_stg.act2_account_bank_number,
tic_stg.act2_account_branch,
tic_stg.act2_product_type,
tic_stg.act2_account_open_date,
tic_stg.act2_account_close_date,
tic_stg.act2_account_maint_date,
tic_stg.gl_acct_id,
tic_stg.unq_id_in_src_syst,
tic_stg.acct_dsc,
cust_acct.ztag__acct_rel,
cust2_cust.ztag__rel,
cif_cust.ztag__name,
cif_cust.ztag__first_name,
cif_cust.ztag__last_name,
cif_cust.ztag__name_ext,
cif_cust.ztag__addr_line_0,
cif_cust.ztag__addr_line_1,
cif_cust.ztag__addr_line_2,
cif_cust.ztag__addr_line_3,
cif_cust.ztag__addr_line_4,
cif_cust.ztag__city,
cif_cust.ztag__st,
cif_cust.ztag__zip15,
cif_cust.ztag__zip69,
cif_cust.ztag__ssn,
cif_cust.ztag__cust_type,
cif_cust.ztag__deceased_ind,
cif_cust.ztag__country_code,
cif_cust.ztag__dob,
nvl(floor(datediff(to_date(from_unixtime(unix_timestamp())),to_date(from_unixtime(unix_timestamp(cast(ztag__dob as string),'yyyyMMdd'))
))/365.25),0) as mem_cust_age,
cif_cust.ztag__extract_date,
cif_cust.ztag__cif_key,
dda.MST_EXT_AVG_LEDGR_BAL_YTD,
dda.MST_EXT_ACT_STATUS,
dda.MST_EXT_DATE_OD,
dda.MST_EXT_DECEASED_DATE,
dda.MST_EXT_NO_TIMES_OD_12,
dda.MST_EXT_STMT_MAIL_CODE,
dda.MST_EXT_DATE_DORMANT,
tic_stg.load_date

from wrk_tic_integrated_stg_1  tic_stg

left outer join 
   (select cust2acct1.*
    from (select ztag__acct_rel,ztag__keymaster,ztag__acct_number,ztag__acct_type,ztag__acct_bank_number,load_date,
	ROW_NUMBER() OVER (PARTITION BY ztag__acct_number ORDER BY cast(cust.ztag__acct_rel as string) ) as RIND
	from ${hv_db_etl}.CIF_PNC_IMS_CUST2ACCT cust where cust.load_date = ${load_date}) cust2acct1 where cust2acct1.RIND = 1) cust_acct
	ON tic_stg.log_acctnbr = cust_acct.ztag__acct_number
	and tic_stg.act2_type = cust_acct.ztag__acct_type
	and tic_stg.act2_account_bank_number = cust_acct.ztag__acct_bank_number
left outer join 
	${hv_db_etl}.CIF_PNC_IMS_CUST2CUST cust2_cust
	ON cust_acct.ztag__keymaster= cust2_cust.ztag__rel_keymaster
	and cust2_cust.load_date = '2016-04-27'
left outer JOIN
	${hv_db_etl}.CIF_PNC_IMS_CUSTOMER cif_cust
	ON cust_acct.ztag__keymaster = cif_cust.ztag__perm_key 
	and cif_cust.load_date = ${load_date}
	and cust_acct.ztag__acct_rel in ("F-N" ,"BEN" )
left outer join 
	${hv_db_etl}.dda_pnc_hogan_master dda
	ON tic_stg.log_acctnbr = cast(dda.mst_ext_key as string)
	and dda.load_date = ${load_date}
;