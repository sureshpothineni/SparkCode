set hive.exec.dynamic.partition.mode=nonstrict;

SET mapred.compress.map.output=true ;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;
SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

set parquet.compression=snappy;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=134217728;
set hive.merge.size.per.task=209715200;

set mapred.job.queue.name=root.bdh.med;

use ${hv_db_efgifi};

insert overwrite table tic_index partition(load_date)
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
tic_stg.ztag__acct_rel,
tic_stg.ztag__rel,
tic_stg.ztag__name,
tic_stg.ztag__first_name,
tic_stg.ztag__last_name,
tic_stg.ztag__name_ext,
tic_stg.ztag__addr_line_0,
tic_stg.ztag__addr_line_1,
tic_stg.ztag__addr_line_2,
tic_stg.ztag__addr_line_3,
tic_stg.ztag__addr_line_4,
tic_stg.ztag__city,
tic_stg.ztag__st,
tic_stg.ztag__zip15,
tic_stg.ztag__zip69,
tic_stg.ztag__ssn,
tic_stg.ztag__cust_type,
tic_stg.ztag__deceased_ind,
tic_stg.ztag__country_code,
tic_stg.ztag__dob,
tic_stg.mem_cust_age,
tic_stg.ztag__extract_date,
tic_stg.ztag__cif_key,
tic_stg.MST_EXT_AVG_LEDGR_BAL_YTD,
tic_stg.MST_EXT_ACT_STATUS,
tic_stg.MST_EXT_DATE_OD,
tic_stg.MST_EXT_DECEASED_DATE,
tic_stg.MST_EXT_NO_TIMES_OD_12,
tic_stg.MST_EXT_STMT_MAIL_CODE,
tic_stg.MST_EXT_DATE_DORMANT,
case when empl_rltd.c2a_ssn is not null then 'Y' else 'N' end as mem_is_empl_related,
--not sure of these columns reverify
empl_rltd.c2a_ssn as  mem_cust_empl_ssn,
empl_rltd.mem_empl_status,
empl_rltd.hr_empl_jobtitle as mem_cust_empl_job_title,
empl_rltd.hr_empl_jobcode as mem_cust_empl_job_code,
empl_rltd.hr_empl_dept_name as mem_cust_empl_dept_name,
empl_rltd.acct_empl_rehire_dt,
case when idda.accountnumber is not null then 'Y' else 'N' end as mem_is_idda, 
idda.accountnumber,
idda.accountdescription,
mem_data.ads_pur_denoinds1, 
mem_data.ads_pur_amtdue1,  
mem_data.ads_pur_amtfee1,   
mem_data.ads_pur_number1,   
mem_data.ads_pur_purchsr1,  
mem_data.ads_pur_payto1,    
mem_data.ads_pur_payto2,    
mem_data.ads_pur_numitem1,  
mem_data.ads_pur_numacct, 
branch.brn_branch_name,
concat(branch.brn_br_addr1,branch.brn_br_addr2) as brn_addr,
branch.brn_br_city, 
branch.brn_br_state,
branch.brn_br_zip ,
concat(substr(tic_stg.log_caldate,1,4),'-',substr(tic_stg.log_caldate,5,2),'-',substr(tic_stg.log_caldate,7,2),'T',substr(tic_stg.log_time,1,2),':',substr(tic_stg.log_time,3,2),':',substr(tic_stg.log_time,5,2),'.',substr(tic_stg.log_time,7,2)) as tic_timestamp,
${load_date} as load_date

from ${hv_db_efgifi_wrk}.wrk_tic_integrated_stg_2 tic_stg 

LEFT OUTER JOIN 
	${hv_db_efgifi_wrk}.tic_employee_related empl_rltd
	ON tic_stg.log_acctnbr = empl_rltd.c2a_account_number
	and tic_stg.act2_type = empl_rltd.c2a_account_type
	and tic_stg.act2_account_bank_number = empl_rltd.c2a_account_bank_number

LEFT OUTER JOIN 
	${hv_db_efgifi}.view_itrust idda
	on tic_stg.log_acctnbr = idda.accountnumber

LEFT OUTER JOIN 
	(select * from 
		(select ads_regionid, ads_officeid, ads_cashid, ads_transeq, ads_pur_operid, 
			ads_pur_denoinds1,ads_pur_amtdue1,ads_pur_amtfee1,ads_pur_number1,ads_pur_purchsr1,ads_pur_payto1,ads_pur_payto2,
			ads_pur_numitem1,ads_pur_numacct, 
			row_number() over (partition by ads_pur_operid order by ads_pur_number1 desc) as RIND
			from ${hv_db_efgifi}.ticofficial_extract_teloffck 
			where load_date = ${load_date}
		) a where a.RIND=1
	)mem_data
	on tic_stg.log_bknbr = cast( mem_data.ads_regionid as int)
	and tic_stg.log_brnbr = cast(mem_data.ads_officeid as int)
	and  substr(tic_stg.teller_key,6,2) = mem_data.ads_cashid --(last two digitis only)
	and tic_stg.sqnbr_key = cast(mem_data.ads_transeq as int)
	and tic_stg.log_logonid = mem_data.ads_pur_operid

LEFT OUTER JOIN 
	${hv_db_efgifi}.ticofficial_branch_data branch
	on
	tic_stg.log_bknbr = cast(branch.brn_bl_mkt_id as int)   
	and tic_stg.log_brnbr = cast(branch.brn_bl_branch as int) 
	and branch.load_date = ${load_date}
;