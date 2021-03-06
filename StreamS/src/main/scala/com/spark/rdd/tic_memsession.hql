
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

use ${hv_db_efgifi_wrk};

insert overwrite table TELLER_TXN_DLY_MEMGEN partition(z_load_date)
select
teller.rec_num
,teller.cdc_cd
,teller.teller_key8
,teller.teller_dup
,teller.sqnbr_key
,teller.log_key_exp
,teller.tran_comm_seq
,teller.tran_appl_id
,teller.log_bknbr
,teller.log_brnbr
,teller.log_frdaba
,teller.log_acctnbr
,teller.log_trncode
,teller.log_bkdate
,teller.log_caldate
,teller.log_time
,teller.log_wsid
,teller.log_logonid
,teller.log_check_number
,teller.log_locnbr
,teller.log_cust_id_type_primid
,teller.log_cust_id_number_primid
,teller.log_cust_id_exp_date_primid
,teller.log_cust_id_bir_date_primid
,teller.log_cust_id_st_cntry_primid
,teller.log_cust_id_comment_primid
,teller.log_cust_id_type_secid
,teller.log_cust_id_number_secid
,teller.log_cust_id_exp_date_secid
,teller.log_cust_id_bir_date_secid
,teller.log_cust_id_st_cntry_secid
,teller.log_cust_id_comment_secid
,teller.log_cust_name1
,teller.log_store_num
,teller.log_override_affil
,teller.log_ej_proclist
,teller.log_rev_sqnbr
,teller.log_cpcs_itemseq
,teller.log_micr_line
,teller.log_tellnbr
,teller.log_bknbrc
,teller.log_frdabac
,teller.log_acctnbrc
,teller.log_cust_name
,teller.log_subprod
,teller.log_package_code
,teller.log_deposit_seq
,teller.log_deposit_type
,teller.log_hold_seq
,teller.log_hold_type
,teller.log_dyshold
,teller.log_tamt
,teller.log_cash
,teller.log_check
,teller.log_availchk
,teller.log_ouamt
,teller.log_feeamt
,teller.log_holdamt
,teller.log_cash_back
,teller.log_cb_balance
,teller.log_amt1
,teller.log_amt2
,teller.log_amt3
,teller.log_amt4
,teller.log_amt5
,teller.log_online
,teller.log_sf
,teller.log_custidct
,teller.log_dbcr
,teller.log_reverse
,teller.log_rev_session
,teller.log_newflg
,teller.log_scan
,teller.log_trunc
,teller.log_ouind
,teller.log_accttype
,teller.log_pnc_cust
,teller.log_trainmd
,teller.log_extrlt
,teller.log_regcomm
,teller.log_maker_ind
,teller.log_pull_ind
,teller.log_force_flag
,teller.log_ampm
,teller.log_nitebag
,teller.log_not_validated
,teller.log_declare
,teller.log_profile
,teller.log_tcr
,teller.log_eflag1
,teller.log_eflag2
,teller.log_eflag3
,teller.log_eflag4
,teller.log_value
,teller.log_source_flag
,teller.log_reversed
,teller.log_cd_hundreds
,teller.log_cd_fifties
,teller.log_cd_twenties
,teller.log_cd_tens
,teller.log_cd_fives
,teller.log_cd_twos
,teller.log_cd_ones
,teller.log_cd_dollarsc
,teller.log_cd_halves
,teller.log_cd_quarters
,teller.log_cd_dimes
,teller.log_cd_nickels
,teller.log_cd_pennies
,teller.log_message_d
,teller.log_message_fil1
,teller.log_message_o1
,teller.log_message_o2
,teller.log_message_o3
,teller.log_message_o4
,teller.log_message_o5
,teller.log_message_fil2
,teller.log_message_i1
,teller.log_message_i2
,teller.log_message_i3
,teller.log_message_fil3
,teller.log_host_comp
,teller.log_host_rej
,teller.log_ovrdcod
,teller.log_message_fil4
,teller.log_odp_acct_nbr
,teller.log_odp_acct_type
,teller.log_fdr_apply_to_dda
,teller.log_odp_hold_seq
,teller.log_odp_hold_type
,teller.log_which_protector
,teller.log_fdr_auth_code
,teller.log_mscdata
,teller.log_misc2
,teller.log_cust_id_comments_2
,teller.log_misc4
,teller.log_misc5
,teller.log_appl_flag_1
,teller.log_appl_flag_2
,teller.log_appl_flag_3
,teller.log_appl_flag_4
,teller.log_appl_flag_5
,teller.log_appl_flag_6
,teller.log_appl_flag_7
,teller.log_appl_flag_8
,teller.log_appl_flag_9
,teller.log_appl_flag_10
,teller.log_appl_misc_1
,teller.log_appl_misc_2
,teller.log_appl_misc_3
,teller.log_appl_misc_4
,teller.log_appl_misc_5
,teller.log_appl_misc_6
,teller.log_appl_misc_7
,teller.log_appl_amt_1
,teller.log_appl_amt_2
,teller.log_appl_amt_3
,teller.log_appl_amt_4
,teller.log_appl_amt_5
,teller.log_appl_amt_6
,teller.log_appl_amt_7
,teller.log_appl_amt_8
,teller.log_appl_amt_9
,teller.log_appl_amt_10
,teller.log_appl_amt_11
,teller.log_appl_amt_12
,teller.log_appl_amt_13
,teller.log_appl_amt_14
,teller.log_appl_amt_15
,teller.log_appl_amt_16
,teller.log_appl_amt_17
,teller.log_appl_amt_18
,teller.log_appl_amt_19
,teller.log_appl_amt_20
,teller.log_appl_amt_21
,teller.log_appl_amt_22
,teller.log_appl_amt_23
,teller.log_appl_amt_24
,teller.log_appl_amt_25
,teller.log_appl_amt_26
,teller.log_appl_amt_27
,teller.log_appl_amt_28
,teller.log_appl_amt_29
,teller.log_appl_amt_30
,teller.log_appl_amt_31
,teller.log_appl_amt_32
,teller.log_appl_amt_33
,teller.log_appl_amt_34
,teller.log_appl_amt_35
,teller.log_appl_amt_36
,teller.log_appl_amt_37
,teller.log_appl_amt_38
,teller.log_appl_amt_39
,teller.log_appl_amt_40
,teller.log_appl_count_1
,teller.log_appl_count_2
,teller.log_appl_count_3
,teller.log_appl_count_4
,teller.log_appl_count_5
,teller.log_appl_count_6
,teller.log_appl_count_7
,teller.log_appl_count_8
,teller.log_appl_count_9
,teller.log_appl_count_10
,teller.log_appl_count_11
,teller.log_appl_count_12
,teller.log_appl_count_13
,teller.log_appl_count_14
,teller.log_appl_count_15
,teller.teller_key
,teller.teller_inc
,teller.teller_key7
,teller.log_misc_filler
,teller.log_audit_auditor
,teller.log_audit_auditor_logon
,teller.log_audit_bait_money
,teller.log_audit_begcash
,teller.log_audit_cash_counted
,teller.log_audit_cashin
,teller.log_audit_cashout
,teller.log_audit_check_cashing
,teller.log_audit_date_last_used
,teller.log_audit_device_type
,teller.log_audit_differ
,teller.log_audit_drawer_amt
,teller.log_audit_drawer_limits
,teller.log_audit_end_cash
,teller.log_audit_locks
,teller.log_audit_over_short
,teller.log_audit_reason
,teller.log_audit_station_conforms
,teller.log_audit_tot_cash
,teller.log_audit_unprocess_work
,teller.log_audit_vault_amt
,teller.log_audit_whom_last_used
,teller.log_audit_witness
,teller.log_audit_witness_logon
,teller.log_batch_cut_itmcnt
,teller.log_cash_in
,teller.log_cshfile_sum_tot_cash
,teller.log_cshfile_summ_ci_amt_1
,teller.log_cshfile_summ_ci_amt_2
,teller.log_cshfile_summ_ci_amt_3
,teller.log_cshfile_summ_ci_amt_4
,teller.log_cshfile_summ_ci_amt_5
,teller.log_cshfile_summ_ci_amt_6
,teller.log_cshfile_summ_ci_ari_1
,teller.log_cshfile_summ_ci_ari_2
,teller.log_cshfile_summ_ci_ari_3
,teller.log_cshfile_summ_ci_ari_4
,teller.log_cshfile_summ_ci_ari_5
,teller.log_cshfile_summ_ci_ari_6
,teller.log_cshfile_summ_ci_br_1
,teller.log_cshfile_summ_ci_br_2
,teller.log_cshfile_summ_ci_br_3
,teller.log_cshfile_summ_ci_br_4
,teller.log_cshfile_summ_ci_br_5
,teller.log_cshfile_summ_ci_br_6
,teller.log_cshfile_summ_ci_cb_1
,teller.log_cshfile_summ_ci_cb_2
,teller.log_cshfile_summ_ci_cb_3
,teller.log_cshfile_summ_ci_cb_4
,teller.log_cshfile_summ_ci_cb_5
,teller.log_cshfile_summ_ci_cb_6
,teller.log_cshfile_summ_ci_cpi_1
,teller.log_cshfile_summ_ci_cpi_2
,teller.log_cshfile_summ_ci_cpi_3
,teller.log_cshfile_summ_ci_cpi_4
,teller.log_cshfile_summ_ci_cpi_5
,teller.log_cshfile_summ_ci_cpi_6
,teller.log_cshfile_summ_ci_fil_1
,teller.log_cshfile_summ_ci_fil_2
,teller.log_cshfile_summ_ci_fil_3
,teller.log_cshfile_summ_ci_fil_4
,teller.log_cshfile_summ_ci_fil_5
,teller.log_cshfile_summ_ci_fil_6
,teller.log_cshfile_summ_ci_rt_1
,teller.log_cshfile_summ_ci_rt_2
,teller.log_cshfile_summ_ci_rt_3
,teller.log_cshfile_summ_ci_rt_4
,teller.log_cshfile_summ_ci_rt_5
,teller.log_cshfile_summ_ci_rt_6
,teller.log_cshfile_summ_ci_ser_1
,teller.log_cshfile_summ_ci_ser_2
,teller.log_cshfile_summ_ci_ser_3
,teller.log_cshfile_summ_ci_ser_4
,teller.log_cshfile_summ_ci_ser_5
,teller.log_cshfile_summ_ci_ser_6
,teller.log_cshfile_summ_co_amt
,teller.log_cshfile_summ_co_ari
,teller.log_cshfile_summ_co_br
,teller.log_cshfile_summ_co_cb
,teller.log_cshfile_summ_co_cpi
,teller.log_cshfile_summ_co_fil
,teller.log_cshfile_summ_co_rt
,teller.log_cshfile_summ_co_ser
,teller.log_dep_pymt_cshbk_amt
,teller.log_eoc_cashin
,teller.log_eoc_cashout
,teller.log_eoc_onus_chk
,teller.log_eoc_onus_csh_out
,teller.log_eoc_onus_ttl_in
,teller.log_eoc_pnc_recourse_acct
,teller.log_eoc_transit_chk
,teller.log_eoc_transit_csh_out
,teller.log_eoc_transit_ttl_in
,teller.log_for_cur_action
,teller.log_for_cur_exchange_rate
,teller.log_for_cur_from_amt
,teller.log_for_cur_from_code
,teller.log_for_cur_to_amt
,teller.log_for_cur_to_code
,teller.log_over_short_crdrdesc
,teller.log_over_short_db19203
,teller.log_over_short_hostid
,teller.log_over_short_outdate
,teller.log_qd_verify_counted_cash
,teller.log_qd_verify_counted_check
,teller.log_qd_verify_counted_nbrch
,teller.log_qd_verify_cust_dbcr_amt
,teller.log_qd_verify_cust_dbcr_ind
,teller.log_qd_verify_orig_seq
,teller.log_qd_verify_orig_teller
,teller.log_set_cash_cb01
,teller.log_set_cash_cb01_atm
,teller.log_set_cash_cb02
,teller.log_set_cash_cb02_atm
,teller.log_set_cash_cb03
,teller.log_set_cash_cb03_atm
,teller.log_set_cash_cb04
,teller.log_set_cash_cb04_atm
,teller.log_set_cash_cb05
,teller.log_set_cash_cb06
,teller.log_set_cash_cb07
,teller.log_set_cash_cb08
,teller.log_set_cash_cb09
,teller.log_set_cash_cb10
,teller.log_set_cash_cb11
,teller.log_set_cash_cb12
,teller.log_set_cash_cb13
,teller.log_set_cash_cb14
,teller.log_set_cash_cb15
,teller.log_set_cash_end01
,teller.log_set_cash_end01_atm
,teller.log_set_cash_end02
,teller.log_set_cash_end02_atm
,teller.log_set_cash_end03
,teller.log_set_cash_end03_atm
,teller.log_set_cash_end04
,teller.log_set_cash_end04_atm
,teller.log_set_cash_end05
,teller.log_set_cash_end06
,teller.log_set_cash_end07
,teller.log_set_cash_end08
,teller.log_set_cash_end09
,teller.log_set_cash_end10
,teller.log_set_cash_end11
,teller.log_set_cash_end12
,teller.log_set_cash_end13
,teller.log_set_cash_end14
,teller.log_set_cash_end15
,teller.log_set_cash_start01
,teller.log_set_cash_start01_atm
,teller.log_set_cash_start02
,teller.log_set_cash_start02_atm
,teller.log_set_cash_start03
,teller.log_set_cash_start03_atm
,teller.log_set_cash_start04
,teller.log_set_cash_start04_atm
,teller.log_set_cash_start05
,teller.log_set_cash_start06
,teller.log_set_cash_start07
,teller.log_set_cash_start08
,teller.log_set_cash_start09
,teller.log_set_cash_start10
,teller.log_set_cash_start11
,teller.log_set_cash_start12
,teller.log_set_cash_start13
,teller.log_set_cash_start14
,teller.log_set_cash_start15
,teller.log_settle_bait
,teller.log_settle_begcash
,teller.log_settle_cashin
,teller.log_settle_cashout
,teller.log_settle_device_type
,teller.log_settle_differ
,teller.log_settle_dimes_l
,teller.log_settle_dimes_w
,teller.log_settle_dollars_w
,teller.log_settle_dollarsc_l
,teller.log_settle_endcash
,teller.log_settle_fifties_l
,teller.log_settle_fifties_s
,teller.log_settle_fives_l
,teller.log_settle_fives_s
,teller.log_settle_halves_l
,teller.log_settle_halves_w
,teller.log_settle_hundreds_l
,teller.log_settle_hundreds_s
,teller.log_settle_misc
,teller.log_settle_mutil
,teller.log_settle_nickels_l
,teller.log_settle_nickels_w
,teller.log_settle_ones_l
,teller.log_settle_ones_s
,teller.log_settle_over_short
,teller.log_settle_pennies_l
,teller.log_settle_pennies_w
,teller.log_settle_quarters_l
,teller.log_settle_quarters_w
,teller.log_settle_tcr_fine_count
,teller.log_settle_tcr_tcd
,teller.log_settle_tens_l
,teller.log_settle_tens_s
,teller.log_settle_tot_cash
,teller.log_settle_tot_vault
,teller.log_settle_twenties_l
,teller.log_settle_twenties_s
,teller.log_settle_twos_l
,teller.log_settle_twos_s
,teller.log_vault_bait1
,teller.log_vault_bait2
,teller.log_vault_begcash
,teller.log_vault_cashin
,teller.log_vault_cashout
,teller.log_vault_differ
,teller.log_vault_dimes_v1
,teller.log_vault_dimes_v2
,teller.log_vault_dollars_v2
,teller.log_vault_dollarsc_v1
,teller.log_vault_endcash
,teller.log_vault_fifties_v1
,teller.log_vault_fifties_v2
,teller.log_vault_fives_v1
,teller.log_vault_fives_v2
,teller.log_vault_halves_v1
,teller.log_vault_halves_v2
,teller.log_vault_hundreds_v1
,teller.log_vault_hundreds_v2
,teller.log_vault_misc
,teller.log_vault_mutil
,teller.log_vault_nickels_v1
,teller.log_vault_nickels_v2
,teller.log_vault_ones_v1
,teller.log_vault_ones_v2
,teller.log_vault_over_short
,teller.log_vault_pennies_v1
,teller.log_vault_pennies_v2
,teller.log_vault_quarters_v1
,teller.log_vault_quarters_v2
,teller.log_vault_tens_v1
,teller.log_vault_tens_v2
,teller.log_vault_tot_cash
,teller.log_vault_twenties_v1
,teller.log_vault_twenties_v2
,teller.log_vault_twos_v1
,teller.log_vault_twos_v2
,teller.log_gl_dbcr_title
,teller.log_gl_dbcr_comment
,teller.log_gl_dbcr_desc
,teller.log_atm_cash_env_seq_no
,teller.log_tda_acct_type
,teller.log_tda_flag1
,teller.log_tda_flag2
,teller.log_tda_flag3
,teller.log_tda_flag4
,teller.log_tda_flag5
,teller.log_tda_tel_trancode
,teller.log_tda_reject_code
,teller.log_tda_co_id
,teller.log_tda_reject_reason
,teller.log_tda_short_name
,teller.log_tda_rtn_post_dte
,teller.log_tda_rtn_time
,teller.log_tda_rtn_trans_seq
,teller.log_tda_rtn_cost_center
,teller.log_atmcnt_bill_cntrs
,teller.log_atmcnt_atmid
,teller.log_atmcnt_receipt_cash
,teller.log_atmcnt_settlement
,teller.log_atmcnt_divert_cash
,teller.log_atmdep_cash_cancel
,teller.log_atmdep_cash_atmid
,teller.log_atmdep_cash_dep_amt
,teller.log_atmdep_hundreds
,teller.log_atmdep_fifties
,teller.log_atmdep_twenties
,teller.log_atmdep_tens
,teller.log_atmdep_fives
,teller.log_atmdep_twos
,teller.log_atmdep_ones
,teller.log_cd_rea_gl_cr_title
,teller.log_cd_rea_gl_cr_comment
,teller.log_cd_rea_gl_dr_title
,teller.log_cd_rea_gl_dr_comment
,teller.log_cd_rea_gl_dr_desc
,teller.log_buysell_type
,teller.log_buysell_to_from_id
,teller.log_bs_hundreds
,teller.log_bs_fifties
,teller.log_bs_twenties
,teller.log_bs_tens
,teller.log_bs_fives
,teller.log_bs_twos
,teller.log_bs_ones
,teller.log_bs_dollarsc
,teller.log_bs_halves
,teller.log_bs_quarters
,teller.log_bs_dimes
,teller.log_bs_nickels
,teller.log_bs_pennies
,teller.log_rescan_process_date
,teller.log_rescan_teller_id
,teller.log_rescan_orig_item_seq
,teller.log_rescan_aba
,teller.log_rescan_account_nbr
,teller.log_rescan_check_nbr
,teller.log_rescan_amount
,teller.log_bond_red_id_type
,teller.log_bond_red_ssn_txid
,teller.log_bond_red_interest
,teller.log_bond_red_amount
,teller.log_bond_red_number
,teller.log_int_rep1099_tin_type
,teller.log_int_rep1099_name
,teller.log_int_rep1099_addr1
,teller.log_int_rep1099_addr2
,teller.log_int_rep1099_city
,teller.log_int_rep1099_state
,teller.log_int_rep1099_zip
,teller.log_int_rep1099_tin
,teller.log_int_rep1099
,teller.log_int_rep1099_nbr_bonds
,teller.log_soso_process_date
,teller.log_soso_next_pro_date
,teller.log_soso_cashin
,teller.log_soso_cashout
,teller.log_soso_begcash
,teller.log_soso_endcash
,teller.log_online_offline_ind
,teller.log_bal_inq_short_name
,teller.log_bal_inq_subprod_code
,teller.log_bal_inq_curr_bal
,teller.log_bal_inq_avail_bal
,teller.log_bal_inq_ledgr_bal
,teller.log_dep_acct_type
,teller.log_dep_last_deposit
,teller.log_dep_disp_bag
,teller.log_dep_drcr_ind
,teller.log_dep_out_of_bal
,teller.log_dep_deposit_amt
,teller.log_dep_cash_back_amt
,teller.log_dep_num_checks
,teller.log_trans_fr_appl
,teller.log_trans_fr_affil
,teller.log_trans_fr_acct
,teller.log_trans_fr_appl_new
,teller.log_trans_fr_affil_new
,teller.log_trans_fr_acct_new
,teller.log_trans_fr_subprod
,teller.log_trans_fr_subown
,teller.log_trans_fr_branch
,teller.log_trans_fr_cust_name
,teller.log_trans_to_appl
,teller.log_trans_to_affil
,teller.log_trans_to_acct
,teller.log_trans_to_appl_new
,teller.log_trans_to_affil_new
,teller.log_trans_to_acct_new
,teller.log_trans_to_subprod
,teller.log_trans_to_subown
,teller.log_trans_to_branch
,teller.log_trans_to_cust_name
,teller.log_ccf_exchg_type
,teller.log_ccf_acct_type
,teller.log_ccf_tot_curr_in
,teller.log_ccf_hundreds_in
,teller.log_ccf_fifties_in
,teller.log_ccf_twenties_in
,teller.log_ccf_tens_in
,teller.log_ccf_fives_in
,teller.log_ccf_twos_in
,teller.log_ccf_ones_in
,teller.log_ccf_dollarsc_in
,teller.log_ccf_halves_in
,teller.log_ccf_quarters_in
,teller.log_ccf_dimes_in
,teller.log_ccf_nickels_in
,teller.log_ccf_pennies_in
,teller.log_ccf_tot_curr_out
,teller.log_ccf_hundreds_out
,teller.log_ccf_fifties_out
,teller.log_ccf_twenties_out
,teller.log_ccf_tens_out
,teller.log_ccf_fives_out
,teller.log_ccf_twos_out
,teller.log_ccf_ones_out
,teller.log_ccf_dollarsc_out
,teller.log_ccf_halves_out
,teller.log_ccf_quarters_out
,teller.log_ccf_dimes_out
,teller.log_ccf_nickels_out
,teller.log_ccf_pennies_out
,teller.log_ccs_envelopes
,teller.log_ccs_straps
,teller.log_ccs_wrappers
,teller.log_ccs_bag_seals
,teller.log_ccs_disp_night_bags
,teller.log_ms_descript
,teller.log_ms_official_checks
,teller.log_ms_other_services
,teller.log_qa_gl_match_dep
,teller.log_qa_gl_match_date
,teller.log_ohio_lottery_confirm
,teller.log_ohio_lottery_payout
,teller.log_wd_chk_charge_type
,teller.log_wd_waive_code
,teller.log_wd_check_type
,teller.log_wd_drcr_ind
,teller.log_wd_business_id
,teller.log_wd_pnc_acct
,teller.log_wd_cif_type
,teller.log_wd_mkt_of_acct
,teller.log_wd_brch_of_acct
,teller.log_wd_subowner
,teller.log_wd_transit
,teller.log_wd_chk_charge_acct
,teller.log_pb_init_book_bal
,teller.log_pb_lumped
,teller.log_pb_item_print
,teller.log_bond_redt_id_type
,teller.log_bond_redt_ssn_txid
,teller.log_bond_redt_type
,teller.log_bond_redt_interest
,teller.log_bond_redt_number
,teller.log_tcd_buysell_device
,teller.log_tcd_buysell_cbfr
,teller.log_tcd_buysell_cbto
,teller.log_tcd_buysell_cntl1
,teller.log_tcd_buysell_cntl2
,teller.log_tcd_hundreds
,teller.log_tcd_fifties
,teller.log_tcd_twenties
,teller.log_tcd_tens
,teller.log_tcd_fives
,teller.log_tcd_twos
,teller.log_tcd_ones
,teller.log_tcd_dollarsc
,teller.log_tcd_halves
,teller.log_tcd_quarters
,teller.log_tcd_dimes
,teller.log_tcd_nickels
,teller.log_tcd_pennies
,teller.log_tcr_buysell_device
,teller.log_tcr_buysell_cbfr
,teller.log_tcr_buysell_cbto
,teller.log_tcr_buysell_cntl1
,teller.log_tcr_buysell_cntl2
,teller.log_tcr_hundreds_inv
,teller.log_tcr_fifties_inv
,teller.log_tcr_twenties_inv
,teller.log_tcr_tens_inv
,teller.log_tcr_fives_inv
,teller.log_tcr_ones_inv
,teller.log_tcr_hundreds_disp
,teller.log_tcr_fifties_disp
,teller.log_tcr_twenties_disp
,teller.log_tcr_tens_disp
,teller.log_tcr_fives_disp
,teller.log_tcr_ones_disp
,teller.log_tcr_hundreds_div
,teller.log_tcr_fifties_div
,teller.log_tcr_twenties_div
,teller.log_tcr_tens_div
,teller.log_tcr_fives_div
,teller.log_tcr_ones_div
,teller.log_sdb_cash_back_amt
,teller.log_acls_autopay_setup
,teller.log_acls_autopay_replace
,teller.log_acls_choice_acct
,teller.log_acls_highest_check_aba
,teller.log_acls_add_principal
,teller.log_acls_highest_check_amt
,teller.log_acls_cash_back_amt
,teller.log_cca_fdr_acct_type
,teller.log_cca_cash_back_amt
,teller.log_ckc_cash_back_amt
,teller.log_abs_regular_payment
,teller.log_abs_add_principal
,teller.log_abs_cash_back
,teller.log_mtg_payment_type
,teller.log_mtg_principal_escrow
,teller.log_mtg_cash_back
,teller.log_cmtg_cash_back
,teller.log_util_payment_coupon_no
,teller.log_tax_payment_coupon_no
,teller.log_off_chk_rec_acct
,teller.log_off_chk_fee_type_1
,teller.log_off_chk_fee_type_2
,teller.log_off_chk_fee_type_3
,teller.log_off_chk_fee_type_4
,teller.log_off_chk_fee_type_5
,teller.log_off_chk_fee_type_6
,teller.log_off_chk_fee_type_7
,teller.log_off_chk_fee_type_8
,teller.log_off_chk_fee_type_9
,teller.log_off_chk_fee_type_10
,teller.log_off_chk_fee_type_11
,teller.log_off_chk_fee_type_12
,teller.log_off_chk_fee_type_13
,teller.log_off_chk_fee_type_14
,teller.log_off_chk_fee_type_15
,teller.log_off_chk_serial_no_1
,teller.log_off_chk_serial_no_2
,teller.log_off_chk_serial_no_3
,teller.log_off_chk_serial_no_4
,teller.log_off_chk_serial_no_5
,teller.log_off_chk_serial_no_6
,teller.log_off_chk_serial_no_7
,teller.log_off_chk_serial_no_8
,teller.log_off_chk_serial_no_9
,teller.log_off_chk_serial_no_10
,teller.log_off_chk_serial_no_11
,teller.log_off_chk_serial_no_12
,teller.log_off_chk_serial_no_13
,teller.log_off_chk_serial_no_14
,teller.log_off_chk_serial_no_15
,teller.log_off_chk_acct_type_1
,teller.log_off_chk_acct_type_2
,teller.log_off_chk_acct_type_3
,teller.log_off_chk_acct_type_4
,teller.log_off_chk_acct_type_5
,teller.log_off_chk_acct_type_6
,teller.log_off_chk_acct_type_7
,teller.log_off_chk_acct_type_8
,teller.log_off_chk_acct_type_9
,teller.log_off_chk_acct_type_10
,teller.log_off_chk_acct_type_11
,teller.log_off_chk_acct_type_12
,teller.log_off_chk_acct_type_13
,teller.log_off_chk_acct_type_14
,teller.log_off_chk_acct_type_15
,teller.log_off_chk_face_amt_1
,teller.log_off_chk_face_amt_2
,teller.log_off_chk_face_amt_3
,teller.log_off_chk_face_amt_4
,teller.log_off_chk_face_amt_5
,teller.log_off_chk_face_amt_6
,teller.log_off_chk_face_amt_7
,teller.log_off_chk_face_amt_8
,teller.log_off_chk_face_amt_9
,teller.log_off_chk_face_amt_10
,teller.log_off_chk_face_amt_11
,teller.log_off_chk_face_amt_12
,teller.log_off_chk_face_amt_13
,teller.log_off_chk_face_amt_14
,teller.log_off_chk_face_amt_15
,teller.log_off_chk_fee_amt_1
,teller.log_off_chk_fee_amt_2
,teller.log_off_chk_fee_amt_3
,teller.log_off_chk_fee_amt_4
,teller.log_off_chk_fee_amt_5
,teller.log_off_chk_fee_amt_6
,teller.log_off_chk_fee_amt_7
,teller.log_off_chk_fee_amt_8
,teller.log_off_chk_fee_amt_9
,teller.log_off_chk_fee_amt_10
,teller.log_off_chk_fee_amt_11
,teller.log_off_chk_fee_amt_12
,teller.log_off_chk_fee_amt_13
,teller.log_off_chk_fee_amt_14
,teller.log_off_chk_fee_amt_15
,teller.log_visa_recourse_acct
,teller.log_visa_gift_fee_type_1
,teller.log_visa_gift_fee_type_2
,teller.log_visa_gift_fee_type_3
,teller.log_visa_gift_fee_type_4
,teller.log_visa_gift_fee_type_5
,teller.log_visa_gift_fee_type_6
,teller.log_visa_gift_fee_type_7
,teller.log_visa_gift_fee_type_8
,teller.log_visa_gift_fee_type_9
,teller.log_visa_gift_fee_type_10
,teller.log_visa_gift_fee_type_11
,teller.log_visa_gift_fee_type_12
,teller.log_visa_gift_fee_type_13
,teller.log_visa_gift_fee_type_14
,teller.log_visa_gift_fee_type_15
,teller.log_visa_gift_purchaser
,teller.log_visa_gift_card_no_1
,teller.log_visa_gift_card_no_2
,teller.log_visa_gift_card_no_3
,teller.log_visa_gift_card_no_4
,teller.log_visa_gift_card_no_5
,teller.log_visa_gift_card_no_6
,teller.log_visa_gift_card_no_7
,teller.log_visa_gift_card_no_8
,teller.log_visa_gift_card_no_9
,teller.log_visa_gift_card_no_10
,teller.log_visa_gift_card_no_11
,teller.log_visa_gift_card_no_12
,teller.log_visa_gift_card_no_13
,teller.log_visa_gift_card_no_14
,teller.log_visa_gift_card_no_15
,teller.log_visa_gift_expire_date_1
,teller.log_visa_gift_expire_date_2
,teller.log_visa_gift_expire_date_3
,teller.log_visa_gift_expire_date_4
,teller.log_visa_gift_expire_date_5
,teller.log_visa_gift_expire_date_6
,teller.log_visa_gift_expire_date_7
,teller.log_visa_gift_expire_date_8
,teller.log_visa_gift_expire_date_9
,teller.log_visa_gift_expire_date_10
,teller.log_visa_gift_expire_date_11
,teller.log_visa_gift_expire_date_12
,teller.log_visa_gift_expire_date_13
,teller.log_visa_gift_expire_date_14
,teller.log_visa_gift_expire_date_15
,teller.log_visa_gift_card_amt_1
,teller.log_visa_gift_card_amt_2
,teller.log_visa_gift_card_amt_3
,teller.log_visa_gift_card_amt_4
,teller.log_visa_gift_card_amt_5
,teller.log_visa_gift_card_amt_6
,teller.log_visa_gift_card_amt_7
,teller.log_visa_gift_card_amt_8
,teller.log_visa_gift_card_amt_9
,teller.log_visa_gift_card_amt_10
,teller.log_visa_gift_card_amt_11
,teller.log_visa_gift_card_amt_12
,teller.log_visa_gift_card_amt_13
,teller.log_visa_gift_card_amt_14
,teller.log_visa_gift_card_amt_15
,teller.log_visa_gift_card_fee_1
,teller.log_visa_gift_card_fee_2
,teller.log_visa_gift_card_fee_3
,teller.log_visa_gift_card_fee_4
,teller.log_visa_gift_card_fee_5
,teller.log_visa_gift_card_fee_6
,teller.log_visa_gift_card_fee_7
,teller.log_visa_gift_card_fee_8
,teller.log_visa_gift_card_fee_9
,teller.log_visa_gift_card_fee_10
,teller.log_visa_gift_card_fee_11
,teller.log_visa_gift_card_fee_12
,teller.log_visa_gift_card_fee_13
,teller.log_visa_gift_card_fee_14
,teller.log_visa_gift_card_fee_15
,teller.log_money_order_rec_acct
,teller.log_money_order_fee_type_1
,teller.log_money_order_fee_type_2
,teller.log_money_order_fee_type_3
,teller.log_money_order_fee_type_4
,teller.log_money_order_fee_type_5
,teller.log_money_order_fee_type_6
,teller.log_money_order_fee_type_7
,teller.log_money_order_fee_type_8
,teller.log_money_order_fee_type_9
,teller.log_money_order_fee_type_10
,teller.log_money_order_fee_type_11
,teller.log_money_order_fee_type_12
,teller.log_money_order_fee_type_13
,teller.log_money_order_fee_type_14
,teller.log_money_order_fee_type_15
,teller.log_money_order_serial_no_1
,teller.log_money_order_serial_no_2
,teller.log_money_order_serial_no_3
,teller.log_money_order_serial_no_4
,teller.log_money_order_serial_no_5
,teller.log_money_order_serial_no_6
,teller.log_money_order_serial_no_7
,teller.log_money_order_serial_no_8
,teller.log_money_order_serial_no_9
,teller.log_money_order_serial_no_10
,teller.log_money_order_serial_no_11
,teller.log_money_order_serial_no_12
,teller.log_money_order_serial_no_13
,teller.log_money_order_serial_no_14
,teller.log_money_order_serial_no_15
,teller.log_money_order_face_amt_1
,teller.log_money_order_face_amt_2
,teller.log_money_order_face_amt_3
,teller.log_money_order_face_amt_4
,teller.log_money_order_face_amt_5
,teller.log_money_order_face_amt_6
,teller.log_money_order_face_amt_7
,teller.log_money_order_face_amt_8
,teller.log_money_order_face_amt_9
,teller.log_money_order_face_amt_10
,teller.log_money_order_face_amt_11
,teller.log_money_order_face_amt_12
,teller.log_money_order_face_amt_13
,teller.log_money_order_face_amt_14
,teller.log_money_order_face_amt_15
,teller.log_money_order_fee_amt_1
,teller.log_money_order_fee_amt_2
,teller.log_money_order_fee_amt_3
,teller.log_money_order_fee_amt_4
,teller.log_money_order_fee_amt_5
,teller.log_money_order_fee_amt_6
,teller.log_money_order_fee_amt_7
,teller.log_money_order_fee_amt_8
,teller.log_money_order_fee_amt_9
,teller.log_money_order_fee_amt_10
,teller.log_money_order_fee_amt_11
,teller.log_money_order_fee_amt_12
,teller.log_money_order_fee_amt_13
,teller.log_money_order_fee_amt_14
,teller.log_money_order_fee_amt_15
,teller.log_trav_chk_rec_acct
,teller.log_trav_chk_fee_type_1
,teller.log_trav_chk_fee_type_2
,teller.log_trav_chk_fee_type_3
,teller.log_trav_chk_fee_type_4
,teller.log_trav_chk_fee_type_5
,teller.log_trav_chk_fee_type_6
,teller.log_trav_chk_fee_type_7
,teller.log_trav_chk_fee_type_8
,teller.log_trav_chk_fee_type_9
,teller.log_trav_chk_fee_type_10
,teller.log_trav_chk_fee_type_11
,teller.log_trav_chk_fee_type_12
,teller.log_trav_chk_fee_type_13
,teller.log_trav_chk_fee_type_14
,teller.log_trav_chk_fee_type_15
,teller.log_trav_chk_serial_no_1
,teller.log_trav_chk_serial_no_2
,teller.log_trav_chk_serial_no_3
,teller.log_trav_chk_serial_no_4
,teller.log_trav_chk_serial_no_5
,teller.log_trav_chk_serial_no_6
,teller.log_trav_chk_serial_no_7
,teller.log_trav_chk_serial_no_8
,teller.log_trav_chk_serial_no_9
,teller.log_trav_chk_serial_no_10
,teller.log_trav_chk_serial_no_11
,teller.log_trav_chk_serial_no_12
,teller.log_trav_chk_serial_no_13
,teller.log_trav_chk_serial_no_14
,teller.log_trav_chk_serial_no_15
,teller.log_trav_chk_denom_1
,teller.log_trav_chk_denom_2
,teller.log_trav_chk_denom_3
,teller.log_trav_chk_denom_4
,teller.log_trav_chk_denom_5
,teller.log_trav_chk_denom_6
,teller.log_trav_chk_denom_7
,teller.log_trav_chk_denom_8
,teller.log_trav_chk_denom_9
,teller.log_trav_chk_denom_10
,teller.log_trav_chk_denom_11
,teller.log_trav_chk_denom_12
,teller.log_trav_chk_denom_13
,teller.log_trav_chk_denom_14
,teller.log_trav_chk_denom_15
,teller.log_trav_chk_serial_prefix_1
,teller.log_trav_chk_serial_prefix_2
,teller.log_trav_chk_serial_prefix_3
,teller.log_trav_chk_serial_prefix_4
,teller.log_trav_chk_serial_prefix_5
,teller.log_trav_chk_serial_prefix_6
,teller.log_trav_chk_serial_prefix_7
,teller.log_trav_chk_serial_prefix_8
,teller.log_trav_chk_serial_prefix_9
,teller.log_trav_chk_serial_prefix_10
,teller.log_trav_chk_serial_prefix_11
,teller.log_trav_chk_serial_prefix_12
,teller.log_trav_chk_serial_prefix_13
,teller.log_trav_chk_serial_prefix_14
,teller.log_trav_chk_serial_prefix_15
,teller.log_trav_chk_fee_amt_1
,teller.log_trav_chk_fee_amt_2
,teller.log_trav_chk_fee_amt_3
,teller.log_trav_chk_fee_amt_4
,teller.log_trav_chk_fee_amt_5
,teller.log_trav_chk_fee_amt_6
,teller.log_trav_chk_fee_amt_7
,teller.log_trav_chk_fee_amt_8
,teller.log_trav_chk_fee_amt_9
,teller.log_trav_chk_fee_amt_10
,teller.log_trav_chk_fee_amt_11
,teller.log_trav_chk_fee_amt_12
,teller.log_trav_chk_fee_amt_13
,teller.log_trav_chk_fee_amt_14
,teller.log_trav_chk_fee_amt_15
,teller.log_visamc_foreign_flag
,teller.log_visamc_card_type
,teller.log_visamc_adv_auth_code
,teller.log_visamc_adv_exp_date
,teller.log_visamc_adv_four_digit
,teller.log_visamc_adv_addr1
,teller.log_visamc_adv_addr2
,teller.log_visamc_adv_city
,teller.log_visamc_adv_state
,teller.log_visamc_adv_zip
,teller.log_visamc_adv_name
,teller.log_visamc_adv_country
,teller.log_credit_line_cc_type
,teller.log_credit_line_cc_prod_cd
,teller.log_prompt_inq_applid
,teller.log_prompt_inq_on_off_sw
,teller.log_prompt_upd_cif_disp
,teller.log_prompt_upd_cif_verify
,teller.log_prompt_upd_prompt_disp
,teller.log_prompt_upd_cif_key
,teller.log_prompt_upd_opt_id
,teller.log_prompt_upd_seq_nbr
,teller.log_ctr_tc1
,teller.log_ctr_tc2
,teller.log_ctr_tc3
,teller.log_ctr_tc4
,teller.log_ctr_tc5
,teller.log_ctr_tc6
,teller.log_ctr_cond_fl_1
,teller.log_ctr_cond_fl_2
,teller.log_ctr_cond_fl_3
,teller.log_ctr_cond_fl_4
,teller.log_ctr_bene_tin
,teller.log_ctr_cond_tin
,teller.log_ctr_lcr_trans_no
,teller.log_ctr_lcr_record_no
,teller.log_ctr_trans_flag
,teller.log_ctr_trans_type
,teller.log_ctr_tran_date
,teller.log_ctr_tc_desc_in
,teller.log_ctr_tc_desc_out
,teller.log_ctr_trans_count
,teller.log_mil_for_addr_fl
,teller.log_mil_no_expdt_fl
,teller.log_mil_name
,teller.log_mil_addr1
,teller.log_mil_addr2
,teller.log_mil_city
,teller.log_mil_state
,teller.log_mil_zip
,teller.log_mil_tin
,teller.log_mil_country
,teller.log_mil_birth_date
,teller.log_mil_cash_use_reason
,teller.log_mil_apprvl_comments
,teller.log_unscan_orig_item_seq
,teller.log_unscan_serial_nbr
,teller.log_unscan_aba
,teller.log_unscan_pc_field
,teller.log_unscan_account_nbr
,teller.log_unscan_amount
,mem.mem_session
,teller.hdfs_load_ts
,teller.z_load_date
from ${hv_db_etl}.TELLER_TXN_DLY teller
left outer join 
	${hv_db_efgifi_wrk}.memgen mem
	ON teller.rec_num = mem.rec_num
where teller.z_load_date =${lastPreviousDayBusinessday_yyyy_mm_dd};