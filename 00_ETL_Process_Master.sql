alter SESSION set NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss';

select max(acty_crtn_dt) from hepo.stg_pgm_activity;

-- 1. SET THE DATE VALUES FOR THE DATA PROCESSING
SELECT * FROM HEPO.DIM_DP_DT;

/*  
  -- AGG report data processing should be done twice for end of month --
*/

---- QUARTER --
--UPDATE HEPO.DIM_DP_DT
--set bgn_rng_dt = to_date('20170101','yyyymmdd'),
--end_rng_dt = to_date('20170328','yyyymmdd');
--COMMIT;

--JAN
UPDATE HEPO.DIM_DP_DT
set bgn_rng_dt = to_date('20170101','yyyymmdd'),
end_rng_dt = to_date('20170131','yyyymmdd');
COMMIT;

--FEB
UPDATE HEPO.DIM_DP_DT
set bgn_rng_dt = to_date('20170201','yyyymmdd'),
end_rng_dt = to_date('20170228','yyyymmdd');
COMMIT;

--MAR
UPDATE HEPO.DIM_DP_DT
set bgn_rng_dt = to_date('20170301','yyyymmdd'),
--end_rng_dt = to_date('20170328','yyyymmdd');
end_rng_dt = to_date('20170331','yyyymmdd');
COMMIT;

--APR
UPDATE HEPO.DIM_DP_DT
set bgn_rng_dt = to_date('20170401','yyyymmdd'),
end_rng_dt = to_date('20170404','yyyymmdd');
COMMIT;

--UPDATE HEPO.DIM_DP_DT
--set bgn_rng_dt = to_date('20170401','yyyymmdd'),
--end_rng_dt = to_date('20170415','yyyymmdd');

--commit;
rollback ;

SELECT * FROM HEPO.DIM_DP_DT;

/* *****************************************************************************  */
-- 01
delete from hepo.vld_rpt_mth_agg;
 COMMIT;
 ROLLBACK;
 
--D1 : INSERT 
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\01_D1_Activity_To_vld_rpt_mth_agg_v3.SQL';
--D2D3 : INSERT 
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\01_D2D3_Activity_To_vld_rpt_mth_agg_v3.SQL';
--P1P2 : INSERT 
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\01_P1P2_Activity_To_vld_rpt_mth_agg_v3.SQL';

 COMMIT;
 ROLLBACK;

--MERGE : INSERT UPDATE
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\01_Merge_vld_rpt_mth_agg_To_rpt_mth_agg_v1.SQL';
 COMMIT;
 ROLLBACK;

--check for duplicates
--select RGN_CD_TX,grp_tx,sbgrp_tx,CLNDR_YR_MO,count(*) from hepo.rpt_mth_agg 
--group by RGN_CD_TX,grp_tx,sbgrp_tx,CLNDR_YR_MO having count(*)>1;


/* *****************************************************************************  */
-- 02 STD IND CUME
--D1 : DELETE + INSERT CUME
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\02_D1_Activity_To_rpt_ind_cume_v3.SQL';
--D2D3 : DELETE + INSERT CUME
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\02_D2D3_Activity_To_rpt_ind_cume_v3.SQL';
--P1P2 : DELETE + INSERT CUME
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\02_P1P2_Activity_To_rpt_ind_cume_v3.SQL';

 COMMIT;
 ROLLBACK;


/* *****************************************************************************  */
-- 03  STD IND CHG

--- MOVE DATA to HIST table
---- STEP 1: Move data from CHG to CHG_HIST table before the process start[TRUNC+LOAD]
--INSERT INTO HEPO.RPT_IND_CHG_HIST 
--SELECT * FROM HEPO.RPT_IND_CHG WHERE pgm_id_tx IN (SELECT PGM_ID_TX FROM HEPO.RPT_PARAM_REF WHERE rpt_cume_cd='CHG' 
--                        AND rpt_typ_cd IN ('D2_IND1','D3_IND2','D3_IND1','D2_IND2','D2_IND3','D3_IND3')  );
--
--COMMIT;

--DELETE FROM HEPO.RPT_IND_CHG WHERE pgm_id_tx IN (SELECT PGM_ID_TX FROM HEPO.RPT_PARAM_REF WHERE rpt_cume_cd='CHG' 
--                        AND rpt_typ_cd IN ('D2_IND1','D3_IND2','D3_IND1','D2_IND2','D2_IND3','D3_IND3')  );
--
-- commit;

-- STD IND CHG : INSERT INTO HIST + DELETE FROM CHG + INSERT NEW INTO RPT_IND_CHG
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\03_D2D3_Activity_To_rpt_ind_chg_v3.SQL';

COMMIT;
ROLLBACK;


/* *****************************************************************************  */
-- 04 CUSTOM AGG
DELETE FROM HEPO.VLD_RPT_MTHCSTM_AGG;

 COMMIT;
 ROLLBACK;
 
--D1 : INSERT 
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\04_D1_Activity_To_vld_rpt_mthcstm_agg_v3.SQL';
--D2D3 : INSERT 
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\04_D2D3_Activity_To_vld_rpt_mthcstm_agg_v3.SQL';
--P1P2 : INSERT 
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\04_P1P2_Activity_To_vld_rpt_mthcstm_agg_v3.SQL';

 COMMIT;
 ROLLBACK;

--MERGE : INSERT UPDATE
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\04_Merge_vld_rpt_mthcstm_agg_To_rpt_mthcstm_agg_v1.SQL';
 COMMIT;
 ROLLBACK;


--AGG VIEW : DELETE INSERT
DELETE FROM hepo.tmp_aggc_pre_v;

COMMIT;
ROLLBACK;

@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\04_Load_To_tmp_aggc_pre_v_v3.1.SQL';
 COMMIT;
 ROLLBACK;

/* *****************************************************************************  */
-- 05 CUSTOM IND CUME
--D1 : DELETE + INSERT CUME
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\05_D1_Activity_To_rpt_indcstm_cume_v3.SQL';
--D2D3 : DELETE + INSERT CUME
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\05_D2D3_Activity_To_rpt_indcstm_cume_v3.SQL';
--P1P2 : DELETE + INSERT CUME
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\05_P1P2_Activity_To_rpt_indcstm_cume_v3.SQL';

 COMMIT;
 ROLLBACK;


/* *****************************************************************************  */
-- 06 CUSTOM IND CHG
-- WEX[GENERAL] : INSERT INTO HIST + DELETE FROM CHG + INSERT NEW INTO CHG
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\06_WEX_Activity_To_rpt_indcstm_chg_v3.SQL';
--WEX[FEDS] : INSERT INTO HIST + DELETE FROM CHG + INSERT NEW INTO CHG
@'V:\HEPO ADMIN\HEPO Transformation\Development\Code - ETL files - V3\06_FEDS_Activity_To_rpt_indcstm_chg_v3.SQL';

 COMMIT;
 ROLLBACK;






