-- SQL for table: EDW_RM_RTPAY
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'RCNO' AS DATA_CHK_COL,
RCNO AS DATA_VAL,
SDG.UFN_NUMCHECK(RCNO) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'TRMSEQ' AS DATA_CHK_COL,
TRMSEQ AS DATA_VAL,
SDG.UFN_NUMCHECK(TRMSEQ) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'TXTNO' AS DATA_CHK_COL,
TXTNO AS DATA_VAL,
SDG.UFN_NUMCHECK(TXTNO) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'REMAMT' AS DATA_CHK_COL,
REMAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(REMAMT) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'FEE' AS DATA_CHK_COL,
FEE AS DATA_VAL,
SDG.UFN_NUMCHECK(FEE) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BSHAMT' AS DATA_CHK_COL,
BSHAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(BSHAMT) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BOTHER_1' AS DATA_CHK_COL,
BOTHER_1 AS DATA_VAL,
SDG.UFN_NUMCHECK(BOTHER_1) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BRPAMT_1' AS DATA_CHK_COL,
BRPAMT_1 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRPAMT_1) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BOTHER_2' AS DATA_CHK_COL,
BOTHER_2 AS DATA_VAL,
SDG.UFN_NUMCHECK(BOTHER_2) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BRPAMT_2' AS DATA_CHK_COL,
BRPAMT_2 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRPAMT_2) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BOTHER_3' AS DATA_CHK_COL,
BOTHER_3 AS DATA_VAL,
SDG.UFN_NUMCHECK(BOTHER_3) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BRPAMT_3' AS DATA_CHK_COL,
BRPAMT_3 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRPAMT_3) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'SFEE' AS DATA_CHK_COL,
SFEE AS DATA_VAL,
SDG.UFN_NUMCHECK(SFEE) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'CKEAMT' AS DATA_CHK_COL,
CKEAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(CKEAMT) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0';

/* need to modify above 記得要有;號*/
DELETE FROM SDG.SDG_DATA_ERR_TBL WHERE TAB_NM='${TARGET_TABLE}' AND GROUP_CD='${EXEC_GCD}' AND BD_EXCH_DATE=TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd') AND BD_EXCH_TIME='${BD_EXCH_TIME}';

INSERT INTO SDG.SDG_DATA_ERR_TBL
SELECT
'${TARGET_TABLE}' AS TAB_NM,
'${EXEC_GCD}' AS GROUP_CD,
TO_DATE('${BD_EXCH_DATE}', 'YYYY-MM-DD') AS BD_EXCH_DATE,
'${BD_EXCH_TIME}' AS BD_EXCH_TIME,
C1.DATA_CHK_TP,
C1.DATA_CHK_COL,
C1.DATA_VAL,
S1.SRC_PK,
CURRENT_TIMESTAMP AS UPDATE_DATETIME
FROM
(
SELECT * FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK
) C1
LEFT JOIN 
(
SELECT CAST(ROWID AS VARCHAR2(100)) AS CHK_ID , ${SQL_STMT} AS SRC_PK FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
) S1
ON C1.CHK_ID=S1.CHK_ID
;

-- SQL for table: EDW_RM_RTPAY
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
TXDAY,
KINBR,
RCNO,
TRMSEQ,
TXTNO,
REMAMT,
FEE,
BSHAMT,
STATUS,
BRPFG,
BTYPE_1,
BOTHER_1,
BNOTE_1,
BRPAMT_1,
BACTNO_1,
BPBCHN_1,
BTYPE_2,
BOTHER_2,
BNOTE_2,
BRPAMT_2,
BACTNO_2,
BPBCHN_2,
BTYPE_3,
BOTHER_3,
BNOTE_3,
BRPAMT_3,
BACTNO_3,
BPBCHN_3,
TLRNO,
REMTYP,
SUBTYP,
RECACT,
TSTKEY,
"TIME",
LTLRNO,
SUPNO,
SFEE,
FEETYP,
CKEAMT,
CIFKEY,
REMNM,
RECNM,
AGENTID,
AGENTNM,
AMLONOFF,
REFID,
REMRESULT,
RECRESULT,
AGTRESULT,
FORM,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDAY))='0' THEN NULL ELSE TXDAY END) AS TXDAY,
KINBR,
RCNO,
TRMSEQ,
TXTNO,
REMAMT,
FEE,
BSHAMT,
STATUS,
BRPFG,
BTYPE_1,
BOTHER_1,
BNOTE_1,
BRPAMT_1,
BACTNO_1,
BPBCHN_1,
BTYPE_2,
BOTHER_2,
BNOTE_2,
BRPAMT_2,
BACTNO_2,
BPBCHN_2,
BTYPE_3,
BOTHER_3,
BNOTE_3,
BRPAMT_3,
BACTNO_3,
BPBCHN_3,
TLRNO,
REMTYP,
SUBTYP,
RECACT,
TSTKEY,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM("TIME"), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM("TIME"))='0' THEN NULL ELSE "TIME" END) AS "TIME",
LTLRNO,
SUPNO,
SFEE,
FEETYP,
CKEAMT,
CIFKEY,
REMNM,
RECNM,
AGENTID,
AGENTNM,
AMLONOFF,
REFID,
REMRESULT,
RECRESULT,
AGTRESULT,
FORM,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
