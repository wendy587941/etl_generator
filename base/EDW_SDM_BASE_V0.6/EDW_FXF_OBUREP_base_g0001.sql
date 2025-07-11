-- SQL for table: EDW_FXF_OBUREP
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
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
'AMT' AS DATA_CHK_COL,
AMT AS DATA_VAL,
SDG.UFN_NUMCHECK(AMT) AS CHECK_RESULT
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
'USDAMT' AS DATA_CHK_COL,
USDAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(USDAMT) AS CHECK_RESULT
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
'CORPNO' AS DATA_CHK_COL,
CORPNO AS DATA_VAL,
SDG.UFN_NUMCHECK(CORPNO) AS CHECK_RESULT
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
'INST' AS DATA_CHK_COL,
INST AS DATA_VAL,
SDG.UFN_NUMCHECK(INST) AS CHECK_RESULT
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
'INST2' AS DATA_CHK_COL,
INST2 AS DATA_VAL,
SDG.UFN_NUMCHECK(INST2) AS CHECK_RESULT
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
'DPGAIN' AS DATA_CHK_COL,
DPGAIN AS DATA_VAL,
SDG.UFN_NUMCHECK(DPGAIN) AS CHECK_RESULT
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
'CTGAIN' AS DATA_CHK_COL,
CTGAIN AS DATA_VAL,
SDG.UFN_NUMCHECK(CTGAIN) AS CHECK_RESULT
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
'EXGL' AS DATA_CHK_COL,
EXGL AS DATA_VAL,
SDG.UFN_NUMCHECK(EXGL) AS CHECK_RESULT
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
'OTGL' AS DATA_CHK_COL,
OTGL AS DATA_VAL,
SDG.UFN_NUMCHECK(OTGL) AS CHECK_RESULT
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
'INSTDY' AS DATA_CHK_COL,
INSTDY AS DATA_VAL,
SDG.UFN_NUMCHECK(INSTDY) AS CHECK_RESULT
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
'IRT' AS DATA_CHK_COL,
IRT AS DATA_VAL,
SDG.UFN_NUMCHECK(IRT) AS CHECK_RESULT
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
'INSTDY2' AS DATA_CHK_COL,
INSTDY2 AS DATA_VAL,
SDG.UFN_NUMCHECK(INSTDY2) AS CHECK_RESULT
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
'IRT2' AS DATA_CHK_COL,
IRT2 AS DATA_VAL,
SDG.UFN_NUMCHECK(IRT2) AS CHECK_RESULT
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

-- SQL for table: EDW_FXF_OBUREP
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
WSNO,
TXTNO,
TXCD,
APTYPE,
CURCD,
AMT,
USDAMT,
BKCODE,
BKSWFCD,
CUTYPE,
CLASS,
CIFKEY,
CORPNO,
REFNO,
DOCNO,
CN,
DIRECT,
INST,
INST2,
FEE,
DPGAIN,
CTGAIN,
EXGL,
OTGL,
INSTDY,
IRT,
INSTDY2,
IRT2,
RBRNO,
ACBRNO,
OBUFG,
SCORE,
ERRCD,
CNTRY,
MBRNO,
MDATE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDAY))='0' THEN NULL ELSE TXDAY END) AS TXDAY,
KINBR,
WSNO,
TXTNO,
TXCD,
APTYPE,
CURCD,
AMT,
USDAMT,
BKCODE,
BKSWFCD,
CUTYPE,
CLASS,
CIFKEY,
CORPNO,
REFNO,
DOCNO,
CN,
DIRECT,
INST,
INST2,
FEE,
DPGAIN,
CTGAIN,
EXGL,
OTGL,
INSTDY,
IRT,
INSTDY2,
IRT2,
RBRNO,
ACBRNO,
OBUFG,
SCORE,
ERRCD,
CNTRY,
MBRNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MDATE))='0' THEN NULL ELSE MDATE END) AS MDATE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
