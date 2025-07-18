-- SQL for table: EDW_LNL_LLCLMT
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'CURAMT' AS DATA_CHK_COL,
CURAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(CURAMT) AS CHECK_RESULT
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
'CURBAL' AS DATA_CHK_COL,
CURBAL AS DATA_VAL,
SDG.UFN_NUMCHECK(CURBAL) AS CHECK_RESULT
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
'MPAMTP' AS DATA_CHK_COL,
MPAMTP AS DATA_VAL,
SDG.UFN_NUMCHECK(MPAMTP) AS CHECK_RESULT
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
'XRATEC' AS DATA_CHK_COL,
XRATEC AS DATA_VAL,
SDG.UFN_NUMCHECK(XRATEC) AS CHECK_RESULT
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
'ULNLMT' AS DATA_CHK_COL,
ULNLMT AS DATA_VAL,
SDG.UFN_NUMCHECK(ULNLMT) AS CHECK_RESULT
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
'ULNBAL' AS DATA_CHK_COL,
ULNBAL AS DATA_VAL,
SDG.UFN_NUMCHECK(ULNBAL) AS CHECK_RESULT
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
'TENOR' AS DATA_CHK_COL,
TENOR AS DATA_VAL,
SDG.UFN_NUMCHECK(TENOR) AS CHECK_RESULT
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
'FXRATEC' AS DATA_CHK_COL,
FXRATEC AS DATA_VAL,
SDG.UFN_NUMCHECK(FXRATEC) AS CHECK_RESULT
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
'RSKAMT' AS DATA_CHK_COL,
RSKAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(RSKAMT) AS CHECK_RESULT
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
'RSKBAL' AS DATA_CHK_COL,
RSKBAL AS DATA_VAL,
SDG.UFN_NUMCHECK(RSKBAL) AS CHECK_RESULT
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
'NDYLMT' AS DATA_CHK_COL,
NDYLMT AS DATA_VAL,
SDG.UFN_NUMCHECK(NDYLMT) AS CHECK_RESULT
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

-- SQL for table: EDW_LNL_LLCLMT
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
ACTNO,
BRNO,
APNO,
SRNO,
CHKDG,
SQNO,
CURAMT,
CURBAL,
LCNO,
LCCUR,
AMT,
ISUDT,
EDAY,
LCTYP,
MPAMTP,
LASTDT,
ACTCLS,
XRATEC,
ULNLMT,
ULNBAL,
TENOR,
BKCUR,
BKSRNO,
FXRATEC,
RSKAMT,
RSKBAL,
NDYLMT,
NTXDAY,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BRNO||APNO||SRNO||CHKDG,
BRNO,
APNO,
SRNO,
CHKDG,
SQNO,
CURAMT,
CURBAL,
LCNO,
LCCUR,
AMT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(ISUDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(ISUDT))='0' THEN NULL ELSE ISUDT END) AS ISUDT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(EDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(EDAY))='0' THEN NULL ELSE EDAY END) AS EDAY,
LCTYP,
MPAMTP,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LASTDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LASTDT))='0' THEN NULL ELSE LASTDT END) AS LASTDT,
ACTCLS,
XRATEC,
ULNLMT,
ULNBAL,
TENOR,
BKCUR,
BKSRNO,
FXRATEC,
RSKAMT,
RSKBAL,
NDYLMT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NTXDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NTXDAY))='0' THEN NULL ELSE NTXDAY END) AS NTXDAY,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
