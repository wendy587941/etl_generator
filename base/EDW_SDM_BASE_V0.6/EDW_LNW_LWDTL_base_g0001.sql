-- SQL for table: EDW_LNW_LWDTL
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'CKBAL' AS DATA_CHK_COL,
CKBAL AS DATA_VAL,
SDG.UFN_NUMCHECK(CKBAL) AS CHECK_RESULT
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
'MGSHARE' AS DATA_CHK_COL,
MGSHARE AS DATA_VAL,
SDG.UFN_NUMCHECK(MGSHARE) AS CHECK_RESULT
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
'ACPAMT' AS DATA_CHK_COL,
ACPAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(ACPAMT) AS CHECK_RESULT
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

-- SQL for table: EDW_LNW_LWDTL
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
CBNO,
CURCD,
CKBAL,
CKSDAY,
CKEDAY,
MGSHARE,
APDATE,
ADCBK,
ACPAMT,
ACPCD,
LASTDT,
HOLD,
LGNO,
IPSQNO,
CRTHLD,
BRHLD,
OSQNO,
RGDAY,
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
CBNO,
CURCD,
CKBAL,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CKSDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CKSDAY))='0' THEN NULL ELSE CKSDAY END) AS CKSDAY,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CKEDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CKEDAY))='0' THEN NULL ELSE CKEDAY END) AS CKEDAY,
MGSHARE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(APDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(APDATE))='0' THEN NULL ELSE APDATE END) AS APDATE,
ADCBK,
ACPAMT,
ACPCD,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LASTDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LASTDT))='0' THEN NULL ELSE LASTDT END) AS LASTDT,
HOLD,
LGNO,
IPSQNO,
CRTHLD,
BRHLD,
OSQNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(RGDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(RGDAY))='0' THEN NULL ELSE RGDAY END) AS RGDAY,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NTXDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NTXDAY))='0' THEN NULL ELSE NTXDAY END) AS NTXDAY,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
