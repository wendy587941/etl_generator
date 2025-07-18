-- SQL for table: EDW_FXF_FXCRT
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'BCURCD' AS DATA_CHK_COL,
BCURCD AS DATA_VAL,
SDG.UFN_NUMCHECK(BCURCD) AS CHECK_RESULT
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
'CURCD' AS DATA_CHK_COL,
CURCD AS DATA_VAL,
SDG.UFN_NUMCHECK(CURCD) AS CHECK_RESULT
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
'MRKTTM' AS DATA_CHK_COL,
MRKTTM AS DATA_VAL,
SDG.UFN_NUMCHECK(MRKTTM) AS CHECK_RESULT
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
'MRKTCD' AS DATA_CHK_COL,
MRKTCD AS DATA_VAL,
SDG.UFN_NUMCHECK(MRKTCD) AS CHECK_RESULT
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
'RCNT' AS DATA_CHK_COL,
RCNT AS DATA_VAL,
SDG.UFN_NUMCHECK(RCNT) AS CHECK_RESULT
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
'BRATS' AS DATA_CHK_COL,
BRATS AS DATA_VAL,
SDG.UFN_NUMCHECK(BRATS) AS CHECK_RESULT
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
'SRATS' AS DATA_CHK_COL,
SRATS AS DATA_VAL,
SDG.UFN_NUMCHECK(SRATS) AS CHECK_RESULT
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
'BCASH' AS DATA_CHK_COL,
BCASH AS DATA_VAL,
SDG.UFN_NUMCHECK(BCASH) AS CHECK_RESULT
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
'SCASH' AS DATA_CHK_COL,
SCASH AS DATA_VAL,
SDG.UFN_NUMCHECK(SCASH) AS CHECK_RESULT
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
'BRAT1' AS DATA_CHK_COL,
BRAT1 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT1) AS CHECK_RESULT
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
'BRAT3' AS DATA_CHK_COL,
BRAT3 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT3) AS CHECK_RESULT
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
'BRAT6' AS DATA_CHK_COL,
BRAT6 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT6) AS CHECK_RESULT
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
'BRAT9' AS DATA_CHK_COL,
BRAT9 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT9) AS CHECK_RESULT
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
'BRAT12' AS DATA_CHK_COL,
BRAT12 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT12) AS CHECK_RESULT
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
'BRAT15' AS DATA_CHK_COL,
BRAT15 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT15) AS CHECK_RESULT
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
'BRAT18' AS DATA_CHK_COL,
BRAT18 AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT18) AS CHECK_RESULT
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
'BRAT1Y' AS DATA_CHK_COL,
BRAT1Y AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT1Y) AS CHECK_RESULT
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
'BRAT2Y' AS DATA_CHK_COL,
BRAT2Y AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT2Y) AS CHECK_RESULT
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
'BRAT3Y' AS DATA_CHK_COL,
BRAT3Y AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT3Y) AS CHECK_RESULT
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
'BRAT4Y' AS DATA_CHK_COL,
BRAT4Y AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT4Y) AS CHECK_RESULT
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
'BRAT5Y' AS DATA_CHK_COL,
BRAT5Y AS DATA_VAL,
SDG.UFN_NUMCHECK(BRAT5Y) AS CHECK_RESULT
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
'SRAT1' AS DATA_CHK_COL,
SRAT1 AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT1) AS CHECK_RESULT
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
'SRAT3' AS DATA_CHK_COL,
SRAT3 AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT3) AS CHECK_RESULT
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
'SRAT6' AS DATA_CHK_COL,
SRAT6 AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT6) AS CHECK_RESULT
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
'SRAT9' AS DATA_CHK_COL,
SRAT9 AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT9) AS CHECK_RESULT
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
'SRAT12' AS DATA_CHK_COL,
SRAT12 AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT12) AS CHECK_RESULT
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
'SRAT15' AS DATA_CHK_COL,
SRAT15 AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT15) AS CHECK_RESULT
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
'SRAT18' AS DATA_CHK_COL,
SRAT18 AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT18) AS CHECK_RESULT
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
'SRAT1Y' AS DATA_CHK_COL,
SRAT1Y AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT1Y) AS CHECK_RESULT
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
'SRAT2Y' AS DATA_CHK_COL,
SRAT2Y AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT2Y) AS CHECK_RESULT
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
'SRAT3Y' AS DATA_CHK_COL,
SRAT3Y AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT3Y) AS CHECK_RESULT
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
'SRAT4Y' AS DATA_CHK_COL,
SRAT4Y AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT4Y) AS CHECK_RESULT
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
'SRAT5Y' AS DATA_CHK_COL,
SRAT5Y AS DATA_VAL,
SDG.UFN_NUMCHECK(SRAT5Y) AS CHECK_RESULT
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
'USCRAT' AS DATA_CHK_COL,
USCRAT AS DATA_VAL,
SDG.UFN_NUMCHECK(USCRAT) AS CHECK_RESULT
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
'FXRAT' AS DATA_CHK_COL,
FXRAT AS DATA_VAL,
SDG.UFN_NUMCHECK(FXRAT) AS CHECK_RESULT
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
'ACRAT' AS DATA_CHK_COL,
ACRAT AS DATA_VAL,
SDG.UFN_NUMCHECK(ACRAT) AS CHECK_RESULT
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
'LMTB' AS DATA_CHK_COL,
LMTB AS DATA_VAL,
SDG.UFN_NUMCHECK(LMTB) AS CHECK_RESULT
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
'LMTS' AS DATA_CHK_COL,
LMTS AS DATA_VAL,
SDG.UFN_NUMCHECK(LMTS) AS CHECK_RESULT
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
'KINBR' AS DATA_CHK_COL,
KINBR AS DATA_VAL,
SDG.UFN_NUMCHECK(KINBR) AS CHECK_RESULT
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

-- SQL for table: EDW_FXF_FXCRT
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
BCURCD,
CURCD,
MRKTDT,
MRKTTM,
MRKTCD,
RCNT,
BRATS,
SRATS,
BCASH,
SCASH,
BRAT1,
BRAT3,
BRAT6,
BRAT9,
BRAT12,
BRAT15,
BRAT18,
BRAT1Y,
BRAT2Y,
BRAT3Y,
BRAT4Y,
BRAT5Y,
SRAT1,
SRAT3,
SRAT6,
SRAT9,
SRAT12,
SRAT15,
SRAT18,
SRAT1Y,
SRAT2Y,
SRAT3Y,
SRAT4Y,
SRAT5Y,
USCRAT,
FXRAT,
ACRAT,
LMTB,
LMTS,
KINBR,
TLRNO,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BCURCD,
CURCD,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MRKTDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MRKTDT))='0' THEN NULL ELSE MRKTDT END) AS MRKTDT,
MRKTTM,
MRKTCD,
RCNT,
BRATS,
SRATS,
BCASH,
SCASH,
BRAT1,
BRAT3,
BRAT6,
BRAT9,
BRAT12,
BRAT15,
BRAT18,
BRAT1Y,
BRAT2Y,
BRAT3Y,
BRAT4Y,
BRAT5Y,
SRAT1,
SRAT3,
SRAT6,
SRAT9,
SRAT12,
SRAT15,
SRAT18,
SRAT1Y,
SRAT2Y,
SRAT3Y,
SRAT4Y,
SRAT5Y,
USCRAT,
FXRAT,
ACRAT,
LMTB,
LMTS,
KINBR,
TLRNO,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
