-- SQL for table: EDW_LNL_LCIF
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'MLINVAMT' AS DATA_CHK_COL,
MLINVAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(MLINVAMT) AS CHECK_RESULT
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
'MLINCOME' AS DATA_CHK_COL,
MLINCOME AS DATA_VAL,
SDG.UFN_NUMCHECK(MLINCOME) AS CHECK_RESULT
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
'INCOME' AS DATA_CHK_COL,
INCOME AS DATA_VAL,
SDG.UFN_NUMCHECK(INCOME) AS CHECK_RESULT
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
'STOT' AS DATA_CHK_COL,
STOT AS DATA_VAL,
SDG.UFN_NUMCHECK(STOT) AS CHECK_RESULT
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
'SHBTOT' AS DATA_CHK_COL,
SHBTOT AS DATA_VAL,
SDG.UFN_NUMCHECK(SHBTOT) AS CHECK_RESULT
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
'TRPIXPR' AS DATA_CHK_COL,
TRPIXPR AS DATA_VAL,
SDG.UFN_NUMCHECK(TRPIXPR) AS CHECK_RESULT
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
'FITIRT' AS DATA_CHK_COL,
FITIRT AS DATA_VAL,
SDG.UFN_NUMCHECK(FITIRT) AS CHECK_RESULT
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
'IRID' AS DATA_CHK_COL,
IRID AS DATA_VAL,
SDG.UFN_NUMCHECK(IRID) AS CHECK_RESULT
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
'IXPR' AS DATA_CHK_COL,
IXPR AS DATA_VAL,
SDG.UFN_NUMCHECK(IXPR) AS CHECK_RESULT
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
'IXID' AS DATA_CHK_COL,
IXID AS DATA_VAL,
SDG.UFN_NUMCHECK(IXID) AS CHECK_RESULT
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

-- SQL for table: EDW_LNL_LCIF
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
ENTPNO,
FILLER,
PIDERR,
ACTCLS,
GRADE,
GRDT,
CTLCD,
CTLDT,
TRACEDT,
RELDT,
MLINVAMT,
MLINCOME,
MLBRNO,
MLDT,
ACCHK,
INCOME,
STOT,
SHBTOT,
SERVICECMP,
TRPFLG,
TRPRGDT,
TRPCLDT,
TRPIXPR,
TRPACT,
IRCDDT,
IRCD,
FITIRT,
IRID,
IXPR,
IXID,
EXTEND,
LIFLAG,
LISDAY,
LIEDAY,
CLLSBL,
APPDAY,
APPRSN,
CTLNAP,
BRNO,
CSDATE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
ENTPNO,
FILLER,
PIDERR,
ACTCLS,
GRADE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(GRDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(GRDT))='0' THEN NULL ELSE GRDT END) AS GRDT,
CTLCD,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CTLDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CTLDT))='0' THEN NULL ELSE CTLDT END) AS CTLDT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TRACEDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TRACEDT))='0' THEN NULL ELSE TRACEDT END) AS TRACEDT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(RELDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(RELDT))='0' THEN NULL ELSE RELDT END) AS RELDT,
MLINVAMT,
MLINCOME,
MLBRNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MLDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MLDT))='0' THEN NULL ELSE MLDT END) AS MLDT,
ACCHK,
INCOME,
STOT,
SHBTOT,
SERVICECMP,
TRPFLG,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TRPRGDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TRPRGDT))='0' THEN NULL ELSE TRPRGDT END) AS TRPRGDT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TRPCLDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TRPCLDT))='0' THEN NULL ELSE TRPCLDT END) AS TRPCLDT,
TRPIXPR,
TRPACT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(IRCDDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(IRCDDT))='0' THEN NULL ELSE IRCDDT END) AS IRCDDT,
IRCD,
FITIRT,
IRID,
IXPR,
IXID,
EXTEND,
LIFLAG,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LISDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LISDAY))='0' THEN NULL ELSE LISDAY END) AS LISDAY,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LIEDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LIEDAY))='0' THEN NULL ELSE LIEDAY END) AS LIEDAY,
CLLSBL,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(APPDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(APPDAY))='0' THEN NULL ELSE APPDAY END) AS APPDAY,
APPRSN,
CTLNAP,
BRNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CSDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CSDATE))='0' THEN NULL ELSE CSDATE END) AS CSDATE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
