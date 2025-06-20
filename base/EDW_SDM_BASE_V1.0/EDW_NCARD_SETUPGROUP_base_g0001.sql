-- SQL for table: EDW_NCARD_SETUPGROUP
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'INT_YEAR_BASE' AS DATA_CHK_COL,
INT_YEAR_BASE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_YEAR_BASE) AS CHECK_RESULT
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
'INT_1_RATE' AS DATA_CHK_COL,
INT_1_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_1_RATE) AS CHECK_RESULT
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
'INT_2_RATE' AS DATA_CHK_COL,
INT_2_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_2_RATE) AS CHECK_RESULT
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
'INT_3_RATE' AS DATA_CHK_COL,
INT_3_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_3_RATE) AS CHECK_RESULT
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
'INT_4_RATE' AS DATA_CHK_COL,
INT_4_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_4_RATE) AS CHECK_RESULT
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
'INT_5_RATE' AS DATA_CHK_COL,
INT_5_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_5_RATE) AS CHECK_RESULT
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
'INT_1_MAX_RATE' AS DATA_CHK_COL,
INT_1_MAX_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_1_MAX_RATE) AS CHECK_RESULT
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
'INT_2_MAX_RATE' AS DATA_CHK_COL,
INT_2_MAX_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_2_MAX_RATE) AS CHECK_RESULT
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
'INT_3_MAX_RATE' AS DATA_CHK_COL,
INT_3_MAX_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_3_MAX_RATE) AS CHECK_RESULT
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
'INT_4_MAX_RATE' AS DATA_CHK_COL,
INT_4_MAX_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_4_MAX_RATE) AS CHECK_RESULT
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
'INT_5_MAX_RATE' AS DATA_CHK_COL,
INT_5_MAX_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_5_MAX_RATE) AS CHECK_RESULT
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
'NEXT_INT_1_RATE' AS DATA_CHK_COL,
NEXT_INT_1_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(NEXT_INT_1_RATE) AS CHECK_RESULT
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
'NEXT_INT_2_RATE' AS DATA_CHK_COL,
NEXT_INT_2_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(NEXT_INT_2_RATE) AS CHECK_RESULT
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
'NEXT_INT_3_RATE' AS DATA_CHK_COL,
NEXT_INT_3_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(NEXT_INT_3_RATE) AS CHECK_RESULT
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
'NEXT_INT_4_RATE' AS DATA_CHK_COL,
NEXT_INT_4_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(NEXT_INT_4_RATE) AS CHECK_RESULT
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
'NEXT_INT_5_RATE' AS DATA_CHK_COL,
NEXT_INT_5_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(NEXT_INT_5_RATE) AS CHECK_RESULT
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
'LC_FIXED_AMT' AS DATA_CHK_COL,
LC_FIXED_AMT AS DATA_VAL,
SDG.UFN_NUMCHECK(LC_FIXED_AMT) AS CHECK_RESULT
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
'LC_FIXED_PCT' AS DATA_CHK_COL,
LC_FIXED_PCT AS DATA_VAL,
SDG.UFN_NUMCHECK(LC_FIXED_PCT) AS CHECK_RESULT
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
'LC_CHG_MIN_CHG' AS DATA_CHK_COL,
LC_CHG_MIN_CHG AS DATA_VAL,
SDG.UFN_NUMCHECK(LC_CHG_MIN_CHG) AS CHECK_RESULT
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
'LC_CHG_MAX_CHG' AS DATA_CHK_COL,
LC_CHG_MAX_CHG AS DATA_VAL,
SDG.UFN_NUMCHECK(LC_CHG_MAX_CHG) AS CHECK_RESULT
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
'LC_CHG_MAX_CNT' AS DATA_CHK_COL,
LC_CHG_MAX_CNT AS DATA_VAL,
SDG.UFN_NUMCHECK(LC_CHG_MAX_CNT) AS CHECK_RESULT
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
'LC_CHG_MAX_CNT_YTD' AS DATA_CHK_COL,
LC_CHG_MAX_CNT_YTD AS DATA_VAL,
SDG.UFN_NUMCHECK(LC_CHG_MAX_CNT_YTD) AS CHECK_RESULT
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
'MEMBER_FEE_WAIVE_TIMES' AS DATA_CHK_COL,
MEMBER_FEE_WAIVE_TIMES AS DATA_VAL,
SDG.UFN_NUMCHECK(MEMBER_FEE_WAIVE_TIMES) AS CHECK_RESULT
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
'MEMBER_FEE_WAIVE_AMT' AS DATA_CHK_COL,
MEMBER_FEE_WAIVE_AMT AS DATA_VAL,
SDG.UFN_NUMCHECK(MEMBER_FEE_WAIVE_AMT) AS CHECK_RESULT
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
'MEMBER_FEE_AMT' AS DATA_CHK_COL,
MEMBER_FEE_AMT AS DATA_VAL,
SDG.UFN_NUMCHECK(MEMBER_FEE_AMT) AS CHECK_RESULT
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
'MIN_PAY_PRIN_1_CTD' AS DATA_CHK_COL,
MIN_PAY_PRIN_1_CTD AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_1_CTD) AS CHECK_RESULT
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
'MIN_PAY_PRIN_2_CTD' AS DATA_CHK_COL,
MIN_PAY_PRIN_2_CTD AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_2_CTD) AS CHECK_RESULT
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
'MIN_PAY_PRIN_3_CTD' AS DATA_CHK_COL,
MIN_PAY_PRIN_3_CTD AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_3_CTD) AS CHECK_RESULT
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
'MIN_PAY_PRIN_4_CTD' AS DATA_CHK_COL,
MIN_PAY_PRIN_4_CTD AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_4_CTD) AS CHECK_RESULT
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
'MIN_PAY_PRIN_5_CTD' AS DATA_CHK_COL,
MIN_PAY_PRIN_5_CTD AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_5_CTD) AS CHECK_RESULT
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
'MIN_PAY_PRIN_1_BILL' AS DATA_CHK_COL,
MIN_PAY_PRIN_1_BILL AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_1_BILL) AS CHECK_RESULT
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
'MIN_PAY_PRIN_2_BILL' AS DATA_CHK_COL,
MIN_PAY_PRIN_2_BILL AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_2_BILL) AS CHECK_RESULT
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
'MIN_PAY_PRIN_3_BILL' AS DATA_CHK_COL,
MIN_PAY_PRIN_3_BILL AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_3_BILL) AS CHECK_RESULT
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
'MIN_PAY_PRIN_4_BILL' AS DATA_CHK_COL,
MIN_PAY_PRIN_4_BILL AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_4_BILL) AS CHECK_RESULT
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
'MIN_PAY_PRIN_5_BILL' AS DATA_CHK_COL,
MIN_PAY_PRIN_5_BILL AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_5_BILL) AS CHECK_RESULT
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
'MIN_PAY_PRIN_1_LLS' AS DATA_CHK_COL,
MIN_PAY_PRIN_1_LLS AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_1_LLS) AS CHECK_RESULT
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
'MIN_PAY_PRIN_2_LLS' AS DATA_CHK_COL,
MIN_PAY_PRIN_2_LLS AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_2_LLS) AS CHECK_RESULT
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
'MIN_PAY_PRIN_3_LLS' AS DATA_CHK_COL,
MIN_PAY_PRIN_3_LLS AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_3_LLS) AS CHECK_RESULT
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
'MIN_PAY_PRIN_4_LLS' AS DATA_CHK_COL,
MIN_PAY_PRIN_4_LLS AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_4_LLS) AS CHECK_RESULT
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
'MIN_PAY_PRIN_5_LLS' AS DATA_CHK_COL,
MIN_PAY_PRIN_5_LLS AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_PRIN_5_LLS) AS CHECK_RESULT
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
'MIN_PAY_ROUND' AS DATA_CHK_COL,
MIN_PAY_ROUND AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_ROUND) AS CHECK_RESULT
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
'MIN_PAY_AMT' AS DATA_CHK_COL,
MIN_PAY_AMT AS DATA_VAL,
SDG.UFN_NUMCHECK(MIN_PAY_AMT) AS CHECK_RESULT
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

-- SQL for table: EDW_NCARD_SETUPGROUP
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
BU,
PRODUCT,
ACCT_GROUP,
DESCR,
DESCR_SHORT,
INT_1_START,
INT_2_START,
INT_3_START,
INT_4_START,
INT_5_START,
INT_ACTION,
INT_YEAR_BASE,
INT_1_RATE,
INT_2_RATE,
INT_3_RATE,
INT_4_RATE,
INT_5_RATE,
INT_1_MAX_RATE,
INT_2_MAX_RATE,
INT_3_MAX_RATE,
INT_4_MAX_RATE,
INT_5_MAX_RATE,
NEXT_INT_1_RATE,
NEXT_INT_2_RATE,
NEXT_INT_3_RATE,
NEXT_INT_4_RATE,
NEXT_INT_5_RATE,
NEXT_INT_1_EFF_DTE,
NEXT_INT_2_EFF_DTE,
NEXT_INT_3_EFF_DTE,
NEXT_INT_4_EFF_DTE,
NEXT_INT_5_EFF_DTE,
LC_ACTION,
LC_FIXED_AMT,
LC_FIXED_PCT,
LC_CHG_MIN_DELQ,
LC_CHG_MAX_DELQ,
LC_CHG_MIN_CHG,
LC_CHG_MAX_CHG,
LC_CHG_MAX_CNT,
LC_CHG_MAX_CNT_YTD,
MEMBER_FEE_ACTION,
MEMBER_FEE_WAIVE,
MEMBER_FEE_WAIVE_TIMES,
MEMBER_FEE_WAIVE_AMT,
MEMBER_FEE_AMT,
MIN_PAY_PRIN_1_CTD,
MIN_PAY_PRIN_2_CTD,
MIN_PAY_PRIN_3_CTD,
MIN_PAY_PRIN_4_CTD,
MIN_PAY_PRIN_5_CTD,
MIN_PAY_PRIN_1_BILL,
MIN_PAY_PRIN_2_BILL,
MIN_PAY_PRIN_3_BILL,
MIN_PAY_PRIN_4_BILL,
MIN_PAY_PRIN_5_BILL,
MIN_PAY_PRIN_1_LLS,
MIN_PAY_PRIN_2_LLS,
MIN_PAY_PRIN_3_LLS,
MIN_PAY_PRIN_4_LLS,
MIN_PAY_PRIN_5_LLS,
MIN_PAY_LLS_DTE,
MIN_PAY_ROUND,
MIN_PAY_AMT,
OVER_PMT_ACTION,
ACCU_MIN_ACTION,
POST_RESULT,
MNT_DT,
MNT_USER,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BU,
PRODUCT,
ACCT_GROUP,
DESCR,
DESCR_SHORT,
INT_1_START,
INT_2_START,
INT_3_START,
INT_4_START,
INT_5_START,
INT_ACTION,
INT_YEAR_BASE,
INT_1_RATE,
INT_2_RATE,
INT_3_RATE,
INT_4_RATE,
INT_5_RATE,
INT_1_MAX_RATE,
INT_2_MAX_RATE,
INT_3_MAX_RATE,
INT_4_MAX_RATE,
INT_5_MAX_RATE,
NEXT_INT_1_RATE,
NEXT_INT_2_RATE,
NEXT_INT_3_RATE,
NEXT_INT_4_RATE,
NEXT_INT_5_RATE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NEXT_INT_1_EFF_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NEXT_INT_1_EFF_DTE))='0' THEN NULL ELSE NEXT_INT_1_EFF_DTE END) AS NEXT_INT_1_EFF_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NEXT_INT_2_EFF_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NEXT_INT_2_EFF_DTE))='0' THEN NULL ELSE NEXT_INT_2_EFF_DTE END) AS NEXT_INT_2_EFF_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NEXT_INT_3_EFF_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NEXT_INT_3_EFF_DTE))='0' THEN NULL ELSE NEXT_INT_3_EFF_DTE END) AS NEXT_INT_3_EFF_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NEXT_INT_4_EFF_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NEXT_INT_4_EFF_DTE))='0' THEN NULL ELSE NEXT_INT_4_EFF_DTE END) AS NEXT_INT_4_EFF_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NEXT_INT_5_EFF_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NEXT_INT_5_EFF_DTE))='0' THEN NULL ELSE NEXT_INT_5_EFF_DTE END) AS NEXT_INT_5_EFF_DTE,
LC_ACTION,
LC_FIXED_AMT,
LC_FIXED_PCT,
LC_CHG_MIN_DELQ,
LC_CHG_MAX_DELQ,
LC_CHG_MIN_CHG,
LC_CHG_MAX_CHG,
LC_CHG_MAX_CNT,
LC_CHG_MAX_CNT_YTD,
MEMBER_FEE_ACTION,
MEMBER_FEE_WAIVE,
MEMBER_FEE_WAIVE_TIMES,
MEMBER_FEE_WAIVE_AMT,
MEMBER_FEE_AMT,
MIN_PAY_PRIN_1_CTD,
MIN_PAY_PRIN_2_CTD,
MIN_PAY_PRIN_3_CTD,
MIN_PAY_PRIN_4_CTD,
MIN_PAY_PRIN_5_CTD,
MIN_PAY_PRIN_1_BILL,
MIN_PAY_PRIN_2_BILL,
MIN_PAY_PRIN_3_BILL,
MIN_PAY_PRIN_4_BILL,
MIN_PAY_PRIN_5_BILL,
MIN_PAY_PRIN_1_LLS,
MIN_PAY_PRIN_2_LLS,
MIN_PAY_PRIN_3_LLS,
MIN_PAY_PRIN_4_LLS,
MIN_PAY_PRIN_5_LLS,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MIN_PAY_LLS_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MIN_PAY_LLS_DTE))='0' THEN NULL ELSE MIN_PAY_LLS_DTE END) AS MIN_PAY_LLS_DTE,
MIN_PAY_ROUND,
MIN_PAY_AMT,
OVER_PMT_ACTION,
ACCU_MIN_ACTION,
POST_RESULT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MNT_DT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MNT_DT))='0' THEN NULL ELSE MNT_DT END) AS MNT_DT,
MNT_USER,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
