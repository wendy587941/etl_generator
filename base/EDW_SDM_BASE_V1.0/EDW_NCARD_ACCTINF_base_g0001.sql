-- SQL for table: EDW_NCARD_ACCTINF
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'DD1_ASSIGN_AMT' AS DATA_CHK_COL,
DD1_ASSIGN_AMT AS DATA_VAL,
SDG.UFN_NUMCHECK(DD1_ASSIGN_AMT) AS CHECK_RESULT
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
'DD1_ASSIGN_PCT' AS DATA_CHK_COL,
DD1_ASSIGN_PCT AS DATA_VAL,
SDG.UFN_NUMCHECK(DD1_ASSIGN_PCT) AS CHECK_RESULT
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
'DD2_ASSIGN_AMT' AS DATA_CHK_COL,
DD2_ASSIGN_AMT AS DATA_VAL,
SDG.UFN_NUMCHECK(DD2_ASSIGN_AMT) AS CHECK_RESULT
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
'INT_RATE_VARIANCE' AS DATA_CHK_COL,
INT_RATE_VARIANCE AS DATA_VAL,
SDG.UFN_NUMCHECK(INT_RATE_VARIANCE) AS CHECK_RESULT
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
'MAX_DUE' AS DATA_CHK_COL,
MAX_DUE AS DATA_VAL,
SDG.UFN_NUMCHECK(MAX_DUE) AS CHECK_RESULT
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
'MAX_REVOLVING' AS DATA_CHK_COL,
MAX_REVOLVING AS DATA_VAL,
SDG.UFN_NUMCHECK(MAX_REVOLVING) AS CHECK_RESULT
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
'CREDIT_SCORE' AS DATA_CHK_COL,
CREDIT_SCORE AS DATA_VAL,
SDG.UFN_NUMCHECK(CREDIT_SCORE) AS CHECK_RESULT
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
'USER_AMT_1' AS DATA_CHK_COL,
USER_AMT_1 AS DATA_VAL,
SDG.UFN_NUMCHECK(USER_AMT_1) AS CHECK_RESULT
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
'USER_AMT_2' AS DATA_CHK_COL,
USER_AMT_2 AS DATA_VAL,
SDG.UFN_NUMCHECK(USER_AMT_2) AS CHECK_RESULT
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
'USER_AMT_3' AS DATA_CHK_COL,
USER_AMT_3 AS DATA_VAL,
SDG.UFN_NUMCHECK(USER_AMT_3) AS CHECK_RESULT
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
'USER_AMT_4' AS DATA_CHK_COL,
USER_AMT_4 AS DATA_VAL,
SDG.UFN_NUMCHECK(USER_AMT_4) AS CHECK_RESULT
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
'USER_AMT_5' AS DATA_CHK_COL,
USER_AMT_5 AS DATA_VAL,
SDG.UFN_NUMCHECK(USER_AMT_5) AS CHECK_RESULT
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

-- SQL for table: EDW_NCARD_ACCTINF
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
BU,
ACCT_NBR,
PRODUCT,
CURRENCY,
ACCT_STATUS,
CUST_BU,
CUST_NBR,
CUST_ALT_FLAG,
CUST_ALT_DTE,
CUST_ALT_BU,
CUST_ALT_NBR,
CYCLE,
CYCLE_NEXT,
CYCLE_NEXT_DTE,
STMT_FLAG,
STMT_ACTION,
ACCT_GROUP,
OPEN_DTE,
COLL_DTE,
CHGOFF_DTE,
DORMANT_DTE,
CTL_CODE,
CTL_CODE_DTE,
CTL_CODE_REASON,
INT_WAIVE_DTE,
LC_WAIVE_DTE,
CASH_FEE_WAIVE_DTE,
MEMBER_FEE_WAIVE_DTE,
SERVICE_FEE_WAIVE_DTE,
USER_FEE_1_WAIVE_DTE,
USER_FEE_2_WAIVE_DTE,
USER_FEE_3_WAIVE_DTE,
USER_FEE_4_WAIVE_DTE,
USER_FEE_5_WAIVE_DTE,
USER_FEE_6_WAIVE_DTE,
DD1_BANK,
DD1_BANK_NBR,
DD1_START_DTE,
DD1_END_DTE,
DD1_METHOD,
DD1_ASSIGN_AMT,
DD1_ASSIGN_PCT,
DD_OWNER,
DD2_BANK_NBR,
DD2_START_DTE,
DD2_END_DTE,
ACCT_GROUP_REASON,
DD2_ASSIGN_AMT,
INT_RATE_VARIANCE,
DC_BANK,
DC_BANK_NBR,
DC_START_DTE,
DC_END_DTE,
MAX_DUE,
MAX_REVOLVING,
CLASS,
TEMP_CLASS,
TEMP_CLASS_START_DTE,
TEMP_CLASS_END_DTE,
CREDIT_SCORE,
ACCT_GROUP_NEXT,
ACCT_GROUP_NEXT_DTE,
COLM_START_DTE,
COLM_END_DTE,
COLM_END,
COLM_ACTION,
STOP_DELQ,
KK4_CLOSE_DTE,
FORCE_STOP_DTE,
USER_CHAR_1,
USER_CHAR_2,
USER_CHAR_3,
USER_CHAR_4,
USER_CHAR_5,
USER_DTE_1,
USER_DTE_2,
USER_DTE_3,
USER_DTE_4,
USER_DTE_5,
USER_AMT_1,
USER_AMT_2,
USER_AMT_3,
USER_AMT_4,
USER_AMT_5,
MNT_DT,
MNT_USER,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BU,
ACCT_NBR,
PRODUCT,
CURRENCY,
ACCT_STATUS,
CUST_BU,
CUST_NBR,
CUST_ALT_FLAG,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CUST_ALT_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CUST_ALT_DTE))='0' THEN NULL ELSE CUST_ALT_DTE END) AS CUST_ALT_DTE,
CUST_ALT_BU,
CUST_ALT_NBR,
CYCLE,
CYCLE_NEXT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CYCLE_NEXT_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CYCLE_NEXT_DTE))='0' THEN NULL ELSE CYCLE_NEXT_DTE END) AS CYCLE_NEXT_DTE,
STMT_FLAG,
STMT_ACTION,
ACCT_GROUP,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(OPEN_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(OPEN_DTE))='0' THEN NULL ELSE OPEN_DTE END) AS OPEN_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(COLL_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(COLL_DTE))='0' THEN NULL ELSE COLL_DTE END) AS COLL_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CHGOFF_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CHGOFF_DTE))='0' THEN NULL ELSE CHGOFF_DTE END) AS CHGOFF_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DORMANT_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DORMANT_DTE))='0' THEN NULL ELSE DORMANT_DTE END) AS DORMANT_DTE,
CTL_CODE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CTL_CODE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CTL_CODE_DTE))='0' THEN NULL ELSE CTL_CODE_DTE END) AS CTL_CODE_DTE,
CTL_CODE_REASON,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(INT_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(INT_WAIVE_DTE))='0' THEN NULL ELSE INT_WAIVE_DTE END) AS INT_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LC_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LC_WAIVE_DTE))='0' THEN NULL ELSE LC_WAIVE_DTE END) AS LC_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CASH_FEE_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CASH_FEE_WAIVE_DTE))='0' THEN NULL ELSE CASH_FEE_WAIVE_DTE END) AS CASH_FEE_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MEMBER_FEE_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MEMBER_FEE_WAIVE_DTE))='0' THEN NULL ELSE MEMBER_FEE_WAIVE_DTE END) AS MEMBER_FEE_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(SERVICE_FEE_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(SERVICE_FEE_WAIVE_DTE))='0' THEN NULL ELSE SERVICE_FEE_WAIVE_DTE END) AS SERVICE_FEE_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_FEE_1_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_FEE_1_WAIVE_DTE))='0' THEN NULL ELSE USER_FEE_1_WAIVE_DTE END) AS USER_FEE_1_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_FEE_2_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_FEE_2_WAIVE_DTE))='0' THEN NULL ELSE USER_FEE_2_WAIVE_DTE END) AS USER_FEE_2_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_FEE_3_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_FEE_3_WAIVE_DTE))='0' THEN NULL ELSE USER_FEE_3_WAIVE_DTE END) AS USER_FEE_3_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_FEE_4_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_FEE_4_WAIVE_DTE))='0' THEN NULL ELSE USER_FEE_4_WAIVE_DTE END) AS USER_FEE_4_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_FEE_5_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_FEE_5_WAIVE_DTE))='0' THEN NULL ELSE USER_FEE_5_WAIVE_DTE END) AS USER_FEE_5_WAIVE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_FEE_6_WAIVE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_FEE_6_WAIVE_DTE))='0' THEN NULL ELSE USER_FEE_6_WAIVE_DTE END) AS USER_FEE_6_WAIVE_DTE,
DD1_BANK,
DD1_BANK_NBR,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DD1_START_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DD1_START_DTE))='0' THEN NULL ELSE DD1_START_DTE END) AS DD1_START_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DD1_END_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DD1_END_DTE))='0' THEN NULL ELSE DD1_END_DTE END) AS DD1_END_DTE,
DD1_METHOD,
DD1_ASSIGN_AMT,
DD1_ASSIGN_PCT,
DD_OWNER,
DD2_BANK_NBR,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DD2_START_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DD2_START_DTE))='0' THEN NULL ELSE DD2_START_DTE END) AS DD2_START_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DD2_END_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DD2_END_DTE))='0' THEN NULL ELSE DD2_END_DTE END) AS DD2_END_DTE,
ACCT_GROUP_REASON,
DD2_ASSIGN_AMT,
INT_RATE_VARIANCE,
DC_BANK,
DC_BANK_NBR,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DC_START_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DC_START_DTE))='0' THEN NULL ELSE DC_START_DTE END) AS DC_START_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DC_END_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DC_END_DTE))='0' THEN NULL ELSE DC_END_DTE END) AS DC_END_DTE,
MAX_DUE,
MAX_REVOLVING,
CLASS,
TEMP_CLASS,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TEMP_CLASS_START_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TEMP_CLASS_START_DTE))='0' THEN NULL ELSE TEMP_CLASS_START_DTE END) AS TEMP_CLASS_START_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TEMP_CLASS_END_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TEMP_CLASS_END_DTE))='0' THEN NULL ELSE TEMP_CLASS_END_DTE END) AS TEMP_CLASS_END_DTE,
CREDIT_SCORE,
ACCT_GROUP_NEXT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(ACCT_GROUP_NEXT_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(ACCT_GROUP_NEXT_DTE))='0' THEN NULL ELSE ACCT_GROUP_NEXT_DTE END) AS ACCT_GROUP_NEXT_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(COLM_START_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(COLM_START_DTE))='0' THEN NULL ELSE COLM_START_DTE END) AS COLM_START_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(COLM_END_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(COLM_END_DTE))='0' THEN NULL ELSE COLM_END_DTE END) AS COLM_END_DTE,
COLM_END,
COLM_ACTION,
STOP_DELQ,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(KK4_CLOSE_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(KK4_CLOSE_DTE))='0' THEN NULL ELSE KK4_CLOSE_DTE END) AS KK4_CLOSE_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(FORCE_STOP_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(FORCE_STOP_DTE))='0' THEN NULL ELSE FORCE_STOP_DTE END) AS FORCE_STOP_DTE,
USER_CHAR_1,
USER_CHAR_2,
USER_CHAR_3,
USER_CHAR_4,
USER_CHAR_5,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_DTE_1), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_DTE_1))='0' THEN NULL ELSE USER_DTE_1 END) AS USER_DTE_1,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_DTE_2), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_DTE_2))='0' THEN NULL ELSE USER_DTE_2 END) AS USER_DTE_2,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_DTE_3), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_DTE_3))='0' THEN NULL ELSE USER_DTE_3 END) AS USER_DTE_3,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_DTE_4), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_DTE_4))='0' THEN NULL ELSE USER_DTE_4 END) AS USER_DTE_4,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(USER_DTE_5), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(USER_DTE_5))='0' THEN NULL ELSE USER_DTE_5 END) AS USER_DTE_5,
USER_AMT_1,
USER_AMT_2,
USER_AMT_3,
USER_AMT_4,
USER_AMT_5,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MNT_DT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MNT_DT))='0' THEN NULL ELSE MNT_DT END) AS MNT_DT,
MNT_USER,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
