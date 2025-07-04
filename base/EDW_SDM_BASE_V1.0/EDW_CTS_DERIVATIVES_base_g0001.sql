-- SQL for table: EDW_CTS_DERIVATIVES
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'CUR_BOOK_BAL' AS DATA_CHK_COL,
CUR_BOOK_BAL AS DATA_VAL,
SDG.UFN_NUMCHECK(CUR_BOOK_BAL) AS CHECK_RESULT
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
'CUR_BOOK_BAL_TWD' AS DATA_CHK_COL,
CUR_BOOK_BAL_TWD AS DATA_VAL,
SDG.UFN_NUMCHECK(CUR_BOOK_BAL_TWD) AS CHECK_RESULT
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
'CUR_BOOK_BAL_USD' AS DATA_CHK_COL,
CUR_BOOK_BAL_USD AS DATA_VAL,
SDG.UFN_NUMCHECK(CUR_BOOK_BAL_USD) AS CHECK_RESULT
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
'AVG_BOOK_BAL' AS DATA_CHK_COL,
AVG_BOOK_BAL AS DATA_VAL,
SDG.UFN_NUMCHECK(AVG_BOOK_BAL) AS CHECK_RESULT
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
'AVG_BOOK_BAL_TWD' AS DATA_CHK_COL,
AVG_BOOK_BAL_TWD AS DATA_VAL,
SDG.UFN_NUMCHECK(AVG_BOOK_BAL_TWD) AS CHECK_RESULT
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
'CUR_NET_RATE' AS DATA_CHK_COL,
CUR_NET_RATE AS DATA_VAL,
SDG.UFN_NUMCHECK(CUR_NET_RATE) AS CHECK_RESULT
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
'CUR_PAYMENT' AS DATA_CHK_COL,
CUR_PAYMENT AS DATA_VAL,
SDG.UFN_NUMCHECK(CUR_PAYMENT) AS CHECK_RESULT
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
'PMT_FREQ' AS DATA_CHK_COL,
PMT_FREQ AS DATA_VAL,
SDG.UFN_NUMCHECK(PMT_FREQ) AS CHECK_RESULT
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
'ACCRUED_INTEREST' AS DATA_CHK_COL,
ACCRUED_INTEREST AS DATA_VAL,
SDG.UFN_NUMCHECK(ACCRUED_INTEREST) AS CHECK_RESULT
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
'REPRICE_FREQ' AS DATA_CHK_COL,
REPRICE_FREQ AS DATA_VAL,
SDG.UFN_NUMCHECK(REPRICE_FREQ) AS CHECK_RESULT
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
'INTEREST_INC_EXP' AS DATA_CHK_COL,
INTEREST_INC_EXP AS DATA_VAL,
SDG.UFN_NUMCHECK(INTEREST_INC_EXP) AS CHECK_RESULT
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
'INTEREST_INC_EXP_TWD' AS DATA_CHK_COL,
INTEREST_INC_EXP_TWD AS DATA_VAL,
SDG.UFN_NUMCHECK(INTEREST_INC_EXP_TWD) AS CHECK_RESULT
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
'DELTA' AS DATA_CHK_COL,
DELTA AS DATA_VAL,
SDG.UFN_NUMCHECK(DELTA) AS CHECK_RESULT
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
'PREMIUM' AS DATA_CHK_COL,
PREMIUM AS DATA_VAL,
SDG.UFN_NUMCHECK(PREMIUM) AS CHECK_RESULT
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
'REPREMIUM' AS DATA_CHK_COL,
REPREMIUM AS DATA_VAL,
SDG.UFN_NUMCHECK(REPREMIUM) AS CHECK_RESULT
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
'CUR_BOOK_BAL_QUOTE' AS DATA_CHK_COL,
CUR_BOOK_BAL_QUOTE AS DATA_VAL,
SDG.UFN_NUMCHECK(CUR_BOOK_BAL_QUOTE) AS CHECK_RESULT
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

-- SQL for table: EDW_CTS_DERIVATIVES
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
AS_OF_DATE,
ACCOUNT_NUMBER,
GL_ACCOUNT_ID,
ORIGINATION_DATE,
MATURITY_DATE,
CUR_BOOK_BAL,
CUR_BOOK_BAL_TWD,
CUR_BOOK_BAL_USD,
AVG_BOOK_BAL,
AVG_BOOK_BAL_TWD,
CUR_NET_RATE,
INTEREST_RATE_CD,
ISO_CURRENCY_CD,
AMRT_TYPE_CD,
CUR_PAYMENT,
LAST_PAYMENT_DATE,
NEXT_PAYMENT_DATE,
PMT_FREQ,
PMT_FREQ_MULT,
ACCRUAL_BASIS_CD,
ADJUSTABLE_TYPE_CD,
ACCRUED_INTEREST,
LAST_REPRICE_DATE,
NEXT_REPRICE_DATE,
REPRICE_FREQ,
REPRICE_FREQ_MULT,
PRODUCT_TYPE,
PRODUCT_CODE,
BRANCH_CODE,
PROFIT_BRANCH_CD,
CIF_KEY,
SIC_CD,
BANK_TRADE_FLAG,
INTEREST_INC_EXP,
INTEREST_INC_EXP_TWD,
ACCOUNT_CLOSE_DATE,
LONG_SHORT_FLAG,
EMBEDDED_OPTIONS_FLAG,
EXERCISE_DATE,
AREA,
PORTFOLIO,
ISSUE_DATE,
BUY_SELL_FLAG,
DEAL_CODE,
DELTA,
PREMIUM,
PREMIUM_CURRENCY_CD,
REPREMIUM,
CUR_BOOK_BAL_QUOTE,
GL_ACCOUNT_ID_PRE,
ISO_CURRENCY_CD_QUOTE,
NOTIONAL_SWAP,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(AS_OF_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(AS_OF_DATE))='0' THEN NULL ELSE AS_OF_DATE END) AS AS_OF_DATE,
ACCOUNT_NUMBER,
GL_ACCOUNT_ID,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(ORIGINATION_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(ORIGINATION_DATE))='0' THEN NULL ELSE ORIGINATION_DATE END) AS ORIGINATION_DATE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MATURITY_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MATURITY_DATE))='0' THEN NULL ELSE MATURITY_DATE END) AS MATURITY_DATE,
CUR_BOOK_BAL,
CUR_BOOK_BAL_TWD,
CUR_BOOK_BAL_USD,
AVG_BOOK_BAL,
AVG_BOOK_BAL_TWD,
CUR_NET_RATE,
INTEREST_RATE_CD,
ISO_CURRENCY_CD,
AMRT_TYPE_CD,
CUR_PAYMENT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LAST_PAYMENT_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LAST_PAYMENT_DATE))='0' THEN NULL ELSE LAST_PAYMENT_DATE END) AS LAST_PAYMENT_DATE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NEXT_PAYMENT_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NEXT_PAYMENT_DATE))='0' THEN NULL ELSE NEXT_PAYMENT_DATE END) AS NEXT_PAYMENT_DATE,
PMT_FREQ,
PMT_FREQ_MULT,
ACCRUAL_BASIS_CD,
ADJUSTABLE_TYPE_CD,
ACCRUED_INTEREST,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LAST_REPRICE_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LAST_REPRICE_DATE))='0' THEN NULL ELSE LAST_REPRICE_DATE END) AS LAST_REPRICE_DATE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(NEXT_REPRICE_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(NEXT_REPRICE_DATE))='0' THEN NULL ELSE NEXT_REPRICE_DATE END) AS NEXT_REPRICE_DATE,
REPRICE_FREQ,
REPRICE_FREQ_MULT,
PRODUCT_TYPE,
PRODUCT_CODE,
BRANCH_CODE,
PROFIT_BRANCH_CD,
CIF_KEY,
SIC_CD,
BANK_TRADE_FLAG,
INTEREST_INC_EXP,
INTEREST_INC_EXP_TWD,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(ACCOUNT_CLOSE_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(ACCOUNT_CLOSE_DATE))='0' THEN NULL ELSE ACCOUNT_CLOSE_DATE END) AS ACCOUNT_CLOSE_DATE,
LONG_SHORT_FLAG,
EMBEDDED_OPTIONS_FLAG,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(EXERCISE_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(EXERCISE_DATE))='0' THEN NULL ELSE EXERCISE_DATE END) AS EXERCISE_DATE,
AREA,
PORTFOLIO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(ISSUE_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(ISSUE_DATE))='0' THEN NULL ELSE ISSUE_DATE END) AS ISSUE_DATE,
BUY_SELL_FLAG,
DEAL_CODE,
DELTA,
PREMIUM,
PREMIUM_CURRENCY_CD,
REPREMIUM,
CUR_BOOK_BAL_QUOTE,
GL_ACCOUNT_ID_PRE,
ISO_CURRENCY_CD_QUOTE,
NOTIONAL_SWAP,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
