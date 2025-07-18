-- SQL for table: EDW_NCARD_CUSTLIMIT
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
BU,
CUST_NBR,
LIMIT_CTL,
CREDIT_LIMIT,
CREDIT_AVAIL,
PERM_LIMIT,
CASH_LIMIT,
CASH_AVAIL,
SPEC1_LIMIT,
SPEC1_AVAIL,
SPEC2_LIMIT,
SPEC2_AVAIL,
SPEC3_LIMIT,
SPEC3_AVAIL,
TEMP_LIMIT,
TEMP_LIMIT_START_DTE,
TEMP_LIMIT_END_DTE,
MEMO_DB_AMT,
MEMO_CR_AMT,
BAL_AMT,
CUST_LIMIT_AMT_1,
CUST_LIMIT_AMT_2,
CUST_LIMIT_AMT_3,
CUST_LIMIT_AMT_4,
CUST_LIMIT_AMT_5,
CUST_LIMIT_CHAR_1,
CUST_LIMIT_CHAR_2,
CUST_LIMIT_CHAR_3,
CUST_LIMIT_CHAR_4,
CUST_LIMIT_CHAR_5,
MNT_DT,
MNT_USER,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BU,
CUST_NBR,
LIMIT_CTL,
CREDIT_LIMIT,
CREDIT_AVAIL,
PERM_LIMIT,
CASH_LIMIT,
CASH_AVAIL,
SPEC1_LIMIT,
SPEC1_AVAIL,
SPEC2_LIMIT,
SPEC2_AVAIL,
SPEC3_LIMIT,
SPEC3_AVAIL,
TEMP_LIMIT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TEMP_LIMIT_START_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TEMP_LIMIT_START_DTE))='0' THEN NULL ELSE TEMP_LIMIT_START_DTE END) AS TEMP_LIMIT_START_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TEMP_LIMIT_END_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TEMP_LIMIT_END_DTE))='0' THEN NULL ELSE TEMP_LIMIT_END_DTE END) AS TEMP_LIMIT_END_DTE,
MEMO_DB_AMT,
MEMO_CR_AMT,
BAL_AMT,
CUST_LIMIT_AMT_1,
CUST_LIMIT_AMT_2,
CUST_LIMIT_AMT_3,
CUST_LIMIT_AMT_4,
CUST_LIMIT_AMT_5,
CUST_LIMIT_CHAR_1,
CUST_LIMIT_CHAR_2,
CUST_LIMIT_CHAR_3,
CUST_LIMIT_CHAR_4,
CUST_LIMIT_CHAR_5,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MNT_DT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MNT_DT))='0' THEN NULL ELSE MNT_DT END) AS MNT_DT,
MNT_USER,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
