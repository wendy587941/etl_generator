-- SQL for table: EDW_AROBO_FPNPMSETS_STAGE
/* TRANSFORM_DATA */

/* 1. Delete 目的表 Stage data_exch_date = '#排程啟動日#' */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取 Prestage 同期別進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
CUS_CODE,
IPMSET_CODE,
IPMSET_NAME,
IPMSETTYPE_CODE,
CUS_AGE,
INV_STD_DATE,
INV_PERIOD,
INV_CUR_CODE,
RANK_CODE,
FINISH_YN,
FINISH_DATE,
REMOVE_YN,
ACTIVE_YN,
FIRST_PLAN_DATE,
CUS_AGE_GRADE,
INV_OBJ,
INV_OBJ_ITEM,
INV_END_DATE,
INV_RANK_CODE,
CUS_INCOME_GRADE,
STATUS,
EXP_AMT_FC,
ACTIVE_DATE,
RETIRE_AGE,
EXPECT_AGE,
EXPENSES,
CUS_AUM_GRADE,
PROJECT_CODE,
REG_INV_STATUS,
INV_TYPE,
PASSIVE_INCOME,
ASSETCOM_ACTIVE_DATE,
WITHDRAW_ACNO,
REFERRAL_BRAN_CODE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
CUS_CODE,
IPMSET_CODE,
IPMSET_NAME,
IPMSETTYPE_CODE,
CUS_AGE,
INV_STD_DATE,
INV_PERIOD,
INV_CUR_CODE,
RANK_CODE,
FINISH_YN,
FINISH_DATE,
REMOVE_YN,
ACTIVE_YN,
FIRST_PLAN_DATE,
CUS_AGE_GRADE,
INV_OBJ,
INV_OBJ_ITEM,
INV_END_DATE,
INV_RANK_CODE,
CUS_INCOME_GRADE,
STATUS,
EXP_AMT_FC,
ACTIVE_DATE,
RETIRE_AGE,
EXPECT_AGE,
EXPENSES,
CUS_AUM_GRADE,
PROJECT_CODE,
REG_INV_STATUS,
INV_TYPE,
PASSIVE_INCOME,
ASSETCOM_ACTIVE_DATE,
WITHDRAW_ACNO,
REFERRAL_BRAN_CODE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
