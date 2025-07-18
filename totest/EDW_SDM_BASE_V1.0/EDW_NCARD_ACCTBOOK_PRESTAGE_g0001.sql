-- SQL for table: EDW_NCARD_ACCTBOOK_PRESTAGE
-- Trinity SQL Script
/* TRANSFORM_DATA */

/* 1.Truncate 目的表 */
TRUNCATE TABLE ${TARGET_SCHEMA}.${TARGET_TABLE};

/* 
2. 抽取 正式區Stage DATA_EXCH_DATE='#資料交換期別#' 之資料
3. 遮罩後寫入驗證區Prestage 
*/
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
BU,
ACCT_NBR,
PRODUCT,
CURRENCY,
BEG_BAL,
BEG_DUE,
CURR_BAL,
OVER_PMT,
PMT_CTD,
DB_CTD,
CR_CTD,
PRIN_1_CTD,
PRIN_2_CTD,
PRIN_3_CTD,
PRIN_4_CTD,
PRIN_5_CTD,
INT_CTD,
LC_CTD,
CASH_FEE_CTD,
MEMBER_FEE_CTD,
SERVICE_FEE_CTD,
USER_FEE_1_CTD,
USER_FEE_2_CTD,
USER_FEE_3_CTD,
USER_FEE_4_CTD,
USER_FEE_5_CTD,
USER_FEE_6_CTD,
PRIN_1_BILL,
PRIN_2_BILL,
PRIN_3_BILL,
PRIN_4_BILL,
PRIN_5_BILL,
INT_BILL,
LC_BILL,
CASH_FEE_BILL,
MEMBER_FEE_BILL,
SERVICE_FEE_BILL,
USER_FEE_1_BILL,
USER_FEE_2_BILL,
USER_FEE_3_BILL,
USER_FEE_4_BILL,
USER_FEE_5_BILL,
USER_FEE_6_BILL,
PRIN_1_COLL,
PRIN_2_COLL,
PRIN_3_COLL,
PRIN_4_COLL,
PRIN_5_COLL,
INT_COLL,
LC_COLL,
CASH_FEE_COLL,
MEMBER_FEE_COLL,
SERVICE_FEE_COLL,
USER_FEE_1_COLL,
USER_FEE_2_COLL,
USER_FEE_3_COLL,
USER_FEE_4_COLL,
USER_FEE_5_COLL,
USER_FEE_6_COLL,
PRIN_1_CHGOFF,
PRIN_2_CHGOFF,
PRIN_3_CHGOFF,
PRIN_4_CHGOFF,
PRIN_5_CHGOFF,
INT_CHGOFF,
LC_CHGOFF,
CASH_FEE_CHGOFF,
MEMBER_FEE_CHGOFF,
SERVICE_FEE_CHGOFF,
USER_FEE_1_CHGOFF,
USER_FEE_2_CHGOFF,
USER_FEE_3_CHGOFF,
USER_FEE_4_CHGOFF,
USER_FEE_5_CHGOFF,
USER_FEE_6_CHGOFF,
PRIN_1_REVOLVING,
PRIN_2_REVOLVING,
PRIN_3_REVOLVING,
PRIN_4_REVOLVING,
PRIN_5_REVOLVING,
STMT_BAL,
STMT_DUE,
DUE_DTE,
GRACE_DTE,
LATE_DTE,
PAY_IN_FULL,
PAY_IN_MINI,
DELQ_STATUS_HIST,
PMT_STATUS_HIST,
STMT_DATE,
DELQ_BUCKET_AMT_1,
DELQ_BUCKET_AMT_2,
DELQ_BUCKET_AMT_3,
DELQ_BUCKET_AMT_4,
DELQ_BUCKET_AMT_5,
DELQ_BUCKET_AMT_6,
DELQ_BUCKET_AMT_7,
DELQ_BUCKET_AMT_8,
DELQ_BUCKET_AMT_9,
DELQ_BUCKET_AMT_10,
DELQ_BUCKET_AMT_11,
DELQ_BUCKET_AMT_12,
BAL_BUCKET_AMT_1,
BAL_BUCKET_AMT_2,
BAL_BUCKET_AMT_3,
BAL_BUCKET_AMT_4,
BAL_BUCKET_AMT_5,
BAL_BUCKET_AMT_6,
BAL_BUCKET_AMT_7,
BAL_BUCKET_AMT_8,
BAL_BUCKET_AMT_9,
BAL_BUCKET_AMT_10,
BAL_BUCKET_AMT_11,
BAL_BUCKET_AMT_12,
PAYOFF_DTE,
LC_CHG_CNT,
M0_STMT_DTE,
MNT_DT,
MNT_USER,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BU,
SDG.UFN_MASK_ACCT(ACCT_NBR),
PRODUCT,
CURRENCY,
BEG_BAL,
BEG_DUE,
CURR_BAL,
OVER_PMT,
PMT_CTD,
DB_CTD,
CR_CTD,
PRIN_1_CTD,
PRIN_2_CTD,
PRIN_3_CTD,
PRIN_4_CTD,
PRIN_5_CTD,
INT_CTD,
LC_CTD,
CASH_FEE_CTD,
MEMBER_FEE_CTD,
SERVICE_FEE_CTD,
USER_FEE_1_CTD,
USER_FEE_2_CTD,
USER_FEE_3_CTD,
USER_FEE_4_CTD,
USER_FEE_5_CTD,
USER_FEE_6_CTD,
PRIN_1_BILL,
PRIN_2_BILL,
PRIN_3_BILL,
PRIN_4_BILL,
PRIN_5_BILL,
INT_BILL,
LC_BILL,
CASH_FEE_BILL,
MEMBER_FEE_BILL,
SERVICE_FEE_BILL,
USER_FEE_1_BILL,
USER_FEE_2_BILL,
USER_FEE_3_BILL,
USER_FEE_4_BILL,
USER_FEE_5_BILL,
USER_FEE_6_BILL,
PRIN_1_COLL,
PRIN_2_COLL,
PRIN_3_COLL,
PRIN_4_COLL,
PRIN_5_COLL,
INT_COLL,
LC_COLL,
CASH_FEE_COLL,
MEMBER_FEE_COLL,
SERVICE_FEE_COLL,
USER_FEE_1_COLL,
USER_FEE_2_COLL,
USER_FEE_3_COLL,
USER_FEE_4_COLL,
USER_FEE_5_COLL,
USER_FEE_6_COLL,
PRIN_1_CHGOFF,
PRIN_2_CHGOFF,
PRIN_3_CHGOFF,
PRIN_4_CHGOFF,
PRIN_5_CHGOFF,
INT_CHGOFF,
LC_CHGOFF,
CASH_FEE_CHGOFF,
MEMBER_FEE_CHGOFF,
SERVICE_FEE_CHGOFF,
USER_FEE_1_CHGOFF,
USER_FEE_2_CHGOFF,
USER_FEE_3_CHGOFF,
USER_FEE_4_CHGOFF,
USER_FEE_5_CHGOFF,
USER_FEE_6_CHGOFF,
PRIN_1_REVOLVING,
PRIN_2_REVOLVING,
PRIN_3_REVOLVING,
PRIN_4_REVOLVING,
PRIN_5_REVOLVING,
STMT_BAL,
STMT_DUE,
DUE_DTE,
GRACE_DTE,
LATE_DTE,
PAY_IN_FULL,
PAY_IN_MINI,
DELQ_STATUS_HIST,
PMT_STATUS_HIST,
STMT_DATE,
DELQ_BUCKET_AMT_1,
DELQ_BUCKET_AMT_2,
DELQ_BUCKET_AMT_3,
DELQ_BUCKET_AMT_4,
DELQ_BUCKET_AMT_5,
DELQ_BUCKET_AMT_6,
DELQ_BUCKET_AMT_7,
DELQ_BUCKET_AMT_8,
DELQ_BUCKET_AMT_9,
DELQ_BUCKET_AMT_10,
DELQ_BUCKET_AMT_11,
DELQ_BUCKET_AMT_12,
BAL_BUCKET_AMT_1,
BAL_BUCKET_AMT_2,
BAL_BUCKET_AMT_3,
BAL_BUCKET_AMT_4,
BAL_BUCKET_AMT_5,
BAL_BUCKET_AMT_6,
BAL_BUCKET_AMT_7,
BAL_BUCKET_AMT_8,
BAL_BUCKET_AMT_9,
BAL_BUCKET_AMT_10,
BAL_BUCKET_AMT_11,
BAL_BUCKET_AMT_12,
PAYOFF_DTE,
LC_CHG_CNT,
M0_STMT_DTE,
MNT_DT,
MNT_USER,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
