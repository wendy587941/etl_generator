-- SQL for table: EDW_NCARD_CARDINF_PRESTAGE
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
CARD_NBR,
CARD_PRODUCT,
TC_CARD_NBR,
CARD_FLAG,
CARD_REL,
NEW_CUST_FLAG,
NEW_ACTIVE_FLAG,
CARDHOLDER_NBR,
CUST_NBR,
CTL_CODE,
CTL_CODE_DTE,
CTL_CODE_REASON,
EXPIR_DTE,
EXPIR_DTE_LAST,
APPLY_DTE,
APPROVE_DTE,
ISSUE_FLAG,
ISSUE_DTE,
ISSUE_REASON,
FIRST_ISSUE_DTE,
PREV_ISSUE_DTE,
OPEN_FLAG,
OPEN_DTE,
PREV_OPEN,
PREV_OPEN_DTE,
IC_CARD_FLAG,
PREV_IC_CARD_FLAG,
PIN_OFFSET,
PIN_OFFSET_MN_DT,
PREV_PIN_OFFSET,
PREV_PIN_OFFSET_MN_DT,
IC_TEMPLATE,
IC_TEMPLATE_PREV,
SERVICE_CODE,
PREV_SERVICE_CODE,
IC_SERVICE_CODE,
PREV_IC_SERVICE_CODE,
LINK_BANK_NBR,
CARD_ADDR_TYPE,
CARD_ADDR_TYPE_REISSUE,
CARD_MAIL_TYPE,
CARD_MAIL_TYPE_REISSUE,
EMBOSSING_STATUS,
EMBOSSING_DTE,
CLASS,
CARD_TERM,
EXPIR_DTE_NEXT,
ISSUE_BANK,
ADDR_BRANCH,
RISK_AMT,
SPECIAL_FLAG,
PIN_APPLY_REJECT,
PIN_APPLY_CNT,
CASH_PIN_ERROR,
CASH_PIN_DTE,
VOICE_PIN_ERROR,
VOICE_PIN_DTE,
HSRC_PIN_ERROR,
HSRC_PIN_DTE,
TEMP_CLASS,
TEMP_CLASS_START_DTE,
TEMP_CLASS_END_DTE,
SVC_PARM,
SVC_PARM_START,
SVC_PARM_END,
TXMSG_STATUS,
TXMSG_STATUS_DTE,
ATC,
PREV_ATC,
LINK_COBRAND_1_NBR,
LINK_COBRAND_2_NBR,
LINK_COBRAND_3_NBR,
USER_FEE_1_WAIVE_FLAG,
USER_FEE_1_WAIVE_DTE,
USER_FEE_2_WAIVE_FLAG,
USER_FEE_2_WAIVE_DTE,
USER_FEE_3_WAIVE_FLAG,
USER_FEE_3_WAIVE_DTE,
USER_FEE_4_WAIVE_FLAG,
USER_FEE_4_WAIVE_DTE,
USER_FEE_5_WAIVE_FLAG,
USER_FEE_5_WAIVE_DTE,
USER_FEE_6_WAIVE_FLAG,
USER_FEE_6_WAIVE_DTE,
USER_CHAR_1,
USER_CHAR_2,
USER_CHAR_3,
USER_CHAR_4,
USER_CHAR_5,
USER_CHAR_6,
USER_CHAR_7,
USER_CHAR_8,
USER_CHAR_9,
USER_CHAR_10,
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
POST_RESULT,
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
SDG.UFN_MASK_CRD_CARD(CARD_NBR),
CARD_PRODUCT,
SDG.UFN_MASK_CRD_CARD(TC_CARD_NBR),
CARD_FLAG,
CARD_REL,
NEW_CUST_FLAG,
NEW_ACTIVE_FLAG,
CARDHOLDER_NBR,
CUST_NBR,
CTL_CODE,
CTL_CODE_DTE,
CTL_CODE_REASON,
EXPIR_DTE,
EXPIR_DTE_LAST,
APPLY_DTE,
APPROVE_DTE,
ISSUE_FLAG,
ISSUE_DTE,
ISSUE_REASON,
FIRST_ISSUE_DTE,
PREV_ISSUE_DTE,
OPEN_FLAG,
OPEN_DTE,
PREV_OPEN,
PREV_OPEN_DTE,
IC_CARD_FLAG,
PREV_IC_CARD_FLAG,
PIN_OFFSET,
PIN_OFFSET_MN_DT,
PREV_PIN_OFFSET,
PREV_PIN_OFFSET_MN_DT,
IC_TEMPLATE,
IC_TEMPLATE_PREV,
SERVICE_CODE,
PREV_SERVICE_CODE,
IC_SERVICE_CODE,
PREV_IC_SERVICE_CODE,
LINK_BANK_NBR,
CARD_ADDR_TYPE,
CARD_ADDR_TYPE_REISSUE,
CARD_MAIL_TYPE,
CARD_MAIL_TYPE_REISSUE,
EMBOSSING_STATUS,
EMBOSSING_DTE,
CLASS,
CARD_TERM,
EXPIR_DTE_NEXT,
ISSUE_BANK,
ADDR_BRANCH,
RISK_AMT,
SPECIAL_FLAG,
PIN_APPLY_REJECT,
PIN_APPLY_CNT,
CASH_PIN_ERROR,
CASH_PIN_DTE,
VOICE_PIN_ERROR,
VOICE_PIN_DTE,
HSRC_PIN_ERROR,
HSRC_PIN_DTE,
TEMP_CLASS,
TEMP_CLASS_START_DTE,
TEMP_CLASS_END_DTE,
SVC_PARM,
SVC_PARM_START,
SVC_PARM_END,
TXMSG_STATUS,
TXMSG_STATUS_DTE,
ATC,
PREV_ATC,
SDG.UFN_MASK_ACCT(LINK_COBRAND_1_NBR),
SDG.UFN_MASK_ACCT(LINK_COBRAND_2_NBR),
SDG.UFN_MASK_ACCT(LINK_COBRAND_3_NBR),
USER_FEE_1_WAIVE_FLAG,
USER_FEE_1_WAIVE_DTE,
USER_FEE_2_WAIVE_FLAG,
USER_FEE_2_WAIVE_DTE,
USER_FEE_3_WAIVE_FLAG,
USER_FEE_3_WAIVE_DTE,
USER_FEE_4_WAIVE_FLAG,
USER_FEE_4_WAIVE_DTE,
USER_FEE_5_WAIVE_FLAG,
USER_FEE_5_WAIVE_DTE,
USER_FEE_6_WAIVE_FLAG,
USER_FEE_6_WAIVE_DTE,
USER_CHAR_1,
USER_CHAR_2,
USER_CHAR_3,
USER_CHAR_4,
USER_CHAR_5,
USER_CHAR_6,
USER_CHAR_7,
USER_CHAR_8,
USER_CHAR_9,
USER_CHAR_10,
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
POST_RESULT,
MNT_DT,
MNT_USER,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
