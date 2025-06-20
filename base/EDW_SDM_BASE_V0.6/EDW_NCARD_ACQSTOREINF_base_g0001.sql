-- SQL for table: EDW_NCARD_ACQSTOREINF
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
ACQ_BU,
MER_NBR,
BR_NO,
UNIT_NO,
MER_NAME,
LOGO_NAME,
PIC_URL,
CHAIN_FLAG,
CHAIN_PROPERTY,
CHAIN_MER_NBR,
ENG_NAME,
BIZ_TYPE,
MCC_CODE,
STMT_ACTION,
MER_GROUP,
CREDIT_SCORE,
BIZ_ADDR_ZIP,
BIZ_ADDR_ZONE,
BIZ_ADDR,
BIZ_PHONE,
BIZ_FAX,
COUNTRY_CODE,
CITY_CODE,
STATE_NAME,
EMAIL_1,
EMAIL_2,
ESTBL_NAME,
ESTBL_ADDR_ZIP,
ESTBL_ADDR_ZONE,
ESTBL_ADDR,
ESTBL_PHONE,
ESTBL_CAP,
ESTBL_DTE,
PIC_NAME,
PIC_ID,
PIC_PHONE,
PIC_CELL_PHONE,
PIC_EMAIL,
REGISTER_ADDR_ZIP,
REGISTER_ADDR_ZONE,
REGISTER_ADDR,
STMT_NAME,
STMT_ADDR_ZIP,
STMT_ADDR_ZONE,
STMT_ADDR,
STMT_PHONE,
RECEIPT_NAME,
RECEIPT_ADDR_ZIP,
RECEIPT_ADDR_ZONE,
RECEIPT_ADDR,
CTC_NAME,
CTC_ID,
CTC_PHONE,
CTC_CELL_PHONE,
CTC_EMAIL,
SPONSOR_NAME_1,
SPONSOR_NBR_1,
SPONSOR_DEPOSIT_1,
SPONSOR_PHONE_1,
SPONSOR_NAME_2,
SPONSOR_NBR_2,
SPONSOR_DEPOSIT_2,
SPONSOR_PHONE_2,
CONTRACT_DTE,
CONTRACT_DTE_LAST,
ACQ_TYPE_V,
ACQ_TYPE_M,
ACQ_TYPE_J,
ACQ_TYPE_AE,
ACQ_TYPE_CUP,
CONTRACT_EFF_DTE_V,
CONTRACT_EFF_DTE_M,
CONTRACT_EFF_DTE_J,
CONTRACT_EFF_DTE_AE,
CONTRACT_EFF_DTE_CUP,
COMMEND_ACCT_V,
COMMEND_ACCT_M,
COMMEND_ACCT_J,
COMMEND_ACCT_AE,
COMMEND_ACCT_CUP,
CXL_CODE,
CXL_CODE_RSN,
CXL_DTE,
MER_STATUS,
MER_WARN_CODE,
MER_EST_VOL,
MER_EDC_FLAG,
MER_PINPAD_FLAG,
MER_CTL_FLAG,
MER_IMPRINTER_FLAG,
MER_MPOS_FLAG,
MER_EC_FLAG,
MER_AIR_FLAG,
MER_QRC_FLAG,
MER_NFC_FLAG,
MER_CAM_FLAG,
MER_TRAN_TYPE,
MER_PREPAID_FLAG,
MER_NOTPAY_FLAG,
MER_WEBSITE,
MER_3D_WEB_IP,
MER_3D_WEB_PORT,
MER_3D_FLAG_V,
MER_3D_FLAG_M,
MER_3D_FLAG_J,
MER_3D_FLAG_AE,
MER_3D_FLAG_CUP,
MER_3D_EFF_DTE_V,
MER_3D_EFF_DTE_M,
MER_3D_EFF_DTE_J,
MER_3D_EFF_DTE_AE,
MER_3D_EFF_DTE_CUP,
MER_3D_END_DTE_V,
MER_3D_END_DTE_M,
MER_3D_END_DTE_J,
MER_3D_END_DTE_AE,
MER_3D_END_DTE_CUP,
DC_BANK,
DC_BANK_NBR,
DC_METHOD,
DC_OWNER,
DC_OWNER_ID,
DC_FEE,
PMT_FREQ,
PMT_CYCLE,
PMT_DAYS,
PMT_CHAIN,
PMT_HOLD,
MER_USER_CHAR_1,
MER_USER_CHAR_2,
MER_USER_CHAR_3,
MER_USER_CHAR_4,
MER_USER_CHAR_5,
MER_USER_CHAR_6,
MER_USER_CHAR_7,
MER_USER_CHAR_8,
MER_USER_CHAR_9,
MER_USER_CHAR_10,
MER_USER_DTE_1,
MER_USER_DTE_2,
MER_USER_DTE_3,
MER_USER_DTE_4,
MER_USER_DTE_5,
MER_USER_AMT_1,
MER_USER_AMT_2,
MER_USER_AMT_3,
MER_USER_AMT_4,
MER_USER_AMT_5,
POST_RESULT,
CREATE_DT,
CREATE_USER,
MNT_DT,
MNT_USER,
CRD_FEE_RATE,
CUP_FEE_RATE,
CRD_COST_ONUS_RATE,
CRD_COST_OTH_RATE,
CUP_COST_ONUS_RATE,
CUP_COST_OTH_RATE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
ACQ_BU,
MER_NBR,
BR_NO,
UNIT_NO,
MER_NAME,
LOGO_NAME,
PIC_URL,
CHAIN_FLAG,
CHAIN_PROPERTY,
CHAIN_MER_NBR,
ENG_NAME,
BIZ_TYPE,
MCC_CODE,
STMT_ACTION,
MER_GROUP,
CREDIT_SCORE,
BIZ_ADDR_ZIP,
BIZ_ADDR_ZONE,
BIZ_ADDR,
BIZ_PHONE,
BIZ_FAX,
COUNTRY_CODE,
CITY_CODE,
STATE_NAME,
EMAIL_1,
EMAIL_2,
ESTBL_NAME,
ESTBL_ADDR_ZIP,
ESTBL_ADDR_ZONE,
ESTBL_ADDR,
ESTBL_PHONE,
ESTBL_CAP,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(ESTBL_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(ESTBL_DTE))='0' THEN NULL ELSE ESTBL_DTE END) AS ESTBL_DTE,
PIC_NAME,
PIC_ID,
PIC_PHONE,
PIC_CELL_PHONE,
PIC_EMAIL,
REGISTER_ADDR_ZIP,
REGISTER_ADDR_ZONE,
REGISTER_ADDR,
STMT_NAME,
STMT_ADDR_ZIP,
STMT_ADDR_ZONE,
STMT_ADDR,
STMT_PHONE,
RECEIPT_NAME,
RECEIPT_ADDR_ZIP,
RECEIPT_ADDR_ZONE,
RECEIPT_ADDR,
CTC_NAME,
CTC_ID,
CTC_PHONE,
CTC_CELL_PHONE,
CTC_EMAIL,
SPONSOR_NAME_1,
SPONSOR_NBR_1,
SPONSOR_DEPOSIT_1,
SPONSOR_PHONE_1,
SPONSOR_NAME_2,
SPONSOR_NBR_2,
SPONSOR_DEPOSIT_2,
SPONSOR_PHONE_2,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTRACT_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTRACT_DTE))='0' THEN NULL ELSE CONTRACT_DTE END) AS CONTRACT_DTE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTRACT_DTE_LAST), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTRACT_DTE_LAST))='0' THEN NULL ELSE CONTRACT_DTE_LAST END) AS CONTRACT_DTE_LAST,
ACQ_TYPE_V,
ACQ_TYPE_M,
ACQ_TYPE_J,
ACQ_TYPE_AE,
ACQ_TYPE_CUP,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTRACT_EFF_DTE_V), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTRACT_EFF_DTE_V))='0' THEN NULL ELSE CONTRACT_EFF_DTE_V END) AS CONTRACT_EFF_DTE_V,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTRACT_EFF_DTE_M), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTRACT_EFF_DTE_M))='0' THEN NULL ELSE CONTRACT_EFF_DTE_M END) AS CONTRACT_EFF_DTE_M,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTRACT_EFF_DTE_J), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTRACT_EFF_DTE_J))='0' THEN NULL ELSE CONTRACT_EFF_DTE_J END) AS CONTRACT_EFF_DTE_J,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTRACT_EFF_DTE_AE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTRACT_EFF_DTE_AE))='0' THEN NULL ELSE CONTRACT_EFF_DTE_AE END) AS CONTRACT_EFF_DTE_AE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTRACT_EFF_DTE_CUP), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTRACT_EFF_DTE_CUP))='0' THEN NULL ELSE CONTRACT_EFF_DTE_CUP END) AS CONTRACT_EFF_DTE_CUP,
COMMEND_ACCT_V,
COMMEND_ACCT_M,
COMMEND_ACCT_J,
COMMEND_ACCT_AE,
COMMEND_ACCT_CUP,
CXL_CODE,
CXL_CODE_RSN,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CXL_DTE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CXL_DTE))='0' THEN NULL ELSE CXL_DTE END) AS CXL_DTE,
MER_STATUS,
MER_WARN_CODE,
MER_EST_VOL,
MER_EDC_FLAG,
MER_PINPAD_FLAG,
MER_CTL_FLAG,
MER_IMPRINTER_FLAG,
MER_MPOS_FLAG,
MER_EC_FLAG,
MER_AIR_FLAG,
MER_QRC_FLAG,
MER_NFC_FLAG,
MER_CAM_FLAG,
MER_TRAN_TYPE,
MER_PREPAID_FLAG,
MER_NOTPAY_FLAG,
MER_WEBSITE,
MER_3D_WEB_IP,
MER_3D_WEB_PORT,
MER_3D_FLAG_V,
MER_3D_FLAG_M,
MER_3D_FLAG_J,
MER_3D_FLAG_AE,
MER_3D_FLAG_CUP,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_EFF_DTE_V), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_EFF_DTE_V))='0' THEN NULL ELSE MER_3D_EFF_DTE_V END) AS MER_3D_EFF_DTE_V,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_EFF_DTE_M), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_EFF_DTE_M))='0' THEN NULL ELSE MER_3D_EFF_DTE_M END) AS MER_3D_EFF_DTE_M,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_EFF_DTE_J), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_EFF_DTE_J))='0' THEN NULL ELSE MER_3D_EFF_DTE_J END) AS MER_3D_EFF_DTE_J,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_EFF_DTE_AE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_EFF_DTE_AE))='0' THEN NULL ELSE MER_3D_EFF_DTE_AE END) AS MER_3D_EFF_DTE_AE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_EFF_DTE_CUP), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_EFF_DTE_CUP))='0' THEN NULL ELSE MER_3D_EFF_DTE_CUP END) AS MER_3D_EFF_DTE_CUP,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_END_DTE_V), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_END_DTE_V))='0' THEN NULL ELSE MER_3D_END_DTE_V END) AS MER_3D_END_DTE_V,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_END_DTE_M), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_END_DTE_M))='0' THEN NULL ELSE MER_3D_END_DTE_M END) AS MER_3D_END_DTE_M,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_END_DTE_J), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_END_DTE_J))='0' THEN NULL ELSE MER_3D_END_DTE_J END) AS MER_3D_END_DTE_J,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_END_DTE_AE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_END_DTE_AE))='0' THEN NULL ELSE MER_3D_END_DTE_AE END) AS MER_3D_END_DTE_AE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_3D_END_DTE_CUP), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_3D_END_DTE_CUP))='0' THEN NULL ELSE MER_3D_END_DTE_CUP END) AS MER_3D_END_DTE_CUP,
DC_BANK,
DC_BANK_NBR,
DC_METHOD,
DC_OWNER,
DC_OWNER_ID,
DC_FEE,
PMT_FREQ,
PMT_CYCLE,
PMT_DAYS,
PMT_CHAIN,
PMT_HOLD,
MER_USER_CHAR_1,
MER_USER_CHAR_2,
MER_USER_CHAR_3,
MER_USER_CHAR_4,
MER_USER_CHAR_5,
MER_USER_CHAR_6,
MER_USER_CHAR_7,
MER_USER_CHAR_8,
MER_USER_CHAR_9,
MER_USER_CHAR_10,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_USER_DTE_1), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_USER_DTE_1))='0' THEN NULL ELSE MER_USER_DTE_1 END) AS MER_USER_DTE_1,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_USER_DTE_2), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_USER_DTE_2))='0' THEN NULL ELSE MER_USER_DTE_2 END) AS MER_USER_DTE_2,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_USER_DTE_3), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_USER_DTE_3))='0' THEN NULL ELSE MER_USER_DTE_3 END) AS MER_USER_DTE_3,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_USER_DTE_4), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_USER_DTE_4))='0' THEN NULL ELSE MER_USER_DTE_4 END) AS MER_USER_DTE_4,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MER_USER_DTE_5), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MER_USER_DTE_5))='0' THEN NULL ELSE MER_USER_DTE_5 END) AS MER_USER_DTE_5,
MER_USER_AMT_1,
MER_USER_AMT_2,
MER_USER_AMT_3,
MER_USER_AMT_4,
MER_USER_AMT_5,
POST_RESULT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CREATE_DT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CREATE_DT))='0' THEN NULL ELSE CREATE_DT END) AS CREATE_DT,
CREATE_USER,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(MNT_DT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(MNT_DT))='0' THEN NULL ELSE MNT_DT END) AS MNT_DT,
MNT_USER,
CRD_FEE_RATE,
CUP_FEE_RATE,
CRD_COST_ONUS_RATE,
CRD_COST_OTH_RATE,
CUP_COST_ONUS_RATE,
CUP_COST_OTH_RATE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
