-- SQL for table: EDW_ELN_CRCASEINIT
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'EMPNUM' AS DATA_CHK_COL,
EMPNUM AS DATA_VAL,
SDG.UFN_NUMCHECK(EMPNUM) AS CHECK_RESULT
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
'STDNUM' AS DATA_CHK_COL,
STDNUM AS DATA_VAL,
SDG.UFN_NUMCHECK(STDNUM) AS CHECK_RESULT
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
'BANKNUM' AS DATA_CHK_COL,
BANKNUM AS DATA_VAL,
SDG.UFN_NUMCHECK(BANKNUM) AS CHECK_RESULT
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

-- SQL for table: EDW_ELN_CRCASEINIT
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
CASEID,
CRRPTNO,
IDBAN,
RELCOSTUMER,
INITDATE,
EMPNUM,
STDNUM,
BANKNUM,
ZIPCODE,
FSTADDR,
SECADDR,
TRDADDR,
BUSINESS,
SWSALEBIGCHG,
SALEBIGCHGTXT,
SWREL,
HSPEVLYYY,
HSPEVL,
CRRPTER,
CRRPTDATE,
SWFSTOWN,
HSPDESC,
BOTSWREL,
SWCREDIT,
CREDITYEAR,
CREDITMON,
SWRELMEMO,
OWNSW,
SCHEVL,
CAPITALMEMO,
IPO,
STOCK_LISTING,
OTC,
EMERGING,
DELISTING,
FULL_DELIVERY,
PUB_OFFERING,
REV_PUB_OFFERING,
REMARK,
SVDESC,
GPID,
GPINITRECDATE,
GPDATA_DATE,
SCSUPNOTE,
CREDITMEMO,
ADDRNOTE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
CASEID,
CRRPTNO,
IDBAN,
RELCOSTUMER,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(INITDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(INITDATE))='0' THEN NULL ELSE INITDATE END) AS INITDATE,
EMPNUM,
STDNUM,
BANKNUM,
ZIPCODE,
FSTADDR,
SECADDR,
TRDADDR,
BUSINESS,
SWSALEBIGCHG,
SALEBIGCHGTXT,
SWREL,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(HSPEVLYYY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(HSPEVLYYY))='0' THEN NULL ELSE HSPEVLYYY END) AS HSPEVLYYY,
HSPEVL,
CRRPTER,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CRRPTDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CRRPTDATE))='0' THEN NULL ELSE CRRPTDATE END) AS CRRPTDATE,
SWFSTOWN,
HSPDESC,
BOTSWREL,
SWCREDIT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CREDITYEAR), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CREDITYEAR))='0' THEN NULL ELSE CREDITYEAR END) AS CREDITYEAR,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CREDITYEAR)||' '||TRIM(CREDITMON), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CREDITYEAR)||' '||TRIM(CREDITMON))='0' THEN NULL ELSE TRIM(CREDITYEAR)||' '||TRIM(CREDITMON) END) AS CREDITMON,
SWRELMEMO,
OWNSW,
SCHEVL,
CAPITALMEMO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(IPO), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(IPO))='0' THEN NULL ELSE IPO END) AS IPO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(STOCK_LISTING), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(STOCK_LISTING))='0' THEN NULL ELSE STOCK_LISTING END) AS STOCK_LISTING,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(OTC), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(OTC))='0' THEN NULL ELSE OTC END) AS OTC,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(EMERGING), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(EMERGING))='0' THEN NULL ELSE EMERGING END) AS EMERGING,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(DELISTING), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(DELISTING))='0' THEN NULL ELSE DELISTING END) AS DELISTING,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(FULL_DELIVERY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(FULL_DELIVERY))='0' THEN NULL ELSE FULL_DELIVERY END) AS FULL_DELIVERY,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(PUB_OFFERING), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(PUB_OFFERING))='0' THEN NULL ELSE PUB_OFFERING END) AS PUB_OFFERING,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(REV_PUB_OFFERING), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(REV_PUB_OFFERING))='0' THEN NULL ELSE REV_PUB_OFFERING END) AS REV_PUB_OFFERING,
REMARK,
SVDESC,
GPID,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(GPINITRECDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(GPINITRECDATE))='0' THEN NULL ELSE GPINITRECDATE END) AS GPINITRECDATE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(GPDATA_DATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(GPDATA_DATE))='0' THEN NULL ELSE GPDATA_DATE END) AS GPDATA_DATE,
SCSUPNOTE,
CREDITMEMO,
ADDRNOTE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
