-- SQL for table: EDW_FXF_FFBTR
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
AGDNO,
TRCNT,
OBUFG,
RBRNO,
ACBRNO,
KINBR,
ITMCD,
TXDATE,
CBCD,
CURCD,
TENOR,
TXAMT,
TXEQL,
CLRAMT,
CLREQL,
NTAMT,
MGTYP,
MGCUR,
MGAMT,
LPAMT,
LPCD,
IIRT,
RATS,
RATN,
REFNO,
DOCNO,
TXCD,
TLRNO,
TXTIME,
HCODE,
HTRCNT,
SENDFG,
USDRATU,
USDRATS,
"TYPE",
CSDATE,
CEDATE,
RENEWNO,
CONTDATE,
CLASS,
"690CODE",
CNTRYCD,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
AGDNO,
TRCNT,
OBUFG,
RBRNO,
ACBRNO,
KINBR,
ITMCD,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDATE))='0' THEN NULL ELSE TXDATE END) AS TXDATE,
CBCD,
CURCD,
TENOR,
TXAMT,
TXEQL,
CLRAMT,
CLREQL,
NTAMT,
MGTYP,
MGCUR,
MGAMT,
LPAMT,
LPCD,
IIRT,
RATS,
RATN,
REFNO,
DOCNO,
TXCD,
TLRNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDATE)||' '||TRIM(TXTIME), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDATE)||' '||TRIM(TXTIME))='0' THEN NULL ELSE TRIM(TXDATE)||' '||TRIM(TXTIME) END) AS TXTIME,
HCODE,
HTRCNT,
SENDFG,
USDRATU,
USDRATS,
"TYPE",
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CSDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CSDATE))='0' THEN NULL ELSE CSDATE END) AS CSDATE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CEDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CEDATE))='0' THEN NULL ELSE CEDATE END) AS CEDATE,
RENEWNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CONTDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CONTDATE))='0' THEN NULL ELSE CONTDATE END) AS CONTDATE,
CLASS,
"690CODE",
CNTRYCD,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
