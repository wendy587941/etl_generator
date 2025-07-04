-- SQL for table: EDW_FC_FSTPD
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
ACTNO,
BRNO,
APNO,
SRNO,
CHKDG,
STPCD,
FILNO,
RGDAY,
RGAMT,
RSAMT,
CURCD,
ACTCLS,
CLSDAY,
TLRNO,
CKCNT,
CKDAY,
RBRNO,
TXTIME,
MEMO,
UNIT,
RGDATE,
RGCHAR,
RGNO,
RGKIND,
RPAMT,
RPTLR,
RPDATE,
RDMEMO,
RDTLR,
RDDATE,
BHCD,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BRNO ||APNO||SRNO||CHKDG,
BRNO,
APNO,
SRNO,
CHKDG,
STPCD,
FILNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(RGDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(RGDAY))='0' THEN NULL ELSE RGDAY END) AS RGDAY,
RGAMT,
RSAMT,
CURCD,
ACTCLS,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CLSDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CLSDAY))='0' THEN NULL ELSE CLSDAY END) AS CLSDAY,
TLRNO,
CKCNT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CKDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CKDAY))='0' THEN NULL ELSE CKDAY END) AS CKDAY,
RBRNO,
TXTIME,
MEMO,
UNIT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(RGDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(RGDATE))='0' THEN NULL ELSE RGDATE END) AS RGDATE,
RGCHAR,
RGNO,
RGKIND,
RPAMT,
RPTLR,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(RPDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(RPDATE))='0' THEN NULL ELSE RPDATE END) AS RPDATE,
RDMEMO,
RDTLR,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(RDDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(RDDATE))='0' THEN NULL ELSE RDDATE END) AS RDDATE,
BHCD,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
