-- SQL for table: EDW_CF_CIFPSN
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
PIDNO,
NAME,
ADDR,
ZIPNO1,
ZIPNO2,
ACTADDR,
LUDATE,
LUACTNO,
BIRDAY,
"TYPE",
AGREEDT,
HZONE,
HSRNO,
OZONE,
OSRNO,
OEXT,
FZONE,
FSRNO,
AGRBRNO,
NATION,
SPNO,
EMAIL,
OPENDT,
TLRNO,
BRNO,
AGENTID,
BIRTYPE,
FOREID,
CPHONE,
PASSPORT,
TAXCD,
BCODE,
FLAG,
CTTADDR,
CTTZIP1,
CTTZIP2,
CTTDATE,
CTTBRNO,
CTTTLRNO,
CTTSPNO,
SLIPFLG,
EMAILCD,
BTFLAG,
PERFLG,
CTTZONE,
CTTSRNO,
CMPNAME,
ISUDT,
EXPDT,
CLASS,
EXTCNT,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
PIDNO,
NAME,
ADDR,
ZIPNO1,
ZIPNO2,
ACTADDR,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LUDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LUDATE))='0' THEN NULL ELSE LUDATE END) AS LUDATE,
LUACTNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(BIRDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(BIRDAY))='0' THEN NULL ELSE BIRDAY END) AS BIRDAY,
"TYPE",
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(AGREEDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(AGREEDT))='0' THEN NULL ELSE AGREEDT END) AS AGREEDT,
HZONE,
HSRNO,
OZONE,
OSRNO,
OEXT,
FZONE,
FSRNO,
AGRBRNO,
NATION,
SPNO,
EMAIL,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(OPENDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(OPENDT))='0' THEN NULL ELSE OPENDT END) AS OPENDT,
TLRNO,
BRNO,
AGENTID,
BIRTYPE,
FOREID,
CPHONE,
PASSPORT,
TAXCD,
BCODE,
FLAG,
CTTADDR,
CTTZIP1,
CTTZIP2,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(CTTDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(CTTDATE))='0' THEN NULL ELSE CTTDATE END) AS CTTDATE,
CTTBRNO,
CTTTLRNO,
CTTSPNO,
SLIPFLG,
EMAILCD,
BTFLAG,
PERFLG,
CTTZONE,
CTTSRNO,
CMPNAME,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(ISUDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(ISUDT))='0' THEN NULL ELSE ISUDT END) AS ISUDT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(EXPDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(EXPDT))='0' THEN NULL ELSE EXPDT END) AS EXPDT,
CLASS,
EXTCNT,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
