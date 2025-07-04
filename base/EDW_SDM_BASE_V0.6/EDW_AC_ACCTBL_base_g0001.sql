-- SQL for table: EDW_AC_ACCTBL
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
ACNO,
SBNO,
DTLNO,
CNAME,
CRDB,
SBCD,
DTLCD,
SBAF,
RVSCD,
RVSKIND,
TRTYPE,
BGTCTL,
CLSTYPE,
"SHARE",
TAXCD,
BRCHK_1,
BRCHK_2,
BRCHK_3,
BRCHK_4,
BRCHK_5,
BRCHK_6,
BRCHK_7,
BRCHK_8,
BRCHK_9,
BRCHK_10,
BRCHK_11,
BRCHK_12,
"STOP",
LTXDATE,
STAXCD,
"34CD",
SELCD,
ENAME,
OSBAF,
ORVSCD,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
ACNO,
SBNO,
DTLNO,
CNAME,
CRDB,
SBCD,
DTLCD,
SBAF,
RVSCD,
RVSKIND,
TRTYPE,
BGTCTL,
CLSTYPE,
"SHARE",
TAXCD,
BRCHK_1,
BRCHK_2,
BRCHK_3,
BRCHK_4,
BRCHK_5,
BRCHK_6,
BRCHK_7,
BRCHK_8,
BRCHK_9,
BRCHK_10,
BRCHK_11,
BRCHK_12,
"STOP",
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LTXDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LTXDATE))='0' THEN NULL ELSE LTXDATE END) AS LTXDATE,
STAXCD,
"34CD",
SELCD,
ENAME,
OSBAF,
ORVSCD,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
