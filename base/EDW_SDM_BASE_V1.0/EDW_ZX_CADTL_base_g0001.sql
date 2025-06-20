-- SQL for table: EDW_ZX_CADTL
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
CIFKEY,
ATRNO,
APPLY,
CTLNO,
KINBR,
TRMSEQ,
TLRNO,
SUPNO,
TXDAY,
TXTIME,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
CIFKEY,
ATRNO,
APPLY,
CTLNO,
KINBR,
TRMSEQ,
TLRNO,
SUPNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDAY), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDAY))='0' THEN NULL ELSE TXDAY END) AS TXDAY,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDAY)||' '||TRIM(TXTIME), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDAY)||' '||TRIM(TXTIME))='0' THEN NULL ELSE TRIM(TXDAY)||' '||TRIM(TXTIME) END) AS TXTIME,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
