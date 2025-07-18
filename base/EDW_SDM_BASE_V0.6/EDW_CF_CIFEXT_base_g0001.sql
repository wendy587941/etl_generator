-- SQL for table: EDW_CF_CIFEXT
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
KEY,
"TYPE",
SEQ,
NAME,
LUBRNO,
TLRNO,
LUDATE,
LUTIME,
FUNCD,
FLAG1,
FLAG2,
FLAG3,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
KEY,
"TYPE",
SEQ,
NAME,
LUBRNO,
TLRNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(LUDATE), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(LUDATE))='0' THEN NULL ELSE LUDATE END) AS LUDATE,
與LUDATE結合後日期正規化,
FUNCD,
FLAG1,
FLAG2,
FLAG3,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
