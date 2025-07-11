-- SQL for table: EDW_WMS_ORGEMPDATE_HIS
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
SEQ_NO,
EMP_ID,
AO_BRCH,
AO_TYPE,
ON_BOARD_DATE,
OFF_BOARD_DATE,
IS_EXCLUSIVE,
AO_TENURE,
AO_TENURE_M,
AO_TENURE_Q,
MARK_4_DEL,
REMARK,
VERSION,
CREATETIME,
CREATOR,
MODIFIER,
LASTUPDATE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
SEQ_NO,
EMP_ID,
AO_BRCH,
AO_TYPE,
ON_BOARD_DATE,
OFF_BOARD_DATE,
IS_EXCLUSIVE,
AO_TENURE,
AO_TENURE_M,
AO_TENURE_Q,
MARK_4_DEL,
REMARK,
VERSION,
CREATETIME,
CREATOR,
MODIFIER,
LASTUPDATE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
