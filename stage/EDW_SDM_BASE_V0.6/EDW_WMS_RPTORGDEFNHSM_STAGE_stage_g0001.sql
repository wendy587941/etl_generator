-- SQL for table: EDW_WMS_RPTORGDEFNHSM_STAGE
/* TRANSFORM_DATA */

/* 1. Delete 目的表 Stage data_exch_date = '#排程啟動日#' */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取 Prestage 同期別進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
DATA_YYYYMM,
DATA_DATE,
TERRITORY_ID,
DESCR,
BRCH_CLS,
LEVEL_ID,
ORG_SEQ,
PAR_TERRITORY_ID,
VIRTUAL_FLAG,
CLS_FLAG,
VERSION,
CREATETIME,
CREATOR,
MODIFIER,
LASTUPDATE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
DATA_YYYYMM,
DATA_DATE,
TERRITORY_ID,
DESCR,
BRCH_CLS,
LEVEL_ID,
ORG_SEQ,
PAR_TERRITORY_ID,
VIRTUAL_FLAG,
CLS_FLAG,
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
