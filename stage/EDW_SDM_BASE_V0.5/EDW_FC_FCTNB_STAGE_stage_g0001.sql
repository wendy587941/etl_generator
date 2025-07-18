-- SQL for table: EDW_FC_FCTNB_STAGE
/* TRANSFORM_DATA */

/* 1. Delete 目的表 Stage data_exch_date = '#排程啟動日#' */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取 Prestage 同期別進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
MBRNO,
MAPNO,
MSRNO,
MCHKDG,
TXDAY,
TXSEQ,
"TIME",
CLSDAY,
"DATE",
TRMNO,
DSCPTX,
ACTNO,
CURCD,
CTBAL,
EDAY,
OIRT,
PRDCD,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
MBRNO,
MAPNO,
MSRNO,
MCHKDG,
TXDAY,
TXSEQ,
"TIME",
CLSDAY,
"DATE",
TRMNO,
DSCPTX,
ACTNO,
CURCD,
CTBAL,
EDAY,
OIRT,
PRDCD,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
