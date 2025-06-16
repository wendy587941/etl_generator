-- SQL for table: EDW_GL_GLPRDL_PIVOT
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
CIFKEY,
CIFERR,
TXDAY,
"TIME",
KINBR,
WSNO,
TXTNO,
SPCD,
TLRNO,
TXCODE,
MRKEY,
TXAMT,
TWGHT,
CASHAMT,
PBACT,
PBAMT,
CKACT,
CKAMT,
RISKLVL,
TRGRM,
WMFLG,
PEPFLG,
NAME,
OCCURS,
GOODNO,
TXQTY,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
依資料表總表所描述的資料載入規則載入,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
