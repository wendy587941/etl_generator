-- SQL for table: EDW_NCARD_SETUPCODE_PRESTAGE
-- Trinity SQL Script
/* TRANSFORM_DATA */

/* 1.Truncate 目的表 */
TRUNCATE TABLE ${TARGET_SCHEMA}.${TARGET_TABLE};

/* 
2. 抽取 正式區Stage DATA_EXCH_DATE='#資料交換期別#' 之資料
3. 遮罩後寫入驗證區Prestage 
*/
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
"TYPE",
TYPE_SUB,
CODE,
LANGUAGE_FAMILY,
DESCR,
DESCR_SHORT,
ADD_VAL1,
ADD_VAL2,
ADD_VAL3,
ADD_VAL4,
ADD_VAL5,
ADD_VAL6,
POST_RESULT,
MNT_DT,
MNT_USER,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
"TYPE",
TYPE_SUB,
CODE,
LANGUAGE_FAMILY,
DESCR,
DESCR_SHORT,
ADD_VAL1,
ADD_VAL2,
ADD_VAL3,
ADD_VAL4,
ADD_VAL5,
ADD_VAL6,
POST_RESULT,
MNT_DT,
MNT_USER,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
