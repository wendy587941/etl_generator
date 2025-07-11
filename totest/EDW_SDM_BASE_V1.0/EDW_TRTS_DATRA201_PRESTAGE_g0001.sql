-- SQL for table: EDW_TRTS_DATRA201_PRESTAGE
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
WA20101,
WA20102,
WA20103,
WA20104,
WA20105,
WA20106,
WA20107,
WA20108,
WA20109,
WA20110,
WA20111,
WA20112,
WA20113,
WA20114,
WA20115,
WA20116,
WA20117,
WA20118,
WA20119,
WA20120,
WA20121,
WA20122,
WA20123,
WA20124,
WA20125,
WA20126,
WA20127,
WA20128,
WA20129,
WA20130,
WA20131,
WA20132,
WA20133,
WA20134,
WA20135,
WA20136,
WA20137,
WA20138,
WA20139,
WA20140,
WA20141,
WA20142,
WA20143,
WA20144,
WA20145,
WA20146,
WA20147,
WA20148,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
SDG.UFN_MASK_ACCT(WA20101),
WA20102,
SDG.UFN_MASK_IDN(WA20103),
WA20104,
WA20105,
WA20106,
WA20107,
WA20108,
SDG.UFN_MASK_NM(WA20109),
SDG.UFN_MASK_NM(WA20110),
SDG.UFN_MASK_NM(WA20111),
WA20112,
WA20113,
WA20114,
WA20115,
WA20116,
WA20117,
WA20118,
WA20119,
SDG.UFN_MASK_IDN(WA20120),
SDG.UFN_MASK_ACCT_SN(WA20121),
WA20122,
WA20123,
WA20124,
SDG.UFN_MASK_ACCT_SN(WA20125),
WA20126,
WA20127,
WA20128,
SDG.UFN_MASK_ADDR(WA20129),
WA20130,
WA20131,
WA20132,
WA20133,
WA20134,
WA20135,
WA20136,
WA20137,
WA20138,
WA20139,
WA20140,
WA20141,
WA20142,
WA20143,
WA20144,
WA20145,
WA20146,
WA20147,
WA20148,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
