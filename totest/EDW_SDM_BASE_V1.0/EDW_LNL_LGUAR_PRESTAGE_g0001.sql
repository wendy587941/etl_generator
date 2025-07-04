-- SQL for table: EDW_LNL_LGUAR_PRESTAGE
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
MY_RSN,
ENTPNO,
FILLER,
PIDERR,
ACTNO,
CPCNT,
ACTCLS,
CHARCD,
RGDAY,
IDCODE,
REL,
CLSDT,
FLAG,
NAME,
CNTRYCD,
CLASS,
EXTCNT,
NTC,
NTCTYP,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
MY_RSN,
SDG.UFN_MASK_BAN(ENTPNO),
FILLER,
PIDERR,
SDG.UFN_MASK_ACCT(ACTNO),
CPCNT,
ACTCLS,
CHARCD,
RGDAY,
IDCODE,
REL,
CLSDT,
FLAG,
SDG.UFN_MASK_NM(NAME),
CNTRYCD,
CLASS,
EXTCNT,
NTC,
NTCTYP,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
