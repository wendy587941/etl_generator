-- SQL for table: EDW_GL_GLENTR_PRESTAGE
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
BRNO,
KIND,
SRNO,
APLSTA,
GOODNO_01,
APLQTY_01,
ACTQTY_01,
VALUE_01,
GOODNO_02,
APLQTY_02,
ACTQTY_02,
VALUE_02,
GOODNO_03,
APLQTY_03,
ACTQTY_03,
VALUE_03,
GOODNO_04,
APLQTY_04,
ACTQTY_04,
VALUE_04,
GOODNO_05,
APLQTY_05,
ACTQTY_05,
VALUE_05,
GOODNO_06,
APLQTY_06,
ACTQTY_06,
VALUE_06,
GOODNO_07,
APLQTY_07,
ACTQTY_07,
VALUE_07,
GOODNO_08,
APLQTY_08,
ACTQTY_08,
VALUE_08,
GOODNO_09,
APLQTY_09,
ACTQTY_09,
VALUE_09,
GOODNO_10,
APLQTY_10,
ACTQTY_10,
VALUE_10,
APLDAY,
APVDAY,
OUDAY,
INDAY,
OUTLR,
OUSUP,
INTLR,
INSUP,
OUBRNO,
PRVSUP,
REMARK,
SUPNO,
TLRNO,
LTXDAY,
DEBANK,
BRCRGFG,
DEBANKNM,
DENO,
CIFKEY,
CIFERR,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BRNO,
KIND,
SRNO,
APLSTA,
GOODNO_01,
APLQTY_01,
ACTQTY_01,
VALUE_01,
GOODNO_02,
APLQTY_02,
ACTQTY_02,
VALUE_02,
GOODNO_03,
APLQTY_03,
ACTQTY_03,
VALUE_03,
GOODNO_04,
APLQTY_04,
ACTQTY_04,
VALUE_04,
GOODNO_05,
APLQTY_05,
ACTQTY_05,
VALUE_05,
GOODNO_06,
APLQTY_06,
ACTQTY_06,
VALUE_06,
GOODNO_07,
APLQTY_07,
ACTQTY_07,
VALUE_07,
GOODNO_08,
APLQTY_08,
ACTQTY_08,
VALUE_08,
GOODNO_09,
APLQTY_09,
ACTQTY_09,
VALUE_09,
GOODNO_10,
APLQTY_10,
ACTQTY_10,
VALUE_10,
APLDAY,
APVDAY,
OUDAY,
INDAY,
OUTLR,
OUSUP,
INTLR,
INSUP,
OUBRNO,
PRVSUP,
REMARK,
SUPNO,
TLRNO,
LTXDAY,
DEBANK,
BRCRGFG,
SDG.UFN_MASK_NM(DEBANKNM),
DENO,
SDG.UFN_MASK_BAN(CIFKEY),
CIFERR,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
