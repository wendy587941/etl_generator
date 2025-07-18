-- SQL for table: EDW_ELN_CRCASEINITBR_PRESTAGE
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
CASEID,
CRRPTNO,
IDBAN,
RELCOSTUMER,
INITDATE,
EMPNUM,
STDNUM,
BANKNUM,
ZIPCODE,
FSTADDR,
SECADDR,
TRDADDR,
BUSINESS,
SWSALEBIGCHG,
SALEBIGCHGTXT,
SWREL,
HSPEVLYYY,
HSPEVL,
CRRPTER,
CRRPTDATE,
SWFSTOWN,
HSPDESC,
BOTSWREL,
SWCREDIT,
CREDITYEAR,
CREDITMON,
SWRELMEMO,
OWNSW,
SCHEVL,
CAPITALMEMO,
IPO,
STOCK_LISTING,
OTC,
EMERGING,
DELISTING,
FULL_DELIVERY,
PUB_OFFERING,
REV_PUB_OFFERING,
REMARK,
SVDESC,
GPID,
GPINITRECDATE,
GPDATA_DATE,
SCSUPNOTE,
CREDITMEMO,
ADDRNOTE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
CASEID,
CRRPTNO,
CASE WHEN LENGTH(IDBAN) = 8 THEN SDG.UFN_MASK_BAN(IDBAN) WHEN LENGTH(IDBAN) = 10 THEN SDG.UFN_MASK_IDN(IDBAN) END,
RELCOSTUMER,
INITDATE,
EMPNUM,
STDNUM,
BANKNUM,
ZIPCODE,
FSTADDR,
SECADDR,
SDG.UFN_MASK_ADDR(TRDADDR),
BUSINESS,
SWSALEBIGCHG,
SALEBIGCHGTXT,
SWREL,
HSPEVLYYY,
HSPEVL,
CRRPTER,
CRRPTDATE,
SWFSTOWN,
HSPDESC,
BOTSWREL,
SWCREDIT,
CREDITYEAR,
CREDITMON,
SWRELMEMO,
OWNSW,
SCHEVL,
CAPITALMEMO,
IPO,
STOCK_LISTING,
OTC,
EMERGING,
DELISTING,
FULL_DELIVERY,
PUB_OFFERING,
REV_PUB_OFFERING,
REMARK,
SVDESC,
SDG.UFN_MASK_BAN(GPID),
GPINITRECDATE,
GPDATA_DATE,
SCSUPNOTE,
CREDITMEMO,
ADDRNOTE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
