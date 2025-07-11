-- SQL for table: EDW_CK_CKHMR_PRESTAGE
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
YYMM,
BRNO,
APNO,
SRNO,
CHKDG,
TCHGCD,
CHGCD,
OPENH,
ACTCLS,
ACTSLP,
BDACT,
ACTSTP,
U3VCD,
UTFCD,
NEWCD,
CURCD,
CHARCD,
IRTCD,
TAXCD,
IIRT,
LIRT,
EXPER,
YEAR,
NOIBK,
COBKCD,
COBRCD,
BCODE,
SCOP,
MANG,
EIDERR,
ENTPNO,
PIDERR,
PIDNO,
DAMT,
DCNT,
CAMT,
CCNT,
YDBAL,
LMBAL,
LNLMT,
FILLER3,
LASTDT,
OPDAY,
CLSDAY,
CSDAY,
CEDAY,
APPDAY,
APPACD,
APPRONO,
FILLER,
SBRNO,
FLAG,
WADAY,
GRPACNO,
RSLPDAY,
RLSFLAG,
LITR,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
YYMM,
BRNO,
APNO,
SDG.UFN_MASK_ACCT_SN(SRNO),
CHKDG,
TCHGCD,
CHGCD,
OPENH,
ACTCLS,
ACTSLP,
BDACT,
ACTSTP,
U3VCD,
UTFCD,
NEWCD,
CURCD,
CHARCD,
IRTCD,
TAXCD,
IIRT,
LIRT,
EXPER,
YEAR,
NOIBK,
COBKCD,
COBRCD,
BCODE,
SCOP,
MANG,
EIDERR,
SDG.UFN_MASK_BAN(ENTPNO),
PIDERR,
SDG.UFN_MASK_IDN(PIDNO),
DAMT,
DCNT,
CAMT,
CCNT,
YDBAL,
LMBAL,
LNLMT,
FILLER3,
LASTDT,
OPDAY,
CLSDAY,
CSDAY,
CEDAY,
APPDAY,
APPACD,
APPRONO,
FILLER,
SBRNO,
FLAG,
WADAY,
SDG.UFN_MASK_ACCT(GRPACNO),
RSLPDAY,
RLSFLAG,
LITR,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
