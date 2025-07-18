-- SQL for table: EDW_CT_CTHMR_PRESTAGE
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
APNO,
SRNO,
CHKDG,
YYMM,
TCHGCD,
CHGCD,
OPENH,
ACTCLS,
ACTMVO,
ACTMVI,
CHGDP,
CTRVS,
CTRI,
ACTSTP,
CRTHLD,
MRTGE,
BRHLD,
GPACT,
CURCD,
CHARCD,
IRTCD,
TAXCD,
IRTID,
PRDCD,
CTNO,
RXCNT,
AXCNT,
NIRTID,
BCODE,
EIDERR,
ENTPNO,
PIDERR,
PIDNO,
DAMT,
DCNT,
CAMT,
CCNT,
EXPER,
OACTFLG,
U3VCD,
YDBAL,
LMPRB,
CTBAL,
FILLER2,
LASTDT,
OPDAY,
CLSDAY,
ISDAY,
EDAY,
IRDAY,
IVDAY,
XISDAY,
EMPRID,
PAYOID,
LINECD,
"TIME",
SENDFG,
TFLAG,
CONNACT,
FLR,
SBRNO,
IRTSQ,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BRNO,
APNO,
SDG.UFN_MASK_ACCT_SN(SRNO),
CHKDG,
YYMM,
TCHGCD,
CHGCD,
OPENH,
ACTCLS,
ACTMVO,
ACTMVI,
CHGDP,
CTRVS,
CTRI,
ACTSTP,
CRTHLD,
MRTGE,
BRHLD,
GPACT,
CURCD,
CHARCD,
IRTCD,
TAXCD,
IRTID,
PRDCD,
CTNO,
RXCNT,
AXCNT,
NIRTID,
BCODE,
EIDERR,
SDG.UFN_MASK_BAN(ENTPNO),
PIDERR,
SDG.UFN_MASK_IDN(PIDNO),
DAMT,
DCNT,
CAMT,
CCNT,
EXPER,
OACTFLG,
U3VCD,
YDBAL,
LMPRB,
CTBAL,
FILLER2,
LASTDT,
OPDAY,
CLSDAY,
ISDAY,
EDAY,
IRDAY,
IVDAY,
XISDAY,
EMPRID,
PAYOID,
LINECD,
"TIME",
SENDFG,
TFLAG,
CONNACT,
FLR,
SBRNO,
IRTSQ,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
