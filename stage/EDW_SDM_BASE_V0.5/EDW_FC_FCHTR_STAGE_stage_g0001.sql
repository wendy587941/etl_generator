-- SQL for table: EDW_FC_FCHTR_STAGE
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
BRNO,
APNO,
SRNO,
CHKDG,
ENTDAY,
MTRCNT,
TXDAY,
TXTIME,
KINBR,
TRMSEQ,
TXTNO,
TLRNO,
APTYPE,
TXNO,
STXNO,
DSCPT,
HCODE,
CRDB,
CURCD,
TXAMT,
AVBAL,
NBCD,
EMPNOT,
EMPNOS,
RBRNO,
FITIRT,
FXRT,
DINUM,
RINTAMT,
PINTAMT,
GINT,
TAXAMT,
TGINT,
TTAXAMT,
TRNAMT,
RFTAX,
RFINT,
ODINT,
ODDAY,
SWDCNT,
EWDCNT,
EXPER,
NOTE,
AFCLS,
CNTAMT,
ACTCLS,
ACTSTP,
DWCNT,
RGBNO,
TAXRT,
ITEM,
NDYMNTFG,
TMTAMT,
NMTAMT,
TRMTYP,
NHIAMT,
TNHIAMT,
COKEY,
COBKNO,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
MBRNO,
MAPNO,
MSRNO,
MCHKDG,
BRNO,
APNO,
SRNO,
CHKDG,
ENTDAY,
MTRCNT,
TXDAY,
TXTIME,
KINBR,
TRMSEQ,
TXTNO,
TLRNO,
APTYPE,
TXNO,
STXNO,
DSCPT,
HCODE,
CRDB,
CURCD,
TXAMT,
AVBAL,
NBCD,
EMPNOT,
EMPNOS,
RBRNO,
FITIRT,
FXRT,
DINUM,
RINTAMT,
PINTAMT,
GINT,
TAXAMT,
TGINT,
TTAXAMT,
TRNAMT,
RFTAX,
RFINT,
ODINT,
ODDAY,
SWDCNT,
EWDCNT,
EXPER,
NOTE,
AFCLS,
CNTAMT,
ACTCLS,
ACTSTP,
DWCNT,
RGBNO,
TAXRT,
ITEM,
NDYMNTFG,
TMTAMT,
NMTAMT,
TRMTYP,
NHIAMT,
TNHIAMT,
COKEY,
COBKNO,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
