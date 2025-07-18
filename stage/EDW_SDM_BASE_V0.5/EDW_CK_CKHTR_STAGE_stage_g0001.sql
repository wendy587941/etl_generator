-- SQL for table: EDW_CK_CKHTR_STAGE
/* TRANSFORM_DATA */

/* 1. Delete 目的表 Stage data_exch_date = '#排程啟動日#' */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取 Prestage 同期別進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
BRNO,
APNO,
SRNO,
CHKDG,
YEAR,
ENTDAY,
TXTIME,
KINBR,
TRMSEQ,
FILLER1,
TLRNO,
DSCPT,
CRDB,
TXAMT,
TAXAMT,
TXDAY,
HCODE,
CKNO,
FILLER2,
DSPTYPE,
DSPTKD,
DSPTEXT,
TXTYPE,
TRNMOD,
BDTXD,
COKEY,
COBKNO,
TRMTYP,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BRNO,
APNO,
SRNO,
CHKDG,
YEAR,
ENTDAY,
TXTIME,
KINBR,
TRMSEQ,
FILLER1,
TLRNO,
DSCPT,
CRDB,
TXAMT,
TAXAMT,
TXDAY,
HCODE,
CKNO,
FILLER2,
DSPTYPE,
DSPTKD,
DSPTEXT,
TXTYPE,
TRNMOD,
BDTXD,
COKEY,
COBKNO,
TRMTYP,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
