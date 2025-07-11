-- SQL for table: EDW_FXE_FADMR_STAGE
/* TRANSFORM_DATA */

/* 1. Delete 目的表 Stage data_exch_date = '#排程啟動日#' */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取 Prestage 同期別進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
YEAR,
APTYPE,
BRNO,
SRNO,
OBUFG,
RBRNO,
ACBRNO,
KINBR,
ISUBK,
ISWFCD,
LCNO,
CIFKEY,
CORPNO,
NAME1,
NAME2,
ADDR1,
ADDR2,
ITNO,
OTRNO,
LASTDT,
TXDAY,
ADVDAY,
ISUDAY,
EXPDAY,
SHPDAY,
RCLDAY,
CTXDAY,
SWFDAY,
CURCD,
LCAMT,
LTAMT,
NCHGAMT,
CCHGAMT,
CHGAMT,
ACRAT,
TRCNT,
LTCNT,
ADVCD,
MRTCD,
LOSFG,
MODCNT,
TRFCD,
IBRNO,
OBRNO,
TRNFG,
OTRFG,
CHGFG,
GBRNO,
AUTO,
BKCODE,
BKSWFCD,
CUTYPE,
TXTNO,
WSNO,
TLRNO,
CONNO,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
YEAR,
APTYPE,
BRNO,
SRNO,
OBUFG,
RBRNO,
ACBRNO,
KINBR,
ISUBK,
ISWFCD,
LCNO,
CIFKEY,
CORPNO,
NAME1,
NAME2,
ADDR1,
ADDR2,
ITNO,
OTRNO,
LASTDT,
TXDAY,
ADVDAY,
ISUDAY,
EXPDAY,
SHPDAY,
RCLDAY,
CTXDAY,
SWFDAY,
CURCD,
LCAMT,
LTAMT,
NCHGAMT,
CCHGAMT,
CHGAMT,
ACRAT,
TRCNT,
LTCNT,
ADVCD,
MRTCD,
LOSFG,
MODCNT,
TRFCD,
IBRNO,
OBRNO,
TRNFG,
OTRFG,
CHGFG,
GBRNO,
AUTO,
BKCODE,
BKSWFCD,
CUTYPE,
TXTNO,
WSNO,
TLRNO,
CONNO,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
