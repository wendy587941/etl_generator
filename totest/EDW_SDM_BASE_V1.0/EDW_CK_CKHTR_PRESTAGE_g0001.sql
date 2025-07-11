-- SQL for table: EDW_CK_CKHTR_PRESTAGE
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
SDG.UFN_MASK_ACCT_SN(SRNO),
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
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
