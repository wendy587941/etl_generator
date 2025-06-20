-- SQL for table: EDW_CR_BCTL_STAGE
/* TRANSFORM_DATA */

/* 1. Delete 目的表 Stage data_exch_date = '#排程啟動日#' */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取 Prestage 同期別進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
RGNO,
AREA,
BRNO,
FLAG,
BOFF,
MICR,
FREX,
RTOLBR,
RCLTM,
RU1TM,
DCLTM,
DU1TM,
BRNAME,
AFCBVD,
AFCBVL,
AFCBVF,
AFCBVR1,
AFCBVR2,
AFCBVR3,
PREFIX_1,
PREFIX_2,
MONBR,
MAINCD,
XRATE,
LASTDT,
CLASS,
SEQ,
CHNAM,
MNGNAM,
ACTENAM,
ARCD,
ZIPCD,
ADR,
LMNGNO,
LMNGNAM,
LEMPNO,
LEMPNAM,
CHCD,
BQCLS,
CKBR,
BINBR,
SEDCKCLS,
EKPFIX,
EMAIL,
TELNO,
NSEQNO,
CSEQNO,
SNAME,
IPADR,
OLINTCD,
GOLDCD,
ENTPNO,
TAXNO,
ENTPNAME,
GOLD,
CITY,
AFCBVL1,
SBRNO,
ACFLG,
ACDATE,
BRLVL,
BRCLS,
BRCLSEQ,
BRCLSTM,
DEPTCLS_1,
DEPTCLS_2,
DEPTCLS_3,
DEPTCLS_4,
DEPTCLS_5,
DEPTCLS_6,
DEPTCLS_7,
DEPTCLS_8,
DEPTCLS_9,
DEPTCLS_10,
DEPTCLSEQ_1,
DEPTCLSEQ_2,
DEPTCLSEQ_3,
DEPTCLSEQ_4,
DEPTCLSEQ_5,
DEPTCLSEQ_6,
DEPTCLSEQ_7,
DEPTCLSEQ_8,
DEPTCLSEQ_9,
DEPTCLSEQ_10,
DEPTCLSTM_1,
DEPTCLSTM_2,
DEPTCLSTM_3,
DEPTCLSTM_4,
DEPTCLSTM_5,
DEPTCLSTM_6,
DEPTCLSTM_7,
DEPTCLSTM_8,
DEPTCLSTM_9,
DEPTCLSTM_10,
TUNE,
SUMTYPE,
ACDFG1,
ACDFG2,
ACDFG3,
BUGFG,
GOLDFG,
BRFG,
TLRCASH,
RECLMT,
PAYLMT,
FRECLMT,
FPAYLMT,
UFLAG1,
UFLAG2,
CARDFLG,
TREGION,
LOCATE,
LOCATECD,
HSTAXNO,
FBRNAME,
SBRNAME,
BISFLAG,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
RGNO,
AREA,
BRNO,
FLAG,
BOFF,
MICR,
FREX,
RTOLBR,
RCLTM,
RU1TM,
DCLTM,
DU1TM,
BRNAME,
AFCBVD,
AFCBVL,
AFCBVF,
AFCBVR1,
AFCBVR2,
AFCBVR3,
PREFIX_1,
PREFIX_2,
MONBR,
MAINCD,
XRATE,
LASTDT,
CLASS,
SEQ,
CHNAM,
MNGNAM,
ACTENAM,
ARCD,
ZIPCD,
ADR,
LMNGNO,
LMNGNAM,
LEMPNO,
LEMPNAM,
CHCD,
BQCLS,
CKBR,
BINBR,
SEDCKCLS,
EKPFIX,
EMAIL,
TELNO,
NSEQNO,
CSEQNO,
SNAME,
IPADR,
OLINTCD,
GOLDCD,
ENTPNO,
TAXNO,
ENTPNAME,
GOLD,
CITY,
AFCBVL1,
SBRNO,
ACFLG,
ACDATE,
BRLVL,
BRCLS,
BRCLSEQ,
BRCLSTM,
DEPTCLS_1,
DEPTCLS_2,
DEPTCLS_3,
DEPTCLS_4,
DEPTCLS_5,
DEPTCLS_6,
DEPTCLS_7,
DEPTCLS_8,
DEPTCLS_9,
DEPTCLS_10,
DEPTCLSEQ_1,
DEPTCLSEQ_2,
DEPTCLSEQ_3,
DEPTCLSEQ_4,
DEPTCLSEQ_5,
DEPTCLSEQ_6,
DEPTCLSEQ_7,
DEPTCLSEQ_8,
DEPTCLSEQ_9,
DEPTCLSEQ_10,
DEPTCLSTM_1,
DEPTCLSTM_2,
DEPTCLSTM_3,
DEPTCLSTM_4,
DEPTCLSTM_5,
DEPTCLSTM_6,
DEPTCLSTM_7,
DEPTCLSTM_8,
DEPTCLSTM_9,
DEPTCLSTM_10,
TUNE,
SUMTYPE,
ACDFG1,
ACDFG2,
ACDFG3,
BUGFG,
GOLDFG,
BRFG,
TLRCASH,
RECLMT,
PAYLMT,
FRECLMT,
FPAYLMT,
UFLAG1,
UFLAG2,
CARDFLG,
TREGION,
LOCATE,
LOCATECD,
HSTAXNO,
FBRNAME,
SBRNAME,
BISFLAG,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
