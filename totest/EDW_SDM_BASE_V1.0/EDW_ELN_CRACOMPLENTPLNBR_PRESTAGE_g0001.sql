-- SQL for table: EDW_ELN_CRACOMPLENTPLNBR_PRESTAGE
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
QRYIDBAN,
LREL_DATE,
LENTP_DEPART,
LENTP_PID,
LENTP_TBID,
LENTP_ENTPNO,
LENTP_RATIO,
LENTP_POSNO,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
CASEID,
CRRPTNO,
CASE WHEN LENGTH(QRYIDBAN) = 8 THEN SDG.UFN_MASK_BAN(QRYIDBAN) WHEN LENGTH(QRYIDBAN) = 10 THEN SDG.UFN_MASK_IDN(QRYIDBAN) END,
LREL_DATE,
LENTP_DEPART,
CASE WHEN LENGTH(LENTP_PID) = 8 THEN SDG.UFN_MASK_BAN(LENTP_PID) WHEN LENGTH(LENTP_PID) = 10 THEN SDG.UFN_MASK_IDN(LENTP_PID) END,
SDG.UFN_MASK_IDN(LENTP_TBID),
SDG.UFN_MASK_BAN(LENTP_ENTPNO),
LENTP_RATIO,
SDG.UFN_MASK_INFO(LENTP_POSNO),
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
