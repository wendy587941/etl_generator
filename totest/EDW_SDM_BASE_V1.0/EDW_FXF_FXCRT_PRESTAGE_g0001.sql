-- SQL for table: EDW_FXF_FXCRT_PRESTAGE
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
BCURCD,
CURCD,
MRKTDT,
MRKTTM,
MRKTCD,
RCNT,
BRATS,
SRATS,
BCASH,
SCASH,
BRAT1,
BRAT3,
BRAT6,
BRAT9,
BRAT12,
BRAT15,
BRAT18,
BRAT1Y,
BRAT2Y,
BRAT3Y,
BRAT4Y,
BRAT5Y,
SRAT1,
SRAT3,
SRAT6,
SRAT9,
SRAT12,
SRAT15,
SRAT18,
SRAT1Y,
SRAT2Y,
SRAT3Y,
SRAT4Y,
SRAT5Y,
USCRAT,
FXRAT,
ACRAT,
LMTB,
LMTS,
KINBR,
TLRNO,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
BCURCD,
CURCD,
MRKTDT,
MRKTTM,
MRKTCD,
RCNT,
BRATS,
SRATS,
BCASH,
SCASH,
BRAT1,
BRAT3,
BRAT6,
BRAT9,
BRAT12,
BRAT15,
BRAT18,
BRAT1Y,
BRAT2Y,
BRAT3Y,
BRAT4Y,
BRAT5Y,
SRAT1,
SRAT3,
SRAT6,
SRAT9,
SRAT12,
SRAT15,
SRAT18,
SRAT1Y,
SRAT2Y,
SRAT3Y,
SRAT4Y,
SRAT5Y,
USCRAT,
FXRAT,
ACRAT,
LMTB,
LMTS,
KINBR,
TLRNO,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
