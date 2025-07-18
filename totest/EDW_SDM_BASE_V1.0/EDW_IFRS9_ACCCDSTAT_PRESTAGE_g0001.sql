-- SQL for table: EDW_IFRS9_ACCCDSTAT_PRESTAGE
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
DATA_YM,
VER_NO,
BRN_ID,
BRN_NM,
ACC_CD_CATEG,
ACC_CD_ALL,
ACC_CD_TYP,
ACC_CD_NAME,
ACC_CD_BAL,
NO_AMORTIZE_DISC_AMT,
NO_AMORTIZE_ADD_AMT,
IA_BAL,
IA_RATE,
LS_MON_IA_BAL,
IA_BAL_DIFF,
NPL1_ACC_CD_AMT,
NPL2_ACC_CD_AMT,
NPL3_ACC_CD_AMT,
NPL4_ACC_CD_AMT,
NPL5_ACC_CD_AMT,
NPL_ACC_CD_AMT,
NPL_RATIO,
NPL_BAL_AMT_DIFF,
CAL_DATE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
DATA_YM,
VER_NO,
BRN_ID,
BRN_NM,
ACC_CD_CATEG,
ACC_CD_ALL,
ACC_CD_TYP,
ACC_CD_NAME,
ACC_CD_BAL,
NO_AMORTIZE_DISC_AMT,
NO_AMORTIZE_ADD_AMT,
IA_BAL,
IA_RATE,
LS_MON_IA_BAL,
IA_BAL_DIFF,
NPL1_ACC_CD_AMT,
NPL2_ACC_CD_AMT,
NPL3_ACC_CD_AMT,
NPL4_ACC_CD_AMT,
NPL5_ACC_CD_AMT,
NPL_ACC_CD_AMT,
NPL_RATIO,
NPL_BAL_AMT_DIFF,
CAL_DATE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
