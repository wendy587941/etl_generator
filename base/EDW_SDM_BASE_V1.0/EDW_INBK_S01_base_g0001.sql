-- SQL for table: EDW_INBK_S01
/* CHECK_DATA */

-- Trinity SQL Script
INSERT INTO ${TARGET_SCHEMA}.${TASK_NAME}_CHK
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'DD' AS DATA_CHK_COL,
DD AS DATA_VAL,
SDG.UFN_NUMCHECK(DD) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'MSGFLOW' AS DATA_CHK_COL,
MSGFLOW AS DATA_VAL,
SDG.UFN_NUMCHECK(MSGFLOW) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'FILLER1' AS DATA_CHK_COL,
FILLER1 AS DATA_VAL,
SDG.UFN_NUMCHECK(FILLER1) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'UPDFLG' AS DATA_CHK_COL,
UPDFLG AS DATA_VAL,
SDG.UFN_NUMCHECK(UPDFLG) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'PENDING' AS DATA_CHK_COL,
PENDING AS DATA_VAL,
SDG.UFN_NUMCHECK(PENDING) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'TMOUT' AS DATA_CHK_COL,
TMOUT AS DATA_VAL,
SDG.UFN_NUMCHECK(TMOUT) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'RMHC' AS DATA_CHK_COL,
RMHC AS DATA_VAL,
SDG.UFN_NUMCHECK(RMHC) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'CDMODE' AS DATA_CHK_COL,
CDMODE AS DATA_VAL,
SDG.UFN_NUMCHECK(CDMODE) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'TXAMT' AS DATA_CHK_COL,
TXAMT AS DATA_VAL,
SDG.UFN_NUMCHECK(TXAMT) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'FIL1' AS DATA_CHK_COL,
FIL1 AS DATA_VAL,
SDG.UFN_NUMCHECK(FIL1) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0'
UNION ALL
SELECT *
FROM (
SELECT
CAST(ROWID AS VARCHAR2(100)) AS CHK_ID,
'num_err' AS DATA_CHK_TP,
'LOGNO' AS DATA_CHK_COL,
LOGNO AS DATA_VAL,
SDG.UFN_NUMCHECK(LOGNO) AS CHECK_RESULT
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE} 
WHERE DATA_EXCH_DATE = TO_DATE('${TXDATE}', 'YYYY-MM-DD')
) S1
WHERE S1.CHECK_RESULT = '0';

/* need to modify above 記得要有;號*/
DELETE FROM SDG.SDG_DATA_ERR_TBL WHERE TAB_NM='${TARGET_TABLE}' AND GROUP_CD='${EXEC_GCD}' AND BD_EXCH_DATE=TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd') AND BD_EXCH_TIME='${BD_EXCH_TIME}';

INSERT INTO SDG.SDG_DATA_ERR_TBL
SELECT
'${TARGET_TABLE}' AS TAB_NM,
'${EXEC_GCD}' AS GROUP_CD,
TO_DATE('${BD_EXCH_DATE}', 'YYYY-MM-DD') AS BD_EXCH_DATE,
'${BD_EXCH_TIME}' AS BD_EXCH_TIME,
C1.DATA_CHK_TP,
C1.DATA_CHK_COL,
C1.DATA_VAL,
S1.SRC_PK,
CURRENT_TIMESTAMP AS UPDATE_DATETIME
FROM
(
SELECT * FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK
) C1
LEFT JOIN 
(
SELECT CAST(ROWID AS VARCHAR2(100)) AS CHK_ID , ${SQL_STMT} AS SRC_PK FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
) S1
ON C1.CHK_ID=S1.CHK_ID
;

-- SQL for table: EDW_INBK_S01
/* TRANSFORM_DATA */

-- 異動方式待補/待調整，以下目前為公版
/* 1. 刪除Base DATA_EXCH_DATE='#資料交換期別#' 之資料 */
DELETE FROM ${TARGET_SCHEMA}.${TARGET_TABLE} WHERE DATA_EXCH_DATE = TO_DATE('${BD_EXCH_DATE}', 'yyyy-MM-dd');

-- Trinity SQL Script
/* 2. 抽取Stage DATA_EXCH_DATE = '#資料交換期別#'之資料進行新增 */
INSERT INTO ${TARGET_SCHEMA}.${TARGET_TABLE}
(
MSGKIND,
BKNO,
DD,
STAN,
MSGFLOW,
FILLER1,
UPDFLG,
PENDING,
TMOUT,
BUSDT,
ERRNO,
RMHC,
CDMODE,
FISCDT,
BRNO,
TXDT,
TXTM,
DBK,
DBR,
SBK,
SBR,
TXAMT,
RTNCD,
MT,
PC,
FIL1,
FIL2,
LOGNO,
ACTNO,
RC_1,
RC_2,
RC_3,
RC_4,
ER_1,
ER_2,
ER_3,
ER_4,
DATA,
CARDNO,
ACTKIND,
VWD,
COMMDATA,
RECNO_1,
RECNO_2,
RECNO_3,
RECNO_4,
RECNO_5,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
MSGKIND,
BKNO,
DD,
STAN,
MSGFLOW,
FILLER1,
UPDFLG,
PENDING,
TMOUT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(BUSDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(BUSDT))='0' THEN NULL ELSE BUSDT END) AS BUSDT,
ERRNO,
RMHC,
CDMODE,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(FISCDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(FISCDT))='0' THEN NULL ELSE FISCDT END) AS FISCDT,
BRNO,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDT), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDT))='0' THEN NULL ELSE TXDT END) AS TXDT,
SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM(TXDT)||' '||TRIM(TXTM), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM(TXDT)||' '||TRIM(TXTM))='0' THEN NULL ELSE TRIM(TXDT)||' '||TRIM(TXTM) END) AS TXTM,
DBK,
DBR,
SBK,
SBR,
TXAMT,
RTNCD,
MT,
PC,
FIL1,
FIL2,
LOGNO,
ACTNO,
RC_1,
RC_2,
RC_3,
RC_4,
ER_1,
ER_2,
ER_3,
ER_4,
DATA,
CARDNO,
ACTKIND,
VWD,
COMMDATA,
RECNO_1,
RECNO_2,
RECNO_3,
RECNO_4,
RECNO_5,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE DATA_EXCH_DATE = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
AND CAST(ROWID AS VARCHAR2(100)) NOT IN (SELECT DISTINCT CHK_ID FROM ${TARGET_SCHEMA}.${TASK_NAME}_CHK WHERE DATA_CHK_TP LIKE '%err');

.export @LOAD_ROWCNT updatecnt;
