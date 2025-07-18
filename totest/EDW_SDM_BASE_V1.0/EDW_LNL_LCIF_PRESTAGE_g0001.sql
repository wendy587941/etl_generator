-- SQL for table: EDW_LNL_LCIF_PRESTAGE
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
ENTPNO,
FILLER,
PIDERR,
ACTCLS,
GRADE,
GRDT,
CTLCD,
CTLDT,
TRACEDT,
RELDT,
MLINVAMT,
MLINCOME,
MLBRNO,
MLDT,
ACCHK,
INCOME,
STOT,
SHBTOT,
SERVICECMP,
TRPFLG,
TRPRGDT,
TRPCLDT,
TRPIXPR,
TRPACT,
IRCDDT,
IRCD,
FITIRT,
IRID,
IXPR,
IXID,
EXTEND,
LIFLAG,
LISDAY,
LIEDAY,
CLLSBL,
APPDAY,
APPRSN,
CTLNAP,
BRNO,
CSDATE,
DATA_UPDATE_TIME,
DATA_EXCH_DATE
)
SELECT
SDG.UFN_MASK_BAN(ENTPNO),
FILLER,
PIDERR,
ACTCLS,
GRADE,
GRDT,
CTLCD,
CTLDT,
TRACEDT,
RELDT,
MLINVAMT,
MLINCOME,
MLBRNO,
MLDT,
ACCHK,
INCOME,
STOT,
SHBTOT,
SDG.UFN_MASK_NM(SERVICECMP),
TRPFLG,
TRPRGDT,
TRPCLDT,
TRPIXPR,
TRPACT,
IRCDDT,
IRCD,
FITIRT,
IRID,
IXPR,
IXID,
EXTEND,
LIFLAG,
LISDAY,
LIEDAY,
CLLSBL,
APPDAY,
APPRSN,
CTLNAP,
BRNO,
CSDATE,
CURRENT_TIMESTAMP,
to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
FROM ${SOURCE_TABLE_OWNER}.${SOURCE_TABLE}
WHERE ${DATA_LIFE_BASE} = to_date('${BD_EXCH_DATE}', 'yyyy-MM-dd')
;

.export @LOAD_ROWCNT updatecnt;
