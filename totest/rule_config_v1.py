import re

##[20250514][蔡雯欣][修改transformation_mapping 日期正規化(但未考慮是否可為空值)]

# Define a mapping that maps 1 or 2 variables to specific function names
audit_mapping = {
    ("C1.null_err", "NUMBER"): "SDG.UFN_NULLCHECK_NUM",
    ("C1.null_err", "VARCHAR"): "SDG.UFN_NULLCHECK_VAR",
    ("C1.null_err", "DATE"): "SDG.UFN_NULLCHECK_DATE",
    ("C3.date", None): "SDG.UFN_ISADDATE",  # Second variable can be anything
    ("C2.num_err", None): "SDG.UFN_NUMCHECK",  # Second variable can be anything
    ("C4.idn_err", None): "SDG.UFN_IDNCHECK",  # Second variable can be anything
    ("C4.ban_err", None): "SDG.UFN_BANCHECK",  # Second variable can be anything
    ("C4.idn_ban_err", None): "SDG.UFN_IDNBANCHECK"  # Second variable can be anything
    # Add more mappings as needed
}

# 日期不可為空 : SDG.UFN_NORM_DATE(CASE WHEN NVL(trim(SETTLE_DTE), '') = '' THEN '0001-01-01' WHEN SDG.ufn_addatecheck_date(TRIM(SETTLE_DTE)) = '0' THEN '0001-01-01' ELSE SETTLE_DTE END) AS SETTLE_DTE
# 日期可為空 : SDG.UFN_NORM_DATE(CASE WHEN NVL(trim(SETTLE_DTE), '') = '' THEN null  WHEN SDG.ufn_addatecheck_date(TRIM(SETTLE_DTE)) = '0' THEN null ELSE SETTLE_DTE END) AS SETTLE_DTE

# Define a mapping for transformation rules to corresponding functions
transformation_mapping = {
    ("日期正規化", None): "SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM({COL}), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM({COL}))='0' THEN NULL ELSE {COL} END) AS {COL}",  # 日期可為空
    ("日期正規化", "Y"): "SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM({COL}), '')='' THEN '0001-01-01' WHEN SDG.UFN_ADDATECHECK_DATE(TRIM({COL}))='0' THEN '0001-01-01' ELSE {COL} END) AS {COL}",  # 日期不可為空
    ("#系統日期時間#'::timestamp", None): "CURRENT_TIMESTAMP",
    ("#資料交換期別#'::date", None): "to_date('${{BD_EXCH_DATE}}', 'yyyy-MM-dd')",
    ("ME1.姓名/名稱", None): "SDG.UFN_MASK_NM({COL})",
    ("ME2.統一編號", None): "SDG.UFN_MASK_BAN({COL})",
    ("ME3.身份證號", None): "SDG.UFN_MASK_IDN({COL})",
    ("ME4.護照號碼", None): "SDG.UFN_MASK_PNO({COL})",
    ("ME5.出生年月日", None): "SDG.UFN_MASK_DATE({COL})",
    ("ME6.地址", None): "SDG.UFN_MASK_ADDR({COL})",
    ("ME7.電話", None): "SDG.UFN_MASK_TEL({COL})",
    ("ME8.電子郵件", None): "SDG.UFN_MASK_EMAIL({COL})",
    ("ME9.信用卡號", None): "SDG.UFN_MASK_CRD_CARD({COL})",
    ("ME10.銀行帳號", None): "SDG.UFN_MASK_ACCT({COL})",
    ("ME11.職稱", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME12.出生地", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME13.學歷", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME14.經歷", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME15.職業", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME16.暱稱", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME17.婚姻狀態", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME18.服務單位", None): "SDG.UFN_MASK_INFO({COL})",
    ("ME19.流水號", None): "SDG.UFN_MASK_ACCT_SN({COL})",
    # Regex-like key for "與*合併後正規化"
    # This mapping is for reference; actual regex matching logic must be implemented in get_trans_rule.
    ("與(.*)(合併|組合)後日期正規化", None): "SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM({EXTRA})||' '||TRIM({COL}), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM({EXTRA})||' '||TRIM({COL}))='0' THEN NULL ELSE TRIM({EXTRA})||' '||TRIM({COL}) END) AS {COL}",
    ("與(.*)(合併|組合)後日期正規化", "Y"): "SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM({EXTRA})||' '||TRIM({COL}), '')='' THEN '0001-01-01' WHEN SDG.UFN_ADDATECHECK_DATE(TRIM({EXTRA})||' '||TRIM({COL}))='0' THEN '0001-01-01' ELSE TRIM({EXTRA})||' '||TRIM({COL}) END) AS {COL}"
    # Add more mappings as needed
}

def parse_length_mask_rule(rule_text):
    """
    從規則文字中解析長度和遮罩類型
    例如: "若為8碼則使用ME2.統一編號, 若為10碼則使用ME3.身分證號"
    返回: [(8, 'ME2', 'SDG.UFN_MASK_BAN'), (10, 'ME3', 'SDG.UFN_MASK_IDN')]
    """
    if not rule_text:
        return []
    
    # 定義遮罩類型對應的函數
    mask_function_mapping = {
        'ME1': 'SDG.UFN_MASK_NM',
        'ME2': 'SDG.UFN_MASK_BAN',
        'ME3': 'SDG.UFN_MASK_IDN',
        'ME4': 'SDG.UFN_MASK_PNO',
        'ME5': 'SDG.UFN_MASK_DATE',
        'ME6': 'SDG.UFN_MASK_ADDR',
        'ME7': 'SDG.UFN_MASK_TEL',
        'ME8': 'SDG.UFN_MASK_EMAIL',
        'ME9': 'SDG.UFN_MASK_CRD_CARD',
        'ME10': 'SDG.UFN_MASK_ACCT',
        'ME11': 'SDG.UFN_MASK_INFO',
        'ME12': 'SDG.UFN_MASK_INFO',
        'ME13': 'SDG.UFN_MASK_INFO',
        'ME14': 'SDG.UFN_MASK_INFO',
        'ME15': 'SDG.UFN_MASK_INFO',
        'ME16': 'SDG.UFN_MASK_INFO',
        'ME17': 'SDG.UFN_MASK_INFO',
        'ME18': 'SDG.UFN_MASK_INFO',
        'ME19': 'SDG.UFN_MASK_ACCT_SN'
        # 可以繼續添加其他映射
    }
    
    # 正則表達式匹配模式：若為{數字}碼則使用{遮罩類型}
    pattern = r'若為(\d+)碼則使用(ME\d+)'
    matches = re.findall(pattern, rule_text)
    
    result = []
    for length_str, mask_type in matches:
        length = int(length_str)
        function_name = mask_function_mapping.get(mask_type, f'SDG.UFN_MASK_{mask_type}')
        result.append((length, mask_type, function_name))
    
    return result

def generate_case_when_sql(rule_text, column_name):
    """
    根據規則文字生成 CASE WHEN SQL
    """
    parsed_rules = parse_length_mask_rule(rule_text)
    
    if not parsed_rules:
        return None
    
    when_clauses = []
    for length, mask_type, function_name in parsed_rules:
        when_clauses.append(f"WHEN LENGTH({column_name}) = {length} THEN {function_name}({column_name})")
    
    return f"CASE {' '.join(when_clauses)} END"

def get_trans_rule(rule, col=None, nullable=None):
    """
    Get the transformation rule by rule name and nullable flag.
    If nullable is not None, try to match (rule, nullable) first, then (rule, None).
    Also supports regex-like keys in transformation_mapping (keys starting with 'REGEX:').
    """
    # === 新增：處理長度遮罩規則 ===
    if rule and '若為' in rule and '碼則使用' in rule:
        case_when_result = generate_case_when_sql(rule, col)
        if case_when_result:
            return case_when_result
    # === 新增結束 ===

    tpl = transformation_mapping.get((rule, nullable), None)
    if tpl is None:
        tpl = transformation_mapping.get((rule, None), None)
    if tpl and col:
        return tpl.format(COL=col)

    # Regex-like key support
    for (key, val), func in transformation_mapping.items():
        if key and isinstance(key, str):
            # Try to match rule with regex pattern if key looks like a regex pattern
            # Here, we treat keys containing regex meta-characters as regex patterns
            try:
                # Only process keys that look like regex patterns (contain '(' or other regex chars)
                if re.search(r'[\(\)\[\]\.\*\+\?\\]', key):
                    pattern = f"^{key}$"
                    match = re.match(pattern, rule)
                    if match:
                        # If matched, allow for group substitution if needed
                        # For example, {EXTRA} can be replaced with matched group(1) or group(0)
                        func_str = func
                        # If {EXTRA} is in func, replace with matched group(1) if exists
                        if '{EXTRA}' in func_str and match.groups():
                            func_str = func_str.replace('{EXTRA}', match.group(1).upper())
                        if col:
                            func_str = func_str.format(COL=col)
                        return func_str
            except Exception as e:
                print(f"Error in regex matching for key '{key}' and rule '{rule}': {e}")
    return None

# Function to get the corresponding function for a given transformation rule
#def get_trans_rule(rule):
#    return transformation_mapping.get(rule, None)

def quote_col_nm(col_nm):
    col_nm = str(col_nm).strip().upper()
    col_nm = col_nm.replace('\u3000', '')
    if col_nm.lower() in {'date', 'end', 'merge','lock','return','time', 'top', 'stop','usage','value','type', 'share', 'natural','source','new','limit'} \
        or (col_nm and col_nm[0].isdigit()):
        return f'"{col_nm}"'
    return col_nm

# Function to get the corresponding function for given variables
def get_function_by_variables(var1, var2=None):
    return audit_mapping.get((var1, var2), None)