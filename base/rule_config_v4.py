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
    # Regex-like key for "與*合併後正規化"
    # This mapping is for reference; actual regex matching logic must be implemented in get_trans_rule.
    ("與(.*)(合併|組合)後日期正規化", None): "SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM({EXTRA})||' '||TRIM({COL}), '')='' THEN NULL WHEN SDG.UFN_ADDATECHECK_DATE(TRIM({EXTRA})||' '||TRIM({COL}))='0' THEN NULL ELSE TRIM({EXTRA})||' '||TRIM({COL}) END) AS {COL}",
    ("與(.*)(合併|組合)後日期正規化", "Y"): "SDG.UFN_NORM_DATE(CASE WHEN NVL(TRIM({EXTRA})||' '||TRIM({COL}), '')='' THEN '0001-01-01' WHEN SDG.UFN_ADDATECHECK_DATE(TRIM({EXTRA})||' '||TRIM({COL}))='0' THEN '0001-01-01' ELSE TRIM({EXTRA})||' '||TRIM({COL}) END) AS {COL}"
    # Add more mappings as needed
}

def get_trans_rule(rule, col=None, nullable=None):
    """
    Get the transformation rule by rule name and nullable flag.
    If nullable is not None, try to match (rule, nullable) first, then (rule, None).
    Also supports regex-like keys in transformation_mapping (keys starting with 'REGEX:').
    """
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