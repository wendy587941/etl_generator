import pandas as pd
import os
import warnings
from multiprocessing import Pool, Value, Lock, Manager
from rule_config_v1 import *
from sql_template_v1 import *
import time
import sys
from openpyxl import load_workbook

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
pd.options.mode.chained_assignment = None  # Suppress pandas SettingWithCopyWarning


# Add these global variables at the top (after imports)
file_counter = None
file_counter_lock = None
VERSION = None 
VER = None
TARGET_SHEET = None 
OUTPUT_DIR = None
SDM = None


def set_version():
    global VERSION 
    global VER 
    global TARGET_SHEET
    global OUTPUT_DIR 
    global SDM    

    if len(sys.argv) > 1:
        VERSION = sys.argv[1]
    else:
        VERSION = input("Please enter the version (e.g., V0.6): ").strip() or "V0.6" 
    
    VER = VERSION.replace('.', '_')
    SDM = f'EDW_SDM_BASE_{VERSION}.ods'
    #INDEX_SHEET = f"基礎資料表總表{VER}"
    #SOURCE_SHEET = f'Stage資料欄位對應{VER}'
    TARGET_SHEET = f'ToUAT資料欄位對應{VER}'
    OUTPUT_DIR = f'EDW_SDM_BASE_{VERSION}'


def init_counter(counter, lock):
    global file_counter
    global file_counter_lock
    file_counter = counter
    file_counter_lock = lock


def load_data():
    """Load data from Excel sheets."""
    print(f"Reading SDM Excel file: {SDM}")
    #df = pd.read_excel(SDM, sheet_name=[SOURCE_SHEET, TARGET_SHEET, INDEX_SHEET], dtype=str)
    # For faster ODS reading, use 'odfpy' engine with pandas >=2.0 (if installed)
    try:
        df = pd.read_excel(SDM, sheet_name=[TARGET_SHEET], dtype=str, engine="odf")
    except Exception as e:
        print(f"Falling back to default engine due to: {e}")
        df = pd.read_excel(SDM, sheet_name=[TARGET_SHEET], dtype=str)
 
    for sheet in df:
        print(f"Loaded sheet: {sheet} (rows: {len(df[sheet])})")
    #return df[INDEX_SHEET], df[SOURCE_SHEET], df[TARGET_SHEET]
    return df[TARGET_SHEET]

def filter_index_rows(index_df):
    """Filter rows from the index sheet."""
    return index_df[
        (index_df['ETL目錄'] == "TOUAT") &
        (index_df['刪除註記'] != 'Y') &
        (index_df['刪除註記'] != 'X') #20250516 增加X
        #& (index_df['更新狀態(版號/日期/更新人員/更新描述)'].str.startswith(f"[{VERSION}]")) ##20250514 - 取消版號檢查
    ]

def process_filtered_rows(filtered_rows):
    """Process filtered rows to generate SQL content."""
    filtered_rows['資料型態'] = filtered_rows['資料型態'].str.strip().str.upper()
    filtered_rows['資料欄位名稱'] = filtered_rows['資料欄位名稱'].str.strip()
    filtered_rows['通用資料型態'] = filtered_rows['資料型態'].apply(lambda x: 'VARCHAR' if 'VARCHAR' in x else x)
    filtered_rows['Mapped_Function'] = filtered_rows.apply(
        lambda row: get_function_by_variables(
            row['ETL檢核類型'], row['通用資料型態']
        ) if row['ETL檢核類型'][:2] == 'C1' else get_function_by_variables(row['ETL檢核類型']),
        axis=1
    )
    return filtered_rows

def generate_sql_for_table(args):
    """Generate SQL for a single table."""
    table_name, group, audit_tables, OUTPUT_DIR = args
    sql_content_list = []

    for _, row in group.iterrows():
        
        audit_cd = row['ETL檢核類型'].split('.')[-1]
        col_nm = quote_col_nm(row['資料欄位名稱'].strip().upper())

        rule = row['Mapped_Function']
        sql_content = sql.format(audit_cd=audit_cd, col_nm=col_nm, rule=rule)
        sql_content_list.append(sql_content)

    final_sql_content = "\nUNION ALL\n".join(sql_content_list)
    file_content = f"-- SQL for table: {table_name}\n{sql_head}\n{final_sql_content}{sql_tail}\n"

    # Write to file
    with open(os.path.join(OUTPUT_DIR, f'{table_name}_g0001.sql'), 'w', encoding='utf-8') as f:
        f.write(file_content + '\n')
    
    return table_name


def process_transform_rows(data_row, audit_tables, OUTPUT_DIR):
    """Process transform rows and generate SQL content."""

    # 20250516 增加不為空值判斷與新的日期正規化轉換處理
    data_row['不為空值'] = data_row['不為空值'].apply(lambda x: 'Y' if str(x).strip() == 'Y' else '')
    data_row['資料欄位名稱'] = data_row['資料欄位名稱'].str.strip()
    # If '資料欄位名稱' equals to 'date'/'DATE' or starts with 0-9, add quote
    data_row['資料欄位名稱'] = data_row['資料欄位名稱'].apply(quote_col_nm)

    # 只有當 '轉換規則' 不是 NaN 或空字串時才呼叫 get_trans_rule
    data_row['Transform_Function'] = data_row.apply(
        lambda row: get_trans_rule(
            row['轉換規則'],
            row['資料欄位名稱'],
            row['不為空值'] if '不為空值' in row else None
        ) if pd.notna(row['轉換規則']) and str(row['轉換規則']).strip() != '' else None,
        axis=1
    )

    data_row['Transform_Function'] = data_row['Transform_Function'].fillna(
        data_row['轉換規則'].str.strip().str.upper()
    ).fillna(data_row['資料欄位名稱'].str.strip().str.upper())

    grouped_rows = data_row.groupby('資料表名稱')
    for table_name, group in grouped_rows:
        sql_content_list = [f"{row['Transform_Function']},\n" for _, row in group.iterrows()]
        final_sql_content = "".join(sql_content_list).rstrip(",\n")

        col_names = ",\n".join(group['資料欄位名稱'].tolist())
        trans_sql_head_fmt = trans_sql_head.format(col_names=col_names)

        if table_name in audit_tables:
            file_content = f"-- SQL for table: {table_name}\n{trans_sql_head_fmt}\n{final_sql_content}\n{trans_sql_tail}{check_sql}\n\n{trans_sql_tail_2}"
        else:
            file_content = f"-- SQL for table: {table_name}\n{trans_sql_head_fmt}\n{final_sql_content}\n{trans_sql_tail}{non_check_sql}\n\n{trans_sql_tail_2}"

        with open(os.path.join(OUTPUT_DIR, f'{table_name}_g0001.sql'), 'a', encoding='utf-8') as f:
            f.write(file_content + '\n')

        # Progress display
        if file_counter is not None and file_counter_lock is not None:
            with file_counter_lock:
                file_counter.value += 1
                print(f"Generated {file_counter.value} files: {table_name}_g0001.sql")

        else:
            print(f"Generated file: {table_name}_g0001.sql")

def main(OUTPUT_DIR):
    #index_df, stage_df, base_df = load_data()
    #index_df, base_df = load_data()
    #index_row = filter_index_rows(index_df)
    #table_list = index_row['目的資料表名稱'].tolist()
    #20250522 暫時取消從總表取清單
    base_df = load_data()
    #data_row = base_df[(base_df['資料表名稱'].isin(table_list)) & (~base_df['刪除註記'].isin(['X', 'Y']))]
    data_row = base_df[(~base_df['刪除註記'].isin(['X', 'Y']))]

    # 20250516 Replace full-width space (\u3000) with empty string in 'ETL檢核類型'
    data_row['ETL檢核類型'] = data_row['ETL檢核類型'].astype(str).str.replace('\u3000', '', regex=False).replace('nan', '', regex=False)
    num_workers = 4
    
    if not data_row.empty:
        # Always process transform rows for every table
        table_names = data_row['資料表名稱'].unique().tolist()
        audit_tables = []
        print(f"Number of tables to be processed: {len(table_names)}")

        # Only process filtered rows (audit) for tables that need it
        filtered_rows = data_row[data_row['ETL檢核類型'].notna() & (data_row['ETL檢核類型'] != '')]
        if not filtered_rows.empty:
            filtered_rows = process_filtered_rows(filtered_rows)
            grouped_rows = filtered_rows.groupby('資料表名稱')

            print(f"Number of audit tables: {len(grouped_rows)}")
            #for table_name, group in grouped_rows:
            #    generate_sql_for_table((table_name, group, []))
            # Use multiprocessing to generate SQL files in parallel
            with Pool(processes=num_workers) as pool:
                audit_tables = pool.map(generate_sql_for_table, [(table_name, group, [], OUTPUT_DIR) for table_name, group in grouped_rows])

        manager = Manager()
        counter = manager.Value('i', 0)
        lock = manager.Lock()

        #process_transform_rows(data_row, audit_tables)
        # Use multiprocessing to generate SQL files in parallel for transform rows
        print(f"Start file generation for transform rows...")
        with Pool(processes=num_workers, initializer=init_counter, initargs=(counter, lock)) as pool:
            pool.starmap(process_transform_rows, [(data_row[data_row['資料表名稱'] == table_name], audit_tables, OUTPUT_DIR) for table_name in table_names])

    else:
        print("No Table was found.")
      


if __name__ == "__main__":
    start_time = time.time()

    set_version()
    print(f"VERSION: {VERSION}")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)   

    main(OUTPUT_DIR)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")