import re
import sys
from collections import Counter

def normalize_sql(sql, logger=print):
    logger("Normalizing SQL...")
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    logger("SQL normalized successfully")
    return sql.upper()

def extract_statements(sql, logger=print):
    logger("Extracting SQL statements...")
    keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE', 'DROP', 'CALL', 'EXEC', 'TRUNCATE']
    counts = Counter()
    for kw in keywords:
        pattern = r'\b{}\b'.format(kw)
        counts[kw] = len(re.findall(pattern, sql))
    logger("SQL statements extracted successfully")
    return counts

def extract_tables(sql, logger=print):
    logger("Extracting tables...")
    table_pattern = r'\b(?:FROM|JOIN|INTO|UPDATE|MERGE\s+INTO|DELETE\s+FROM)\s+([A-Z0-9_.]+)'
    tables = set(re.findall(table_pattern, sql))
    logger("Tables extracted successfully")
    return tables

def extract_columns(sql, logger=print):
    logger("Extracting columns...")
    col_patterns = [
        r'SELECT\s+(.*?)\s+FROM',
        r'INSERT\s+INTO\s+[A-Z0-9_.]+\s*\((.*?)\)',
        r'UPDATE\s+[A-Z0-9_.]+\s+SET\s+(.*?)\s+(?:WHERE|;)',
        r'ON\s+(.*?)\s+(?:AND|OR|WHERE|;)',
        r'WHERE\s+(.*?)\s+(?:GROUP|ORDER|HAVING|UNION|;)',
    ]
    columns = set()
    for pat in col_patterns:
        for match in re.findall(pat, sql, flags=re.DOTALL):
            for col in re.split(r',|\s|\(|\)|=|<|>|!', match):
                col = col.strip()
                if col and not col.upper() in ['AND', 'OR', 'NOT', 'NULL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'DISTINCT', 'COUNT', 'SUM', 'MIN', 'MAX', 'AVG']:
                    if '.' in col:
                        col = col.split('.')[-1]
                    col = re.sub(r'\(.*\)', '', col)
                    if col and re.match(r'^[A-Z0-9_]+$', col):
                        columns.add(col)
    logger("Columns successfully extracted")
    return columns

def extract_procedure_calls(sql, logger=print):
    logger("Extracting procecure calls...")
    proc_pattern = r'\b(?:CALL|EXEC(?:UTE)?)\s+([A-Z0-9_.]+)'
    logger("Extracted procedure calls successfully")
    return set(re.findall(proc_pattern, sql))

def analyze_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_sql = f.read()
    sql = normalize_sql(raw_sql)
    return {
        'statements': extract_statements(sql),
        'tables': extract_tables(sql),
        'columns': extract_columns(sql),
        'procedures': extract_procedure_calls(sql)
    }

def highlight_diff(set1, set2, logger=print):
    missing = set1 - set2
    extra = set2 - set1
    return missing, extra

def compare_analysis(a1, a2, logger=print):
    logger("Comparing analyses of differences...")
    def fmt_set(s): return ', '.join(sorted(s)) if s else '(none)'
    report = []

    # Statements
    report.append("=== SQL Statement Counts ===")
    for k in sorted(set(a1['statements']) | set(a2['statements'])):
        v1 = a1['statements'].get(k,0)
        v2 = a2['statements'].get(k,0)
        report.append(f"{k}: {v1} vs {v2}")
        if v1 != v2:
            report.append(f"  Difference: {k} count is {'higher' if v1 > v2 else 'lower'} in File 1")
            logger(f"  Difference: {k} count is {'higher' if v1 > v2 else 'lower'} in File 1 \n")

    # Tables
    report.append("\n=== Table References ===")
    logger("\n=== Table References ===")
    
    report.append(f"File 1 ({len(a1['tables'])}): {fmt_set(a1['tables'])}")
    logger(f"File 1 ({len(a1['tables'])}): {fmt_set(a1['tables'])}")
    
    report.append(f"File 2 ({len(a2['tables'])}): {fmt_set(a2['tables'])}")
    logger(f"File 2 ({len(a2['tables'])}): {fmt_set(a2['tables'])}\n")
    
    missing1, extra1 = highlight_diff(a1['tables'], a2['tables'])
    if missing1:
        report.append(f"  Tables in File 1 but not in File 2: {fmt_set(missing1)}")
        logger(f"  Tables in File 1 but not in File 2: {fmt_set(missing1)}")
    if extra1:
        report.append(f"  Tables in File 2 but not in File 1: {fmt_set(extra1)}")
        logger(f"  Tables in File 2 but not in File 1: {fmt_set(extra1)}\n")

    # Columns
    report.append("\n=== Column References ===")
    logger("\n=== Column References ===")
    
    report.append(f"File 1 ({len(a1['columns'])}): {fmt_set(a1['columns'])}")
    logger(f"File 1 ({len(a1['columns'])}): {fmt_set(a1['columns'])}")
    
    report.append(f"File 2 ({len(a2['columns'])}): {fmt_set(a2['columns'])}")
    logger(f"File 2 ({len(a2['columns'])}): {fmt_set(a2['columns'])}\n")
    
    missing2, extra2 = highlight_diff(a1['columns'], a2['columns'])
    if missing2:
        report.append(f"  Columns in File 1 but not in File 2: {fmt_set(missing2)}")
        logger(f"  Columns in File 1 but not in File 2: {fmt_set(missing2)}")
    if extra2:
        report.append(f"  Columns in File 2 but not in File 1: {fmt_set(extra2)}")
        logger(f"  Columns in File 2 but not in File 1: {fmt_set(extra2)}\n")

    # Procedures
    report.append("\n=== Procedure/Function Calls ===")
    logger("\n=== Procedure/Function Calls ===")
    
    report.append(f"File 1 ({len(a1['procedures'])}): {fmt_set(a1['procedures'])}")
    logger(f"File 1 ({len(a1['procedures'])}): {fmt_set(a1['procedures'])}")
    
    report.append(f"File 2 ({len(a2['procedures'])}): {fmt_set(a2['procedures'])}")
    logger(f"File 2 ({len(a2['procedures'])}): {fmt_set(a2['procedures'])}\n")
    
    missing3, extra3 = highlight_diff(a1['procedures'], a2['procedures'])
    if missing3:
        report.append(f"  Procedures in File 1 but not in File 2: {fmt_set(missing3)}")
        logger(f"  Procedures in File 1 but not in File 2: {fmt_set(missing3)}")
    if extra3:
        report.append(f"  Procedures in File 2 but not in File 1: {fmt_set(extra3)}")
        logger(f"  Procedures in File 2 but not in File 1: {fmt_set(extra3)}\n")

    return '\n'.join(report)

# if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Usage: python compare_sql_scripts.py <file1> <file2>")
#         sys.exit(1)
#     file1 = sys.argv[1]
#     file2 = sys.argv[2]
#     analysis1 = analyze_file(file1)
#     analysis2 = analyze_file(file2)
#     print(compare_analysis(analysis1, analysis2))
