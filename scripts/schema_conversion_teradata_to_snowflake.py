import pandas as pd
import os
import logging
import re
import difflib
import json
import uuid


def get_logger_for_file(filename, log_dir='logs'):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times
    if not logger.hasHandlers():
        filename = filename.split('.')[0]
        log_filepath = os.path.join(log_dir, f"{filename}.log")
        fh = logging.FileHandler(log_filepath)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


def _apply_schema_mapping_to_sql(original_sql, mapping_rows):
    """Apply schema mapping to SQL text using marker-based replacement.

    This function replaces schema-qualified identifiers (SCHEMA.TABLE or
    SCHEMA.TABLE.COLUMN) without corrupting multi-part references like
    those found in COMMENT ON COLUMN statements.

    Strategy:
    1. Separate mapping rows into *schema* entries and *table* entries by
       checking whether the SOURCE_SCHEMA value appears as a schema prefix
       (i.e. followed by a dot + identifier) in the SQL.
    2. For schema entries, match only the leading schema portion of a
       dot-qualified name (SCHEMA.TABLE or SCHEMA.TABLE.COLUMN) and
       replace it with the target. A negative look-behind prevents
       matching a name that is itself preceded by a dot (which means
       it's a table or column part, not a schema).
    3. Use UUID placeholder markers so that text inserted by one
       replacement cannot be re-matched by a later replacement row.
    4. After all rows are processed, substitute the markers with their
       final values.

    Args:
        original_sql: The SQL text to transform.
        mapping_rows: List of (old_schema, new_db_schema) tuples from the CSV.

    Returns:
        Tuple of (transformed_sql, total_matches, total_replacements).
    """
    if not mapping_rows:
        return original_sql, 0, 0

    sql = original_sql
    total_matches = 0
    total_replacements = 0

    # --- Phase 1: classify entries ----------------------------------------
    # A "schema entry" is one whose SOURCE_SCHEMA appears in the SQL as a
    # schema prefix, i.e.  SCHEMA.something  (word followed by a dot).
    # Everything else is treated as a "table/object entry".
    schema_entries = []
    table_entries = []

    for old_schema, new_db_schema in mapping_rows:
        # Check if this entry appears as a schema prefix in the original SQL
        probe = rf'\b{re.escape(old_schema)}\b\s*\.'
        if re.search(probe, original_sql, flags=re.IGNORECASE):
            schema_entries.append((old_schema, new_db_schema))
        else:
            table_entries.append((old_schema, new_db_schema))

    # Sort longest-first to prevent partial matches (e.g. "CUSTOMER_ADDRESSES"
    # before "CUSTOMER").
    schema_entries.sort(key=lambda x: len(x[0]), reverse=True)
    table_entries.sort(key=lambda x: len(x[0]), reverse=True)

    # Dict mapping placeholder → final replacement text
    markers = {}

    def _make_marker(replacement_text):
        """Create a unique marker that cannot collide with SQL text."""
        marker = f"__SMAP_{uuid.uuid4().hex}__"
        markers[marker] = replacement_text
        return marker

    # --- Phase 2: replace schema entries ----------------------------------
    # Match  SCHEMA.TABLE  or  SCHEMA.TABLE.COLUMN  but only the schema part.
    # Negative look-behind (?<!\.) ensures we don't match a name that is
    # already after a dot (i.e. a table or column component).
    for old_schema, new_db_schema in schema_entries:
        pattern = rf'(?<!\.)(\b{re.escape(old_schema)}\b)(?=\s*\.)'
        matches = re.findall(pattern, sql, flags=re.IGNORECASE)
        num_matches = len(matches)
        total_matches += num_matches

        if num_matches > 0:
            marker = _make_marker(new_db_schema)
            sql, num_replacements = re.subn(pattern, marker, sql, flags=re.IGNORECASE)
            total_replacements += num_replacements

    # --- Phase 3: replace table entries -----------------------------------
    # Table entries are standalone identifiers that are NOT preceded by a dot
    # (which would mean they're already part of a qualified name that was
    # handled in phase 2).  They may or may not be followed by a dot.
    for old_table, new_table_ref in table_entries:
        pattern = rf'(?<!\.)(\b{re.escape(old_table)}\b)'
        matches = re.findall(pattern, sql, flags=re.IGNORECASE)
        num_matches = len(matches)
        total_matches += num_matches

        if num_matches > 0:
            marker = _make_marker(new_table_ref)
            sql, num_replacements = re.subn(pattern, marker, sql, flags=re.IGNORECASE)
            total_replacements += num_replacements

    # --- Phase 4: resolve markers -----------------------------------------
    for marker, final_value in markers.items():
        sql = sql.replace(marker, final_value)

    return sql, total_matches, total_replacements


def process_sql_with_pandas_replace(csv_file_path, sql_file_path, output_dir, logg=None):
    df = pd.read_csv(csv_file_path)
    summary_data = dict()

    # Build the list of mapping rows once
    mapping_rows = []
    for _, row in df.iterrows():
        old_schema = str(row['SOURCE_SCHEMA']).strip()
        new_db_schema = str(row['TARGET_DB_SCHEMA']).strip()
        if old_schema and new_db_schema:
            mapping_rows.append((old_schema, new_db_schema))

    for filename in os.listdir(sql_file_path):
        summary_file_data = []
        if filename.endswith(('.sql', '.btq', '.ddl')):
            print(filename)
            logger = get_logger_for_file(filename)
            logger.info(f"Started processing {filename} \n")
            if logg:
                logg(f"Started processing {filename} \n")
            file_path = os.path.join(sql_file_path, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                original_sql = f.read()
                before_change_file = original_sql

            os.makedirs(output_dir, exist_ok=True)

            # --- Apply the mapping ----------------------------------------
            result_sql, total_num_matches, total_num_replacements = (
                _apply_schema_mapping_to_sql(original_sql, mapping_rows)
            )
            original_sql = result_sql

            summary_file_data.append(f"Name of the filename : {filename}")
            summary_file_data.append(f"No of places changes expected : {total_num_matches}")
            after_change_file = original_sql

            # Diff logging (same as before)
            before_proc_lines = before_change_file.strip().splitlines()
            after_proc_lines = after_change_file.strip().splitlines()
            diff = difflib.unified_diff(
                before_proc_lines, after_proc_lines,
                fromfile='before_change_file', tofile='after_change_file',
                lineterm='',
            )

            before_lines = []
            after_lines = []
            for line in diff:
                if line.startswith('-') and not line.startswith('---'):
                    before_lines.append(line[1:].strip())
                elif line.startswith('+') and not line.startswith('+++'):
                    after_lines.append(line[1:].strip())

            sp_count = 0
            inside_db_count = 0
            for before, after in zip(before_lines, after_lines):
                logger.info(f"Before: {before}\n")
                if logg:
                    logg(f"Before: {before}\n")
                logger.info(f"After: {after}\n")
                if logg:
                    logg(f"After: {after}\n")
                SP_STRING = "REPLACE PROCEDURE"
                if (SP_STRING in before) and (SP_STRING in after):
                    if before != after:
                        sp_count += 1
                        if "DB_NOT_FOUND.SCHEMA_NOT_FOUND" in after:
                            logger.info("SP Database not found and Schema not found in cross walk \n")
                            if logg:
                                logg("SP Database not found and Schema not found in cross walk \n")
                            logger.info("SP DB Change: NO\n")
                            if logg:
                                logg("SP DB Change: NO\n")
                            total_num_replacements -= 1
                            summary_file_data.append("SP DB Change: NO")
                        else:
                            logger.info("SP DB Change: YES\n")
                            if logg:
                                logg("SP DB Change: YES\n")
                            summary_file_data.append("SP DB Change: YES")
                else:
                    if before != after:
                        inside_db_count += 1
                        if "DB_NOT_FOUND.SCHEMA_NOT_FOUND" in after:
                            logger.info("Inside code Database not found and Schema not found in cross walk \n")
                            logger.info("Inside the code DB Change: NO\n")
                            if logg:
                                logg("Inside code Database not found and Schema not found in cross walk \n")
                                logg("Inside the code DB Change: NO\n")
                            total_num_replacements -= 1
                        else:
                            logger.info("Inside the code DB Change: YES\n")
                            if logg:
                                logg("Inside the code DB Change: YES\n")

            if sp_count == 0:
                logger.info("SP DB Change: NO\n")
                if logg:
                    logg("SP DB Change: NO\n")
                summary_file_data.append("SP DB Change: NO")
                if inside_db_count == 0:
                    logger.info("Inside the code DB Change: NO\n")
                    if logg:
                        logg("Inside the code DB Change: NO\n")
            else:
                if inside_db_count == 0:
                    logger.info("Inside the code DB Change: NO\n")
                    if logg:
                        logg("Inside the code DB Change: NO\n")

            if total_num_matches != total_num_replacements:
                logger.info("In SP or Inside code Database not found and Schema not found in cross walk,please check file \n")
                if logg:
                    logg("In SP or Inside code Database not found and Schema not found in cross walk,please check file \n")

            summary_file_data.append(f"No of places changes implemented: {total_num_replacements}")
            summary_data[filename] = summary_file_data
            logger.info(f"Name of the file {filename}\n")
            logger.info(f"No of places where changes are required {total_num_matches}\n")
            logger.info(f"No of places where changes are implemented {total_num_replacements}\n")
            logger.info(f"Finished processing {filename}\n")
            if logg:
                logg(f"Name of the file {filename}\n")
                logg(f"No of places where changes are required {total_num_matches}\n")
                logg(f"No of places where changes are implemented {total_num_replacements}\n")
                logg(f"Finished processing {filename}\n")

            if filename.endswith(".btq"):
                filename = filename.replace(".btq", ".sql")
            elif filename.endswith(".ddl"):
                filename = filename.replace(".ddl", ".sql")

            output_file = os.path.join(output_dir, f'{filename}')
            with open(output_file, 'w', encoding='utf-8') as out_f:
                out_f.write(original_sql)

            print(f"Saved updated SQL to {output_file}")

    summary_json_file_name = "summary.json"
    with open(summary_json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(summary_data, json_file, indent=4)
