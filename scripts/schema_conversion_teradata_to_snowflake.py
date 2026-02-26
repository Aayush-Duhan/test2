import pandas as pd
import os
import logging
import re
import difflib
import json

def get_logger_for_file(filename,log_dir='logs'):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times
    if not logger.hasHandlers():
        filename=filename.split('.')[0]
        log_filepath = os.path.join(log_dir, f"{filename}.log")
        fh = logging.FileHandler(log_filepath)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

def process_sql_with_pandas_replace(csv_file_path, sql_file_path, output_dir,logg=None):
    df = pd.read_csv(csv_file_path)
    summary_data=dict()
    for filename in os.listdir(sql_file_path):
        summary_file_data=[]
        if filename.endswith(('.sql','.btq','.ddl')):
            print(filename)
            logger = get_logger_for_file(filename)
            logger.info(f"Started processing {filename} \n")
            logg(f"Started processing {filename} \n")
            file_path = os.path.join(sql_file_path, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                original_sql = f.read()
                before_change_file=original_sql
            os.makedirs(output_dir, exist_ok=True)
            total_num_matches=0
            total_num_replacements=0
            for idx, row in df.iterrows():
                    #old_db = row['SOURCE_DB']
                    old_schema = row['SOURCE_SCHEMA']
                    new_db_schema = row['TARGET_DB_SCHEMA']
                    

                    # 2️⃣ Replace schema-only names carefully
                    # Only replace if it's followed by a dot (i.e., schema.table)
                    schema_pattern = rf'\b{old_schema}\b(?=\.)'
                    schema_matches = re.findall(schema_pattern, original_sql, flags=re.IGNORECASE)
                    schema_num_matches = len(schema_matches)
                    total_num_matches=total_num_matches+schema_num_matches
                    original_sql,schema_num_replacements = re.subn(schema_pattern, new_db_schema, original_sql, flags=re.IGNORECASE)
                    total_num_replacements=total_num_replacements+schema_num_replacements

            summary_file_data.append(f"Name of the filename : {filename}")
            summary_file_data.append(f"No of places changes expected : {total_num_matches}")
            after_change_file=original_sql
            # Step 1: Split both procedures into lines
            before_proc_lines = before_change_file.strip().splitlines()
            after_proc_lines = after_change_file.strip().splitlines()
            
            # Step 2: Use difflib to show differences
            diff = difflib.unified_diff(before_proc_lines, after_proc_lines, fromfile='before_change_file', tofile='after_change_file', lineterm='')
            
            
            # Step 3: Extract only the replaced lines (before/after pairs)
            before_lines = []
            after_lines = []

            for line in diff:
                if line.startswith('-') and not line.startswith('---'):
                    before_lines.append(line[1:].strip())
                elif line.startswith('+') and not line.startswith('+++'):
                    after_lines.append(line[1:].strip())

            sp_count=0
            inside_db_count=0
            # Step 4: Zip them into pairs (assumes 1-to-1 line replacement)
            for before, after in zip(before_lines, after_lines):
                #print("Before:", before)
                #print("After :", after)
                #print("---")
                logger.info(f"Before: {before}\n")
                logg(f"Before: {before}\n")
                logger.info(f"After: {after}\n")
                logg(f"After: {after}\n")
                SP_STRING="REPLACE PROCEDURE"
                if (SP_STRING  in before) and  (SP_STRING in after):
                    if  before!=after:
                        sp_count=sp_count+1
                        if "DB_NOT_FOUND.SCHEMA_NOT_FOUND" in after:
                            logger.info("SP Database not found and Schema not found in cross walk \n")  
                            logg("SP Database not found and Schema not found in cross walk \n") 
                            logger.info("SP DB Change: NO\n")
                            logg("SP DB Change: NO\n")
                            total_num_replacements=total_num_replacements-1  
                            summary_file_data.append("SP DB Change: NO") 
                        else:
                            logger.info("SP DB Change: YES\n")
                            logg("SP DB Change: YES\n")
                            summary_file_data.append("SP DB Change: YES")                     
                else:
                    if before!=after:
                        inside_db_count=inside_db_count+1
                        if "DB_NOT_FOUND.SCHEMA_NOT_FOUND" in after:
                            logger.info("Inside code Database not found and Schema not found in cross walk \n") 
                            logger.info("Inside the code DB Change: NO\n")
                            logg("Inside code Database not found and Schema not found in cross walk \n") 
                            logg("Inside the code DB Change: NO\n")
                            total_num_replacements=total_num_replacements-1 
                            #summary_file_data.append("Inside the code DB Change: NO")
                        else:
                            logger.info("Inside the code DB Change: YES\n")
                            logg("Inside the code DB Change: YES\n")
                            #summary_file_data.append("Inside the code DB Change: YES")
            if sp_count==0:
                logger.info("SP DB Change: NO\n")
                logg("SP DB Change: NO\n")
                summary_file_data.append("SP DB Change: NO")
                if (inside_db_count==0):
                    logger.info("Inside the code DB Change: NO\n")
                    logg("Inside the code DB Change: NO\n")
                    #summary_file_data.append("Inside the code DB Change: NO")
            else:
                if (inside_db_count==0):
                    logger.info("Inside the code DB Change: NO\n")
                    logg("Inside the code DB Change: NO\n")
                    #summary_file_data.append("Inside the code DB Change: NO")
            
            if total_num_matches!=total_num_replacements:
                logger.info("In SP or Inside code Database not found and Schema not found in cross walk,please check file \n")
                logg("In SP or Inside code Database not found and Schema not found in cross walk,please check file \n")
            summary_file_data.append(f"No of places changes implemented: {total_num_replacements}")
            #unique_summary_file_data=list(set(summary_file_data))
            summary_data[filename]= summary_file_data      
            logger.info(f"Name of the file {filename}\n")
            logger.info(f"No of places where changes are required {total_num_matches}\n")
            logger.info(f"No of places where changes are implemented {total_num_replacements}\n")
            logger.info(f"Finished processing {filename}\n")
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

    summary_json_file_name="summary.json"
    #with open(summary_log_file_name, 'w', encoding='utf-8') as log_file:
        #log_file.write(f"Summary Data : {summary_data}")
    # Write dictionary to JSON file
    with open(summary_json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(summary_data, json_file, indent=4)

