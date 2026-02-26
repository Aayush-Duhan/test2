import re
import textwrap
import streamlit as st
import os
import sys
from snowflake.snowpark.exceptions import SnowparkSQLException
import snowflake.connector
from snowflake.snowpark import Session

sys.path.append(os.path.abspath("./../../"))



from snowflake.snowpark.exceptions import SnowparkSQLException



# DATABASE = "CENTENE_POC"
# SCHEMA = "CENTENE_POC"
 
# connection_parameters = {
#         "account": "EYGDS-LND_DNA_AZ_USE2",
#         "user": "KOMMULA.SREECHARAN@CTPSANDBOX.COM",
#         "authenticator": "externalbrowser",
#         "role": "EY_DNA_SANDBOX_ROLE_CENTENE_POC_RW",
#         "warehouse": "WH_CENTENE_POC_XS",
#         "database": DATABASE,
#         "schema": SCHEMA
#     }
 
# def snowflake_connector():
#      return snowflake.connector.connect(**connection_parameters)

# session = Session.builder.configs(connection_parameters).create()


def resolve_proc_schema(session, db_name, proc_name):
    df = session.sql(f"""
        SELECT procedure_schema
        FROM {db_name}.INFORMATION_SCHEMA.PROCEDURES
        WHERE procedure_name = '{proc_name.upper()}'
        ORDER BY created DESC
        LIMIT 1
    """).to_pandas()

    if not df.empty:
        return df.iloc[0]["PROCEDURE_SCHEMA"]
    return None

def qualify_call_statements(session, db_name, sql_text):
    pattern = r"\bCALL\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\("
    
    def replacer(match):
        proc_name = match.group(1)
        schema = resolve_proc_schema(session, db_name, proc_name)
        if schema:
            return f"CALL {db_name}.{schema}.{proc_name}("
        else:
            # fallback to default schema from connection
            return f"CALL {db_name}.CENTENE_POC.{proc_name}("
    
    return re.sub(pattern, replacer, sql_text, flags=re.IGNORECASE)


def fix_procedure_with_cortex(session,type, original_code, error_msg, db_name, schema_name,file_path=None, model='claude-4-sonnet'):
    if type=='procedure':
        prompt= f"""
                Consider you are a code correction Agent, The following code for a Snowflake Snowpark stored procedure failed with this error: {error_msg}
                Fix the code to resolve the error.
                Key rules:
                    - First understand in which language the code is written. It can be SQL/python/Java Scripts.
                    - Keep ALL existing schema names exactly as they are in the original code
                    - Do NOT change any schema names (like ETL_TRANSFORM_OWN, CRM_STAGE_OWN, etc.)
                    - ALWAYS use fully qualified names in EVERY SQL statement for tables and other objects (e.g., CREATE OR REPLACE TRANSIENT TABLE {db_name}.{schema_name}.temp_table ...; SELECT * FROM {db_name}.{schema_name}.temp_table). NEVER use unqualified table names like 'temp_table' alone—always include {db_name}.{schema_name}.
                    - If required create temp table to solve the complexities of sql.
                    - Output only the code, no extras no suggestion no explanation, no extra wrapper like '''sql.
                    - error msg is the key to solve the error.
                    - **Important** There is an update statement where the table name is not mentioned just the alias is mentioned like update tgt set col1='text' from db.schame.tbl1,db.schame.tbl2 as tgt where ... then rewrite the update statement as per snowflake standards if there is an error in the update statement "Object 'TGT' does not exist or not authorized." rewrite the update statement as per snowflake standards.
                    - **Important** any part of the code which is starting with "--" or "*/" or "/*" do not consider for code correctio e.g "-- ********"
                    - **Important** the fixed code has to be snowflake compatible.
                    - if it has @param then the sp should take those param as argument and replace by the variable.
                Original Code:
                    {original_code}"""
    elif type=='bteq':
         prompt= f"""
                Consider you are a code convertor Agent, helping us to convert a teradata bteq script to Snowflake Snowpark stored procedure without any error.
                
                Key rules:
                    - The input is the first level of converted script from raw bteq by snowconvert tool. 
                    - First understand in which language the code is written. It can be SQL/python/Java Scripts.
                    - Do NOT change any schema names (like ETL_TRANSFORM_OWN, CRM_STAGE_OWN, etc.)
                    - If required create temp table to solve the complexities of sql.
                    - Output only the code, no extras no suggestion no explanation, no extra wrapper like '''sql.
                    - **Important** if there is some issue with code and you as conversion agent trying to convert it, then you should convert full code and not just some lines.It should not be the case that for similar line of code you mention  ",... (rest of the columns remain same)" or "-- Rest of the complex SELECT query remains same with schema update--" or "        -- Source subquery with formatting logic        -- Keeping existing source query structure". The complete code line by line should be fixed.
                    - **Important** if there is a stored procedure call in the body and it is not commented out, don't skip it mentioning "Since the original code only shows a procedure call without actual logic    -- You would need to implement the actual business logic here". Pass the call as is in the converted code as well.
                    - **Important** the script can have sugession, hint ot comments from snowconvert. Please take the sql statements for conversion and others you can use as a context.
                    - **Important** access if the code is syntactically and programmatically correct to be a snowflake compatible stored procedure.If not do the modification to make it snowflake compatible stred procedure.
                    - **Important** for procedure name take the inference from the file name i.e.{file_path}.just take take the name don't include any special character like . or /
                    - if it has @param then the sp should take those param as argument and replace by the variable.
                Original Code:
                    {original_code}"""
    #result = session.sql(f""" SELECT snowflake.cortex.complete('{model}', '{prompt.replace("'", "''")}')""").collect()
    safe_prompt = prompt.replace('$$', '')  
    cortex_query = f"""
        select ou['choices'][0]['messages']::varchar from (SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            [
                {{
                    'role': 'user',
                    'content': $${safe_prompt}$$
                }}
            ],
            {{
                'temperature': 0.1,
                'max_tokens': 32000
            }}
        ) as ou)
        """
    result = session.sql(cortex_query).collect()    
    print(result)

    if result:
        fixed_code = result[0][0].strip()
        # Enhanced cleanup: Extract code from markdown block if present
        code_match =re.search(r'```(?:sql|python|javascript)?\s*(.*?)```', fixed_code, re.DOTALL | re.IGNORECASE)
        if code_match:
            fixed_code = code_match.group(1).strip()
        cleaned_code = textwrap.dedent(fixed_code)
        return cleaned_code
    else:
        raise ValueError("Cortex completion failed.")

def execute_stored_procedure_with_retries(session, db_name, schema_name, proc_name,file_path=None, deploy_attempts=1,execution_attempt=1,max_attempts=2,
                                            results: dict = None,
                                            errors: dict = None
                                            ):
    
    if results is None:
        results = {}
    #if errors is None:
    errors = {}
    current_proc = proc_name
    if 'bteq' in file_path.lower() and deploy_attempts==1:
        type='bteq'
        proc_name = qualify_call_statements(session, db_name, proc_name)
        fixed_code=fix_procedure_with_cortex(session,type, proc_name, "", db_name, schema_name,file_path)
        # return fixed_code
        return execute_stored_procedure_with_retries(
                    session,
                    db_name,
                    schema_name,
                    fixed_code,
                    "",
                    deploy_attempts ,
                    execution_attempt,
                    max_attempts,
                    results,
                    ""
                )
    else:
        type='procedure'
    ### deployment attempt block ####
        try:
            print(f"Deployment Attempt {deploy_attempts}: Executing {current_proc}")  # Use logging if needed for production
            result = session.sql(proc_name).collect()

           
            import json 
            if result:
                # exeution attempt block ###
                try:
                    print(f"Execution Attempt {execution_attempt}: Executing {current_proc}")  # Use logging if needed for production
                    laterst_proc_name=session.sql(f"SELECT concat(PROCEDURE_CATALOG,'.',PROCEDURE_SCHEMA,'.',PROCEDURE_NAME) FROM {db_name}.INFORMATION_SCHEMA.PROCEDURES ORDER BY CREATED DESC LIMIT 1").to_pandas().values[0][0]
                    result = session.sql(f"CALL {laterst_proc_name}();").collect()[0][0]
                    if result:
                        result=json.loads(result)
                        if result.get("SQLCODE") is not None and result["SQLCODE"] != 0:
                            error_msg = f"{result['SQLCODE']} | {result['SQLERRM']}"
                            fixed_code = fix_procedure_with_cortex(session, type, proc_name, error_msg, db_name, schema_name,"")
                            if execution_attempt<=max_attempts:
                                return execute_stored_procedure_with_retries(
                                    session,
                                    db_name,
                                    schema_name,
                                    fixed_code,
                                    "",
                                    deploy_attempts,
                                    execution_attempt + 1,
                                    max_attempts,
                                    result,
                                    error_msg
                                    )
                            else:
                                return {
                                        "Execution Attempt": execution_attempt,
                                        "results": result,
                                        "errors": error_msg
                                    }
                        else:
                            return {
                                        "Execution Attempt": execution_attempt,
                                        "results": result,
                                        "errors": errors
                                    }
                    else:
                        return {
                                    "Execution Attempt": execution_attempt,
                                    "results": result,
                                    "errors": errors
                                }
                        
                    # return {
                    #             "Execution Attempt": execution_attempt,
                    #             "results": result,
                    #             "errors": errors
                    #         }
                except SnowparkSQLException as e:
                    error_msg=str(e)
                    print(f"SQL Error: {e}")
                    fixed_code = fix_procedure_with_cortex(session,type, proc_name, error_msg, db_name, schema_name,file_path)
                    if execution_attempt<=max_attempts:
                        return execute_stored_procedure_with_retries(
                                        session,
                                        db_name,
                                        schema_name,
                                        fixed_code,
                                        "",
                                        deploy_attempts,
                                        execution_attempt + 1,
                                        max_attempts,
                                        result,
                                        error_msg
                                    )
                except json.JSONDecodeError as e:
                            # print(f"No error JSON found. Proceeding normally. Details: {e}")
                            return result
        except SnowparkSQLException as e:
            error_msg=str(e)
            print(f"SQL Error: {e}")
            fixed_code = fix_procedure_with_cortex(session,type, proc_name, error_msg, db_name, schema_name,file_path)
            print(fixed_code)
            if deploy_attempts<=max_attempts:
                # new_version = f"{proc_name}_v0_{attempts}"
                # create_new_procedure(session, db_name, schema_name, new_version, fixed_code)
                # versions.append(new_version)
                return execute_stored_procedure_with_retries(
                    session,
                    db_name,
                    schema_name,
                    fixed_code,
                    "",
                    deploy_attempts + 1,
                    execution_attempt,
                    max_attempts,
                    results,
                    error_msg
                )
            else:
                errors[current_proc] = error_msg
                print("Max attempts reached. Aborting.")
                return {
                    "Deployment Attempts": deploy_attempts,
                    "Execution Attempt": execution_attempt,
                    "results": results,
                    "errors": errors
                }        
            # new_version = f"{proc_name}_v0.{attempt}"
            # create_new_procedure(session, db_name, schema_name, new_version, fixed_code)
            # versions.append(new_version)
            # current_proc = new_version
        except Exception as e:
            print(f"Unexpected Error: {e}")
            return {
                    "Deployment Attempts": deploy_attempts,
                    "Execution Attempt": execution_attempt,
                    "results": results,
                    "errors": e
                }

# For stored procedures or worksheets: Define the entry point
def main(session, db_name='CENTENE_POC', schema_name='CENTENE_POC', proc_name='DIM_CRM_CALL_DIRECTION_BUILD',file_path=None):
    # Customize parameters here or pass via CALL arguments
    return execute_stored_procedure_with_retries(session, db_name, schema_name, proc_name,file_path)
def read_file_as_string(file_path):
    """
    Reads the content of a file and returns it as a string.
    
    Args:
        file_path (str): Path to the file.
    
    Returns:
        str: Content of the file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
            content = content.lstrip("\ufeff")
        return content
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None
    
def extract_database(file_path):
     with open(file_path, 'r', encoding='utf-8-sig') as f:
            pattern = r'^CREATE OR REPLACE PROCEDURE\s+([^.]+)\.[^.]+\.[^(]+\(\)'
            for line in f:
                line = line.strip()
                match = re.match(pattern, line)
                if match:
                    database_name = match.group(1)
                    database_names=database_name
                    break
                else:
                    database_names=None
     return database_names

def remove_enclosed_strings(text):
    # Pattern to match everything between !!!RESOLVE EWI!!! and ***/!!! including the markers
    pattern = r'!!!RESOLVE EWI!!!.*?\*\*\*/!!!'
    cleaned_text = re.sub(pattern, '', text, flags=re.DOTALL)
    return cleaned_text


def run_self_healing_from_ui(file_path,sf_session):
    content = remove_enclosed_strings(read_file_as_string(file_path))
    database =extract_database(file_path)
    if content and database:
        session=sf_session
        result = main(session, db_name=database, schema_name='CENTENE_POC', proc_name=content, file_path=file_path)
        return result
    elif content and database is None:
        session=sf_session
        result = main(session, db_name='CENTENE_POC', schema_name='CENTENE_POC', proc_name=content, file_path=file_path)
        return result
    else:
        raise ValueError("File is empty or unreadable.")
