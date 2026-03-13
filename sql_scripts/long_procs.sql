-- ======================================================================
-- LONG ORACLE STORED PROCEDURE SCRIPT (~430 lines)
-- ======================================================================
-- This file contains multiple stored procedures to test Oracle→Snowflake
-- conversion capabilities, including self-healing logic.
-- ======================================================================

--------------------------------------------------------------------------------
-- 1) Simple Audit Procedure (Basic; should convert mostly clean)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.AUDIT_LOG", "AUDIT_LOG_SEQ" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_AUDIT_INSERT (p_entity_name VARCHAR, p_entity_key VARCHAR, p_action VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
        v_msg VARCHAR(1000);
    BEGIN
        v_msg := 'Inserted Entity ' || NVL(p_entity_name :: STRING, '') || ' / ' || NVL(p_entity_key :: STRING, '');
        INSERT INTO DBMIG_POC.DBMIG_POC.AUDIT_LOG(
       AUDIT_ID,
       ENTITY_NAME,
       ENTITY_KEY,
       OPERATION,
       CHANGE_TS,
       CHANGED_BY,
       DETAILS
   )
   VALUES (
       AUDIT_LOG_SEQ.NEXTVAL,
       p_entity_name,
       p_entity_key,
       p_action, CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/, CURRENT_USER(),
       v_msg
   );
        --** SSC-FDM-OR0035 - CHECK UDF IMPLEMENTATION FOR DBMS_OUTPUT.PUT_LINE_UDP. **
        CALL DBMS_OUTPUT.PUT_LINE_UDP('Audit Added for ' || NVL(p_entity_key :: STRING, ''));  -- Needs removal

    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 2) Update Customer Status Procedure
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.CUSTOMERS", "DBMIG_POC.DBMIG_POC.AUDIT_LOG", "AUDIT_LOG_SEQ" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_UPDATE_CUSTOMER_STATUS (p_customer_id NUMBER(38, 18), p_new_status VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
        v_old_status VARCHAR(10);
    BEGIN
        SELECT STATUS_FLAG
          INTO v_old_status
          FROM
            DBMIG_POC.DBMIG_POC.CUSTOMERS
         WHERE CUSTOMER_ID = p_customer_id;

        UPDATE DBMIG_POC.DBMIG_POC.CUSTOMERS
           SET STATUS_FLAG = p_new_status,
               UPDATED_TS = CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/
         WHERE CUSTOMER_ID = p_customer_id;

        INSERT INTO DBMIG_POC.DBMIG_POC.AUDIT_LOG(
            AUDIT_ID,
            ENTITY_NAME,
            ENTITY_KEY,
            OPERATION,
            CHANGE_TS,
            CHANGED_BY,
            DETAILS
        )
        VALUES (
            AUDIT_LOG_SEQ.NEXTVAL,
            'CUSTOMER',
            p_customer_id,
            'STATUS_CHANGE', CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/, CURRENT_USER(),
            'Changed from ' || NVL(v_old_status :: STRING, '') || ' to ' || NVL(p_new_status :: STRING, '')
        );
        --** SSC-FDM-OR0035 - CHECK UDF IMPLEMENTATION FOR DBMS_OUTPUT.PUT_LINE_UDP. **
        CALL DBMS_OUTPUT.PUT_LINE_UDP('Customer updated: ' || NVL(p_customer_id :: STRING, ''));
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 3) Promote Customer Tier (uses %TYPE + CASE)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.CUSTOMERS", "DBMIG_POC.DBMIG_POC.ORDERS" **
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
        v_total_spend NUMBER(38, 18) := 0;
        v_new_tier VARCHAR(20);
    BEGIN
        SELECT NVL(SUM(o.ORDER_AMOUNT),0)
          INTO v_total_spend
          FROM
           DBMIG_POC.DBMIG_POC.ORDERS o
         WHERE o.CUSTOMER_ID = p_customer_id;
        CASE
           WHEN v_total_spend > 20000 THEN
               v_new_tier := 'PLATINUM';
           WHEN v_total_spend > 10000 THEN
               v_new_tier := 'GOLD';
            ELSE
               v_new_tier := 'SILVER';
        END CASE;

        UPDATE DBMIG_POC.DBMIG_POC.CUSTOMERS
           SET TIER = v_new_tier
         WHERE CUSTOMER_ID = p_customer_id;
        --** SSC-FDM-OR0035 - CHECK UDF IMPLEMENTATION FOR DBMS_OUTPUT.PUT_LINE_UDP. **
        CALL DBMS_OUTPUT.PUT_LINE_UDP('New Tier: ' || NVL(v_new_tier :: STRING, ''));
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 4) Inventory Refresh Procedure (Cursor Loop)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.INVENTORY", "DBMIG_POC.DBMIG_POC.INVENTORY_SNAP" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_REFRESH_INVENTORY ()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
        --** SSC-PRF-0009 - PERFORMANCE REVIEW - CURSOR USAGE **
        c_inv CURSOR
        FOR
           SELECT STORE_ID, PRODUCT_ID
             FROM
               DBMIG_POC.DBMIG_POC.INVENTORY
            ORDER BY STORE_ID, PRODUCT_ID;
               v_onhand NUMBER(38, 18);
    v_reserved NUMBER(38, 18);
    BEGIN
               OPEN c_inv;
               --** SSC-PRF-0004 - THIS STATEMENT HAS USAGES OF CURSOR FOR LOOP **
               FOR r IN c_inv DO
           SELECT NVL(SUM(QTY),0),
                  NVL(SUM(RESERVED_QTY),0)
           INTO v_onhand, v_reserved
           FROM
               DBMIG_POC.DBMIG_POC.INVENTORY_SNAP s
          WHERE s.STORE_ID = r.STORE_ID
            AND s.PRODUCT_ID = r.PRODUCT_ID;

           UPDATE DBMIG_POC.DBMIG_POC.INVENTORY
              SET ON_HAND_QTY = v_onhand,
                  RESERVED_QTY = v_reserved,
                  UPDATED_TS = CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/
            WHERE STORE_ID = r.STORE_ID
              AND PRODUCT_ID = r.PRODUCT_ID;
               END FOR;
               CLOSE c_inv;
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 5) Customer Merge Procedure (MERGE)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "DBMIG_POC.DBMIG_POC.CUSTOMERS" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_MERGE_CUSTOMER (p_customer_id NUMBER(38, 18), p_customer_name VARCHAR, p_email VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    BEGIN
               MERGE INTO DBMIG_POC.DBMIG_POC.CUSTOMERS tgt
               USING (SELECT p_customer_id AS ID FROM DUAL) src
                  ON (tgt.CUSTOMER_ID = src.ID)
               WHEN MATCHED THEN
                   UPDATE SET CUSTOMER_NAME = p_customer_name,
                              EMAIL_ID = p_email,
                              UPDATED_TS = CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/
               WHEN NOT MATCHED THEN
                   INSERT (CUSTOMER_ID, CUSTOMER_NAME, EMAIL_ID, CREATED_DATE)
                   VALUES (p_customer_id, p_customer_name, p_email, CURRENT_TIMESTAMP());
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 6) Problematic Procedure (Autonomous Transaction)
-- (Will need rewriting by self-healing)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.SHIPMENTS", "DBMIG_POC.DBMIG_POC.AUDIT_LOG", "AUDIT_LOG_SEQ" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_SHIPMENT_AUDIT (p_shipment_id NUMBER(38, 18), p_status VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
--               PRAGMA AUTONOMOUS_TRANSACTION;   -- Oracle-specific: Snowflake can't use this

               v_order_id NUMBER(38, 18);
    BEGIN
               SELECT ORDER_ID
                 INTO v_order_id
                 FROM
           DBMIG_POC.DBMIG_POC.SHIPMENTS
                WHERE SHIPMENT_ID = p_shipment_id;

               INSERT INTO DBMIG_POC.DBMIG_POC.AUDIT_LOG(
                   AUDIT_ID, ENTITY_NAME, ENTITY_KEY, OPERATION,
                   CHANGE_TS, CHANGED_BY, DETAILS
               )
               VALUES (
                   AUDIT_LOG_SEQ.NEXTVAL, 'SHIPMENT', p_shipment_id,
                   p_status, CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/, CURRENT_USER(),
                   'Shipment Status Updated'
               );
               --** SSC-FDM-OR0012 - COMMIT REQUIRES THE APPROPRIATE SETUP TO WORK AS INTENDED **
               COMMIT;   -- Autonomous commit (rewrite needed)

    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 7) Product Attribute Loader (Cursor + Dynamic Expression)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.PRODUCTS", "DBMIG_POC.DBMIG_POC.PRODUCT_ATTR" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_LOAD_PRODUCT_ATTR (p_default_brand VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
               --** SSC-PRF-0009 - PERFORMANCE REVIEW - CURSOR USAGE **
               c_prod CURSOR
               FOR
           SELECT PRODUCT_ID, PRODUCT_NAME FROM
               DBMIG_POC.DBMIG_POC.PRODUCTS;
    BEGIN
               OPEN c_prod;
               --** SSC-PRF-0004 - THIS STATEMENT HAS USAGES OF CURSOR FOR LOOP **
               FOR r IN c_prod DO
           INSERT INTO DBMIG_POC.DBMIG_POC.PRODUCT_ATTR(
               PRODUCT_ID, ATTR_KEY, ATTR_VALUE, UPDATED_TS
           )
           SELECT
               :
               r:PRODUCT_ID,
               'DEFAULT_BRAND',
               p_default_brand,
               CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/;
               END FOR;
               CLOSE c_prod;
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 8) CUSTOMER HEALTH CHECK (Conditional Logic)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.ORDERS", "DBMIG_POC.DBMIG_POC.CUSTOMERS" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_CUSTOMER_HEALTH_CHECK (p_customer_id NUMBER(38, 18)
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
               v_last_order_date TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/;
               v_days NUMBER(38, 18);
    BEGIN
               SELECT MAX(ORDER_DATE)
                 INTO v_last_order_date
                 FROM
           DBMIG_POC.DBMIG_POC.ORDERS
                WHERE CUSTOMER_ID = p_customer_id;
               v_days := PUBLIC.TRUNC_UDF(PUBLIC.DATEDIFF_UDF(
               CURRENT_TIMESTAMP(), v_last_order_date));
               IF (v_days > 180) THEN
                   UPDATE DBMIG_POC.DBMIG_POC.CUSTOMERS
                      SET HEALTH_STATUS = 'AT_RISK'
                    WHERE CUSTOMER_ID = p_customer_id;
               -- Intentional syntax issue: ELSEIF → ELSIF
               ELSEIF (v_days > 90) THEN
                   UPDATE DBMIG_POC.DBMIG_POC.CUSTOMERS
                      SET HEALTH_STATUS = 'WARNING'
                    WHERE CUSTOMER_ID = p_customer_id;
               ELSE
                   UPDATE DBMIG_POC.DBMIG_POC.CUSTOMERS
                      SET HEALTH_STATUS = 'GOOD'
                    WHERE CUSTOMER_ID = p_customer_id;
               END IF;
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 9) ORDER NOTES LOADER (Sequence & DBMS_OUTPUT)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.ORDER_NOTES", "ORDER_NOTES_SEQ" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_LOAD_ORDER_NOTE (p_order_id NUMBER(38, 18), p_note_text VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    BEGIN
               INSERT INTO DBMIG_POC.DBMIG_POC.ORDER_NOTES(
                   NOTE_ID, ORDER_ID, NOTE_TEXT, CREATED_TS
               )
               VALUES (
                   ORDER_NOTES_SEQ.NEXTVAL,
                   p_order_id,
                   p_note_text, CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/
               );
               --** SSC-FDM-OR0035 - CHECK UDF IMPLEMENTATION FOR DBMS_OUTPUT.PUT_LINE_UDP. **
               CALL DBMS_OUTPUT.PUT_LINE_UDP('Note Added for order ' || NVL(p_order_id :: STRING, ''));
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 10) PAYMENT STATUS UPDATE
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.PAYMENTS", "DBMIG_POC.DBMIG_POC.AUDIT_LOG", "AUDIT_LOG_SEQ" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_UPDATE_PAYMENT_STATUS (p_payment_id NUMBER(38, 18), p_status VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    BEGIN
               UPDATE DBMIG_POC.DBMIG_POC.PAYMENTS
                  SET STATUS = p_status,
                      UPDATED_TS = CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/
                WHERE PAYMENT_ID = p_payment_id;

               INSERT INTO DBMIG_POC.DBMIG_POC.AUDIT_LOG(
                   AUDIT_ID, ENTITY_NAME, ENTITY_KEY,
                   OPERATION, CHANGE_TS, CHANGED_BY, DETAILS
               ) VALUES (
                   AUDIT_LOG_SEQ.NEXTVAL,
                   'PAYMENT',
                   p_payment_id,
                   'UPDATE_STATUS', CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/, CURRENT_USER(),
                   'Status changed to ' || NVL(p_status :: STRING, '')
               );
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 11) CUSTOMER PREFERENCE UPLOAD
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECT "DBMIG_POC.DBMIG_POC.CUSTOMER_PREFS" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_UPLOAD_PREFS (p_customer_id NUMBER(38, 18), p_pref_key VARCHAR, p_pref_value VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    BEGIN
               MERGE INTO DBMIG_POC.DBMIG_POC.CUSTOMER_PREFS tgt
               USING (SELECT p_customer_id AS CID, p_pref_key AS PKEY FROM DUAL) src
                  ON (tgt.CUSTOMER_ID = src.CID AND tgt.PREF_KEY = src.PKEY)
               WHEN MATCHED THEN
                   UPDATE SET PREF_VALUE = p_pref_value, UPDATED_TS = CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/
               WHEN NOT MATCHED THEN
                   INSERT (CUSTOMER_ID, PREF_KEY, PREF_VALUE, UPDATED_TS)
                   VALUES (p_customer_id, p_pref_key, p_pref_value, CURRENT_TIMESTAMP() /*** SSC-FDM-OR0047 - YOU MAY NEED TO SET TIMESTAMP OUTPUT FORMAT ('DD-MON-YY HH24.MI.SS.FF AM TZH:TZM') ***/);
    END;
$$;

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- 12) DAILY SALES AGGREGATION (Cursor + Aggregates)
--------------------------------------------------------------------------------
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "DBMIG_POC.DBMIG_POC.STORES", "DBMIG_POC.DBMIG_POC.SALES_FACT", "DBMIG_POC.DBMIG_POC.DAILY_SALES_AGG" **
CREATE OR REPLACE PROCEDURE DBMIG_POC.DBMIG_POC.SP_AGG_DAILY_SALES (p_date TIMESTAMP /*** SSC-FDM-OR0042 - DATE TYPE COLUMN HAS A DIFFERENT BEHAVIOR IN SNOWFLAKE. ***/
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 6,  "patch": "3-PuPr.6" }, "attributes": {  "component": "oracle",  "convertedOn": "03/14/2026",  "domain": "no-domain-provided",  "migrationid": "pOicARrr3HKo+a8H5pmiIw==" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
               --** SSC-PRF-0009 - PERFORMANCE REVIEW - CURSOR USAGE **
               c_stores CURSOR
               FOR
           SELECT STORE_ID FROM
               DBMIG_POC.DBMIG_POC.STORES;
               v_total_orders NUMBER(38, 18);
   v_total_qty NUMBER(38, 18);
   v_total_revenue NUMBER(38, 18);
    BEGIN
               OPEN c_stores;
               --** SSC-PRF-0004 - THIS STATEMENT HAS USAGES OF CURSOR FOR LOOP **
               FOR s IN c_stores DO
           SELECT
               NVL(COUNT(*),0),
               NVL(SUM(QTY),0),
               NVL(SUM(LINE_AMOUNT),0)
           INTO
               v_total_orders,
               v_total_qty,
               v_total_revenue
           FROM
               DBMIG_POC.DBMIG_POC.SALES_FACT
          WHERE STORE_KEY = s.STORE_ID
            AND DATE_KEY = PUBLIC.TO_NUMBER_UDF(TO_CHAR(p_date,'YYYYMMDD') /*** SSC-FDM-0019 - SEMANTIC INFORMATION COULD NOT BE LOADED FOR p_date. CHECK IF THE NAME IS INVALID OR DUPLICATED. ***/);

           INSERT INTO DBMIG_POC.DBMIG_POC.DAILY_SALES_AGG(
               AS_OF_DATE, STORE_ID,
               TOTAL_ORDERS, TOTAL_QTY, TOTAL_REVENUE, CREATED_DATE
           )
           SELECT
               p_date,
               :
               s:STORE_ID,
               v_total_orders,
               v_total_qty,
               v_total_revenue,
               CURRENT_TIMESTAMP();
               END FOR;
               CLOSE c_stores;
    END;
$$;
--------------------------------------------------------------------------------

-- ======================================================================
-- END OF FILE (~430 lines)
-- ======================================================================