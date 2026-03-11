-- =====================================================================
-- Long Oracle DDL Script for Table-Creation Only (POC Stress Test)
-- Schemas used: ODS, STG, EDW, MART
-- =====================================================================

/*----------------------------------------------------------------------
  ODS LAYER TABLES
----------------------------------------------------------------------*/

-- =====================================================
-- 1) ODS.CUSTOMERS
-- =====================================================
CREATE TABLE ODS.CUSTOMERS (
    CUSTOMER_ID           NUMBER(10)            NOT NULL,
    CUSTOMER_UUID         RAW(16),
    CUSTOMER_NAME         VARCHAR2(150)         NOT NULL,
    CUSTOMER_TYPE         VARCHAR2(30)          DEFAULT 'RETAIL' CHECK (CUSTOMER_TYPE IN ('RETAIL','WHOLESALE','INTERNAL')),
    EMAIL_ID              VARCHAR2(200),
    PHONE_NUMBER          VARCHAR2(30),
    STATUS_FLAG           CHAR(1)               DEFAULT 'A' CHECK (STATUS_FLAG IN ('A','I')),
    CREATED_DATE          DATE                  DEFAULT SYSDATE NOT NULL,
    UPDATED_TS            TIMESTAMP(6),
    MARKETING_OPT_IN      CHAR(1)               DEFAULT 'N' CHECK (MARKETING_OPT_IN IN ('Y','N')),
    CREDIT_LIMIT          NUMBER(12,2)          DEFAULT 0,
    LOYALTY_POINTS        NUMBER(10,0)          DEFAULT 0,
    DATE_OF_BIRTH         DATE,
    COUNTRY_CODE          VARCHAR2(2)           DEFAULT 'IN',
    TAX_ID                VARCHAR2(30),
    CONSTRAINT PK_CUSTOMERS PRIMARY KEY (CUSTOMER_ID)
);

COMMENT ON TABLE ODS.CUSTOMERS IS 'Master list of customers across channels.';
COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_ID      IS 'Natural/business key for customers';
COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_UUID    IS 'Binary UUID (RAW(16))';
COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_NAME    IS 'Full name of the customer';
COMMENT ON COLUMN ODS.CUSTOMERS.CUSTOMER_TYPE    IS 'RETAIL/WHOLESALE/INTERNAL';
COMMENT ON COLUMN ODS.CUSTOMERS.EMAIL_ID         IS 'Primary email address';
COMMENT ON COLUMN ODS.CUSTOMERS.PHONE_NUMBER     IS 'Primary phone';
COMMENT ON COLUMN ODS.CUSTOMERS.STATUS_FLAG      IS 'A=Active, I=Inactive';
COMMENT ON COLUMN ODS.CUSTOMERS.CREATED_DATE     IS 'Creation date';
COMMENT ON COLUMN ODS.CUSTOMERS.UPDATED_TS       IS 'Last update timestamp';
COMMENT ON COLUMN ODS.CUSTOMERS.MARKETING_OPT_IN IS 'Y/N marketing consent';
COMMENT ON COLUMN ODS.CUSTOMERS.CREDIT_LIMIT     IS 'Credit limit (currency)';
COMMENT ON COLUMN ODS.CUSTOMERS.LOYALTY_POINTS   IS 'Aggregate loyalty points';
COMMENT ON COLUMN ODS.CUSTOMERS.DATE_OF_BIRTH    IS 'DOB (optional)';
COMMENT ON COLUMN ODS.CUSTOMERS.COUNTRY_CODE     IS 'ISO alpha-2 country code';
COMMENT ON COLUMN ODS.CUSTOMERS.TAX_ID           IS 'Tax identifier (PAN/VAT/etc)';

CREATE INDEX ODS.IX_CUSTOMERS_EMAIL       ON ODS.CUSTOMERS (EMAIL_ID);
CREATE INDEX ODS.IX_CUSTOMERS_STATUS      ON ODS.CUSTOMERS (STATUS_FLAG);
CREATE INDEX ODS.IX_CUSTOMERS_UPDATED_TS  ON ODS.CUSTOMERS (UPDATED_TS);

-- =====================================================
-- 2) ODS.CUSTOMER_ADDRESSES
-- =====================================================
CREATE TABLE ODS.CUSTOMER_ADDRESSES (
    ADDRESS_ID            NUMBER(12)           NOT NULL,
    CUSTOMER_ID           NUMBER(10)           NOT NULL,
    ADDRESS_TYPE          VARCHAR2(20)         DEFAULT 'SHIPPING' CHECK (ADDRESS_TYPE IN ('SHIPPING','BILLING','OTHER')),
    LINE1                 VARCHAR2(200)        NOT NULL,
    LINE2                 VARCHAR2(200),
    CITY                  VARCHAR2(100)        NOT NULL,
    STATE_PROVINCE        VARCHAR2(100),
    POSTAL_CODE           VARCHAR2(20),
    COUNTRY_CODE          VARCHAR2(2)          DEFAULT 'IN' NOT NULL,
    IS_DEFAULT            CHAR(1)              DEFAULT 'N' CHECK (IS_DEFAULT IN ('Y','N')),
    CREATED_DATE          DATE                 DEFAULT SYSDATE NOT NULL,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_CUSTOMER_ADDRESSES PRIMARY KEY (ADDRESS_ID),
    CONSTRAINT FK_CA_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES ODS.CUSTOMERS (CUSTOMER_ID)
);

COMMENT ON TABLE ODS.CUSTOMER_ADDRESSES IS 'Addresses for customers (shipping/billing).';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.ADDRESS_ID     IS 'Surrogate key for address';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.CUSTOMER_ID    IS 'FK to ODS.CUSTOMERS';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.ADDRESS_TYPE   IS 'SHIPPING/BILLING/OTHER';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.LINE1          IS 'Address line 1';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.LINE2          IS 'Address line 2';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.CITY           IS 'City';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.STATE_PROVINCE IS 'State/Province';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.POSTAL_CODE    IS 'Postal/ZIP code';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.COUNTRY_CODE   IS 'ISO alpha-2 country';
COMMENT ON COLUMN ODS.CUSTOMER_ADDRESSES.IS_DEFAULT     IS 'Default address flag';

CREATE INDEX ODS.IX_CA_CUSTOMER      ON ODS.CUSTOMER_ADDRESSES (CUSTOMER_ID);
CREATE INDEX ODS.IX_CA_CITY_COUNTRY  ON ODS.CUSTOMER_ADDRESSES (CITY, COUNTRY_CODE);

-- =====================================================
-- 3) ODS.SUPPLIERS
-- =====================================================
CREATE TABLE ODS.SUPPLIERS (
    SUPPLIER_ID           NUMBER(10)           NOT NULL,
    SUPPLIER_NAME         VARCHAR2(200)        NOT NULL,
    CONTACT_EMAIL         VARCHAR2(200),
    CONTACT_PHONE         VARCHAR2(30),
    COUNTRY_CODE          VARCHAR2(2)          DEFAULT 'IN',
    RATING                NUMBER(3,1)          CHECK (RATING BETWEEN 0 AND 10),
    ACTIVE_FLAG           CHAR(1)              DEFAULT 'Y' CHECK (ACTIVE_FLAG IN ('Y','N')),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_SUPPLIERS PRIMARY KEY (SUPPLIER_ID)
);

COMMENT ON TABLE ODS.SUPPLIERS IS 'Product suppliers.';
CREATE INDEX ODS.IX_SUPPLIERS_ACTIVE ON ODS.SUPPLIERS (ACTIVE_FLAG);

-- =====================================================
-- 4) ODS.PRODUCTS
-- =====================================================
CREATE TABLE ODS.PRODUCTS (
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    PRODUCT_SKU           VARCHAR2(60)         NOT NULL,
    PRODUCT_NAME          VARCHAR2(200)        NOT NULL,
    PRODUCT_CATEGORY      VARCHAR2(100),
    BRAND                 VARCHAR2(100),
    UNIT_PRICE            NUMBER(12,2)         NOT NULL CHECK (UNIT_PRICE >= 0),
    COST_PRICE            NUMBER(12,2)         DEFAULT 0 CHECK (COST_PRICE >= 0),
    TAX_RATE              NUMBER(5,2)          DEFAULT 0 CHECK (TAX_RATE BETWEEN 0 AND 100),
    ACTIVE_FLAG           CHAR(1)              DEFAULT 'Y' CHECK (ACTIVE_FLAG IN ('Y','N')),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    SUPPLIER_ID           NUMBER(10),
    CONSTRAINT PK_PRODUCTS PRIMARY KEY (PRODUCT_ID),
    CONSTRAINT UQ_PRODUCTS_SKU UNIQUE (PRODUCT_SKU),
    CONSTRAINT FK_PRODUCTS_SUPPLIER FOREIGN KEY (SUPPLIER_ID) REFERENCES ODS.SUPPLIERS (SUPPLIER_ID)
);

COMMENT ON TABLE ODS.PRODUCTS IS 'Product master (SKU-level).';
CREATE INDEX ODS.IX_PRODUCTS_ACTIVE   ON ODS.PRODUCTS (ACTIVE_FLAG);
CREATE INDEX ODS.IX_PRODUCTS_CATEGORY ON ODS.PRODUCTS (PRODUCT_CATEGORY);

-- =====================================================
-- 5) ODS.STORES
-- =====================================================
CREATE TABLE ODS.STORES (
    STORE_ID              NUMBER(10)           NOT NULL,
    STORE_CODE            VARCHAR2(30)         NOT NULL,
    STORE_NAME            VARCHAR2(200)        NOT NULL,
    CITY                  VARCHAR2(100),
    STATE_PROVINCE        VARCHAR2(100),
    COUNTRY_CODE          VARCHAR2(2)          DEFAULT 'IN',
    OPENED_DATE           DATE,
    STATUS_FLAG           CHAR(1)              DEFAULT 'O' CHECK (STATUS_FLAG IN ('O','C')), -- O=Open, C=Closed
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_STORES PRIMARY KEY (STORE_ID),
    CONSTRAINT UQ_STORES_CODE UNIQUE (STORE_CODE)
);

COMMENT ON TABLE ODS.STORES IS 'Retail/online store locations.';
CREATE INDEX ODS.IX_STORES_CITY   ON ODS.STORES (CITY);
CREATE INDEX ODS.IX_STORES_STATUS ON ODS.STORES (STATUS_FLAG);

-- =====================================================
-- 6) ODS.INVENTORY (by store & product)
-- =====================================================
CREATE TABLE ODS.INVENTORY (
    STORE_ID              NUMBER(10)           NOT NULL,
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    ON_HAND_QTY           NUMBER(12,2)         DEFAULT 0 CHECK (ON_HAND_QTY >= 0),
    RESERVED_QTY          NUMBER(12,2)         DEFAULT 0 CHECK (RESERVED_QTY >= 0),
    REORDER_LEVEL         NUMBER(12,2)         DEFAULT 0 CHECK (REORDER_LEVEL >= 0),
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_INVENTORY PRIMARY KEY (STORE_ID, PRODUCT_ID),
    CONSTRAINT FK_INV_STORE   FOREIGN KEY (STORE_ID)   REFERENCES ODS.STORES (STORE_ID),
    CONSTRAINT FK_INV_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID)
);

COMMENT ON TABLE ODS.INVENTORY IS 'Current inventory by store and product.';
CREATE INDEX ODS.IX_INV_REORDER ON ODS.INVENTORY (REORDER_LEVEL);

-- =====================================================
-- 7) ODS.PROMOTIONS
-- =====================================================
CREATE TABLE ODS.PROMOTIONS (
    PROMO_ID              NUMBER(10)           NOT NULL,
    PROMO_CODE            VARCHAR2(40)         NOT NULL,
    PROMO_NAME            VARCHAR2(200)        NOT NULL,
    DISCOUNT_TYPE         VARCHAR2(20)         CHECK (DISCOUNT_TYPE IN ('PERCENT','AMOUNT')),
    DISCOUNT_VALUE        NUMBER(12,2)         DEFAULT 0 CHECK (DISCOUNT_VALUE >= 0),
    START_DATE            DATE                 NOT NULL,
    END_DATE              DATE                 NOT NULL,
    ACTIVE_FLAG           CHAR(1)              DEFAULT 'Y' CHECK (ACTIVE_FLAG IN ('Y','N')),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_PROMOTIONS PRIMARY KEY (PROMO_ID),
    CONSTRAINT UQ_PROMO_CODE UNIQUE (PROMO_CODE),
    CONSTRAINT CHK_PROMO_DATES CHECK (END_DATE >= START_DATE)
);

COMMENT ON TABLE ODS.PROMOTIONS IS 'Promotions applicable to orders/products.';

-- =====================================================
-- 8) ODS.CURRENCIES
-- =====================================================
CREATE TABLE ODS.CURRENCIES (
    CURRENCY_CODE         CHAR(3)              NOT NULL,
    CURRENCY_NAME         VARCHAR2(100)        NOT NULL,
    SYMBOL                VARCHAR2(6),
    DECIMAL_PLACES        NUMBER(1,0)          DEFAULT 2 CHECK (DECIMAL_PLACES BETWEEN 0 AND 6),
    ACTIVE_FLAG           CHAR(1)              DEFAULT 'Y' CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT PK_CURRENCIES PRIMARY KEY (CURRENCY_CODE)
);

COMMENT ON TABLE ODS.CURRENCIES IS 'ISO currency codes and metadata.';

/*----------------------------------------------------------------------
  STG LAYER TABLES (landing / staging)
----------------------------------------------------------------------*/

-- =====================================================
-- 9) STG.ORDERS
-- =====================================================
CREATE TABLE STG.ORDERS (
    ORDER_ID              NUMBER(12)           NOT NULL,
    CUSTOMER_ID           NUMBER(10)           NOT NULL,
    STORE_ID              NUMBER(10),
    ORDER_DATE            DATE                 NOT NULL,
    ORDER_STATUS          VARCHAR2(20)         DEFAULT 'NEW' CHECK (ORDER_STATUS IN ('NEW','PAID','SHIPPED','CANCELLED','RETURNED')),
    CURRENCY_CODE         CHAR(3)              DEFAULT 'INR' NOT NULL,
    ORDER_AMOUNT          NUMBER(14,2)         DEFAULT 0 CHECK (ORDER_AMOUNT >= 0),
    TAX_AMOUNT            NUMBER(14,2)         DEFAULT 0 CHECK (TAX_AMOUNT >= 0),
    SHIPPING_AMOUNT       NUMBER(14,2)         DEFAULT 0 CHECK (SHIPPING_AMOUNT >= 0),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    PROMO_ID              NUMBER(10),
    CONSTRAINT PK_STG_ORDERS PRIMARY KEY (ORDER_ID),
    CONSTRAINT FK_STG_ORDERS_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES ODS.CUSTOMERS (CUSTOMER_ID),
    CONSTRAINT FK_STG_ORDERS_STORE    FOREIGN KEY (STORE_ID)    REFERENCES ODS.STORES (STORE_ID),
    CONSTRAINT FK_STG_ORDERS_CURR     FOREIGN KEY (CURRENCY_CODE) REFERENCES ODS.CURRENCIES (CURRENCY_CODE),
    CONSTRAINT FK_STG_ORDERS_PROMO    FOREIGN KEY (PROMO_ID) REFERENCES ODS.PROMOTIONS (PROMO_ID)
);

COMMENT ON TABLE STG.ORDERS IS 'Inbound orders landing area.';
CREATE INDEX STG.IX_STG_ORDERS_DATE   ON STG.ORDERS (ORDER_DATE);
CREATE INDEX STG.IX_STG_ORDERS_STATUS ON STG.ORDERS (ORDER_STATUS);

-- =====================================================
-- 10) STG.ORDER_ITEMS
-- =====================================================
CREATE TABLE STG.ORDER_ITEMS (
    ORDER_ITEM_ID         NUMBER(12)           NOT NULL,
    ORDER_ID              NUMBER(12)           NOT NULL,
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    QTY                   NUMBER(12,2)         NOT NULL CHECK (QTY > 0),
    UNIT_PRICE            NUMBER(14,2)         NOT NULL CHECK (UNIT_PRICE >= 0),
    LINE_AMOUNT           NUMBER(14,2)         GENERATED ALWAYS AS (QTY * UNIT_PRICE) VIRTUAL,
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_STG_ORDER_ITEMS PRIMARY KEY (ORDER_ITEM_ID),
    CONSTRAINT FK_STG_OI_ORDER   FOREIGN KEY (ORDER_ID)   REFERENCES STG.ORDERS (ORDER_ID),
    CONSTRAINT FK_STG_OI_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID)
);

COMMENT ON TABLE STG.ORDER_ITEMS IS 'Line items for STG.ORDERS.';
CREATE INDEX STG.IX_STG_OI_ORDER    ON STG.ORDER_ITEMS (ORDER_ID);
CREATE INDEX STG.IX_STG_OI_PRODUCT  ON STG.ORDER_ITEMS (PRODUCT_ID);

-- =====================================================
-- 11) STG.SHIPMENTS
-- =====================================================
CREATE TABLE STG.SHIPMENTS (
    SHIPMENT_ID           NUMBER(12)           NOT NULL,
    ORDER_ID              NUMBER(12)           NOT NULL,
    SHIPMENT_DATE         DATE,
    CARRIER               VARCHAR2(100),
    TRACKING_NUMBER       VARCHAR2(100),
    STATUS                VARCHAR2(20)         DEFAULT 'CREATED' CHECK (STATUS IN ('CREATED','IN_TRANSIT','DELIVERED','LOST','RETURNED')),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_STG_SHIPMENTS PRIMARY KEY (SHIPMENT_ID),
    CONSTRAINT FK_STG_SHIP_ORDER FOREIGN KEY (ORDER_ID) REFERENCES STG.ORDERS (ORDER_ID)
);

COMMENT ON TABLE STG.SHIPMENTS IS 'Shipments created for orders.';
CREATE INDEX STG.IX_STG_SHIP_ORDER ON STG.SHIPMENTS (ORDER_ID);
CREATE INDEX STG.IX_STG_SHIP_STAT  ON STG.SHIPMENTS (STATUS);

-- =====================================================
-- 12) STG.RETURNS
-- =====================================================
CREATE TABLE STG.RETURNS (
    RETURN_ID             NUMBER(12)           NOT NULL,
    ORDER_ID              NUMBER(12)           NOT NULL,
    RETURN_DATE           DATE                 NOT NULL,
    REASON_CODE           VARCHAR2(50),
    REFUND_AMOUNT         NUMBER(14,2)         DEFAULT 0 CHECK (REFUND_AMOUNT >= 0),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_STG_RETURNS PRIMARY KEY (RETURN_ID),
    CONSTRAINT FK_STG_RETURNS_ORDER FOREIGN KEY (ORDER_ID) REFERENCES STG.ORDERS (ORDER_ID)
);

COMMENT ON TABLE STG.RETURNS IS 'Returns / refunds landing.';
CREATE INDEX STG.IX_STG_RETURNS_ORDER ON STG.RETURNS (ORDER_ID);

-- =====================================================
-- 13) STG.INVENTORY_SNAP
-- =====================================================
CREATE TABLE STG.INVENTORY_SNAP (
    SNAPSHOT_ID           NUMBER(12)           NOT NULL,
    STORE_ID              NUMBER(10)           NOT NULL,
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    SNAPSHOT_DATE         DATE                 NOT NULL,
    ON_HAND_QTY           NUMBER(12,2)         DEFAULT 0,
    RESERVED_QTY          NUMBER(12,2)         DEFAULT 0,
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_STG_INV_SNAP PRIMARY KEY (SNAPSHOT_ID),
    CONSTRAINT FK_STG_INV_SNAP_STORE   FOREIGN KEY (STORE_ID)   REFERENCES ODS.STORES (STORE_ID),
    CONSTRAINT FK_STG_INV_SNAP_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID)
);

COMMENT ON TABLE STG.INVENTORY_SNAP IS 'Daily inventory snapshot from sources.';
CREATE INDEX STG.IX_STG_INV_SNAP_DATE ON STG.INVENTORY_SNAP (SNAPSHOT_DATE);

-- =====================================================
-- 14) STG.PAYMENTS
-- =====================================================
CREATE TABLE STG.PAYMENTS (
    PAYMENT_ID            NUMBER(12)           NOT NULL,
    ORDER_ID              NUMBER(12)           NOT NULL,
    PAYMENT_METHOD        VARCHAR2(30)         CHECK (PAYMENT_METHOD IN ('CARD','UPI','COD','NETBANKING','WALLET')),
    PAYMENT_DATE          DATE                 NOT NULL,
    AMOUNT                NUMBER(14,2)         NOT NULL CHECK (AMOUNT >= 0),
    CURRENCY_CODE         CHAR(3)              DEFAULT 'INR' NOT NULL,
    STATUS                VARCHAR2(20)         DEFAULT 'AUTHORIZED' CHECK (STATUS IN ('AUTHORIZED','CAPTURED','FAILED','REFUNDED')),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_STG_PAYMENTS PRIMARY KEY (PAYMENT_ID),
    CONSTRAINT FK_STG_PAYMENTS_ORDER FOREIGN KEY (ORDER_ID) REFERENCES STG.ORDERS (ORDER_ID),
    CONSTRAINT FK_STG_PAYMENTS_CURR  FOREIGN KEY (CURRENCY_CODE) REFERENCES ODS.CURRENCIES (CURRENCY_CODE)
);

COMMENT ON TABLE STG.PAYMENTS IS 'Payments received for orders.';
CREATE INDEX STG.IX_STG_PAYMENTS_ORDER ON STG.PAYMENTS (ORDER_ID);
CREATE INDEX STG.IX_STG_PAYMENTS_STATUS ON STG.PAYMENTS (STATUS);

/*----------------------------------------------------------------------
  EDW LAYER TABLES (dimensional)
----------------------------------------------------------------------*/

-- =====================================================
-- 15) EDW.DATE_DIM
-- =====================================================
CREATE TABLE EDW.DATE_DIM (
    DATE_KEY              NUMBER(8)            NOT NULL, -- YYYYMMDD
    FULL_DATE             DATE                 NOT NULL,
    DAY_OF_WEEK_NUM       NUMBER(1),
    DAY_OF_WEEK_NAME      VARCHAR2(10),
    DAY_OF_MONTH          NUMBER(2),
    WEEK_OF_YEAR          NUMBER(2),
    MONTH_NUM             NUMBER(2),
    MONTH_NAME            VARCHAR2(20),
    QUARTER_NUM           NUMBER(1),
    YEAR_NUM              NUMBER(4),
    IS_WEEKEND            CHAR(1)              CHECK (IS_WEEKEND IN ('Y','N')),
    CONSTRAINT PK_DATE_DIM PRIMARY KEY (DATE_KEY)
);

COMMENT ON TABLE EDW.DATE_DIM IS 'Calendar date dimension.';

-- =====================================================
-- 16) EDW.CUSTOMER_DIM
-- =====================================================
CREATE TABLE EDW.CUSTOMER_DIM (
    CUSTOMER_KEY          NUMBER(12)           NOT NULL,
    CUSTOMER_ID           NUMBER(10)           NOT NULL,
    CUSTOMER_NAME         VARCHAR2(150),
    CUSTOMER_TYPE         VARCHAR2(30),
    COUNTRY_CODE          VARCHAR2(2),
    STATUS_FLAG           CHAR(1),
    EFFECTIVE_START_DT    DATE                 NOT NULL,
    EFFECTIVE_END_DT      DATE                 NOT NULL,
    CURRENT_FLAG          CHAR(1)              DEFAULT 'Y' CHECK (CURRENT_FLAG IN ('Y','N')),
    CONSTRAINT PK_CUSTOMER_DIM PRIMARY KEY (CUSTOMER_KEY)
);

COMMENT ON TABLE EDW.CUSTOMER_DIM IS 'SCD-2 customer dimension (keys from ODS).';

-- =====================================================
-- 17) EDW.PRODUCT_DIM
-- =====================================================
CREATE TABLE EDW.PRODUCT_DIM (
    PRODUCT_KEY           NUMBER(12)           NOT NULL,
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    PRODUCT_SKU           VARCHAR2(60),
    PRODUCT_NAME          VARCHAR2(200),
    PRODUCT_CATEGORY      VARCHAR2(100),
    BRAND                 VARCHAR2(100),
    ACTIVE_FLAG           CHAR(1),
    EFFECTIVE_START_DT    DATE                 NOT NULL,
    EFFECTIVE_END_DT      DATE                 NOT NULL,
    CURRENT_FLAG          CHAR(1)              DEFAULT 'Y' CHECK (CURRENT_FLAG IN ('Y','N')),
    CONSTRAINT PK_PRODUCT_DIM PRIMARY KEY (PRODUCT_KEY)
);

COMMENT ON TABLE EDW.PRODUCT_DIM IS 'SCD-2 product dimension.';

-- =====================================================
-- 18) EDW.STORE_DIM
-- =====================================================
CREATE TABLE EDW.STORE_DIM (
    STORE_KEY             NUMBER(12)           NOT NULL,
    STORE_ID              NUMBER(10)           NOT NULL,
    STORE_CODE            VARCHAR2(30),
    STORE_NAME            VARCHAR2(200),
    CITY                  VARCHAR2(100),
    STATE_PROVINCE        VARCHAR2(100),
    COUNTRY_CODE          VARCHAR2(2),
    STATUS_FLAG           CHAR(1),
    EFFECTIVE_START_DT    DATE                 NOT NULL,
    EFFECTIVE_END_DT      DATE                 NOT NULL,
    CURRENT_FLAG          CHAR(1)              DEFAULT 'Y' CHECK (CURRENT_FLAG IN ('Y','N')),
    CONSTRAINT PK_STORE_DIM PRIMARY KEY (STORE_KEY)
);

COMMENT ON TABLE EDW.STORE_DIM IS 'SCD-2 store dimension.';

-- =====================================================
-- 19) EDW.SALES_FACT
-- =====================================================
CREATE TABLE EDW.SALES_FACT (
    DATE_KEY              NUMBER(8)            NOT NULL,
    CUSTOMER_KEY          NUMBER(12)           NOT NULL,
    PRODUCT_KEY           NUMBER(12)           NOT NULL,
    STORE_KEY             NUMBER(12)           NOT NULL,
    ORDER_ID              NUMBER(12)           NOT NULL,
    ORDER_ITEM_ID         NUMBER(12)           NOT NULL,
    CURRENCY_CODE         CHAR(3)              NOT NULL,
    QTY                   NUMBER(12,2)         DEFAULT 0,
    UNIT_PRICE            NUMBER(14,2)         DEFAULT 0,
    LINE_AMOUNT           NUMBER(14,2)         DEFAULT 0,
    TAX_AMOUNT            NUMBER(14,2)         DEFAULT 0,
    SHIPPING_AMOUNT       NUMBER(14,2)         DEFAULT 0,
    DISCOUNT_AMOUNT       NUMBER(14,2)         DEFAULT 0,
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_SALES_FACT PRIMARY KEY (DATE_KEY, CUSTOMER_KEY, PRODUCT_KEY, STORE_KEY, ORDER_ID, ORDER_ITEM_ID)
);

COMMENT ON TABLE EDW.SALES_FACT IS 'Grain: one row per order item per store per day.';

CREATE INDEX EDW.IX_SALES_FACT_DATE ON EDW.SALES_FACT (DATE_KEY);
CREATE INDEX EDW.IX_SALES_FACT_CUST ON EDW.SALES_FACT (CUSTOMER_KEY);
CREATE INDEX EDW.IX_SALES_FACT_PROD ON EDW.SALES_FACT (PRODUCT_KEY);

-- =====================================================
-- 20) EDW.PAYMENT_FACT
-- =====================================================
CREATE TABLE EDW.PAYMENT_FACT (
    DATE_KEY              NUMBER(8)            NOT NULL,
    ORDER_ID              NUMBER(12)           NOT NULL,
    PAYMENT_ID            NUMBER(12)           NOT NULL,
    PAYMENT_METHOD        VARCHAR2(30),
    AMOUNT                NUMBER(14,2)         DEFAULT 0,
    CURRENCY_CODE         CHAR(3)              NOT NULL,
    STATUS                VARCHAR2(20),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_PAYMENT_FACT PRIMARY KEY (DATE_KEY, ORDER_ID, PAYMENT_ID)
);

COMMENT ON TABLE EDW.PAYMENT_FACT IS 'Payments fact at transaction grain.';

/*----------------------------------------------------------------------
  MART LAYER TABLES (aggregates and data marts)
----------------------------------------------------------------------*/

-- =====================================================
-- 21) MART.DAILY_SALES_AGG
-- =====================================================
CREATE TABLE MART.DAILY_SALES_AGG (
    AS_OF_DATE            DATE                 NOT NULL,
    STORE_ID              NUMBER(10),
    PRODUCT_CATEGORY      VARCHAR2(100),
    TOTAL_ORDERS          NUMBER(12,0)         DEFAULT 0,
    TOTAL_QTY             NUMBER(14,2)         DEFAULT 0,
    TOTAL_REVENUE         NUMBER(18,2)         DEFAULT 0,
    TOTAL_TAX             NUMBER(18,2)         DEFAULT 0,
    TOTAL_SHIPPING        NUMBER(18,2)         DEFAULT 0,
    AVG_ORDER_VALUE       NUMBER(18,2)         DEFAULT 0,
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_DAILY_SALES_AGG PRIMARY KEY (AS_OF_DATE, STORE_ID, PRODUCT_CATEGORY)
);

COMMENT ON TABLE MART.DAILY_SALES_AGG IS 'Daily aggregated sales metrics by store and product category.';
CREATE INDEX MART.IX_DSA_DATE ON MART.DAILY_SALES_AGG (AS_OF_DATE);

-- =====================================================
-- 22) MART.CUSTOMER_LTV
-- =====================================================
CREATE TABLE MART.CUSTOMER_LTV (
    CUSTOMER_ID           NUMBER(10)           NOT NULL,
    AS_OF_DATE            DATE                 NOT NULL,
    DAYS_ACTIVE           NUMBER(6,0)          DEFAULT 0,
    ORDERS_COUNT          NUMBER(12,0)         DEFAULT 0,
    TOTAL_SPEND           NUMBER(18,2)         DEFAULT 0,
    AVG_TICKET            NUMBER(18,2)         DEFAULT 0,
    CHURN_FLAG            CHAR(1)              DEFAULT 'N' CHECK (CHURN_FLAG IN ('Y','N')),
    SEGMENT               VARCHAR2(30),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_CUSTOMER_LTV PRIMARY KEY (CUSTOMER_ID, AS_OF_DATE)
);

COMMENT ON TABLE MART.CUSTOMER_LTV IS 'Customer lifetime value snapshot by day.';
CREATE INDEX MART.IX_LTV_SEGMENT ON MART.CUSTOMER_LTV (SEGMENT);

-- =====================================================
-- 23) MART.TOP_SELLERS
-- =====================================================
CREATE TABLE MART.TOP_SELLERS (
    RANKING_DATE          DATE                 NOT NULL,
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    RANK_NUM              NUMBER(6,0)          NOT NULL,
    TOTAL_QTY             NUMBER(14,2)         DEFAULT 0,
    TOTAL_REVENUE         NUMBER(18,2)         DEFAULT 0,
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_TOP_SELLERS PRIMARY KEY (RANKING_DATE, PRODUCT_ID)
);

COMMENT ON TABLE MART.TOP_SELLERS IS 'Top selling products per day.';

-- =====================================================
-- 24) MART.RETURN_RATE
-- =====================================================
CREATE TABLE MART.RETURN_RATE (
    AS_OF_DATE            DATE                 NOT NULL,
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    ORDERS_COUNT          NUMBER(12,0)         DEFAULT 0,
    RETURNS_COUNT         NUMBER(12,0)         DEFAULT 0,
    RETURN_RATE_PCT       NUMBER(5,2)          GENERATED ALWAYS AS (CASE WHEN ORDERS_COUNT = 0 THEN 0 ELSE (RETURNS_COUNT / ORDERS_COUNT) * 100 END) VIRTUAL,
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_RETURN_RATE PRIMARY KEY (AS_OF_DATE, PRODUCT_ID)
);

COMMENT ON TABLE MART.RETURN_RATE IS 'Return ratio snapshot by product and day.';

/*----------------------------------------------------------------------
  Additional ODS Bridge/Ref Tables to extend script length and coverage
----------------------------------------------------------------------*/

-- =====================================================
-- 25) ODS.PRODUCT_PROMO_BRIDGE (M:N mapping)
-- =====================================================
CREATE TABLE ODS.PRODUCT_PROMO_BRIDGE (
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    PROMO_ID              NUMBER(10)           NOT NULL,
    START_DATE            DATE                 NOT NULL,
    END_DATE              DATE                 NOT NULL,
    CONSTRAINT PK_PP_BRIDGE PRIMARY KEY (PRODUCT_ID, PROMO_ID, START_DATE),
    CONSTRAINT FK_PP_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID),
    CONSTRAINT FK_PP_PROMO   FOREIGN KEY (PROMO_ID)   REFERENCES ODS.PROMOTIONS (PROMO_ID),
    CONSTRAINT CHK_PP_DATES CHECK (END_DATE >= START_DATE)
);

COMMENT ON TABLE ODS.PRODUCT_PROMO_BRIDGE IS 'Mapping of products to promotions over time.';

-- =====================================================
-- 26) ODS.CUSTOMER_PREFS
-- =====================================================
CREATE TABLE ODS.CUSTOMER_PREFS (
    CUSTOMER_ID           NUMBER(10)           NOT NULL,
    PREF_KEY              VARCHAR2(50)         NOT NULL,
    PREF_VALUE            VARCHAR2(4000),
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_CUSTOMER_PREFS PRIMARY KEY (CUSTOMER_ID, PREF_KEY),
    CONSTRAINT FK_CP_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES ODS.CUSTOMERS (CUSTOMER_ID)
);

COMMENT ON TABLE ODS.CUSTOMER_PREFS IS 'Key/value preferences per customer.';
CREATE INDEX ODS.IX_CP_PREFKEY ON ODS.CUSTOMER_PREFS (PREF_KEY);

-- =====================================================
-- 27) ODS.PRODUCT_ATTR
-- =====================================================
CREATE TABLE ODS.PRODUCT_ATTR (
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    ATTR_KEY              VARCHAR2(50)         NOT NULL,
    ATTR_VALUE            VARCHAR2(4000),
    UPDATED_TS            TIMESTAMP(6),
    CONSTRAINT PK_PRODUCT_ATTR PRIMARY KEY (PRODUCT_ID, ATTR_KEY),
    CONSTRAINT FK_PA_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID)
);

COMMENT ON TABLE ODS.PRODUCT_ATTR IS 'Extensible product attributes.';
CREATE INDEX ODS.IX_PA_ATTRKEY ON ODS.PRODUCT_ATTR (ATTR_KEY);

-- =====================================================
-- 28) ODS.EXCHANGE_RATES
-- =====================================================
CREATE TABLE ODS.EXCHANGE_RATES (
    RATE_DATE             DATE                 NOT NULL,
    FROM_CURRENCY         CHAR(3)              NOT NULL,
    TO_CURRENCY           CHAR(3)              NOT NULL,
    RATE_VALUE            NUMBER(18,8)         NOT NULL CHECK (RATE_VALUE > 0),
    CREATED_DATE          DATE                 DEFAULT SYSDATE,
    CONSTRAINT PK_EXCHANGE_RATES PRIMARY KEY (RATE_DATE, FROM_CURRENCY, TO_CURRENCY),
    CONSTRAINT FK_ER_FROM_CURR FOREIGN KEY (FROM_CURRENCY) REFERENCES ODS.CURRENCIES (CURRENCY_CODE),
    CONSTRAINT FK_ER_TO_CURR   FOREIGN KEY (TO_CURRENCY)   REFERENCES ODS.CURRENCIES (CURRENCY_CODE)
);

COMMENT ON TABLE ODS.EXCHANGE_RATES IS 'Daily FX rates across currencies.';

-- =====================================================
-- 29) ODS.SUPPLIER_PRODUCTS (which products a supplier carries)
-- =====================================================
CREATE TABLE ODS.SUPPLIER_PRODUCTS (
    SUPPLIER_ID           NUMBER(10)           NOT NULL,
    PRODUCT_ID            NUMBER(10)           NOT NULL,
    SUPPLIER_SKU          VARCHAR2(60),
    LEAD_TIME_DAYS        NUMBER(5,0)          DEFAULT 7 CHECK (LEAD_TIME_DAYS >= 0),
    ACTIVE_FLAG           CHAR(1)              DEFAULT 'Y' CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT PK_SUPPLIER_PRODUCTS PRIMARY KEY (SUPPLIER_ID, PRODUCT_ID),
    CONSTRAINT FK_SP_SUPP FOREIGN KEY (SUPPLIER_ID) REFERENCES ODS.SUPPLIERS (SUPPLIER_ID),
    CONSTRAINT FK_SP_PROD FOREIGN KEY (PRODUCT_ID) REFERENCES ODS.PRODUCTS (PRODUCT_ID)
);

COMMENT ON TABLE ODS.SUPPLIER_PRODUCTS IS 'Supplier to product mapping with lead times.';
CREATE INDEX ODS.IX_SP_ACTIVE ON ODS.SUPPLIER_PRODUCTS (ACTIVE_FLAG);

-- =====================================================
-- 30) ODS.STORE_HOLIDAYS
-- =====================================================
CREATE TABLE ODS.STORE_HOLIDAYS (
    STORE_ID              NUMBER(10)           NOT NULL,
    HOLIDAY_DATE          DATE                 NOT NULL,
    DESCRIPTION           VARCHAR2(200),
    IS_CLOSED             CHAR(1)              DEFAULT 'Y' CHECK (IS_CLOSED IN ('Y','N')),
    CONSTRAINT PK_STORE_HOLIDAYS PRIMARY KEY (STORE_ID, HOLIDAY_DATE),
    CONSTRAINT FK_SH_STORE FOREIGN KEY (STORE_ID) REFERENCES ODS.STORES (STORE_ID)
);

COMMENT ON TABLE ODS.STORE_HOLIDAYS IS 'Store-specific holidays/closures.';

-- =====================================================
-- 31) ODS.STORE_HOURS
-- =====================================================
CREATE TABLE ODS.STORE_HOURS (
    STORE_ID              NUMBER(10)           NOT NULL,
    DAY_OF_WEEK_NAME      VARCHAR2(10)         NOT NULL,
    OPEN_TIME             VARCHAR2(8),  -- HH24:MI:SS
    CLOSE_TIME            VARCHAR2(8),
    IS_OPEN               CHAR(1)              DEFAULT 'Y' CHECK (IS_OPEN IN ('Y','N')),
    CONSTRAINT PK_STORE_HOURS PRIMARY KEY (STORE_ID, DAY_OF_WEEk_NAME),
    CONSTRAINT FK_STH_STORE FOREIGN KEY (STORE_ID) REFERENCES ODS.STORES (STORE_ID)
);

COMMENT ON TABLE ODS.STORE_HOURS IS 'Operating hours by store and weekday.';

-- =====================================================
-- 32) ODS.ORDER_NOTES
-- =====================================================
CREATE TABLE ODS.ORDER_NOTES (
    NOTE_ID               NUMBER(12)           NOT NULL,
    ORDER_ID              NUMBER(12)           NOT NULL,
    NOTE_TEXT             VARCHAR2(2000),
    CREATED_BY            VARCHAR2(100),
    CREATED_TS            TIMESTAMP(6)         DEFAULT SYSTIMESTAMP,
    CONSTRAINT PK_ORDER_NOTES PRIMARY KEY (NOTE_ID),
    CONSTRAINT FK_ON_ORDER FOREIGN KEY (ORDER_ID) REFERENCES STG.ORDERS (ORDER_ID)
);

COMMENT ON TABLE ODS.ORDER_NOTES IS 'Operations/investigation notes against orders.';

-- =====================================================
-- 33) ODS.AUDIT_LOG
-- =====================================================
CREATE TABLE ODS.AUDIT_LOG (
    AUDIT_ID              NUMBER(20)           NOT NULL,
    ENTITY_NAME           VARCHAR2(100)        NOT NULL,
    ENTITY_KEY            VARCHAR2(200)        NOT NULL,
    OPERATION             VARCHAR2(20)         CHECK (OPERATION IN ('INSERT','UPDATE','DELETE','MERGE')),
    CHANGE_TS             TIMESTAMP(6)         DEFAULT SYSTIMESTAMP,
    CHANGED_BY            VARCHAR2(100),
    DETAILS               VARCHAR2(4000),
    CONSTRAINT PK_AUDIT_LOG PRIMARY KEY (AUDIT_ID)
);

COMMENT ON TABLE ODS.AUDIT_LOG IS 'Generic audit events.';
CREATE INDEX ODS.IX_AUDIT_ENTITY ON ODS.AUDIT_LOG (ENTITY_NAME, ENTITY_KEY);

/*----------------------------------------------------------------------
  End of DDL
----------------------------------------------------------------------*/
