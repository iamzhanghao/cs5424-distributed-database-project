DROP DATABASE IF EXISTS schema_a;
CREATE DATABASE schema_a;
USE schema_a;

CREATE TABLE warehouse_tab (
    w_id       INT,
    w_name     VARCHAR(10),
    w_street_1 VARCHAR(20),
    w_street_2 VARCHAR(20),
    w_city     VARCHAR(20),
    w_state    CHAR(2),
    w_zip      CHAR(9),
    w_tax      DECIMAL(4,4),
    w_ytd      DECIMAL(12,2),
    PRIMARY KEY (w_id)
);

IMPORT INTO warehouse_tab (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
    CSV DATA (
        'nodelocal://1/project_files/data_files_A/warehouse.csv'
    );

CREATE TABLE district_tab (
    d_w_id      INT,
    d_id        INT,
    d_name      VARCHAR(10),
    d_street_1  VARCHAR(20),
    d_street_2  VARCHAR(20),
    d_city      VARCHAR(20),
    d_state     CHAR(2),
    d_zip       CHAR(9),
    d_tax       DECIMAL(4,4),
    d_ytd       DECIMAL(12,2),
    d_next_o_id INT,
    PRIMARY KEY (d_w_id, d_id)
);

IMPORT INTO district_tab (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
    CSV DATA (
        'nodelocal://1/project_files/data_files_A/district.csv'
    );

CREATE TABLE customer_tab (
    c_w_id         INT,
    c_d_id         INT,
    c_id           INT,
    c_first        VARCHAR(16),
    c_middle       CHAR(2),
    c_last         VARCHAR(16),
    c_street_1     VARCHAR(20),
    c_street_2     VARCHAR(20),
    c_city         VARCHAR(20),
    c_state        CHAR(2),
    c_zip          CHAR(9),
    c_phone        CHAR(16),
    c_since        TIMESTAMP,
    c_credit       CHAR(2),
    c_credit_lim   DECIMAL(12,2),
    c_discount     DECIMAL(4,4),
    c_balance      DECIMAL(12,2),
    c_ytd_payment  FLOAT,
    c_payment_cnt  INT,
    c_delivery_cnt INT,
    c_data         VARCHAR(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

IMPORT INTO customer_tab (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
    CSV DATA (
        'nodelocal://1/project_files/data_files_A/customer.csv'
    );

CREATE TABLE order_tab (
    o_w_id       INT,
    o_d_id       INT,
    o_id         INT,
    o_c_id       INT,
    o_carrier_id INT,
    o_ol_cnt     DECIMAL(2,0),
    o_all_local  DECIMAL(1,0),
    o_entry_d    TIMESTAMP,
    PRIMARY KEY (o_w_id, o_d_id, o_c_id)
);

IMPORT INTO order_tab (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
    CSV DATA (
        'nodelocal://1/project_files/data_files_A/order.csv'
    ) WITH nullif = 'null';

CREATE TABLE item_tab (
    i_id    INT,
    i_name  VARCHAR(24),
    i_price DECIMAL(5,2),
    i_im_id INT,
    i_data  VARCHAR(50),
    PRIMARY KEY (i_id)
);

IMPORT INTO item_tab (i_id, i_name, i_price, i_im_id, i_data)
    CSV DATA (
        'nodelocal://1/project_files/data_files_A/item.csv'
    );

CREATE TABLE order_line_tab (
    ol_w_id        INT,
    ol_d_id        INT,
    ol_o_id        INT,
    ol_number      INT,
    ol_i_id        INT,
    ol_delivery_d  TIMESTAMP,
    ol_amount      NUMERIC,
    ol_supply_w_id INT,
    ol_quantity    DECIMAL(2,0),
    ol_dist_info   CHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

IMPORT INTO order_line_tab (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
    CSV DATA (
        'nodelocal://1/project_files/data_files_A/order-line.csv'
    ) WITH nullif = 'null';

CREATE TABLE stock_tab (
    s_w_id       INT,
    s_i_id       INT,
    s_quantity   DECIMAL(4,0),
    s_ytd        DECIMAL(8,2),
    s_order_cnt  INT,
    s_remote_cnt INT,
    s_dist_01    CHAR(24),
    s_dist_02    CHAR(24),
    s_dist_03    CHAR(24),
    s_dist_04    CHAR(24),
    s_dist_05    CHAR(24),
    s_dist_06    CHAR(24),
    s_dist_07    CHAR(24),
    s_dist_08    CHAR(24),
    s_dist_09    CHAR(24),
    s_dist_10    CHAR(24),
    d_data       VARCHAR(50),
    PRIMARY KEY (s_w_id, s_i_id)
);

IMPORT INTO stock_tab (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, d_data)
    CSV DATA (
        'nodelocal://1/project_files/data_files_A/stock.csv'
    );