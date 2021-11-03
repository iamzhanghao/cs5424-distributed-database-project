DROP KEYSPACE IF EXISTS schema_b;
CREATE KEYSPACE schema_b with replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE schema_b;

CREATE TABLE order_line_tab (
ol_w_id        INT,
ol_d_id        INT,
ol_o_id        INT,
ol_number      INT,
ol_i_id        INT,
ol_delivery_d  TIMESTAMP,
ol_amount      DECIMAL,
ol_supply_w_id INT,
ol_quantity    DECIMAL,
ol_dist_info   VARCHAR,
PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);



COPY order_line_tab (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
FROM 'project_files/data_files/order-line.csv' WITH DELIMITER=',' AND NULL='null' AND HEADER=FALSE;

DROP INDEX IF EXISTS order_line_tab_items;

DROP INDEX IF EXISTS order_line_tab_items_b;

CREATE INDEX IF NOT EXISTS order_line_tab_items_b
    ON schema_b.order_line_tab ( ol_i_id );

CREATE TABLE stock_tab (
s_w_id       INT,
s_i_id       INT,
s_quantity   DECIMAL,
s_ytd        DECIMAL,
s_order_cnt  INT,
s_remote_cnt INT,
s_dist_01    VARCHAR,
s_dist_02    VARCHAR,
s_dist_03    VARCHAR,
s_dist_04    VARCHAR,
s_dist_05    VARCHAR,
s_dist_06    VARCHAR,
s_dist_07    VARCHAR,
s_dist_08    VARCHAR,
s_dist_09    VARCHAR,
s_dist_10    VARCHAR,
d_data       VARCHAR,
PRIMARY KEY (s_w_id, s_i_id)
);

COPY stock_tab (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, d_data)
FROM 'project_files/data_files/stock.csv' WITH DELIMITER=',' AND HEADER=FALSE;

CREATE TABLE warehouse_tab (
w_id       INT,
w_name     VARCHAR,
w_street_1 VARCHAR,
w_street_2 VARCHAR,
w_city     VARCHAR,
w_state    VARCHAR,
w_zip      VARCHAR,
w_tax      DECIMAL,
w_ytd      DECIMAL,
PRIMARY KEY (w_id)
);

COPY warehouse_tab (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
FROM 'project_files/data_files/warehouse.csv' WITH DELIMITER=',' AND HEADER=FALSE;

CREATE TABLE district_tab (
d_w_id      INT,
d_id        INT,
d_name      VARCHAR,
d_street_1  VARCHAR,
d_street_2  VARCHAR,
d_city      VARCHAR,
d_state     VARCHAR,
d_zip       VARCHAR,
d_tax       DECIMAL,
d_ytd       DECIMAL,
d_next_o_id INT,
PRIMARY KEY (d_w_id, d_id)
);

COPY district_tab (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
FROM 'project_files/data_files/district.csv' WITH DELIMITER=',' AND HEADER=FALSE;

CREATE TABLE customer_tab (
c_w_id         INT,
c_d_id         INT,
c_id           INT,
c_first        VARCHAR,
c_middle       VARCHAR,
c_last         VARCHAR,
c_street_1     VARCHAR,
c_street_2     VARCHAR,
c_city         VARCHAR,
c_state        VARCHAR,
c_zip          VARCHAR,
c_phone        VARCHAR,
c_since        TIMESTAMP,
c_credit       VARCHAR,
c_credit_lim   DECIMAL,
c_discount     DECIMAL,
c_balance      DECIMAL,
c_ytd_payment  FLOAT,
c_payment_cnt  INT,
c_delivery_cnt INT,
c_data         VARCHAR,
PRIMARY KEY (c_w_id, c_d_id, c_id)
);

COPY customer_tab (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data)
FROM 'project_files/data_files/customer.csv' WITH DELIMITER=',' AND HEADER=FALSE;

CREATE MATERIALIZED VIEW customer_tab_by_balance AS
SELECT
c_balance,
c_w_id,
c_d_id,
c_first,
c_middle,
c_last,
c_id
FROM
customer_tab
WHERE
c_balance IS NOT NULL
AND c_w_id IS NOT NULL
AND c_d_id IS NOT NULL
AND c_first IS NOT NULL
AND c_middle IS NOT NULL
AND c_id IS NOT NULL
AND c_last IS NOT NULL PRIMARY KEY (
c_w_id, c_balance, c_d_id, c_id
);

CREATE TABLE order_tab (
o_w_id       INT,
o_d_id       INT,
o_c_id       INT,
o_id         INT,
o_carrier_id VARCHAR,
o_ol_cnt     DECIMAL,
o_all_local  DECIMAL,
o_entry_d    TIMESTAMP,
PRIMARY KEY (o_w_id, o_id, o_d_id)
);


COPY order_tab (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
FROM 'project_files/data_files/order.csv' WITH DELIMITER=',' AND NULL='' AND HEADER=FALSE;

CREATE TABLE item_tab (
i_id    INT,
i_name  VARCHAR,
i_price DECIMAL,
i_im_id INT,
i_data  VARCHAR,
PRIMARY KEY (i_id)
);

COPY item_tab (i_id, i_name, i_price, i_im_id, i_data)
FROM 'project_files/data_files/item.csv' WITH DELIMITER=',' AND HEADER=FALSE;