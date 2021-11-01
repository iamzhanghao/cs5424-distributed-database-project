USE schema_b;

CREATE INDEX ON customer_tab (c_d_id);
CREATE INDEX ON customer_tab (c_w_id);
CREATE INDEX ON customer_tab (c_id);
CREATE INDEX ON customer_tab (c_balance);
CREATE INDEX ON customer_tab (c_ytd_payment);
CREATE INDEX ON customer_tab (c_delivery_cnt);

CREATE INDEX ON district_tab (d_next_o_id);
CREATE INDEX ON district_tab (d_ytd);

CREATE INDEX ON item_tab (i_im_id);

CREATE INDEX ON order_line_tab (ol_d_id);
CREATE INDEX ON order_line_tab (ol_w_id);
CREATE INDEX ON order_line_tab (ol_o_id);
CREATE INDEX ON order_line_tab (ol_amount);
CREATE INDEX ON order_line_tab (ol_supply_w_id);
CREATE INDEX ON order_line_tab (ol_quantity);

CREATE INDEX ON order_tab (o_c_id);
CREATE INDEX ON order_tab (o_carrier_id);
CREATE INDEX ON order_tab (o_ol_cnt);

CREATE INDEX ON stock_tab (s_quantity);
CREATE INDEX ON stock_tab (s_ytd);
CREATE INDEX ON stock_tab (s_order_cnt);
CREATE INDEX ON stock_tab (s_remote_cnt);

CREATE INDEX ON warehouse_tab (w_ytd);
