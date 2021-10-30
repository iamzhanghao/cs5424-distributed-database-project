USE schema_a;

CREATE INDEX ON order_line_tab (ol_d_id);
CREATE INDEX ON order_line_tab (ol_w_id);
CREATE INDEX ON order_line_tab (ol_o_id);


CREATE INDEX ON customer_tab (c_d_id);
CREATE INDEX ON customer_tab (c_w_id);
CREATE INDEX ON customer_tab (c_id);
