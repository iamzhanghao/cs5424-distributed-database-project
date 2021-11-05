use schema_a;

select sum(OL_AMOUNT) as ol_amount, sum(OL_QUANTITY) as ol_quantity from schema_a.Order_line_tab;

select sum(S_QUANTITY) as s_quantity, sum(S_YTD) as s_ytd, sum(S_ORDER_CNT) as s_order_cnt, sum(S_REMOTE_CNT) as s_remote_cnt from schema_a.Stock_tab;


select sum(OL_AMOUNT) as ol_amount, sum(OL_QUANTITY) as ol_quantity from schema_b.Order_line_tab;

select sum(S_QUANTITY) as s_quantity, sum(S_YTD) as s_ytd, sum(S_ORDER_CNT) as s_order_cnt, sum(S_REMOTE_CNT) as s_remote_cnt from schema_b.Stock_tab;