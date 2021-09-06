cockroach nodelocal upload project_files/data_files_B/warehouse.csv project_files/data_files_B/warehouse.csv --insecure
cockroach nodelocal upload project_files/data_files_B/district.csv project_files/data_files_B/district.csv --insecure
cockroach nodelocal upload project_files/data_files_B/customer.csv project_files/data_files_B/customer.csv --insecure
cockroach nodelocal upload project_files/data_files_B/order.csv project_files/data_files_B/order.csv --insecure
cockroach nodelocal upload project_files/data_files_B/item.csv project_files/data_files_B/item.csv --insecure
cockroach nodelocal upload project_files/data_files_B/order-line.csv project_files/data_files_B/order-line.csv --insecure
cockroach nodelocal upload project_files/data_files_B/stock.csv project_files/data_files_B/stock.csv --insecure

cockroach sql --insecure -f schema/cockroachdb/schema_b.sql