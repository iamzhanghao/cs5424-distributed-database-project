cockroach nodelocal upload project_files/data_files_A/warehouse.csv project_files/data_files_A/warehouse.csv --insecure
cockroach nodelocal upload project_files/data_files_A/district.csv project_files/data_files_A/district.csv --insecure
cockroach nodelocal upload project_files/data_files_A/customer.csv project_files/data_files_A/customer.csv --insecure
cockroach nodelocal upload project_files/data_files_A/order.csv project_files/data_files_A/order.csv --insecure
cockroach nodelocal upload project_files/data_files_A/item.csv project_files/data_files_A/item.csv --insecure
cockroach nodelocal upload project_files/data_files_A/order-line.csv project_files/data_files_A/order-line.csv --insecure
cockroach nodelocal upload project_files/data_files_A/stock.csv project_files/data_files_A/stock.csv --insecure

cockroach sql --insecure -f schema/cockroachdb/schema_a.sql