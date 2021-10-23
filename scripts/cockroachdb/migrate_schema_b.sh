cockroach nodelocal upload project_files/data_files/warehouse.csv project_files/data_files_B/warehouse.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/district.csv project_files/data_files_B/district.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/customer.csv project_files/data_files_B/customer.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/order.csv project_files/data_files_B/order.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/item.csv project_files/data_files_B/item.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/order-line.csv project_files/data_files_B/order-line.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/stock.csv project_files/data_files_B/stock.csv --certs-dir="certs"

cockroach sql --certs-dir="certs" -f schema/cockroachdb/schema_b.sql