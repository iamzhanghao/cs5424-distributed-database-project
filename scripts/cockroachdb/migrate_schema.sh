cockroach nodelocal upload project_files/data_files/warehouse.csv project_files/data_files/warehouse.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/district.csv project_files/data_files/district.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/customer.csv project_files/data_files/customer.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/order.csv project_files/data_files/order.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/item.csv project_files/data_files/item.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/order-line.csv project_files/data_files/order-line.csv --certs-dir="certs"
cockroach nodelocal upload project_files/data_files/stock.csv project_files/data_files/stock.csv --certs-dir="certs"

cockroach sql --certs-dir="certs" -f schema/cockroachdb/schema.sql
