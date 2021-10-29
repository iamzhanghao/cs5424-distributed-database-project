cockroach init --certs-dir=certs --host=localhost:26267

cockroach nodelocal upload project_files/data_files/warehouse.csv project_files/data_files/warehouse.csv --certs-dir="certs" --host=localhost:26267
cockroach nodelocal upload project_files/data_files/district.csv project_files/data_files/district.csv --certs-dir="certs" --host=localhost:26267
cockroach nodelocal upload project_files/data_files/customer.csv project_files/data_files/customer.csv --certs-dir="certs" --host=localhost:26267
cockroach nodelocal upload project_files/data_files/order.csv project_files/data_files/order.csv --certs-dir="certs" --host=localhost:26267
cockroach nodelocal upload project_files/data_files/item.csv project_files/data_files/item.csv --certs-dir="certs" --host=localhost:26267
cockroach nodelocal upload project_files/data_files/order-line.csv project_files/data_files/order-line.csv --certs-dir="certs" --host=localhost:26267
cockroach nodelocal upload project_files/data_files/stock.csv project_files/data_files/stock.csv --certs-dir="certs" --host=localhost:26267

cockroach sql --certs-dir="certs" -f schema/cockroachdb/schema_a.sql --host=localhost:26267
cockroach sql --certs-dir="certs" -f schema/cockroachdb/schema_b.sql --host=localhost:26267

# For Cockroach DB console https://localhost:8090
cockroach sql --certs-dir="certs" -f schema/cockroachdb/create_user.sql --host=localhost:26267
