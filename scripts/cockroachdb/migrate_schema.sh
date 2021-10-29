cockroach init --insecure --host=localhost:26267

cockroach nodelocal upload project_files/data_files/warehouse.csv project_files/data_files/warehouse.csv --insecure --host=localhost:26267
cockroach nodelocal upload project_files/data_files/district.csv project_files/data_files/district.csv --insecure --host=localhost:26267
cockroach nodelocal upload project_files/data_files/customer.csv project_files/data_files/customer.csv --insecure --host=localhost:26267
cockroach nodelocal upload project_files/data_files/order.csv project_files/data_files/order.csv --insecure --host=localhost:26267
cockroach nodelocal upload project_files/data_files/item.csv project_files/data_files/item.csv --insecure --host=localhost:26267
cockroach nodelocal upload project_files/data_files/order-line.csv project_files/data_files/order-line.csv --insecure --host=localhost:26267
cockroach nodelocal upload project_files/data_files/stock.csv project_files/data_files/stock.csv --insecure --host=localhost:26267

cockroach sql --insecure -f schema/cockroachdb/schema_a.sql --host=localhost:26267
cockroach sql --insecure -f schema/cockroachdb/schema_b.sql --host=localhost:26267

# For Cockroach DB console https://localhost:8090
cockroach sql --insecure -f schema/cockroachdb/create_user.sql --host=localhost:26267
