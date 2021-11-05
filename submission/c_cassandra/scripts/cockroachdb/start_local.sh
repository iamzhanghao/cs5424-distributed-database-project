killall -9 cockroach

rm -rf node1/
rm -rf node2/
rm -rf node3/
rm -rf node4/
rm -rf node5/
rm -rf certs/
rm -rf my-safe-directory/

rm -rf cockroach_store/

cockroach start \
--insecure \
--store=cockroach_store/node1 \
--listen-addr=localhost:26267 \
--http-addr=localhost:8090 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26270,localhost:26271 \
--background

cockroach start \
--insecure \
--store=cockroach_store/node2 \
--listen-addr=localhost:26268 \
--http-addr=localhost:8091 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26270,localhost:26271 \
--background

cockroach start \
--insecure \
--store=cockroach_store/node3 \
--listen-addr=localhost:26269 \
--http-addr=localhost:8092 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26270,localhost:26271 \
--background

cockroach start \
--insecure \
--store=cockroach_store/node4 \
--listen-addr=localhost:26270 \
--http-addr=localhost:8093 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26270,localhost:26271 \
--background

cockroach start \
--insecure \
--store=cockroach_store/node5 \
--listen-addr=localhost:26271 \
--http-addr=localhost:8094 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26270,localhost:26271 \
--background

grep 'node starting' node1/logs/cockroach.log -A 11

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

cockroach sql --insecure -f schema/cockroachdb/add_index_a.sql --host=localhost:26267
cockroach sql --insecure -f schema/cockroachdb/add_index_b.sql --host=localhost:26267
