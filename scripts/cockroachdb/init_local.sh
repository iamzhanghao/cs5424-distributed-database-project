mkdir certs my-safe-directory

cockroach cert create-ca \
--certs-dir=certs \
--ca-key=my-safe-directory/ca.key

cockroach cert create-node \
localhost \
my_db \
--certs-dir=certs \
--ca-key=my-safe-directory/ca.key

cockroach cert create-client \
root \
--certs-dir=certs \
--ca-key=my-safe-directory/ca.key

openssl pkcs8 -topk8 -inform PEM -outform DER -in certs/client.root.key -out certs/client.root.key.pk8 -nocrypt

cockroach start \
--certs-dir=certs \
--store=node1 \
--listen-addr=localhost:26257 \
--http-addr=localhost:8080 \
--join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node2 \
--listen-addr=localhost:26258 \
--http-addr=localhost:8081 \
--join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node3 \
--listen-addr=localhost:26259 \
--http-addr=localhost:8082 \
--join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node4 \
--listen-addr=localhost:26260 \
--http-addr=localhost:8083 \
--join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node5 \
--listen-addr=localhost:26261 \
--http-addr=localhost:8084 \
--join=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 \
--background

cockroach init --certs-dir=certs --host=localhost:26257

grep 'node starting' node1/logs/cockroach.log -A 11