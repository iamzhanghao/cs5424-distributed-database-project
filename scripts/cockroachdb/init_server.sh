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

#TODO: add cluster IP for 5 server
cockroach start \
--certs-dir=certs \
--store=node1 \
--listen-addr=localhost:26267 \
--http-addr=localhost:8090 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node2 \
--listen-addr=localhost:26268 \
--http-addr=localhost:8091 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node3 \
--listen-addr=localhost:26269 \
--http-addr=localhost:8092 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node4 \
--listen-addr=localhost:26260 \
--http-addr=localhost:8093 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26260,localhost:26261 \
--background

cockroach start \
--certs-dir=certs \
--store=node5 \
--listen-addr=localhost:26261 \
--http-addr=localhost:8094 \
--join=localhost:26267,localhost:26268,localhost:26269,localhost:26260,localhost:26261 \
--background

cockroach init --certs-dir=certs --host=localhost:26267

grep 'node starting' node1/logs/cockroach.log -A 11