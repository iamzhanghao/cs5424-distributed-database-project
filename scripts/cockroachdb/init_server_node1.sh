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

cockroach start \
--certs-dir=certs \
--store=cockroach_store/node1 \
--listen-addr=localhost:26267 \
--http-addr=localhost:8090 \
--join=xcnd30.comp.nus.edu.sg:26267,xcnd31.comp.nus.edu.sg:26267,xcnd32.comp.nus.edu.sg:26267,xcnd33.comp.nus.edu.sg:26267,xcnd34.comp.nus.edu.sg:26267 \
--background

grep 'node starting' node1/logs/cockroach.log -A 11