mkdir certs my-safe-directory

cockroach cert create-ca \
--certs-dir=certs \
--ca-key=my-safe-directory/ca.key

cockroach cert create-node \
xcnd30.comp.nus.edu.sg \
my_db \
--certs-dir=certs/node1 \
--ca-key=my-safe-directory/ca.key

cockroach cert create-client \
root \
--certs-dir=certs \
--ca-key=my-safe-directory/ca.key

openssl pkcs8 -topk8 -inform PEM -outform DER -in certs/client.root.key -out certs/client.root.key.pk8 -nocrypt

cockroach start \
--certs-dir=certs \
--store=cockroach_store/node1 \
--listen-addr=xcnd30.comp.nus.edu.sg:26267 \
--http-addr=localhost:8090 \
--join=xcnd30.comp.nus.edu.sg:26267,xcnd31.comp.nus.edu.sg:26267,xcnd32.comp.nus.edu.sg:26267,xcnd33.comp.nus.edu.sg:26267,xcnd34.comp.nus.edu.sg:26267 \
--background

grep 'node starting' cockroach_store/node1/logs/cockroach.log -A 11