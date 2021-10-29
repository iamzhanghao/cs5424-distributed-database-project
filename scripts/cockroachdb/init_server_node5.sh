cockroach start \
--certs-dir=certs \
--store=cockroach_store/node5 \
--listen-addr=localhost:26267 \
--http-addr=localhost:8094 \
--join=xcnd30.comp.nus.edu.sg:26267,xcnd31.comp.nus.edu.sg:26267,xcnd32.comp.nus.edu.sg:26267,xcnd33.comp.nus.edu.sg:26267,xcnd34.comp.nus.edu.sg:26267 \
--background

grep 'node starting' cockroach_store/node5/logs/cockroach.log -A 11