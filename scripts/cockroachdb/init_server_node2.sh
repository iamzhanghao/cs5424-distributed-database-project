cockroach start \
--insecure \
--store=cockroach_store/node2 \
--listen-addr=xcnd31.comp.nus.edu.sg:26267 \
--http-addr=localhost:8090 \
--join=xcnd30.comp.nus.edu.sg:26267,xcnd31.comp.nus.edu.sg:26267,xcnd32.comp.nus.edu.sg:26267,xcnd33.comp.nus.edu.sg:26267,xcnd34.comp.nus.edu.sg:26267 \
#--background

grep 'node starting' cockroach_store/node2/logs/cockroach.log -A 11