
cd cs5424-distributed-database-project;

cockroach start --insecure \
  --store=/temp/CS4224C/cockroach_store/xcnd33 \
  --listen-addr=xcnd33.comp.nus.edu.sg:26267 \
  --http-addr=localhost:8090 \
  --join=xcnd30.comp.nus.edu.sg:26267,xcnd31.comp.nus.edu.sg:26267,xcnd32.comp.nus.edu.sg:26267,xcnd33.comp.nus.edu.sg:26267,xcnd34.comp.nus.edu.sg:26267 \
  --background