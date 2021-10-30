for s in xcnd30 xcnd31 xcnd32 xcnd33 xcnd34
do
ssh cs4224c@${s}.comp.nus.edu.sg -n << EOF
  killall -9 cockroach;
EOF
done


rm -rf cockroach_store;

for s in xcnd30 xcnd31 xcnd32 xcnd33 xcnd34
do

echo ${s}

ssh cs4224c@${s}.comp.nus.edu.sg -n << EOF
  killall -9 cockroach;
  cd cs5424-distributed-database-project/;
  source .bash_profile;
  whoami;
  echo ${s}.comp.nus.edu.sg;

  cockroach start
  --insecure
  --store=cockroach_store/node1
  --listen-addr=${s}.comp.nus.edu.sg:26267
  --http-addr=localhost:8090
  --join=xcnd30.comp.nus.edu.sg:26267,xcnd31.comp.nus.edu.sg:26267,xcnd32.comp.nus.edu.sg:26267,xcnd33.comp.nus.edu.sg:26267,xcnd34.comp.nus.edu.sg:26267
  --background;

EOF
done

cockroach init --insecure --host=xcnd30.comp.nus.edu.sg:26267
