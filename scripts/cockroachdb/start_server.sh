for s in xcnd30 xcnd31 xcnd32 xcnd33 xcnd34
do

echo ${s}

ssh cs4224c@${s}.comp.nus.edu.sg -n << EOF
  killall -9 cockroach;
  cd cs5424-distributed-database-project/;
  source .bash_profile;
  whoami;
  echo ${s}.comp.nus.edu.sg


EOF
done