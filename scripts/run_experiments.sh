#args <exp#> <schema> <db> <port>
#e.g 1 A cockroachdb 26267

exp=$1
schema=$2
db=$3
port=$4

nohup sh scripts/stop_experiments.sh &

mvn clean package

for i in {0..4}
do
  ssh_host=cs4224c@xcnd$(($i+30)).comp.nus.edu.sg
  java_host=xcnd$(($i+30)).comp.nus.edu.sg
  for j in {1..8}
  do
      client=$((i*8 + j))
      nohup ssh ${ssh_host} -n "
        cd cs5424-distributed-database-project;
        pwd;
        java -jar target/${db}.jar ${java_host} ${port} ${schema} ${client}
      " > out/${db}-${schema}-${exp}-${client}.out &
      echo "Started client ${client} on ${java_host}"
      sleep 1;
  done
done


