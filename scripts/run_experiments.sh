#args <exp#> <schema> <db> <port>
#e.g 1 A cockroachdb 26267

exp=$1
schema=$2
db=$3
port=$4

sh scripts/stop_experiments.sh

mvn clean package

for i in {0..4}
do
  host=cs4224c@xcnd$(($i+30)).comp.nus.edu.sg

  for j in {1..8}
  do
      file=$((i*8 + j))
      ssh ${host} -n "
        java -jar target/${db}.jar ${host} ${port} ${schema} project_files/xact_files_${schema}/${file}.txt
      "
      echo $n,
  done
done