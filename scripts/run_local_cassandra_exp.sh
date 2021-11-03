#args <exp#> <schema> <db> <port>
#e.g 1 A cockroachdb 26267

exp='local'
schema='A'
db='cassandra'
port=9042

mvn clean package

for client in {1..8}
  do
      nohup java -jar target/${db}.jar localhost ${port} ${schema} ${client} out/${db}-${schema}-${exp}.csv > out/${db}-${schema}-${exp}-${client}.out &
      echo "Started client ${client} on port ${port}"
      sleep 1;
  done