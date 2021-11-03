mvn clean package

schema=A

for client in {0..39}
do
    java -jar target/cockroachdb.jar localhost 26267 ${schema} ${client} out/cockroachdb-${schema}-seq.csv
done