# cs5424-distributed-database-project

## Common Download
download `project_files` and put in project root

## Compile
```
mvn clean package
```

## CockroachDB
1. start up the cluster

2. create schema A and dump the data by:
```
scripts/cockroachdb/migrate_schema_a.sh
```

3. create schema B and dump the data by:
```
scripts/cockroachdb/migrate_schema_b.sh
```

4. run the driver by
```
java -jar target/target/cockroachdb.jar <host> <port> <database> <data_dir>
```

## Cassandra
1. start up the cluster

2. create schema A and dump the data by:
```
scripts/cassandra/migrate_schema_a.sh
```

3. create schema B and dump the data by:
```
scripts/cassandra/migrate_schema_b.sh
```

4. run the driver by
```
java -jar target/target/cassandra.jar <host> <port> <keyspace> <data_dir>
```
