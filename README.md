# cs5424-distributed-database-project

## Common Download
download `project_files` and put in project root

## Compile
```
mvn clean package
```

## CockroachDB
1. start up the cluster and init ssl

    ```zsh
    $ ./scripts/cockroachdb/init_local.sh
    ```

2. create schema and dump the data by:
    ```zsh
    $ ./scripts/cockroachdb/migrate_schema.sh
    ```

3. run the driver by
    ```zsh
    $ java -jar target/target/cockroachdb.jar <host> <port> <data_dir>
    ```
    or setup IntelliJ Configurations: `<host> <port> <data_dir>`


4. Stop CockroachDB cluster 
    ```zsh
    $ ./scripts/cockroachdb/reset.sh
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
