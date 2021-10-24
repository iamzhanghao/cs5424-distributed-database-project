# cs5424-distributed-database-project

## Common Download
download `project_files` and put in project root

http://www.comp.nus.edu.sg/~cs4224/project_files_4.zip 

## Package install 
```zsh
# Mac
$ brew install cockroachdb/tap/cockroach 

# Linux
$ curl https://binaries.cockroachdb.com/cockroach-v21.1.11.linux-amd64.tgz | tar -xz && sudo cp -i cockroach-v21.1.11.linux-amd64/cockroach /usr/local/bin/
```

## Compile
```
mvn clean package
```

## CockroachDB
1.  init ssl and start up the cluster with 5 nodes

    ```zsh
    # Local Debug
    $ ./scripts/cockroachdb/init_local.sh
    
    # Run on server
    $ ./scripts/cockroachdb/init_server.sh
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
    
2. create schema data by:
    ```zsh
    $ scripts/cassandra/migrate_schema.sh
    ```

3. run the driver by
    ```zsh
    $ java -jar target/target/cassandra.jar <host> <port> <data_dir>
    ```
