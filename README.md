# cs5424-distributed-database-project

## DB File Download

- Download `project_files` and copy to project root http://www.comp.nus.edu.sg/~cs4224/project_files_4.zip

- For Cassandra you need download one more csv file for `combined_order_tab` and copy it under `project_files` directory together with other files https://drive.google.com/file/d/18LKAVnvqQEaqbm0BYpNVqsPFT4wV4Xw5/view?usp=sharing

## Install Cassandra and Cockroach DB

```zsh
# Mac (Cockroach)
$ brew install cockroachdb/tap/cockroach

# Mac (Cassandra)
$ brew install cassandra
$ brew install python
$ pip install cql

# Linux (Cockroach)
$ curl https://binaries.cockroachdb.com/cockroach-v21.1.7.linux-amd64.tgz | tar -xz && sudo cp -i cockroach-v21.1.7.linux-amd64 /temp/CS4224C/

# Linux (Cassandra)
$ curl https://downloads.apache.org/cassandra/4.0.0/apache-cassandra-4.0.0-bin.tar.gz.sha256 | tar -xz && sudo cp -i apache-cassandra-4.0.0-bin /temp/CS4224C/
```

## Configuration

1. Copy lines from `config/bash_profile` to your own `.bash_profile`
2. Use `config/cassandra.yaml` for Cassandra

## Compile `cassandra.jar` and `cockroachdb.jar`

```zsh
$ mvn clean package
```

## Instructions for running experiments

### Cockroach DB

1. Start 5 nodes

   ```zsh
   # On xcnd30.comp.nus.edu.sg
   $ ./scripts/cockroachdb/start_xcnd30.sh
   # On xcnd31.comp.nus.edu.sg
   $ ./scripts/cockroachdb/start_xcnd31.sh
   # On xcnd32.comp.nus.edu.sg
   $ ./scripts/cockroachdb/start_xcnd32.sh
   # On xcnd33.comp.nus.edu.sg
   $ ./scripts/cockroachdb/start_xcnd33.sh
   # On xcnd34.comp.nus.edu.sg
   $ ./scripts/cockroachdb/start_xcnd34.sh

   ```

2. Init cluster and load initial data
   ```zsh
   # Run on xcnd30.comp.nus.edu.sg
   $ ./scripts/cockroachdb/migrate_data.sh
   ```
3. Run experiments for workload A/B with 40 clients

   ```zsh
   $ ./scripts/run_experiments.sh <experiment-number> <workload-type> cockroachdb 26267
   ```

   e.g.

   ```zsh
   $ ./scripts/run_experiments.sh 1 A cockroachdb 26267
   ```

   Outputs for each client will be stored at `out/cockroachdb-<experiment-number>-<workload-type>-<client-id>.out`

   Final statistics will be stored at `out/cockroachdb-<experiment-number>-<workload-type>.csv`

   If you only need to run one client:

   ```zsh
   $ java -jar target/cockroachdb.jar <hostname> 26267 <workload-type> <client-id> <statistics-csv-dir> 0
   ```

   e.g.

   ```zsh
   $ java -jar target/cockroachdb.jar xcnd30.comp.nus.edu.sg 26267 A a clients.csv 0
   ```

4. To abort experiments
   ```
   $ ./scripts/stop_experiments.sh
   ```

### Cassandra

1. Start 5 nodes

   ```zsh
   # On xcnd30.comp.nus.edu.sg
   $ cassandra
   # On xcnd31.comp.nus.edu.sg
   $ cassandra
   # On xcnd32.comp.nus.edu.sg
   $ cassandra
   # On xcnd33.comp.nus.edu.sg
   $ cassandra
   # On xcnd34.comp.nus.edu.sg
   $ cassandra
   ```

2. Init cluster and load initial data
   ```zsh
   # Run on xcnd30.comp.nus.edu.sg
   $ ./scripts/cassandra/migrate_data.sh
   ```
3. Run experiments for workload A/B with 40 clients

   ```zsh
   $ ./scripts/run_experiments.sh <experiment-number> <workload-type> cassandra 3042
   ```

   e.g.

   ```zsh
   $ ./scripts/run_experiments.sh 1 A cassandra 3042
   ```

   Outputs for each client will be stored at `out/cassandra-<experiment-number>-<workload-type>-<client-id>.out`

   Final statistics will be stored at `out/cassandra-<experiment-number>-<workload-type>.csv`

   If you only need to run one client:

   ```zsh
   $ java -jar target/cassandra.jar <hostname> 3042 <workload-type> <client-id> <statistics-csv-dir> 0
   ```

   e.g.

   ```zsh
   $ java -jar target/cassandra.jar xcnd30.comp.nus.edu.sg 3042 A a clients.csv 0
   ```

4. To abort experiments
   ```
   $ ./scripts/stop_experiments.sh
   ```
