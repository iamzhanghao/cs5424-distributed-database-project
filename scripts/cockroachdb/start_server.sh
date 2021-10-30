for s in xcnd30 xcnd31 xcnd32 xcnd33 xcnd34
do
ssh cs4224c@${s}.comp.nus.edu.sg -n "
  killall -9 cockroach;
"
done

rm -rf cockroach_store;
rm -rf cockroach-data;

sleep 5;

for s in xcnd30 xcnd31 xcnd32 xcnd33 xcnd34
do
ssh cs4224c@${s}.comp.nus.edu.sg -n "
  source .bash_profile;
  cd cs5424-distributed-database-project/;
  whoami;
  echo ${s}.comp.nus.edu.sg;

  cockroach start --insecure \
  --store=cockroach_store/${s} \
  --listen-addr=${s}.comp.nus.edu.sg:26267 \
  --http-addr=localhost:8090 \
  --join=xcnd30.comp.nus.edu.sg:26267,xcnd31.comp.nus.edu.sg:26267,xcnd32.comp.nus.edu.sg:26267,xcnd33.comp.nus.edu.sg:26267,xcnd34.comp.nus.edu.sg:26267 \
  --background
"
done;

cockroach init --insecure --host=xcnd30.comp.nus.edu.sg:26267;

cockroach nodelocal upload project_files/data_files/warehouse.csv project_files/data_files/warehouse.csv --insecure --host=xcnd30.comp.nus.edu.sg:26267
cockroach nodelocal upload project_files/data_files/district.csv project_files/data_files/district.csv --insecure --host=xcnd30.comp.nus.edu.sg:26267
cockroach nodelocal upload project_files/data_files/customer.csv project_files/data_files/customer.csv --insecure --host=xcnd30.comp.nus.edu.sg:26267
cockroach nodelocal upload project_files/data_files/order.csv project_files/data_files/order.csv --insecure --host=xcnd30.comp.nus.edu.sg:26267
cockroach nodelocal upload project_files/data_files/item.csv project_files/data_files/item.csv --insecure --host=xcnd30.comp.nus.edu.sg:26267
cockroach nodelocal upload project_files/data_files/order-line.csv project_files/data_files/order-line.csv --insecure --host=xcnd30.comp.nus.edu.sg:26267
cockroach nodelocal upload project_files/data_files/stock.csv project_files/data_files/stock.csv --insecure --host=xcnd30.comp.nus.edu.sg:26267

cockroach sql --insecure -f schema/cockroachdb/schema_a.sql --host=xcnd30.comp.nus.edu.sg:26267
cockroach sql --insecure -f schema/cockroachdb/schema_b.sql --host=xcnd30.comp.nus.edu.sg:26267
