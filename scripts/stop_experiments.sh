for i in {0..4}
do
  host=cs4224c@xcnd$(($i+30)).comp.nus.edu.sg
  ssh ${host} -n "
    killall -9 java;
  "
done