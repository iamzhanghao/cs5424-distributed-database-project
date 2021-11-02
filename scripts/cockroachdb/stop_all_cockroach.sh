for s in xcnd30 xcnd31 xcnd32 xcnd33 xcnd34
do
ssh cs4224c@${s}.comp.nus.edu.sg -n "
  killall -9 cockroach;
" &
done

sleep 5;

rm -rf /temp/CS4224C/cockroach_store;