#!/bin/bash

if [ $# -ne 8 ]; then
  echo 'usage:' $0 'subscriber_count domain_id topic type_name sample_count log_dir run_id zk_connector' 
  exit 1
fi

subscriber_count=$1
domain_id=$2
topic=$3
type_name=$4
sample_count=$5
log_dir=$6
run_id=$7
zk_connector=$8

mkdir -p ~/log/sub

for i in `seq 1 $subscriber_count`;
do
  ( ( nohup java -cp ./pubsubCoord.jar edu.vanderbilt.kharesp.pubsubcoord.clients.ClientSubscriber $domain_id $topic $type_name $sample_count $log_dir $run_id $zk_connector 1>~/log/sub/sub_"$topic"_"$i".log 2>&1 ) & )
  sleep 1
done
