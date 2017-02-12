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

for i in `seq 1 $subscriber_count`;
do
  java -cp ./build/libs/pubsubCoord.jar edu.vanderbilt.kharesp.pubsubcoord.clients.ClientSubscriber $domain_id $topic $type_name $sample_count $log_dir $run_id $zk_connector &
done
