#!/bin/bash

if [ $# -ne 8 ]; then
  echo 'usage:' $0 'publisher_count domain_id topic type_name sample_count sleep_interval run_id zk_connector' 
  exit 1
fi

publisher_count=$1
domain_id=$2
topic=$3
type_name=$4
sample_count=$5
sleep_interval=$6
run_id=$7
zk_connector=$8

for i in `seq 1 $publisher_count`;
do
  java -cp ./build/libs/pubsubCoord.jar edu.vanderbilt.kharesp.pubsubcoord.clients.ClientPublisher $domain_id $topic $type_name $sample_count $sleep_interval $run_id $zk_connector &
done
