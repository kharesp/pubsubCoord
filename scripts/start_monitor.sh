#!/bin/bash

if [ $# -ne 4 ]; then
  echo 'usage:' $0 'broker_type log_dir run_id zk_connector' 
  exit 1
fi

broker_type=$1
log_dir=$2
run_id=$3
zk_connector=$4

( ( nohup java -cp ./pubsubCoord.jar edu.vanderbilt.kharesp.pubsubcoord.monitoring.Monitor $broker_type $log_dir $run_id $zk_connector 1>/dev/null 2>&1 ) & ) 

sleep 1
