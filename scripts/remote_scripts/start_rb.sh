#!/bin/bash

if [ $# -ne 2 ]; then
  echo 'usage:' $0 'zk_connector(address:port) emulated_broker(0/1)'
  exit 1
fi

zk_connector=$1
emulated_broker=$2

mkdir -p ~/infrastructure_log/broker

( ( nohup java -cp ./pubsubCoord.jar edu.vanderbilt.kharesp.pubsubcoord.brokers.RoutingBroker  $zk_connector $emulated_broker 1>~/infrastructure_log/broker/rb.log 2>&1 ) & )
sleep 1
