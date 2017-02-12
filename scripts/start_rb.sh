#!/bin/bash

if [ $# -ne 2 ]; then
  echo 'usage:' $0 'zk_connector(address:port) emulated_broker(0/1)'
  exit 1
fi

zk_connector=$1
emulated_broker=$2

java -cp ./pubsubCoord.jar edu.vanderbilt.kharesp.pubsubcoord.brokers.RoutingBroker  $zk_connector $emulated_broker &
