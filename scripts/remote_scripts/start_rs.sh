#!/bin/bash
mkdir -p ~/infrastructure_log/rs
( ( nohup java -cp ./pubsubCoord.jar edu.vanderbilt.kharesp.pubsubcoord.routing.RoutingService 1>~/infrastructure_log/rs/rs.log 2>&1 ) & ) 
