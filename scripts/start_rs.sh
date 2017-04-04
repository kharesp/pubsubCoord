#!/bin/bash
mkdir -p ~/infrastructure_log/rs
( ( nohup $NDDSHOME/bin/rtiroutingservice -cfgName PubSubCoord -cfgFile xml/administration.xml -verbosity 6  1>~/infrastructure_log/rs/rs.log 2>&1 ) & )
sleep 30
