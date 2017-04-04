#!/bin/bash

for f in /home/ubuntu/log/sub/sub*.log;
do
  tail -1  $f
done
