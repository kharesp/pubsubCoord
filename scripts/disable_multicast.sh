#!/bin/bash

iptables -A INPUT -m pkttype --pkt-type multicast -j DROP
iptables -A OUTPUT -m pkttype --pkt-type multicast -j DROP
