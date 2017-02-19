#outgoing rule
tc qdisc add dev eth0 root netem delay 40ms 10ms distribution normal

#incoming rule
modprobe ifb
ip link set dev ifb0 up
tc qdisc add dev eth0 ingress
tc filter add dev eth0 parent ffff: protocol ip u32 match u32 0 0 flowid 1:1 action mirred egress redirect dev ifb0
tc qdisc add dev ifb0 root netem delay 40ms 10ms distribution normal
