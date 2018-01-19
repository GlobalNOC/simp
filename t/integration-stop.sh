#!/bin/sh

# Stops services started by integration-start.sh

TEST_DIR="$(dirname -- "$(readlink -f "$0")")"

kill -TERM "$(cat "${TEST_DIR}/conf/compData.pid")"
A=$?
kill -TERM "$(cat "${TEST_DIR}/conf/simpData.pid")"
B=$?

# Give CompData and SimpData a few seconds to stop their worker processes
sleep 3

redis-cli -h 127.0.0.1 -p 6380 shutdown
C=$?
sudo rabbitmqctl -n rabbit-simp-integration stop
D=$?

[ 0 -eq "$A" -a 0 -eq "$B" -a 0 -eq "$C" -a 0 -eq "$D" ]
