#!/bin/sh

# Stop 

TEST_DIR="$(dirname -- "$(readlink -f "$0")")"

kill -TERM "$(cat "${TEST_DIR}/conf/compData.pid")"
kill -TERM "$(cat "${TEST_DIR}/conf/simpData.pid")"
redis-cli -h 127.0.0.1 -p 6380 shutdown
sudo rabbitmqctl -n rabbit-simp-integration stop
