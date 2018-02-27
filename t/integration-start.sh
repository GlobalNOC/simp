#!/bin/sh -e

# Starts up RabbitMQ, Redis, SimpData, and CompData for integration tests

TEST_DIR="$(dirname -- "$(readlink -f "$0")")"



# Set up environment for RabbitMQ, then start it:

RABBITMQ_NODENAME=rabbit-simp-integration
RABBITMQ_NODE_PORT=5673
RABBITMQ_DIST_PORT=25673

RMQ_DIR="$(mktemp -d "/tmp/rmq.$$.XXXXXXXX")"
RABBITMQ_MNESIA_BASE="${RMQ_DIR}/mnesia"
RABBITMQ_LOG_BASE="${RMQ_DIR}/logs"
mkdir "$RABBITMQ_MNESIA_BASE" "$RABBITMQ_LOG_BASE"

cp "${TEST_DIR}/conf/rabbitmq.config" "${TEST_DIR}/conf/rabbitmq-adv.config" "${TEST_DIR}/conf/enabled_plugins" "${RMQ_DIR}"

RABBITMQ_CONFIG_FILE="${RMQ_DIR}/rabbitmq"
RABBITMQ_ADVANCED_CONFIG_FILE="${RMQ_DIR}/rabbitmq-adv"
RABBITMQ_ENABLED_PLUGINS_FILE="${RMQ_DIR}/enabled_plugins"
RABBITMQ_PID_FILE="${RMQ_DIR}/rabbitmq.pid"

export RABBITMQ_CONFIG_FILE RABBITMQ_ADVANCED_CONFIG_FILE \
    RABBITMQ_ENABLED_PLUGINS_FILE RABBITMQ_PID_FILE RABBITMQ_NODENAME \
    RABBITMQ_MNESIA_BASE RABBITMQ_LOG_BASE \
    RABBITMQ_DIST_PORT RABBITMQ_NODE_PORT

sudo chown -R rabbitmq:rabbitmq "${RMQ_DIR}"

sudo -E /usr/sbin/rabbitmq-server -detached



# Redis:

REDIS_DIR="$(mktemp -d "${TEST_DIR}/redis.$$.XXXXXXXX")"
mkdir "${REDIS_DIR}/db"
cp -a "${TEST_DIR}/conf/dump.rdb" "${REDIS_DIR}/db/"
sed -e "s,@TEST_DIR@,${TEST_DIR},g" -e "s,@DB_DIR@,${REDIS_DIR},g" < "${TEST_DIR}/conf/redis.conf.in" > "${TEST_DIR}/conf/redis.conf"

redis-server "${TEST_DIR}/conf/redis.conf"

# Give RabbitMQ and Redis a little time to start up
sleep 5



for I in simpDataConfig.xml compDataConfig.xml
do
    sed -e "s,@TEST_DIR@,${TEST_DIR},g" < "${TEST_DIR}/conf/${I}.in" > "${TEST_DIR}/conf/${I}"
done
SIMP_PERL_DIR="${TEST_DIR}/../lib"

# SimpData:
/usr/bin/perl -I"$SIMP_PERL_DIR" "${TEST_DIR}/../bin/simp-data.pl" \
    --config "${TEST_DIR}/conf/simpDataConfig.xml" \
    --logging "${TEST_DIR}/conf/simpDataLogging.conf"



# CompData:
/usr/bin/perl -I"$SIMP_PERL_DIR" "${TEST_DIR}/../bin/simp-comp.pl" \
    --config "${TEST_DIR}/conf/compDataConfig.xml" \
    --logging "${TEST_DIR}/conf/compDataLogging.conf"

# Give SimpData and CompData a few seconds to come up:
sleep 5
