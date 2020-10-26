# SIMP

SIMP is a small system for gathering large amounts of snmp data.

The pacakge contains both a collector and a data service interface.
A multi-process collector gathers SNMP data from a set of hosts and puts that data into a redis database.
A set of data services then provides access to this data via RabbitMQ.

## Running The Collector

You will need a redis and rabbit server, accesible to the SIMP daemons.

For now, you should have redis and rabbitmq installed locally and running.

```bash
simp-poller.pl --config /etc/simp/poller/config.xml --logging /etc/simp/poller/logging.conf --user simp --group simp
```

The config files controls collection interval, and the number of workers to use and what to collect.

```xml
<?xml version="1.0" encoding="UTF-8" ?>

<!-- Simp-Poller Main Configuration -->
<config>
    <!-- Redis host connection information -->
    <redis ip="127.0.0.1" port="6379" reconnect="60" reconnect_every="500" read_timeout="2" write_timeout="3" />

    <!-- How long to keep data in Redis before purging (seconds) -->
    <purge interval="3600" />

    <!-- Default num of results to get per SNMP request -->
    <request_size results="15" />

    <!-- Where to write status files -->
    <status dir="/var/lib/simp/poller/" />
</config>
```

You will need at least one group definition in /etc/simp/poller/groups.d.
Here is an example all_interfaces.xml group.

```xml
<?xml version="1.0" encoding="UTF-8" ?>

<group workers="1" interval="60" timeout="15">
  <mib oid="1.3.6.1.2.1.31.1.1.1.6" /> <!-- inHCbytes -->
  <mib oid="1.3.6.1.2.1.31.1.1.1.10" /> <!-- outHCbytes -->
  <mib oid="1.3.6.1.2.1.2.2.1.14" /> <!-- inerror -->
  <mib oid="1.3.6.1.2.1.2.2.1.20" /> <!-- outerror -->
  <mib oid="1.3.6.1.2.1.2.2.1.11" /> <!-- inUcast -->
  <mib oid="1.3.6.1.2.1.2.2.1.17" /> <!-- outUcast -->
  <mib oid="1.3.6.1.2.1.2.2.1.13" /> <!-- indiscard -->
  <mib oid="1.3.6.1.2.1.2.2.1.19" /> <!-- outdiscard -->
  <mib oid="1.3.6.1.2.1.2.2.1.7" /> <!-- ifAdminStatus -->
  <mib oid="1.3.6.1.2.1.2.2.1.8" /> <!-- ifOperStatus -->
  <mib oid="1.3.6.1.2.1.31.1.1.1.1" /> <!-- ifName -->
  <mib oid="1.3.6.1.2.1.31.1.1.1.18" /> <!-- ifAlias -->
</group>
```

And you need a file in /etc/simp/poller/hosts.d to define the device collections.
The group id must match the file name in /etc/simp/poller/groups.d.

```xml
<?xml version="1.0" encoding="UTF-8"?>

<config>
    <!-- Connecticut Education 24x7-mem nodes -->
    <host name="switch2.example.host" ip="1.1.1.1" community="its_a_secret_to_everyone" snmp_version="2c">
        <group id="all_interfaces" />
    </host>
</config>
```

## Running the Data Service

```bash
/usr/bin/simp-data.pl --config /etc/simp/data/config.xml --logging /etc/simp/data/logging.conf --user simp --group simp
```

The config is similar to that used by the poller with less details required.

```xml
<?xml version="1.0" encoding="UTF-8" ?>

<!-- Simp-Data Main Config -->
<config workers="4">
    <redis ip="127.0.0.1" port="6379" reconnect="60" reconnect_every="500" read_timeout="2" write_timeout="3" />
    <rabbitmq ip="127.0.0.1" port="5672" user="guest" password="guest" />
</config>
```

## Running the composite service

```bash
/usr/bin/simp-comp.pl --config /etc/simp/comp/config.xml --logging /etc/simp/comp/logging.conf --user simp --group simp
```

Config example:

```xml
<?xml version="1.0" encoding="UTF-8" ?>

<config workers="4">
    <rabbitmq ip="127.0.0.1" port="5672" user="guest" password="guest" />
</config>
```

## Testing

#### Testing information can be found [here](https://github.com/GlobalNOC/simp/tree/master/t)
