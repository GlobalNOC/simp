# Simp

A small system for gathering large amounts of snmp data.  The pacakge contains both a collector and a data service interface.  A multi-process collector gathers SNMP data from a set of hosts and put that data into a local Redis database.  A set of data services then provides access to this data via RabbitMQ.  Currently this code is PoC without proper init scripts etc, so the following is a bit... rustic.

## Running The Collector

For now, you should have redis and rabbitmq installed locally and running.

```sh
./simp.pl --config /home/ebalas/config.xml --logging ../logging.conf
```

The config file controls collection interval, and the number of workers to use and what to collect.

```xml
<!--
  config.xml defines the config for the simp collector, with the individual hosts defined in hosts.conf
-->

<config>
 <! -- redis: defines how to connect to redis, which is used for data storage -->
 <redis host="127.0.0.1" port="6379"/>

  <group name="int" active="1" workers="2" interval="60" retention="6">
    <!-- mib: specifies the data to gather using getTree
      oid: is set to an oid substing expressed as dotted decimal
    -->
    <mib oid="1.3.6.1.2.1.2.2"/>
    <mib oid="1.3.6.1.2.1.31.1.1.1"/>
  </group>

  <group name="bgp" active="0" workers="2" interval="30" retention="2">
    <mib oid="1.3.6.1.2.1.15"/>
  </group>

  <group name="mac2ip" active="0" workers="5" interval="60" retention="2"
    <mib oid="1.3.6.1.2.1.17.4.3"/>
  </group>

</config>
```

The host.conf file contains the set of hosts to collect from and defines the collection group assignments

```xml
<config>
  <host ip="10.13.1.2" community="come on">
    <group id="int"/>
    <group id="bgp"/>
  </host>
  <host ip="10.13.1.1" community="farva man">
    <group id="int"/>
    <group id="bgp"/>
  </host>
  <host ip="10.13.2.2" community="same">
    <group id="int"/>
  </host>
  <host ip="10.13.2.1" community="team">
    <group id="mac2ip"/>
  </host>
  </group>
</config>
```

## Running the Data Service

```sh
./simpData.pl --config ../simpDataConfig.xml --logging ../logging.conf
```

The config is similar to that used by the poller with less details required.

```xml
<config workers="3" >
 <redis host="127.0.0.1" port="6379"/>
 <rabbitMQ host="127.0.0.1" port="5672"/>
</config>
```

## Testing

#### Testing information can be found [here](https://github.com/GlobalNOC/simp/tree/master/t)

