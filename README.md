# simp
A small system for gathering large amounts of snmp data.  The pacakge contains both a collector and a data service interface.  A multi-process collector gathers SNMP data from a set of hosts and put that data into a local Redis database.  A set of data services then provides access to this data via RabbitMQ.

##running the collector:
```
./simp.pl --config /home/ebalas/config.xml --logging ../logging.conf
```

The config file controls collection interval, and the number of workers to use, what to collect and from whom.

```
<config workers="1" poll_interval="10">
 <redis host="127.0.0.1" port="6379"/>
  <group>
    <mib oid="1.3.6.1.2.1.2.2.1"/>
    <mib oid="1.3.6.1.2.1.1.3"/>
    <host ip="10.13.1.2" community="easyecaneat"/>
    <host ip="10.13.1.1" community="abigfaaat"/> 
  </group>
</config>
```
