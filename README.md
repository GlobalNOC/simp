# simp
A small system for gathering large amounts of snmp data.  The pacakge contains both a collector and a data service interface.  A multi-process collector gathers SNMP data from a set of hosts and put that data into a local Redis database.  A set of data services then provides access to this data via RabbitMQ.  Currently this code is PoC without proper init scripts etc, so the following is a bit... rustic.

##running the collector:
For now, you should have redis and rabbitmq installed locally and running.
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
##running the data service:
```
./simpData.pl --config ../simpDataConfig.xml --logging ../logging.conf 
```
The config is similar to that used by the poller with less details required.
```
<config workers="3" >
 <redis host="127.0.0.1" port="6379"/>
 <rabbitMQ host="127.0.0.1" port="5672"/>
</config>
```
##Testing
Currently there are no unit tests.  To stress test some there are scripts in *bin* called *genTestData.pl* and *testClient.pl* 
