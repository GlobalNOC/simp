# simp
A small system for gathering large amounts of snmp data.  The pacakge contains both a collector and a data service interface.  A multi-process collector gathers SNMP data from a set of hosts and put that data into a local Redis database.  A set of data services then provides access to this data via RabbitMQ.
