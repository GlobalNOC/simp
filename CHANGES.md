# SIMP ChangeLog

## 1.11.0 - Dec 22, 2022
* Added simp-poller support for multiple host configurations for a single host
* Added simp-poller support for all Net::SNMP parameters
* Added simp-poller support for logging group worker configuration parameters at startup
* Improved simp-poller worker configuration handling
* Improved simp-poller worker balancing in preparation for automatic worker scaling
* Fixed issue where simp-poller would not allow remote Redis server configuration
* Fixed issue where simp-poller would not properly identify failing SNMP requests for status monitoring
* Fixed issue where simp-poller would discard status monitoring data for hosts with multiple SNMP configurations
* Fixed issue where simp-poller would spawn zombie workers with no hosts configured for their group
* Fixed issue where simp-poller would ignore additional configurations for the same host
* Fixed issue where simp-tsds would fail while encoding JSON
* Deprecated simp-poller "use_unix_socket" flag, now implied by the specification of a "unix_socket"

## 1.10.0 - Aug 22, 2022
* Added IPv6 SNMP support to Poller
* Fixed issue where Poller could not properly lookup previous polling cycles
* Fixed issue where Poller would not combine groups for a host when defined across multiple host configuration files.

## 1.9.1 - Apr 26, 2022
* hotfix: hash for tracking workers now cleared when master is restarted with HUP

## 1.9.0 - Apr 15, 2022

* added monitoring features to simp-tsds for push operations
  * workers write status files, reporting any errors while pushing to TSDS
  * master checks these for freshness & errors, writes an aggregate report
* refactored some code in Master.pm, re: bugs caused by AnyEvent

## 1.4.1 - Mar 12, 2020

* added new required config settings to control redis connection settings
  * read_timeout, write_timeout, reconnect, reconnect_every settings exposed in config for data and poller

* added more complete SNMPv3 connection options
  * auth_password, auth_protocol, priv_key, priv_password, priv_protocol. See Net::SNMP docs for meanings

## 1.4.0 - Sept 18, 2019

* add require_match/invert_match to data fields
  * allows for dropping of an entire composite colletion if the match fails
  * require_match is a regex pattern
  * invert_match can be used to negate the matching
* add drop conversion
  * allows for dropping of a single field within the collection
  * can be used when you need a field for a calculation, but don't want the field itself

## 1.3.0 - Sept 6, 2019

* Add support for single OID collections, indicated by adding the `single="1"` attribute to the OIDs
in Poller configuration.
* Comp output will now always include all fields defined in the composite, even if no associated value
was returned from Data. This keeps the output data structure consistent regardless of what data has
been polled. Missing fields are added with null values.

## 1.2.6 - July 29, 2019

* Comp no longer writes logs in /tmp/
* Poller no longer writes status files for a group unless there are no pending replies left for the host

## 1.2.4 and 1.2.5 - July 25, 2019

* Updated poller to delete status files for groups that are no longer configured for a host
* Updated poller to write statuses only after the initial run to prevent it from clearing active alarm status

## 1.2.3 - July 19, 2019

* Fixed bug where XSD validation files would not be replaced upon update or reinstallation

## 1.2.1 and 1.2.2 - July 19, 2019

* Comp can now use user-defined constants as variables and values within a composite
* Comp can now use OID suffix values as data values
* Validation bug for host variables in poller has been fixed
* Minor bug fixes

## 1.2.0 - July 15, 2019

* Simp now uses a cleaner configuration architecture
* Simp now has automatic validation for every config file it requires and logs validity errors
* Simp now has more consistent process naming
* Comp is no longer known or packaged as CompData
* Comp can now use retrieved data values as input for functions and other data conversions
* Data now ignores errant data spikes caused whenever a calculated rate is an impossible value
* Defaults have been added for less-used configuration attributes which can now be omitted.

## 1.1.0 - May 06, 2019

* Simp now has packaging and installation support for EL6
* Simp now has init.d scripts for simp-poller, simp-data, simp-comp, and simp-tsds to support EL6 hosts
* Poller now writes status files for each polling group per host for simp-poller-monitoring
* Poller now has various error checks for monitoring
* Comp now has the ability to scan a static OID
* Comp now has the ability to use scan parameters from a scan within N other scans
* Comp scans dependent on other scans will now perserve dependencies
* Comp now has a refactored data structure for results
* Comp has had various optimizations
* Comp now outputs an array of data objects instead of a hash
* TSDS has been adjusted to use new output from Comp

## 1.0.11 - Dec 11, 2018

* Multiple performance enhancements. All SNMP queries now have limited retries.
* SNMP queries to the same host are now spaced out from one another by 1s to help avoid causing large request and CPU bursts.
* Net::SNMP::XS is used in place of Net::SNMP for much better performance parsing SNMP responses, particularly in large cases.
* Default tunings applied to AnyEvent::SNMP settings, particularly the MAX_OUTSTANDING value. This should provide a more consistent
startup behavior vs behavior after running for a while and that value having been autotuned. This may be a good config option
in the future.
* Some minor code refactor to remove copypasta.

## 1.0.10 - Dec 4, 2018

* Updated all processes to use systemd style process management instead of init.d
* Added "reload" capability to simp-poller and simp-tsds to allow for processes to stay up but reinitialize their configuration.
* Fixed issue where for classes with multiple OIDs to poll would never complete if one or more OIDs did not return successfully.

## 1.0.9 - Nov 29, 2018

* Added `stagger_interval` option to the simp-tsds configuration. Controls the spacing between when worker proceses in simp-tsds will make queries for data. This helps to prevent the case where all workers will query at the same time and overwhelm the comp/simp-data processes. Defaults to 5sec if not specified.
* Added "--dir" option to simp-tsds.pl to control which directory it is reading conf.d configs from. This betters allows for running multiple instances that are sending to different places.
* Fixed file paths, user/group names in simp-tsds service unit file.
