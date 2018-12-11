## SIMP 1.0.11
Dec 11 2018

### Features:

 * Multiple performance enhancements. All SNMP queries now have limited retries.

 * SNMP queries to the same host are now spaced out from one another by 1s to help avoid causing large request and CPU bursts.

 * Net::SNMP::XS is used in place of Net::SNMP for much better performance parsing SNMP responses, particularly in large cases.

 * Default tunings applied to AnyEvent::SNMP settings, particularly the MAX_OUTSTANDING value. This should provide a more consistent
startup behavior vs behavior after running for a while and that value having been autotuned. This may be a good config option
in the future.

 * Some minor code refactor to remove copypasta.


## SIMP 1.0.10  
Dec 4 2018

### Features:

 * Updated all processes to use systemd style process management instead of init.d

 * Added "reload" capability to simp-poller and simp-tsds to allow for processes to stay up but reinitialize their configuration.

### Bug Fixes:
 
 * Fixed issue where for classes with multiple OIDs to poll would never complete if one or more OIDs did not return successfully.



## SIMP 1.0.9
Nov 29 2018

### Features:

 * Added `stagger_interval` option to the simp-tsds configuration. Controls the spacing between when worker proceses in simp-tsds will make queries for data. This helps to prevent the case where all workers will query at the same time and overwhelm the comp/simp-data processes. Defaults to 5sec if not specified.

 * Added "--dir" option to simp-tsds.pl to control which directory it is reading conf.d configs from. This betters allows for running multiple instances that are sending to different places.

### Bug Fixes:
 
 * Fixed file paths, user/group names in simp-tsds service unit file.

