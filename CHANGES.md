## SIMP 1.0.9

### Features:

 * Added `stagger_interval` option to the simp-tsds configuration. Controls the spacing between when worker proceses in simp-tsds will make queries for data. This helps to prevent the case where all workers will query at the same time and overwhelm the comp/simp-data processes. Defaults to 5sec if not specified.

 * Added "--dir" option to simp-tsds.pl to control which directory it is reading conf.d configs from. This betters allows for running multiple instances that are sending to different places.

### Bug Fixes:
 
 * Fixed file paths, user/group names in simp-tsds service unit file.

