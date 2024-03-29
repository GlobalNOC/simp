# Testing Simp Packages
---
## Running Tests
Running tests can be very useful for development to identify how changes may impact a package. In order to run any tests, first you must make sure you're current working directory is the main `simp/` directory. Check the commands below to see how to perform different tests.

####  Test All Packages
This is most commonly done to ensure changes are safe before publishing a release.
```sh
$ make test
```
#### Test Specific Packages
You can test a specific package by running the following command that uses [the package's test level](#test-naming-conventions) and glob (`*`) to identify which test files to run.
```sh
$ make test TEST_FILES=t/$TestLevel*.t
```
#### Test One Specific Thing
You can run a specific test by specifying which test file to the previous command as shown below (example tests only composites)
```sh
$ make test TEST_FILES=t/01-comp.t
```

---
## Benchmarking & Profiling
### Some tests, like `01-comp.t`, have a secondary function that allows them to be used to perform software profiling and benchmarking
To run a benchmark or software profile using a test file, we can run the test script from the command line with [Devel::NYTProf software profiler for perl](https://metacpan.org/pod/Devel::NYTProf) from CPAN. You will need to install `perl-Devel-NYTProf` before you can do this. In the following example, we benchmark the performance of the SIMP Comp processor package which is common to do prior to a new release.

1. In your local repository, determine the test you want to use for profiling. We'll use `01-comp.t`.
2. Adjust any flags or determine command-line arguments needed to run the test in benchmarking/profiling mode. For `01-comp.t`, we will set a variable called `benchmarking` to `true`/`1` before running it with the profiler.
3. Once the test script is ready to run for benchmarking, run it like so from the root of your local repository:
`/usr/bin/perl -d:NYTProf -I ./lib t/01-comp.t`
4. You should have a new file from the output in the root of your repository called `nytprof.out`.
`ls -l | grep "nytprof.out"`
5. The Devel::NYTProf package includes a nice utility for quickly building web assets for viewing your profile results in a browser. To do this, run `nytprofhtml ./nytprof.out` from the directory where `nytprof.out` is located.
6. You should now have a directory called `nytprof/` in that same directory.
7. To view the profile's website, move the `nytprof/` dir to an appropriate location for a webserver running on your machine. In our case, this is usually `/var/www/html/`. When moving the directory, it is helpful to also rename it based upon the current version and profiling attempt to distinguish it from other profiles. 
`mv ./nytprof/ /var/www/html/comp_profile_X.Y.Z_1`
8. Open your browser and enter the appropriate URL for your hostname and the location/alias of your profile directory. In our case, this might be `https://test.node.com/comp_profile_X.Y.Z_1/`.
9. You should see a website showing the profile of the software, including a flame graph of execution times as well as a table of different files/subroutines/code lines and their execution times. Simple inefficiencies are usually found quickly where a subroutine or line of code that is trivial or otherwise non-essential is consuming more execution time than the main processing functionalities (e.g. a debug logging line might take more time than an input parser).
10. Happy profiling!

---
## File Locations
#### All tests and testing-related files should be located in the ```simp/t/``` directory
- `t/conf` - Holds configuration files used in testing
- `t/conf/data_sets/` - Has `input/` and `output/` that hold expected input and output data
- `t/conf/composites/` - Has composite configs for Simp-Comp tests

---
## Test Naming Conventions
#### File names for tests should use the format `LN-T-S.t` 
- `L` is the **test level** *(correlates to a specific package or stage in the collection process)*.
- `N` is the **test number** *(indicates execution order for the test from within the test level)*.
- `T` is a **descriptive name** for the test.
- `S` is an optional name for a test related to `T` that needs a separate standalone test.
- `.t` is the **file extension** for all tests.

#### Test Levels
###### `0N`: **Precursory tests**, such as checking Perl lib imports and file-loading.
###### `1N`: Tests related to the function of **Simp-Poller**
###### `2N`: Tests related to the function of **Simp-Data**
###### `3N`: Tests related to the function of **Simp-Comp**
###### `4N` - Tests related to the function of **Simp-TSDS**
...

#### Example File Names

```sh
01-load.t
11-snmp-v2.t
12-snmp-v3.t
41-push_to_tsds.t
```

---
## Adding Composite Tests for Simp-Comp
The test file for composites is `t/01-comp.t`, which can be run individually when testing a composite.


To AUTOMATICALLY synchronize or add composite test files from composites installed on a host running SIMP:
1. Change to the `t/` directory in your repository.
2. Enter the command `util/sync_test_composites.py`.
3. The script will run, synchronizing any composites the host has installed in `/etc/simp/comp/composites.d/` with ones currently included in `t/conf/composites/`.
4. Any composites the host has installed that aren't in `t/conf/composites/` will be shown. If you want to add their composite files and create template JSON for them in `t/conf/data_sets/input` and `t/conf/data_sets/output`, simply answer "y" when asked if you'd like to add them.
5. If new files were created in `t/conf/data_sets`, you will need to fill out those template files for the composite manually.


To MANUALLY add a test for a *new* composite do the following:
1. Add the composite's XML configuration file to `t/conf/composites/`
2. Create a new JSON file in `t/conf/data_sets/input/` with the name like `composite_name.json`
3. Add the data expected from the node as would be retrieved by Simp-Data to the JSON file. Other [composite input files](https://github.com/GlobalNOC/simp/tree/master/t/conf/data_sets/input) are a great example.
4. Create a new JSON file in `t/conf/data_sets/output/` with the name like `composite_name.json`
5. Add the data expected from Simp-Comp after processing has finished to the JSON file. Other [composite output files](https://github.com/GlobalNOC/simp/tree/master/t/conf/data_sets/output) are a great example. *Note: The JSON is converted to Perl hashes. For this reason, the JSON must pass linting and values expected to be `undef` must be specified as `null` in the JSON to be converted appropriately.*
6. Test the new composite by running the test `t/01-comp.t` as shown [here](#test-one-specific-thing).

---
## Debugging a Composite Using Tests
The test file for composites, `01-comp.t`, includes a feature for debugging. This can be especially useful for identifying issues occurring for a single composite when it fails its test.
*Note: The feature is only accessible by editing the code of the composite test file.*

To turn on debugging mode for a specific composite:
1. Open `t/01-comp.t` in your text editor
2. Find the variable called `$debug` near the top
3. Set "enable" to `1` to allow debug logging every time you run `01-comp.t`
4. Isolate testing to one composite by specifying its name as the value for `composite`
5. Run `t/01-comp.t` as shown [here](#test-one-specific-thing) and see that the output is more detailed and is specific to the problem composite.

Using this method, it is much easier to identify why a composite isn't working after changes were made to the lib files. 
