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
$ make test TEST_FILES=t/51-composites.t
```

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
The test file for composites is `t/51-composites.t`, which can be run individually when testing a composite.

To add a test for a *new* composite do the following:
1. Add the composite's XML configuration file to `t/conf/composites/`
2. Create a new JSON file in `t/conf/data_sets/input/` with the name like `composite_name.json`
3. Add the data expected from the node as would be retrieved by Simp-Data to the JSON file ([Other composite's input files](https://github.com/GlobalNOC/simp/tree/master/t/conf/data_sets/input) make a great example)
4. Create a new JSON file in `t/conf/data_sets/output/` with the name like `composite_name.json`
5. Add the data expected from Simp-Comp after processing has finished to the JSON file. Other [composite input files](https://github.com/GlobalNOC/simp/tree/master/t/conf/data_sets/input) are a great example. *Note: The JSON is converted to Perl hashes. For this reason, the JSON must pass linting and values expected to be `undef` must be specified as `null` in the JSON to be converted appropriately.*
6. Test the new composite by running the test `t/51-composites.t` as shown [here](#test-one-specific-thing).

