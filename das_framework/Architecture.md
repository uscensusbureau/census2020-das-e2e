# Requirements for the Disclosure Avoidance System (DAS)

* Experimental configuration is stored in a configuration file read with Python `ConfigParser`

* Framework can run DAS once or multiple times:

    * Each run can have the same or a different random seed

    * Each run can have a different set of configuration parameters

* Configuration options will include:

    * Location and form of input data (e.g. S3, HDFS, CEF, or Numpy histogram file)

    * Location of temp space.

    * Whether or not to delete temp files (keep them around for debugging)

    * Location of output MDF (if it is being generated)

    * Which DAS engine is being used (e.g. DAS0, DAS1, DAS2, ...)
    
    * Values for key parameters fed to DAS engine.

    * Python embedded in the configuration file will be able to specify ranges for variables that are used in multiple experiments. This can be done by having a function that returns a dictionary of parameters that supercede default values. 
    
    * Whether or not to run `py.test` unit tests prior to actually using the DAS.  Options are: 1) Do not run unit tests; 2) Run unit tests and HALT if they fail.
    

## Inputs and outputs

* Input to runs will include:

    * The configuration options

    * The CEF

    * The DAS program engine to be used

* Output of the run will include:

    * The MDF

    * Logfile of operation

    * Provenance for the run, to include:

        * Checksums for the input

        * The version # of every program used

        * Time that the run was started and ended

        * CPU and RAM required by the run

## DAS Engines
The DAS Engine is the program that performs the actual disclosure avoidance.

This framework comes with two DAS engines:

* *DAS0* performs no disclosure avoidance, merely copies inputs to outputs. This corresponds to an epsilon of infinity. 

* *DAS1* performs total disclosure avoidance, setting all outputs to "9" (not provided) that are not invariants. 

Other DAS systems will be provided by the `census-das-dev` and `census-das-prod` git modules.

## Modes of operation
The DAS framework needs to support the range of DAS operations, including development, testing, and production. It needs to run on a small, restricted environment (e.g. Dan's laptop), it needs to run in a multiprocessing environment (e.g. a system with 64 cores), on a computer with a job distribution framework (e.g. PBS), and on the large-scale AWS processing environment (e.g. Apache Spark). We can implement such a system by having each instance look like a PBS *array job*--- that is, each task runs as a deterministic, single-threaded task which takes as input the task number. The tasks can then be run sequentially, run as subprocesses from functions launched using the Python `multiprocessing` module, run as actual PBS array jobs, or run as subprocesses from functions launch as part of an Apache Spark `.map()` operation.

DAS is designed to be *idempotent*. Each module's operation can be repeated without harm; the modules should check to see if their outputs already exist and, if so, simply not execute. We then get fault recovery by simply restarting the system.  For example, if we are running multiple experiments with different random sees, each one of those seeds could be regarded as a "run" of the Experiment Runner, each would store its output in a different directory, each output would be independently evaluated by the Quality Assessment module, and the report generator would produce a summary report.

Invocation Options:

* BOOTSTRAP ON MICRO - DAS is run from a bootstrap script on a non-EMR node. It:

  1. Creates an EMR cluster.
  2. Gets the configuration from a predetermined location. 
  3. Runs as specified in the config file.
  4. Shuts down the cluster.

* EXISTING CLUSTER - DAS is run from the command-line on the head-end of an AWS EMR cluster. 
  1. Configuration specified on the command line. 
  2. Runs as specified in the config file.
  3. Optionally shuts down the cluster if specified on the command line. (So you can go home and not have the cluster running after it is no longer needed.)

* DESKTOP - DAS is run on a small system
  * This looks like EXISTING CLUSTER.




Operation Modes:

* DEV. This mode is used by algorithm developers to get their code working.

* EXP. This mode initiates multiple runs of the DAS with different parameters and compares the results.

* PROD.  This is the real thing, used for the 2018 E2E and the 2020 Census. 

* POST.  This mode is used for post-2020 activities, including geography changes and special tabulations. We haven't figured out how to implement it yet, and how to account for its privacy budget.


# Modules

## Driver

* Command line program.

* Argument specifies config file.

* Override config arguments (some of them) on command line:

    * Debug level

* Reads the CEF (if necessary)

* One or more times (currently not parallelized, but could be):

    * Runs the DAS Engine
 
    * Runs the Quality Assessment tool

* Writes the MDF (if necessary)


## CEF Reader
* Reads the CEF.

* Validates the CEF

* Outputs CEF in a numpy histogram file stored on local file system or in HDFS.

## Experiment Runner
* Runs the DAS providing it with configuration information.

    * Parameters come from the config file but are overwritten by a dictionary. This lets us run multiple experiments with the same config file. The experiments might be run at the same time, or sequentially.

* Reads from the numpy histogram file

* Writes to a numpy histogram file

## Quality Assessment 

* Generates PDF certificate of output accuracy (certificate to be predetermined).  Destination specified in the config file.

* Accuracy resolution (e.g. state-by-state, block-by-block) to be specified in config file.

* Accuracy calculations to be included in overall privacy budget, but we believe it will be very minor unless done block-by-block.

* Specifies privacy consumed by the DAS.


## MDF Writer

* Operation is determined by the config file; MDF won't be written if we are running a series of experiments.

* Reads the numpy histogram file

* Writes a MDF to the specified location

## MDF Validator

* Makes sure that the MDF is properly formatted

## Experimental Report Generator

* When running multiple experiments, produces summary reports. 

* Not included in privacy budget.

* Not run in production mode.


# Terminology

* DAS - Disclosure Avoidance System. Refers to the full set of code used to perform disclosure avoidance for the 2018 End-to-End test and the 2020 Census

* CEF - Census Edited File, the input of the DAS.

* MDF - Master Detail File, the output of the DAS.
