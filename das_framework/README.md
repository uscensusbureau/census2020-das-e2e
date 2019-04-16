# Introduction

This is the disclosure avoidance system (DAS) framework being developed for the US Census Bureau. This is designed to be a general framework for running and evaluating disclosure avoidance systems. The goals of the framework are:

* Provide an easy-to-use framework that can be employed in stand-alone single-threaded, multi-threaded, and Apache-SPARK based systems.

* Have all disclosure avoidance described by a configuration file, and have the framework run the parts of the DAS based on what the file specifies.

* Decompose the DAS into several key parts, including:

** The *reader*, which reads the data to undergo disclosure avoidance.

** The *engine*, which performs the disclosure avoidance.

** The *writer*, which writes out the data after it has undergone disclosure avoidance.

** The *verifier*, which compares the data that was read to the data that was verified and affirms that any invariants present have been preserved.

** A *setup* process that initializes the system.

** A *taredown* process that shuts down the system.

Each of these modules are designed to be subclassed from Abstract Super Classes that are present in the `driver.py` file.

* A framework that can be used for both production and experimental purposes.

* A 100% Python 3 implementation


The framework is designed to be run as git module that is included as a submodule in another project. In this directory you will find a set of demo components that work with the framework. Other examples can be found in the `tests` subdirectory.  These are implemented with the `py.test` testing framework.


# Contents of this repository:

* [Architecture.md](Architecture.md) The architecture of the modules in the framework.

* A framework that will repeatedly run a specified DAS with different
values of ε, allowing us to programmatically draw the ROC curve
showing the accuracy/privacy-loss-budget tradeoff.
