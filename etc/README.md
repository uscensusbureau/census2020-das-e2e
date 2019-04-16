These files are for use in the stand-alone version of the 2018/2020 DAS.

The ideas of the "stand-alone" version is to allow a limited run
of the DAS to be performed using the stand-alone version of Apache
Spark and a single Gurobi instance. This is done by specifying
that Spark would use a single executor.

We have been able to run the DAS successfully in this configuration
using the 1940 data from IPUMS and a modified version of the
configuration file that was used to produce the data for the 2018
End-to-End test. Nevertheless, running the DAS in this configuration
is not supported by the US Census Bureau. Rather, they provide a means
to produce results for third-party verification.

See the main README for instructions for running DAS in standalone mode.

**Note:** These files are not used in the Census Bureau's production
implementation.

