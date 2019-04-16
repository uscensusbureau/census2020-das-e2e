# List of packages that are required

For each package, please indicate:
* Name
* Version Number
* Source URL (if relevant)
* Hash (if relevant)

These packages are required in both the _development_ and the _production_ environment:

|Package|Version|URL|Notes|Hash|License|# Licenses   |
|-------|-------|---|-----|----|-------|-------------|
|Linux|Doesn't Matter||No requirements for a specific version that we know of.|||
|Anaconda|3-4.4.0|https://www.anaconda.com/download/|Provides Python 3.6.1 and several packages that are required.||New BSD (w/ commercial phone support)|N/A|
|py.test|3.0.7||Validates installation and operational readiness.||MIT|NA|
|Spark|2.4.1|https://spark.apache.org/downloads.html|Used to move data and distribute optimizer jobs to nodes||
|Hadoop|2.7+|https://hadoop.apache.org/releases.html|Spark dependency||
|pyspark|2.4.1||Python bindings for Spark||
|Gurobi|7.5.0|http://www.gurobi.com/downloads/download-center|ldconfig may need to be called in bootstrap to correct slave node links to Gurobi .so||Gurobi Token Server License|1800|
|gurobipy|7.5.0||Python module to interface with Gurobi. contained in gurobi; setup.py must be run & environment variables set||Gurobi Token Server License|1024|
|git|2.13.5|https://git-scm.com/|DAS runtime and configuration from TI GitHub. ||GPL|NA|
|HDFS or S3|||Access is needed to a distributed file system for storing temporary files and final results||
|pdflatex|3.1415926-2.5-1.40.14|| PDF output from TeX. Necessary for generating end-of-run certificate ||GPL||


These are only needed in the development environment:

|Package|Version|URL|Notes|Hash|License|# Licenses|
|-------|-------|---|-----|----|-------|-----------|
|emacs|24.3.1||text editor||GPL|NA|
|vim|8.0.503||text editor||GPL|NA|
|nano editor|||text editor||GPL|NA|
|kate editor|||text editor||GPL|NA|
  
  
These are included in Anaconda and do not need to be separately installed:

|Package|Version|URL|Notes|Hash|License|# Licenses|
|-------|-------|-----------------------------------|----------------------------|----|-------|-----------|
|numpy  |1.12.1 |https://anaconda.org/anaconda/numpy|numpy is a common dependency|    |BSD-new|NA|
|scipy  |0.19.0 |https://anaconda.org/anaconda/scipy|scipy is a common dependency|    |BSD-new|NA|
|ecos   |2.0.4  |https://anaconda.org/cvxgrp/ecos|changes to numpy version may require ecos recompile||GPL (with linking exception)|NA  |

  
  
