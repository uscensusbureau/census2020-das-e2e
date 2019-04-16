# Concept of Operations: Startup, operation, and Shutdown of the Disclosure Avoidance System (DAS)

This document describes overall how DAS uses Amazon Web Services. It
then describes three specific modes of operation that the DAS ATO is
required to support.

# Overall Theory of operation
1. DAS starts up with an `aws emr create-cluster` command. The command should specify a bootstrap script and a Step 1 script.
   
2. Script assumes that the input file with the unprotected data is in Amazon S3. The location is specified in file [das_decennial/E2E_1940_CONFIG.ini](das_decennial/E2E_1940_CONFIG.ini]

3. DAS performs consistency and sanity checks on the input file.
   * Primary consistency check: the input file is properly formatted.

4. DAS starts up the DAS driver
   * Apache Spark program that performs the top-down algorithm.
   * The algorithm reads the input data and creates a series of data frames, one for each geography level (geolevel). The geolevels are specified in the config file as the  `[geodict][geolevel_names]`.
   
   |1940 Geolevel | 1940 Count | 2018 Geolevel | 2018 Count | 2020 Geolevel | 2020 Count  |
   |--------------|------------|---------------|------------|---------------|-------------|
   |National      | 1          |               |            | National      | 1           |
   |State         | 50         |               |     1      | State         | 51          |
   |County        |            | County        |            | County        | 3007        |
   |Enumdist      |            | Tract         |            | Tract         | ~70,000     |
   |              |            | Block Group   |            | Block Group   | ~210,000    |
   |              |            | Block         |            | Block         |             |


   * Each row of each data frame is turned into an optimization problem for the Gurobi optimizer.
   * The Gurobi optimizer is run for each row of each data frame. This is done with a distributed `map` operation, executed using Spark.
   * The result is a histogram for each block in the US.
   * The block-level histograms are converted back to microdata that are written to HDFS as the MDF (Microdata Detail File)

 6. The DAS writes the MDF back to S3.

 7. Quality metrics are computed.

 8. DAS exits.

