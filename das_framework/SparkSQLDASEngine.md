## Spark SQL DAS Engine Module Architecture

\label{sec:sparksqldasenginearch}

The section below describes the Spark SQL DAS Engine module implemented in 

* *spark_sql_das_engine.py*, 
* *spark_sql_aggregator.py* and 
* *noisealgorithms.py*

files which performs disclosure avoidance based on SQL queries and `[table_variable]` sections set up in the config file.

This module has an empty prototype function to modify config file, *elaborate_config()*, which can be used in the 
subclasses to automate generation of SQL queries and other disclosure avoidance parameter setup for a particular
project / set of experiments.


### Engine Module Operation
* Creates **NoiseAlgorithm** object for each of the variables that are indicated in *table_name* options of `[variables2addnoise]` section for adding noise.
* Checks the values of ε set in each NoiseAlgorithm, checks that it is the same within each group of composable variables, sets it to the maximal otherwise. 
Calculates the effective ε and checks that it is equal to the "master"/"release" ε set in `[engine]` section.
* Checks that each **NoiseAlgorithm** is given appropriate parameters (e.g., that smooth sensitivity is bounded in each).
* Calls **Aggregator** to aggregate the tables before adding noise.
* Loops over tables in `[prenoise_tables]` OR `[NoNoiseDA]` section, maps each DataFrame, going over the rows, applying appropriate noise
* Calls **Aggregator** to aggregate the tables post noise.
* Creates *privatized_data*, list of Spark or Pandas DataFrames, and changes *original_data*, adding *original_data["true_tables"]*,
*original_data["noisy_tables"]* from the **Aggregator** output, *original_data["params"]*, containing parameters of noise application, 
such as algorithms, values of ε-s and other parameters and *original_data["exvars"]*, containing names and current values of 
variables in experimental loops.

    
### Engine Module Inputs
* *original_data*, a dictionary including a Spark DataFrame under original_data["original_data"]
* *config*, a **ConfigParser** object
* *setup_data* the output of **setup** module

#### Engine Module Config Options
1. `[variables2addnoise]` section, with `table_name = list_of_variables` option format
1. `[prenoise_tables]` section, with `table_name = query` option format (engine uses it to make sure all of them run through noise application)
1. `[engine]`/*epsilon*, desired "master"/"release" ε
1. `[engine]`/*input_table_name* (name(s) of SQL table(s) in the input data before any aggregation). The **engine** DOES NOT read this option,
but SETS it for use by **Aggregator**. It gets the input table names from the input data, supplied as a dictionary of Spark DataFrames by the **reader** module.
The keys of the dictionary are the input table names. However, the **reader** module might make use of this option. 
Make sure the names do not coincide, nor are fully included into any of the variable names in the queries 
(e.g., in BDS we use "estabs" to denote Number of Establishments, so a table should not be named "estabs" or "estab", but can be named "estabsfile")
1. Variables in experimental loops and their current values
1. `[engine]`/*seed*, default=urandom. For reproducible results, it should be set to an integer value, and `numpy.random` should be seeded with it in the Setup Module.
1. In sections named `[table_var]`, algorithm+parameters for each of the variables listed in `[variables2addnoise]` section.
1. "Master" parameters (for all variables in all tables, unless overridden in the `[table_var]` section) from `[engine]` section.
1. `[engine]`/*ignore_privacy_guarantee_check*, default=False. For SmoothLaplace, calculate and output the noisy results even if the smooth
sensitivity is unbounded / there is no privacy guarantee (the results don't depend on $\delta$, so could be used with another $\delta$)

### Engine Module Outputs
* *privatized_data*, dictionary with "true tables," "noisy tables," parameters of performed DA, and values of experimental loop variables.
* *true_data*, constructed before applying noise.


### Engine Module Member Functions & Module Classes

#### apply_noise_to_cell(noisifiers, row).

Applies noise to a cell of a table (which is represented as a row of 2D table).

* Loops over *noisifiers*, an iterable of **NoiseAlgorithm** objects, which know variable name and have *noisify* function.
* Substitues the variable with the result of the *noisify*(*row*), where *noisify* is the method of the **NoiseAlgorithm** object.
* Returns the data with substitution as Spark **Row** object.

#### apply_noise_to_partition_iter(self,noisifiers, index, iter).
Same as above, but iterates over rows within a Spark Partition.

#### class NoiseAlgorithm.

Implements noise algorithms
<ul>
<li>Initializes, setting ε and other parameters for an instance, including "algorithm", from 
<p><code>
[tablename+'_'+varname]
</code></p>
 config file section</li>
<li>Has function *noisify*(*row*), which applies the noise algorithm to a single variable of the row</li>
<li>Subclasses implement particular privacy algorithms.</li>
</ul>


## Spark SQL DAS Aggregator

A part of the Spark SQL DAS Engine implemented in **spark_sql_aggregator.py**.

The aggregator performs table transformations (summarize/aggregate, add or drop variables) via applying Spark SQL queries,
and/or Pandas.


For example, given the following inputs:

* Table with N interacted variables defining cells and K measure variables (as Spark DataFrame), 
(e.g. EmploymentPrevious, EmploymentCurrent x State x Industry x FirmSize (N=3, K=2)),
* List of columns to keep (~ group by) (e.g., keep State, or GROUP BY State),
* List of columns to summarize (on which to perform the operation) (e.g. EmploymentPrevious, EmploymentCurrent),
* List of reduce operations (e.g., sum, count, mean),
* List of new measure variables calculated from the given K measure variables,
* List of variables to drop from the final table,

the aggregator will output a table with M interacted variables defining cells and L measure variables (M < N) (as Spark DataFrame). 

These inputs are provided in the form of SQL queries optionally mixed with adding/dropping variables after conversion to Pandas.

Example of an SQL query:

```sql
SELECT 
    State,
    SUM(EmploymentCurrent) AS Employment,
    SUM(EmploymentPrevious - EmploymentCurrent) AS NetJobCreation,
GROUP BY State
FROM PreNoiseTable1
```

In the example above the output table variables are: `Employment` and `NetJobCreation X State` (M=1 < N=3, L=2),
that is, two measure variables -- `Employment` and `NetJobCreation` (hence, L=2), -- and one cell variable -- `State` (hence M=1).

The input table, `PreNoiseTable1` could have had 3 cell variables: `State`, `Industry` and `FirmSize` (N=3), and two
measure variables (`EmploymentPrevious` and `EmploymentCurrent`).

Query can contain construction of new variables, e.g., 
```sql
empcy-emppy AS net_job_creation
```
(`emppy` and `empcy` here are the variable names for previous year employment and current year employment,
but the aggregator is agnostic of particular variable naming conventions, everything can be set by SQL queries).

Aggregator will go through a list of queries and create new Spark DataFrames based on them.

### Aggregator Inputs
* Data to aggregate (true or noisy) -- Spark DataFrames, existing within Spark Context and accessed via TempViews (SQL table names) created by **engine** 
or another **aggregator.**
* *config*, a **ConfigParser** object
* *setup_data*, the output of **setup** module
* *position* (when called), "pre-noise"/"post-noise-true"/"post-noise-noisy" implemented as subclasses: 
    * PreNoiseAggregator
    * NoisyAggregator
    * TrueAggregator
    * NoNoiseDAAggregator
* In the config, the SQL queries + (optionally) lists of variables to add and drop in Pandas

#### Aggregator Config Options
* `[prenoise_tables]` section, with `table_name = query` options format (each option corresponds to a table)
* `[out_tables]` section, with `table_name = query` options format (each option corresponds to a table)
* `[NoNoiseDA]` section, with `table_name = query` options format (each option corresponds to a table)
* `[engine]`/*pandas_output*, default=`True`, convert the DataFrames from Spark to Pandas after aggregation
* `[out_tables_add_vars_pandas]` section, with `table_name = list_of_vars_to_add` option format, with list split by comma in each config option, semicolon indicating where aggregation takes place
* `[out_tables_drop_vars_pandas]` section, with `table_name = list_of_vars_to_drop`, option format, with list split by comma in each config option
* `[out_tables_aggregate_pandas]` section,  with `table = list-split-by-comma; operation` options format (each option corresponds to a table)
* `[out_tables_aggregate_pandas]` section,  with `table = list-split-by-comma; operation` options format (each option corresponds to a table)
* `[validator]`/*cell_id_vars*, list of field names in the data that define the cell as opposed to being a measure/value

### Aggregator Operation
* PreNoiseAggregator (makes aggregation that needs to happen for both true tables and noisy tables, but before application of noise)
    1. Loops over options of `[prenoise_tables]` section (suppose the option name is *table_name*)
    1. Applies the queries, which are the values of the options
    1. Registers the Spark SQL views named *table_name*
* NoisyAggregator /  TrueAggregator (post-noise) (makes aggregation that needs to happen for both true tables (right after preaggregation) and noisy tables (right after adding noise))
    <ol>
    <li>Loops over options of `[out_tables]` section</li>
    <li>Applies the queries, which are the values of the options, to either true (TrueAggregator) or noisy (NoisyAggregator)  tables and returns the data frames</li>
    <li>Optionally, converts to Pandas, applying functions from 
    <p>`[out_tables_add_vars_pandas]`</p> 
    to create new measure vars, aggregating according to <p>`[out_tables_aggregate_pandas]`</p> and dropping variables 
    from `[out_tables_drop_vars_pandas]` section</li>
    </ol>
* NoNoiseDAAggregator (makes aggregation that takes place for noisy tables *instead of* pre-noise aggregation. Useful when it depends on parameters)
    1. Loops over `[NoNoiseDA]` section (suppose the option name is *table_name*)
    1. Applies the queries, which are the values of the options 
    1. Registers the Spark SQL views named *table_name*


### Config File Excerpts Examples for Aggregator
The following example does not perform any aggregation after adding noise.
```ini
[prenoise_tables]
byfage4bymsapre =
    SELECT fage4, msa, 
        SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs 
    FROM estabsfile GROUP BY fage4, msa 
bystatepre = 
    SELECT state, 
        SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs 
    FROM estabsfile GROUP BY state
totalpre = 
    SELECT 
        SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs     
    FROM bystate
                                                                                                                        ;
[out_tables]
byfage4bymsa = SELECT * from byfage4bymsapre
bystate = SELECT * from bystatepre
total = SELECT * from totalpre
```

"estabsfile" is the default name of the input table, returned by the **reader** module and can be directly used in the queries (this name can be changed
via `[engine]`/*input_table_name* config option).


The following example does not perform pre-noise aggregation (hence `[prenoise_tables]` is empty) and adds noise directly to the table "estabsfile" returned by the **reader** module.

```ini
[out_tables]
byfage4bymsa = 
    SELECT fage4, msa, 
        SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs 
     FROM estabsfile GROUP BY fage4, msa
bystate = 
    SELECT state, 
        SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs 
    FROM estabsfile GROUP BY state
total = 
    SELECT 
        SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs 
    FROM bystate
```

The following example does pre-aggregation and post-aggregation:

```ini
[prenoise_tables]
byfage4byfsizebysic1bymsa = 
    SELECT 
        fage4,fsize,sic1,msa,grower,continuer, 
        SUM(empcy) AS empcy, 
        SUM(emppy) AS emppy, 
        SUM(estabs) as estabs, 
        MAX(empcy) AS ssmax, 
        MAX(emppy) AS ssmaxp 
    FROM estabsfile 
    GROUP BY fage4,fsize,sic1,msa,grower,continuer
                                                                                                                        ;
[out_tables]
byfage4bymsa = 
    SELECT fage4,msa, 
        SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs 
    FROM byfage4byfsizebysic1bymsa GROUP BY fage4,msa
bysic1 = 
    SELECT sic1, 
        SUM(empcy) AS empcy, 
        SUM(emppy) AS emppy, 
        SUM(estabs) as estabs 
    FROM byfage4byfsizebysic1bymsa GROUP BY sic1
total = SELECT SUM(empcy) AS empcy, SUM(emppy) AS emppy, SUM(estabs) as estabs 
        FROM byfage4byfsizebysic1bymsa
```

#### Adding/dropping of measure variables to the output tables.

The user has an option to convert the output tables to Pandas at the end of post-noise aggregation.
Since some datasets are small, that may be desirable, because it may be faster and/or easier; and
for error calculations downstream of the engine, the Spark DataFrames/RDDs have to be joined across
partitions anyway.
This option is ON by default, and may be changed in `[engine]`/*pandas_output* config option

There are three ways to add new variables:

1. Via Pandas (using config file sections described above for add, aggregate, and drop). The variable names for adding have to be functions 
within the engine module (which will be
a subclass of the **spark_sql_das_engine.engine** class) indicated in config (`[engine]`/engine) 
The calculation of the variables proceeds in the order of the list, so the former may be used in latter calculations. 
The ";" sign in the list indicates a place where aggregation described in the `[out_tables_aggregate_pandas]` takes place. 
Dropping of the variables happens afterwards. This is the most concise (with respect to
config file) way. `[engine]`/*pandas_output* config option has to be ON for this way to work.

1. Via SQL supplied directly to Spark, which performs calculations within the queries. This is the most flexible way.

1. Same as previous point, but calculations can be implemented in **pyspark** UDF (user-defined functions), 
which can be called from within the queries.
The UDFs have to be implemented and registered in *setup_udf()* member-function of the *engine* module (which will be
a subclass of the **spark_sql_das_engine.engine** class)

Examples below show each of the ways, producing the same result.

##### 1. Example of adding/dropping via Pandas:

```ini
[out_tables_add_vars_pandas]
bysic1 = denom,
    net_job_creation,
    job_creation,
    job_destruction;
    net_job_creation_rate,
    job_creation_rate,
    job_destruction_rate   
total  = denom,
    net_job_creation,
    job_creation,
    job_destruction;
    net_job_creation_rate,
    job_creation_rate,
    job_destruction_rate
                                                                                                                        ;
[out_tables_aggregate_pandas]
bysic1 = grower,continuer;sum
total = grower,continuer;sum
                                                                                                                        ;
[out_tables_drop_vars_pandas]
bysic1 = emppy,ssmax,ssmaxp,grower, continuer
total = emppy,ssmax,ssmaxp,grower, continuer
```

The example is taken from BDS-DAS, where
the *denom(), net_job_creation(), job_creation(), job_destruction(), net_job_creation_rate(), job_creation_rate(), job_destruction_rate()* functions
are defined in *bds_das_spark_engine.py* engine module for BDS.

Example (file *bds_das_spark_engine.py*):

```Python
def denom(df):
    if check_fields(df, ['empcy', 'emppy'], 'denom'):
        df['denom'] = .5 * (df['emppy'] + df['empcy'])
    return df
                                                                                                                        #
...more functions, for net_job_creation() etc...
                                                                                                                        #
class engine(spark_sql_das_engine.engine):
    ...
```
 
The module implements an **engine** class which is a subclass of **spark_sql_das_engine.engine**. 
The subclass can be customized, by adding more variable calculation routines or routines for automating SQL queries formation.

##### 2. The same example implemented directly via SQL:

```ini
[out_tables]
bysic1 =
    SELECT
        *,
        100*net_job_creation/denom AS net_job_creation_rate,
        100*job_creation/denom AS job_creation_rate,
        100*job_destruction/denom AS job_destruction_rate,
    FROM
        (SELECT
            sic1,
            SUM(empcy) AS empcy,
            SUM(denom) AS denom,
            SUM(net_job_creation) AS net_job_creation,
            SUM(job_creation) AS job_creation,
            SUM(job_destruction) AS job_destruction,
        FROM
            (SELECT
                *,
                0.5*(empcy+emppy) AS denom,
                (empcy-emppy) AS net_job_creation,
                (empcy-emppy)*CAST(grower AS INT) AS job_creation,
                -(empcy-emppy)*(1-CAST(grower as INT)) AS job_destruction,
            FROM byfage4byfsizebysic1bymsa)
        GROUP BY sic1)
                                                                                                                    ;
total =
    SELECT
        *,
        100*net_job_creation/denom AS net_job_creation_rate,
        100*job_creation/denom AS job_creation_rate,
        100*job_destruction/denom AS job_destruction_rate,
    FROM
        (SELECT
            SUM(empcy) AS empcy,
            SUM(denom) AS denom,
            SUM(net_job_creation) AS net_job_creation,
            SUM(job_creation) AS job_creation,
            SUM(job_destruction) AS job_destruction,
        FROM
            (SELECT
                *,
                0.5*(empcy+emppy) AS denom,
                (empcy-emppy) AS net_job_creation,
                (empcy-emppy)*CAST(grower AS INT) AS job_creation,
                -(empcy-emppy)*(1-CAST(grower as INT)) AS job_destruction,
            FROM byfage4byfsizebysic1bymsa))
```

##### 3. The same example implemented directly via SQL using predefined Spark UDF:

```ini
[out_tables]
bysic1 =
    SELECT
        *,
        rate(net_job_creation,denom) AS net_job_creation_rate,
        rate(job_creation,denom) AS job_creation_rate,
        rate(job_destruction,denom) AS job_destruction_rate,
    FROM
        (SELECT
            sic1,
            SUM(empcy) AS empcy,
            SUM(denom) AS denom,
            SUM(net_job_creation) AS net_job_creation,
            SUM(job_creation) AS job_creation,
            SUM(job_destruction) AS job_destruction,
        FROM
            (SELECT
                *,
                denom(empcy,emppy) AS denom,
                net_job_creation(empcy,emppy) AS net_job_creation,
                job_creation(empcy,emppy,grower) AS job_creation,
                job_destruction(empcy,emppy,grower) AS job_destruction,
            FROM byfage4byfsizebysic1bymsa)
        GROUP BY sic1)
                                                                                                                        ;     
total =
    SELECT
        *,
        rate(net_job_creation,denom) AS net_job_creation_rate,
        rate(job_creation,denom) AS job_creation_rate,
        rate(job_destruction,denom) AS job_destruction_rate,
    FROM
        (SELECT
            SUM(empcy) AS empcy,
            SUM(denom) AS denom,
            SUM(net_job_creation) AS net_job_creation,
            SUM(job_creation) AS job_creation,
            SUM(job_destruction) AS job_destruction,
        FROM
            (SELECT
                *,
                denom(empcy,emppy) AS denom,
                net_job_creation(empcy,emppy) AS net_job_creation,
                job_creation(empcy,emppy,grower) AS job_creation,
                job_destruction(empcy,emppy,grower) AS job_destruction,
            FROM byfage4byfsizebysic1bymsa))
```

As mentioned above, for this example, the 

* *denom()*, 
* *net_job_creation()*, 
* *job_creation()*, 
* *job_destruction()*, 
* *net_job_creation_rate()*, 
* *job_creation_rate()*, 
* *job_destruction_rate()* 

functions have to be defined and registered in Spark in the *setup_udf()* member function of the **engine** class implementing the engine module. In this case (BDS-DAS), it is done
in the *engine.setup_udf()* function of the **engine** class implemented in *bds_das_spark_engine.py* (part of BDS-DAS), which is a subclass of the **engine** class implemented in 
*spark_sql_das_engine.py* (part of **das-framework**).

Here are excerpts from *bds_das_spark_engine.py*:

```Python
class engine(spark_sql_das_engine.engine):
                                                                                                                        #   
    def elaborate_config(self):
        ...
        pass
                                                                                                                        #
    def setup_udf(self):
        spark = SparkSession.builder.\
            appName('BDS-DAS Spark Setting Up UDF').getOrCreate()
                                                                                                                        #
        def denom_udf(empcy, emppy):
            return .5 * (empcy + emppy)
                                                                                                                        #
        spark.udf.register('denom', denom_udf)
                                                                                                                        #
        ... more function definitions and registrations ...
                                                                                                                        #
        pass            
```

##### Example showing the flexibility of using SQL directly.

As mentioned above, using SQL is the most flexible way to aggregate data with **das-frameworks**'s Spark SQL DAS Engine / Aggregator and even apply some disclosure avoidance procedures.
The following example shows how cell suppression can be applied before (optionally) adding noise (see also Appendices \ref{meth:LargeEmpSupp}, \ref{meth:DiffPrivEstabsSupp}, of the Technical Manual):

```ini
[NoNoiseDA]
bystate = 
    SELECT state,
        SUM(IF(CAST(emp2015 AS INT)<%(theta)s,CAST(emp2014 AS INT),0)) 
            AS emppy,
        SUM(IF(CAST(emp2015 AS INT)<%(theta)s,CAST(emp2015 AS INT),0)) 
            AS empcy,
        NOT(CAST(exit as BOOLEAN) OR CAST(entry as BOOLEAN)) as continuer,
        CAST(emp2015 AS INT)>CAST(emp2014 AS INT) as grower,
        SUM(IF(CAST(emp2015 AS INT)<%(theta)s,1,0)) as estabs 
    FROM estabsfile GROUP BY state, grower, continuer
```

In this example from BDS-DAS, "estabsfile" in the last line is the name of the table of `estab20142015` file, and `\%(theta)s` stands for the maximal employer size,
so that the query above would suppress (report as zero employment) all the employers with 2015 employment higher than $\theta$. One can
just put a number instead of `\%(theta)s`, e.g. 20000, then all the employers larger than 20000 will be suppressed by the above query.
The tables produced by queries in `[NoNoiseDA]` section are then available for the Noise Algorithms applied by Engine Module.

The example above shows a convenient way to use config file variables (like `\%(theta)s`), if one wants to run an experiment with different values of
$\theta$. Then, the value of $\theta$ should be set up in `[DEFAULT]` section and it should be in a loop of `[experiment]` section:

```ini
[DEFAULT]
name = bdstest
root = /path/to/experiment/folder
mode = 0
loglevel = INFO
theta = 2
                                                                                                                        ;
[experiment]
loop1 = FOR engine.run  = 1 TO 3 STEP 1
loop2 = FOR engine.epsilon = 0.125 TO 10 MULSTEP 2
loop3 = FOR DEFAULT.theta IN 2,20,50,100,200,500,1000,5000,10000,20000
```

With the config above, $\theta$ will take values listed in `loop3`, and the query in `[NoNoiseDA]` section, which is listed above
will change accordingly.

### Aggregator Outputs
* The aggregated tables (returns a pointer to itself and contains either Spark DataFrames and views for them (modifies Spark Session) 
or a dictionary of Pandas dataframes)