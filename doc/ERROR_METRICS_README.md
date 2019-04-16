Error Metrics
=

### How to use:
In the config file, under the [error_metrics] heading, add `metric_groups` to create different sets of error metrics.

For each metric group, the following properties are _*required*_:
* **.geolevels**: The names of the geolevels to run in this metric (e.g. `State, County, Block`)
* **.aggtypes**: Keywords that tell the module how to aggregate/collect the error metrics (e.g. `sum, avg, summary`)
    * `sum`: Uses Spark to sum up error metrics across a geolevel
    * `avg`: Uses Spark to average error metrics across a geolevel by the number of geounitNodes in the geolevel (i.e. metric_sum / num_of_geounitNodes)
    * `summary`: Uses Spark to compute the quantiles across geounitNodes in a geolevel
* **.metrics**: The names of the functions that calculate error metrics (_full list found below_)

Note that the keyword *__all__* can be used for each required property; it's a shorthand way of specifying that all options within a property should be used (i.e. `group.aggtypes: all` is a shorter way of writing `group.aggtypes: sum, avg, summary`)

Additionally, for each metric group, there are two new *_optional_* properties:
* **.data_pair**: The two data variables within a geounitNode to feed into the error metrics functions
   * `raw, syn` is the default pair
* **.quantiles**: A list of numbers in [0,1] (inclusive) that specify which quantiles to calculate
   * `0.0, 0.25, 0.5, 0.75, 1.0` is the default
   * Note that this property only affects the `summary` aggregation type
   * Using a `_` followed by an integer as the _first entry_ in the list tells numpy's linspace function how many samples to take from [0,1] (e.g. `_6` will generate `0.0, 0.2, 0.4, 0.6, 0.8, 1.0`)
   * `_num` can also be followed by a list of specific quantiles (e.g. `_3, 0.15, 0.89` will generate `0.0, 0.15, 0.5, 0.89, 1.0`)

Note that the current delimeter for the error_metrics config section is `", "`. As such, write out lists for the properties by separating them by a comma and a single whitespace character. (e.g. `group.metrics: L1_geounit, L2_geounit, LInf_geounit`)

### Saving Error Metric Results: ###
Use the `save_location` property to save error metric results. `save_location` requires a path to a directory/folder; this is where the following files will be placed **for each metric group**:
* **group.json**: A "human-readable" version of the error metrics (it will be in the form of a Python dictionary)
* **group.pickle**: This file contains the `MetricGroup` object associated with this group
* **group_config.txt**: This contains the (expanded) configuration settings used for this metric group 

### Working with Error Metric Results: ###
#### Metric Groups ####
Error metric results come packed in `MetricGroup` objects. MetricGroups contain a wealth of information about the error metrics including:
* **metric_group_name**: The name of this metric group (given in the config file).
* **runtime_in_seconds**: The time it took to compute this metric group's error metrics.
* **config_settings**: The original config file settings for this metric group.
* **results**: The error metric results. They can be accessed through a dictionary or a `Resma` object.

#### Resma ####
The most useful construct for accessing and working with error metric results is the `Resma` _(Results multiarray)_. Resmas are structured as 3-dimensional arrays and follow a pattern similar to the error metric results keys: `(geolevels, aggtypes, metrics)`. Resmas make it easier to analyze and visualize the error metric results.

Resmas have additional features for manipulating the error metrics:
* **shape**: The shape of the underlying numpy array - `(geolevels, aggtypes, metrics)`.
* **geolevels**: The levels of the geolevels variable (identical to those specified in the config file).
* **aggtypes**: The levels of the aggtypes variable (identical to those specified in the config file).
* **metrics**: The levels of the metrics variable (identical to those specified in the config file).
* **subset**: Allows the user to create a subset of the Resma's underlying numpy array.
* **split**: Allows the user to split the Resma into a list of multiple Resma objects, across one or more axes.
* **squeeze**: Collapse dimensions of the the Resma's underlying numpy array when they have only one level.
* **flatten**: Turn the Resma's underlying numpy array into a 1-dimensional array.
* **keys**: Look at the `geolevel.aggtype.metric` keys (in the same shape as the underlying numpy array).
* **values**: Look at the `geolevel.aggtype.metric` values (in the same shape as the underlying numpy array).

Note that `subset` can also be used to reorder the Resma's underlying numpy array.

##### Metric Group and Resma Example #####
For example, let's say our MetricGroup was created using the following config settings:
   * `mg_name.geolevels: ["State", "County", "Tract", "Block"]`
   * `mg_name.aggtypes: ["sum", "summary"]`
   * `mg_name.metrics: ["L1_geounit", "L2_geounit", "LInf_geounit"]`

If we assign the MetricGroup object to `mg`, we can access the results. Let's look at the Resma:
```python
resma = mg.getResma()

# Look at the shape of the resma
resma.shape  # (4,2,3)

# Create a subset of the resma
# Want only
#   "State" and "Block" of the geolevel variable
#   "sum" from the aggtypes variable
#   everything from the metrics variable
resma_sub = resma.subset(geolevels = ["State", "Block"],
                         aggtypes  = ["sum"]
   )

# Shape of resma_sub
resma_sub.shape  # (2,1,3)

# Squeeze resma_sub
# now just a numpy array (not a Resma object)
squeezed_array = resma_sub.squeeze()

# Shape of squeezed_array
squeezed_array.shape  # (2,3)

# Flatten resma_sub
# now just a numpy array (not a Resma object)
flat_array = resma_sub.flatten()

# Shape of flat_array
flat_array.shape  # (6,)

# Split the resma by aggtype
# Now a list of 2 Resma objects of shape (4,1,3)
resma_aggtype = resma.split("aggtype")

# Split the resma by geolevel
# Now a list of 4 Resma objects of shape (1,2,3)
resma_geolevel = resma.split("geolevel")

# Split the resma by aggtype and metric
# Now a list of 2*3 Resma objects of shape (4,1,1)
# Can use (1,2) instead of ("aggtype", "metric"), if desired
resma_am = resma.split((1,2))

# Prepare the resma for a visualization of the "Summed L1 errors across each geolevel"
resma_forVis = resma.subset(aggtype=["sum"], metrics=["L1_geounit"])

# Shape of resma_forVis
resma_forVis.shape  # (4,1,1)

# Access and flatten the values
values_resma_forVis = resma_forVis.values.flatten()
```

### List of error metrics:
The __ErrorMetricNode__ class contains the following functions for calculating error metrics using the _raw_ and _syn_ data in a single __geounitNode__:
```
L1_cell
L1_geounit
avgL1_geounit
L2_geounit
avgL2_geounit
relL1_cell
relL1_geounit
relL2_geounit
LInf_geounit
avgLInf_geounit
relLInf_geounit
tvd_cell
tvd_geounit
L1_housingMarg
```

### Config file example:
```ini

[error_metrics]
error_metrics: programs.error_metrics.das_error_metrics.error_metrics

save_location: /cenhome/dms-p0-992/pl94topdown/das_error_metrics/em_save_data/

metric_groups: sample1, sample2

sample1.geolevels: County
sample1.metrics: L1_geounit, L2_geounit, LInf_geounit
sample1.aggtypes: all
sample1.quantiles: 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0
sample1.data_pair: raw, syn

sample2.geolevels: State, County, Tract
sample2.metrics: L1_housingMarg
sample2.aggtypes: sum, summary
sample2.quantiles: _11, 0.92, 0.94, 0.96, 0.98
```
