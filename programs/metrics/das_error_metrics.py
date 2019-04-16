import logging
import os
import numpy as np
from das_framework.driver import AbstractDASErrorMetrics
import programs.metrics.error_metrics_node as em
import programs.metrics.metric_group as mgmod
import sys
import time
from pyspark.sql import Row
import ast
import json
import pickle

CONFIG_DELIM = ", "
BLANK = ""
NATIONAL_GEOCODE = "0"
MULTIARRAY_GROUP_SEP = "||"

ERROR_METRICS = "error_metrics"

# save file currently uses json format only
# note that rdd's are not serializable in json format, so make sure AGG_RDD isn't used when saving
METRIC_GROUP_SAVE_LOCATION = "save_location"

# Note that if there are no metrics listed, the error metrics module will return None without running any error metric calculating code
METRIC_GROUPS = "metric_groups"
GROUP_GEOLEVELS = ".geolevels"
GROUP_METRICS = ".metrics"
GROUP_AGGREGATION_TYPES = ".aggtypes"
GROUP_DATA_PAIR = ".data_pair"
GROUP_QUANTILES = ".quantiles"

# metric aggregation types:
#   AGG_SUM - Calculates the totals across the RDD/geolevel for each error metric
#   AGG_AVG - Calculates the averages across the RDD/geolevel for each error metric (averages over number of geounits in a geolevel)
#   AGG_SUMMARY - Calculates summary statistics across the RDD/geolevel for each error metric
AGG_SUM = "sum"
AGG_AVG = "avg"
AGG_SUMMARY = "summary"
AGG_COLLECT = "collect"
AGG_TYPES = [AGG_SUM, AGG_AVG, AGG_SUMMARY, AGG_COLLECT]

DEFAULT_DATA_PAIR = "raw, syn"
DEFAULT_QUANTILES = [0.0, 0.25, 0.5, 0.75, 1.0]

QUANTILE_LINSPACE_CHAR = "_"
LINSPACE_ROUNDING_ERROR = 8

# a way of easily specifying that a particular metric should be run over all options
# i.e. metric1.geolevels: all  =>  run the metrics on "all" geolevels within the passed dictionary
ALL = "all"

class error_metrics(AbstractDASErrorMetrics):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def run(self, geolevelRDD_dict):
        """
        """
        config = self.config
        
        # get metric specifications
        if ERROR_METRICS not in config:
            logging.debug("'[{}]' section not found in the config file. This must be present to calculate error metrics.".format(ERROR_METRICS))
            return None
        if METRIC_GROUPS not in config[ERROR_METRICS]:
            logging.debug("To calculate error metrics, add the '{}' option to the config file and add at least one metric group name.".format(METRIC_GROUPS))
            return None
        
        ########################################
        # Get metric groups
        ########################################
        metric_groups = config[ERROR_METRICS][METRIC_GROUPS]
        
        if metric_groups == BLANK:
            logging.debug("Add at least one metric group name to calculate error metrics.")
            return None
        
        metric_groups = metric_groups.split(CONFIG_DELIM)
        logging.debug("metric groups: {}".format(metric_groups))
        
        ########################################
        # Create the DASErrorMetrics object
        ########################################
        # prepare the error metrics nodes 
        das_error = DASErrorMetrics(geolevelRDD_dict)
        
        ########################################
        # Iterate through metric groups
        ########################################
        metric_group_list = []
        # iterate over each metric specification to gather the appropriate error metric statistics
        for mg in metric_groups:
            metric_group_config = {}
            
            ########################################
            # Config option for Comparison Pair
            ########################################
            # look to see if any different comparisons should be made during error metric calculations
            if mg + GROUP_DATA_PAIR not in config[ERROR_METRICS]:
                mg_data_pair = DEFAULT_DATA_PAIR.split(CONFIG_DELIM)
            else:
                mg_data_pair = config[ERROR_METRICS][mg + GROUP_DATA_PAIR].split(CONFIG_DELIM)
                assert len(mg_data_pair) == 2, "Error metrics require two datasets to compare."
            
            logging.debug("{}: {}".format(mg + GROUP_DATA_PAIR, mg_data_pair))
            metric_group_config[mg + GROUP_DATA_PAIR] = CONFIG_DELIM.join(mg_data_pair)
            
            
            ########################################
            # Config option for the Geolevels
            ########################################
            # check to see if any geolevels were specified in the config file
            # if BLANK, assume the user wants to calculate over "all" geolevels
            if mg + GROUP_GEOLEVELS not in config[ERROR_METRICS]:
                continue
            
            mg_geolevels = config[ERROR_METRICS][mg + GROUP_GEOLEVELS]
            if mg_geolevels == BLANK or mg_geolevels == ALL:
                mg_geolevels = list(das_error.metricRDD_dict.keys())
            else:
                mg_geolevels = mg_geolevels.split(CONFIG_DELIM)
            
            logging.debug("{}: {}".format(mg + GROUP_GEOLEVELS, mg_geolevels))
            metric_group_config[mg + GROUP_GEOLEVELS] = CONFIG_DELIM.join(mg_geolevels)
            
            
            ########################################
            # Config option for the Error Metrics to use
            ########################################
            # check to see if any metric_keys were specified in the config file
            # if BLANK, assume the user wants to calculate with "all" error metric functions
            if mg + GROUP_METRICS not in config[ERROR_METRICS]:
                continue
            
            mg_metrics = config[ERROR_METRICS][mg + GROUP_METRICS]
            if mg_metrics == BLANK or mg_metrics == ALL:
                mg_metrics = das_error.all_metric_keys
            else:
                mg_metrics = mg_metrics.split(CONFIG_DELIM)
            
            logging.debug("{}: {}".format(mg + GROUP_METRICS, mg_metrics))
            metric_group_config[mg + GROUP_METRICS] = CONFIG_DELIM.join(mg_metrics)
            
            
            ########################################
            # Config option for the Aggregation Types
            ########################################
            # check to see if any aggregation types were specified in the config file
            # if BLANK, assume the user wants to calculate using all aggregation types
            if mg + GROUP_AGGREGATION_TYPES not in config[ERROR_METRICS]:
                continue
            
            mg_aggregation_types = config[ERROR_METRICS][mg + GROUP_AGGREGATION_TYPES]
            if mg_aggregation_types == BLANK or mg_aggregation_types == ALL:
                mg_aggregation_types = AGG_TYPES
            else:
                mg_aggregation_types = mg_aggregation_types.split(CONFIG_DELIM)
            
            logging.debug("{}: {}".format(mg + GROUP_AGGREGATION_TYPES, mg_aggregation_types))
            metric_group_config[mg + GROUP_AGGREGATION_TYPES] = CONFIG_DELIM.join(mg_aggregation_types)
            
            
            ########################################
            # Config option for the Summary Quantiles
            ########################################
            mg_quantiles = []
            if mg + GROUP_QUANTILES not in config[ERROR_METRICS]:
                mg_quantiles = DEFAULT_QUANTILES
            elif config[ERROR_METRICS][mg + GROUP_QUANTILES] == BLANK:
                mg_quantiles = DEFAULT_QUANTILES
            else:
                mg_quantile_list = config[ERROR_METRICS][mg + GROUP_QUANTILES].split(CONFIG_DELIM)
                elem1 = mg_quantile_list[0]
                if elem1[0] == QUANTILE_LINSPACE_CHAR:
                    num = int(elem1[1:])
                    mg_quantiles += [round(qu, LINSPACE_ROUNDING_ERROR) for qu in np.linspace(0, 1, num).tolist()]
                    mg_quantile_list = mg_quantile_list[1:]
                
                mg_quantiles += [float(x) for x in mg_quantile_list]
                mg_quantiles = list(set(mg_quantiles))
                mg_quantiles.sort()
                
            logging.debug("{}: {}".format(mg + GROUP_QUANTILES, mg_quantiles))
            metric_group_config[mg + GROUP_QUANTILES] = CONFIG_DELIM.join([str(x) for x in mg_quantiles])
            
            ########################################
            # Calculate Error Metrics for this metric group
            ########################################
            t0 = time.time() 
            metric_results = das_error.getErrorMetric(geolevels         = mg_geolevels,
                                                      metrics           = mg_metrics,
                                                      agg_types         = mg_aggregation_types,
                                                      data_pair         = mg_data_pair,
                                                      summary_quantiles = mg_quantiles
                )
            
            metric_results_dict = {}
            metric_results_dict["metric_group"] = mg
            
            metric_results_dict["config_settings"] = { GROUP_GEOLEVELS[1:]         : mg_geolevels,
                                                       GROUP_METRICS[1:]           : mg_metrics,
                                                       GROUP_AGGREGATION_TYPES[1:] : mg_aggregation_types,
                                                       GROUP_DATA_PAIR[1:]         : mg_data_pair,
                                                       GROUP_QUANTILES[1:]         : mg_quantiles
                }
            metric_results_dict["results"] = metric_results
            
            t1 = time.time()
            
            metric_results_dict["runtime_in_seconds"] = t1 - t0
            
            metric_results_node = mgmod.MetricGroup(metric_results_dict)
            
            metric_group_list.append(metric_results_node)
            logging.debug("\n\nTook {} seconds to run this metric.".format(t1-t0))
            
            
            ########################################
            # Save Error Metrics, Config options
            ########################################
            if METRIC_GROUP_SAVE_LOCATION in config[ERROR_METRICS]:
                save_as = config[ERROR_METRICS][METRIC_GROUP_SAVE_LOCATION]
                if save_as is not BLANK:
                    if save_as[-1] is not "/":
                        save_as += "/"
                    
                    # save human-readable version of the error metrics
                    save_error_metrics_as = save_as + mg + ".json"
                    with open(save_error_metrics_as, "w") as f:
                        json.dump(prepForSaving(metric_results_dict), f, indent=2, sort_keys=True)
                    
                    # save original configuration options for this metric run/group
                    logging.debug("{}".format(metric_group_config))
                    save_metric_configs_as = save_as + mg + "_config.txt"
                    with open(save_metric_configs_as, "w") as f:
                        for (k,v) in metric_group_config.items():
                            f.write("{}: {}\n".format(k,v))
                    
                    # save "pickled" form of the error metric results node; it's nicer to work with than the pure dictionary form used to generate the readable JSON file
                    save_error_metrics_pickle_as = save_as + mg + ".pickle"
                    with open(save_error_metrics_pickle_as, 'wb') as f:
                        pickle.dump(metric_results_node, f)
                    
                    logging.debug("Saved metric info here: {}\n".format(save_as))
            
        
        return metric_group_list
        


class DASErrorMetrics():
    """
    Manages the geounitNode objects and the ErrorMetricNode objects created from them
    """
    def __init__(self, geolevelRDD_dict):
        """
        Constructor for DASErrorMetrics class
        Inputs:
            geolevelRDD_dict (dict): a dictionary of RDDs containing geounitNode objects
        Notes:
            Creates RDDs of ErrorMetricsNode objects
        """
        # dictionary containing RDDs that contain geounitNodes
        self.geolevelRDD_dict = geolevelRDD_dict
        
        # dictionary containing RDDs that contain ErrorMetricsNodes
        self.metricRDD_dict = {}
        for i,key in enumerate(list(self.geolevelRDD_dict.keys())):
            self.metricRDD_dict[key] = self.geolevelRDD_dict[key].map(lambda geo_node: errorWrapper(geo_node))
        
        # list of all error metric functions/keys for using functions in the ErrorMetricsNode class 
        self.all_metric_keys = self.getErrorMetricKeys()
        
    
    def getErrorMetric(self, geolevels=None, metrics=None, agg_types=None, data_pair=None, summary_quantiles=None):
        """
        Calculates error metrics given geolevel, metric, and aggregation specifications defined in the config file
        Inputs:
            geolevels: a list of (str) geolevels for which to calculate error metrics
            metrics: a list of (str) keys that refer to the error metrics to be calculated
            agg_types: a list of (str) aggregation types for specifying how to aggregate over Spark RDDs
            data_pair: a list of 2 (str) data sets (living within geounitNodes) that will be compared via ErrorMetricsNode functions
            summary_quantiles: a list of values between 0 and 1 for calculating the quantiles of the RDDs
        Outputs:
            a dictionary containing the results of the error metrics
        """
        # get the desired subset of geolevel RDDs to work with
        geoRDD_dict = self.__getGeolevelRDDs(geolevels)
        metricRDD_dict = self.__getMetrics(geoRDD_dict, metrics, data_pair)
        metric_results = self.__aggregateByType(metricRDD_dict, agg_types, summary_quantiles)
        
        return metric_results
    
    def __getGeolevelRDDs(self, geolevels):
        """
        Gives a dictionary with the correct subset of RDDs corresponding to the provided geolevel specifications
        Inputs:
            geolevels: a list of (str) geolevels for which to calculate error metrics
        Outputs:
            a dictionary of RDDs
        """
        rdd_dict = {}
        for i,k in enumerate(geolevels):
            assert k in self.metricRDD_dict, "Provided geolevel '{}' not found.".format(k)
            rdd_dict[k] = self.metricRDD_dict[k]
        
        return rdd_dict
    
    def __getMetrics(self, rdd_dict, metrics, data_pair):
        """
        Gives a dictionary with the calculated, required error metrics specified in the config file
        Inputs:
            rdd_dict: a dictionary containing the geolevel RDDs
            metrics: a list of (str) keys that refer to the error metrics to be calculated
        Outputs:
            a dictionary containing RDDs containing ErrorMetricsNodes that contain the required error metric calculations
        """
        metric_rdd_dict = {}
        
        for rdd in list(rdd_dict.keys()):
            metric_rdd_dict[rdd] = rdd_dict[rdd].map(lambda node: node.updateCompPair(data_pair).calculateErrorMetrics(metrics))
        
        return metric_rdd_dict
    
    def __aggregateByType(self, metric_rdd_dict, agg_types, summary_quantiles):
        """
        Combines or collects error metrics based on the aggregation specifications in the config file
        Inputs:
            metric_rdd_dict: a dictionary of RDDs containing ErrorMetricsNodes
            agg_types: a list of (str) aggregation types for specifying how to aggregate over Spark RDDs
        Outputs:
            a dictionary
        Notes:
            For AGG_SUM and AGG_AVG, all geounitNodes are summed under the NATIONAL_GEOCODE, regardless of whether
            the nodes span the entire Nation or not.
        """
        
        rdd_results = {}
        # move through each RDD of ErrorMetricsNodes that was specified in the config file
        for rdd_key in list(metric_rdd_dict.keys()):
            geolevel_key = rdd_key
            # grab error metrics based on the aggregation types specified in the config file
            for agg in agg_types:
                assert agg in AGG_TYPES, "Aggregation type '{}' not found.".format(agg)
                if agg == AGG_SUM:
                    # gives an error_metrics_node
                    myrdd = metric_rdd_dict[rdd_key]
                    mycount = myrdd.count()
                    #print("Agg Sum -- Number of geounits in geolevel {}: {}".format(geolevel_key, mycount))
                    sum_error_metrics_node = (myrdd.map(lambda node: (NATIONAL_GEOCODE, node))
                                                   .reduceByKey(sparkAggSum_ErrorMetricsNodes)
                                                   .map(lambda kv: kv[1])
                                                   .collect()
                                                   .pop()
                        )
                    # set geocode to be the "national geocode", which is "0"
                    sum_error_metrics_node.geocode = NATIONAL_GEOCODE
                    # only provide the dictionary within the error metrics node
                    for em_key, em_val in sum_error_metrics_node.error_metrics.items():
                        rdd_results["{}.{}.{}".format(geolevel_key, agg, em_key)] = em_val 
                    #rdd_results["{}.{}".format(geolevel_key, agg)] = sum_error_metrics_node.error_metrics
                
                elif agg == AGG_AVG:
                    # gives an error_metrics_node
                    myrdd = metric_rdd_dict[rdd_key]
                    myrdd_count = myrdd.count()
                    sum_error_metrics_node = (myrdd.map(lambda node: (NATIONAL_GEOCODE, node))
                                                   .reduceByKey(sparkAggSum_ErrorMetricsNodes)
                                                   .map(lambda kv: kv[1])
                                                   .collect()
                                                   .pop()
                        )
                    avg_error_metrics_node = sum_error_metrics_node.divideMetricsBy(myrdd_count)
                    # set geocode to be the "national geocode", which is "0"
                    avg_error_metrics_node.geocode = NATIONAL_GEOCODE
                    # only provide the dictionary within the error metrics node
                    for em_key, em_val in sum_error_metrics_node.error_metrics.items():
                        rdd_results["{}.{}.{}".format(geolevel_key, agg, em_key)] = em_val
                    #rdd_results["{}.{}".format(geolevel_key, agg)] = avg_error_metrics_node.error_metrics
                
                elif agg == AGG_SUMMARY:
                    # gives summary statistics for each error metric over all nodes in a geolevel
                    myrdd = metric_rdd_dict[rdd_key]
                    summary = getSummaryStats(myrdd, summary_quantiles)
                    for summary_key, summary_val in summary.items():
                        rdd_results["{}.{}.{}".format(geolevel_key, agg, summary_key)] = summary_val
                    #rdd_results["{}.{}".format(geolevel_key, agg)] = summary
                
                elif agg == AGG_COLLECT:
                    # 09 Aug 2018 - bam
                    # Note that this functionality will break the error_metrics class found above (the one that
                    # works within the das_framework), so only use it with the separate analysis code
                    myrdd = metric_rdd_dict[rdd_key]
                    mycount = myrdd.count()
                    #print("Agg Collect -- Number of geounits in geolevel {}: {}".format(geolevel_key, mycount))
                    
                    if mycount == 1:
                        collect_emnode_rdd = myrdd.map(sparkAggCollect_MakeAllMetricsIntoLists).collect().pop()
                    else:
                        collect_emnode_rdd = (myrdd.map(sparkAggCollect_MakeAllMetricsIntoLists)
                                                   .map(lambda node: (NATIONAL_GEOCODE, node))
                                                   .reduceByKey(sparkAggCollect_CombineErrorMetricsIntoOne)
                                                   .map(lambda kv: kv[1])
                                                   .collect()
                                                   .pop()
                            )
                    emnode_lengths = [(k,len(v)) for k,v in collect_emnode_rdd.error_metrics.items()]
                    #print("Agg Collect -- Number of geounits in geolevel {} after collect: {}".format(geolevel_key, emnode_lengths))
                    for em_key, em_val in collect_emnode_rdd.error_metrics.items():
                        rdd_results["{}.{}.{}".format(geolevel_key, agg, em_key)] = em_val
                    
                
            
        
        return rdd_results
    
    def getRDDGeolevels(self):
        """
        Provides the geolevel for each RDD passed into the constructor
        Outputs:
            a list of strings corresponding to the 'geolevel' of each RDD
        Notes:
            Only checks one geounitNode within each RDD and assumes homogeneity amongst geounitNode geolevels within an RDD
        """
        return [self.geolevelRDD_dict[k].take(1)[0].geolevel for k in list(self.geolevelRDD_dict.keys())]
    
    def getErrorMetricKeys(self):
        """
        Provides all error metric keys contained within ErrorMetricNode objects
        Outputs:
            a list of keys for each error metric
        """
        key = list(self.metricRDD_dict.keys())[0]
        return self.metricRDD_dict[key].take(1).pop().getErrorMetricKeys()



def getSummaryStats(rdd, quantiles, max_collect=25000):
    count = rdd.count()
    summary_stats = None
    if count <= max_collect:
        summary_stats = getSummaryStats_collect(rdd, quantiles)
    else:
        summary_stats = getSummaryStats_DF(rdd, quantiles)
    
    summary_stats = reshapeQuantileMultiarraysInDict(summary_stats)
    
    return summary_stats

def getSummaryStats_DF(rdd, quantiles, quantile_approx_rel_err=0.001):
    flatrdd = (rdd
                .map(lambda node: node.error_metrics)
                .map(lambda emdict: multiarrayToKey(emdict))
                .map(lambda emdict: Row(**emdict))
        )
    df = sqlContext.createDataFrame(flatrdd)
    flatkeys = df.columns
    
    all_quantiles = {}
    for k in flatkeys:
        all_quantiles[k] = dict(zip(quantiles, df.approxQuantile(k, quantiles, quantile_approx_rel_err)))
    
    return all_quantiles

def getSummaryStats_collect(rdd, quantiles):
    # items is a list of dictionaries
    items = (rdd
                .map(lambda node: node.error_metrics)
                .collect()
        )
    
    flatitems = [multiarrayToKey(x) for x in items]
    keys = list(flatitems[0].keys())
    
    metric_summaries = {}
    for k in keys:
        metric = np.array([x[k] for x in flatitems])
        percentiles = [100.0*q for q in quantiles]
        metric_summaries[k] = dict(zip(quantiles, np.percentile(metric, percentiles)))
    
    return metric_summaries

def getShapeFromMultiarrayDict(madict):
    tuple_list = [ast.literal_eval(k) for k in madict.keys()]
    tuple_list.sort()
    shape = []
    for dim in tuple_list[-1]:
        shape.append(dim+1)
    
    return tuple(shape)


def reshapeQuantileMultiarraysInDict(qdict):
    summary_dict = {}
    
    multiarray_dict = dict([x for x in qdict.items() if MULTIARRAY_GROUP_SEP in x[0]])
    multiarray_groups = set([x.split(MULTIARRAY_GROUP_SEP)[0] for x in multiarray_dict.keys()])
    for group in multiarray_groups:
        summary_dict[group] = quantileMultiarray(multiarray_dict, group)
    
    other_dict = dict([x for x in qdict.items() if x[0] not in multiarray_dict])
    for k in other_dict.keys():
        summary_dict[k] = other_dict[k]
    
    return summary_dict

def quantileMultiarray(qdict, key):
    madict = dict([(k.split(MULTIARRAY_GROUP_SEP)[1], qdict[k]) for k in qdict.keys() if str(key)+MULTIARRAY_GROUP_SEP in k])
    shape = getShapeFromMultiarrayDict(madict)
    quantile_multiarray = np.zeros(shape, dtype="object")
    for (k,v) in madict.items():
        ind = ast.literal_eval(k)
        quantile_multiarray[ind] = v
    
    return quantile_multiarray

def multiarrayToKey(x):
    # x is a dictionary of metric key,value pairs
    # some of the values are numpy multiarrays
    # each cell needs to have its own key,value pair for the
    # spark data frame to work and be able to generate approx quantiles
    # y is the new, multiarray-expanded dictionary
    y = {}
    for (k,v) in x.items():
        if isinstance(v, np.ndarray):
            for i in np.ndindex(v.shape):
                y[str(k) + MULTIARRAY_GROUP_SEP + str(i)] = float(v[i])
            
        else:
            y[k] = float(v)
        
    return y


# modified from: https://stackoverflow.com/questions/3229419/how-to-pretty-print-nested-dictionaries/3314411#3314411
def pretty(d, indent=0):
    s = ""
    items = list(d.items())
    items.sort()
    for key, value in items:
        s += '..' * indent + str(key) + "\n"
        if isinstance(value, dict):
            s += pretty(value, indent+1)
        else:
            s += '..' * (indent+1) + str(value) + "\n"
        
    return s

def prepForSaving(mdict):
    newdict = {}
    for key, value in mdict.items():
        if isinstance(value, dict):
            value = prepForSaving(value)
        elif isinstance(value, np.ndarray):
            value = value.tolist()
        else:
            value = value
        
        newdict[key] = value
    return newdict

def sparkAggSum_ErrorMetricsNodes(nodeAgg, node):
    """
    Adds error metrics for the ErrorMetricsNodes objects
    Notes:
        Used in a Spark reduceByKey operation
    """
    for k in list(nodeAgg.error_metrics.keys()):
        nodeAgg.error_metrics[k] += node.error_metrics[k]
    
    return nodeAgg

def sparkAggCollect_MakeAllMetricsIntoLists(node):
    """
    Notes:
        Used in a Spark map operation
    """
    for k in node.error_metrics.keys():
        node.error_metrics[k] = [(node.geocode, node.error_metrics[k])]
    
    return node

def sparkAggCollect_CombineErrorMetricsIntoOne(nodeA, nodeB):
    """
    Generate a list of items for collection purposes
    Notes:
        Used in a Spark reduceByKey operation
    """
    for k in nodeA.error_metrics.keys():
        nodeA.error_metrics[k] += nodeB.error_metrics[k]
    
    return nodeA


def errorWrapper(geo_node):
    """
    a function used within DASErrorMetrics that creates the ErrorMetricsNode objects
    Inputs:
        geo_node: a geounitNode object
    Outputs:
        an ErrorMetricsNode object
    Notes:
        Due to Spark sending code along to different cores that may not have the error_metrics_node module available,
        this wrapper function helps make the module accessible to Spark
    """
    import programs.metrics.error_metrics_node as em
    return em.ErrorMetricsNode(geo_node)
