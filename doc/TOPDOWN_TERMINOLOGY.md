# das-pl94-topdown
This is the repository for the US Census Bureau's top-down Disclosure Avoidance System (_DAS_) implementation for the 2020 Decennial Census, targeted at the Public Law 94-171 (_PL94_) query workload (to be later adapted to the Summary File 1, _SF1_, query workload).

This document outlines at a high level the operation of the top-down _DAS_ and defines technical terms used in describing the top-down _DAS_. The top-down DAS proceeds by _ALLOCATING_ or _IMPUTING_ records associated with a higher geographic unit or _PARENT_ G to the immediately lower geographic units or _CHILDREN_ G<sub>1</sub>,G<sub>2</sub>,...,G<sub>k</sub> that constitute G. For example, G might be a blockgroup and G<sub>1</sub>,G<sub>2</sub>,...,G<sub>k</sub> its blocks, or G might be a county and G<sub>1</sub>,G<sub>2</sub>...,G<sub>k</sub> its tracts. We can think of each geographic unit G as an _(ALLOCATION) NODE_ in a (weakly) connected directed acyclic graph (a directed tree with the national _ALLOCATION NODE_ as its root and all edges directed away from the root), taking inputs:

* _ASSIGNMENTS_ for this geography. (We are expressing these assigned records as a histogram, but they could equivalently be expressed as a set of rows.) At the top level this is the _NATIONAL ALLOCATIONS_.

* _GEOGRAPHY LEVEL_ at which the node is running. (Must be one of Nation, State, County, Tract, or Block Group.)

* _GEOGRAPHY IDENTIFIER_ for the geographic unit, i.e. the _GEOCODE_ (sub)string that uniquely identifies geographic unit G.

* _QUERY MODEL_ for the sub-geographies. (The set of queries that we are attempting to optimize is the _(QUERY) WORKLOAD_, in keeping with Matrix Mechanism language.)

* _INVARIANTS_ for the sub-geographies. (The set of publicly known counts for each sub-sub-geography, e.g. number of _GROUP QUARTERS FACILITIES_. Note that these may not be directly represented in the _ALLOCATIONS_.)

* _(INVARIANT) CONSTRAINTS_ for the sub-geographies. (The set of constraints implied on sub-geography counts directly represented in the _ALLOCATIONS_ by the sub-sub-geography information in _INVARIANTS_, e.g. _GROUP QUARTERS FACILITIES_ implies a lower bound on persons in GQ facilities. In addition, _(INVARIANT) CONSTRAINTS_ contains information on quantities like the number of Voting Age persons in GQs, which are not given directly as invariants but for which bounds at higher geographic levels can be deductively derived from block-level _INVARIANTS_.)

# pseudocode for the DAS
For a given table, the DAS proceeds as follows:

_high level description of top-down DAS to be added here_

The outputs are:

* _ASSIGNMENTS_ to each _CHILD GEOGRAPHY_. (This is a set of (_GEOGRAPHY LEVEL_, _GEOCODE_, histogram) tuples for each geographic unit G<sub>i</sub> constituting G.)

* _ACCURACY_ measures on each _ALLOCATION_, each calculated using some norm and a subset of the _WORKLOAD_. (We will use this for our development, but it is not used elsewhere.)

# producer-consumer variant
Simson could use a producer/consumer system where each _NODE_ reads the (_GEOGRAPHY LEVEL_, _GEOCODE_, _ALLOCATIONS_) to queue and writes to the queue the (_CHILD GEOGRAPHY LEVEL_, _CHILD GEOCODE_, _CHILD ALLOCATIONS_). Simson has a highly reliable producer-consumer system like this that I wrote in C++ and used for 10 years in production, but it only runs on a single shared memory system. I know how to extend it to a cluster using ZMQ. However, Simson has a model for extending it to _SPARK_ as well. Essentially, it would use a data frame for each _GEOGRAPHY LEVEL_ and the rows would be the _GEOCODES_. This is similar to what William Sexton has been developing, Simson believes.

The _QUERY MODEL_ is built before the _ALLOCATIONS_ are performed. The _QUERY MODEL_ has no dependencies but must be computed at each geography. The problem in computing the _QUERY MODEL_ is that at the _STATE_ level it may be slow to compute, so we would parallelize the creation within each _STATE_, while at the _BLOCK_ level it is fast to compute, so we would which to compute each block in a single thread. However, there are no dependencies between any of the rows.

Simson needs to know how we are representing the individual _QUERIES_ that are computed to create the _QUERY MODEL_ at each _GEOGRAPHY LEVEL_ and _GEOCODE_, and how we are representing the results.  Presumably this is being used as the inputs for Gurobi.

Individual _QUERIES_ or sets of _QUERIES_ are represented using the Query class object we created in python (see cenquery.py).  It takes up to three arguments:
1.  array_dims - a tuple that has the dimensions of the array we are describing e.g (17,2,116,2,63), (3,2,255)
2.  subset - a tuple of slices that should match the number of array dimensions that represent what subset of the array we should use to construct the queries e.g ( slice(0,1) , slice(0,2), slice(0,116), slice(0,2), slice(0,63), slice(0,52))
3.  add_over_margins - which margins (dimensions) should we sum over to construct the query/constraint(s) e.g. (0,1,2)

Query class objects together with noisy differentially private answers together form objects of the class DPquery (again see cenquery.py).  The DPquery objects are used as inputs to Gurobi.

Similarly to the DPquery class, _CONSTRAINTS_ are represented as a Query class object together with a RHS vector and a sign (= , \le, \ge).
