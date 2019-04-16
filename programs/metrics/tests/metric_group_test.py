
import sys
sys.path.append("..")

try:
    import metric_group as mgmod
except ImportError as e:
    import programs.metrics.metric_group as mgmod


RUNTIME = "runtime_in_seconds"
NAME = "metric_group"
SETTINGS = "config_settings"
GEOLEVELS = "geolevels"
AGGTYPES = "aggtypes"
METRICS = "metrics"
QUANTILES = "quantiles"
DATA_PAIR = "data_pair"
RESULTS = "results"


def buildMetricGroup( name      = "mgtest",
                      geolevels = ["State", "County"],
                      aggtypes  = ["sum"],
                      metrics   = ["L1_geounit", "L2_geounit", "LInf_geounit"],
                      quantiles = [0.0, 0.5, 1.0],
                      data_pair = ["raw", "syn"] ):
    
    example_runtime = 50.32
    example_error_data = 4.567
    
    mgdict = {}
    mgdict[RUNTIME] = example_runtime
    mgdict[NAME] = name
    mgdict[SETTINGS] = {}
    settings = mgdict[SETTINGS]
    settings[GEOLEVELS] = geolevels
    settings[AGGTYPES] = aggtypes
    settings[METRICS] = metrics
    settings[QUANTILES] = quantiles
    settings[DATA_PAIR] = data_pair
    mgdict[RESULTS] = {}
    res = mgdict[RESULTS]
    for g in geolevels:
        for a in aggtypes:
            for m in metrics:
                res["{}.{}.{}".format(g,a,m)] = example_error_data
    
    return mgmod.MetricGroup(mgdict)


def test_Resma_shape():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert resma.shape == (3,1,5)

def test_Resma_geolevels():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert (resma.geolevels == geolevels).all()

def test_Resma_aggtypes():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert (resma.aggtypes == aggtypes).all()

def test_Resma_metrics():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    assert (resma.metrics == metrics).all()

def test_Resma_subset():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    
    sub = resma.subset([0], [0], [0])
    assert sub.shape == (1,1,1)
    
    sub = resma.subset(["a",2], metrics=[0,2,4])
    assert sub.shape == (2,1,3)
    assert (sub.geolevels == ["a", "c"]).all()
    assert sub.aggtypes == ["sum"]
    assert (sub.metrics == ["z", "x", "v"]).all()
    
    sub = resma.subset(metrics=["x"])
    assert sub.shape == (3,1,1)
    assert (sub.geolevels == ["a", "b", "c"]).all()
    assert sub.aggtypes == ["sum"]
    assert sub.metrics == ["x"]
    

def test_Resma_split():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    
    split = resma.split("metric")
    assert len(split) == 5
    
    split = resma.split((0,2))
    assert len(split) == 3*5
    

def test_Resma_squeeze():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    
    sub = resma.subset([0])
    assert sub.shape == (1,1,5)
    squeezed = sub.squeeze()
    assert squeezed.shape == (5,)
    

def test_Resma_flatten():
    geolevels = ["a", "b", "c"]
    aggtypes = ["sum"]
    metrics = ["z", "y", "x", "w", "v"]
    mg = buildMetricGroup(geolevels=geolevels, aggtypes=aggtypes, metrics=metrics)
    resma = mg.getResma()
    
    flat = resma.flatten()
    assert len(flat) == 3*1*5
    




