import numpy as np
import logging

class ErrorMetricsNode():
    """
    Calculates error metrics on geounitNode objects
    """
    def __init__(self, geo_node):
        """
        Constructor for ErrorMetricsNode class
        Inputs:
            geo_node: a geounitNode object
        Notes:
            Calculates error metrics and stores them in a dictionary (error_dict)
            Unless otherwise specified, all errors are calculated between the syn and raw numpy arrays
        """
        self.geo_node = geo_node
        
        self.comp1 = self.geo_node.raw
        self.comp2 = self.geo_node.syn
        self.geocode = self.geo_node.geocode
        self.geolevel = self.geo_node.geolevel
        
        self.comp1_count = self.comp1.sum()
        self.comp2_count = self.comp2.sum()
        
        self.error_metric_fxn_dict = {
                "geoCount"              : self.geoCount,
                "L1_cell"               : self.L1_cell,
                "L1_geounit"            : self.L1_geounit,
                "avgL1_geounit"         : self.avgL1_geounit,
                "L2_geounit"            : self.L2_geounit,
                "avgL2_geounit"         : self.avgL2_geounit,
                "relL1_cell"            : self.relL1_cell,
                "relL1_geounit"         : self.relL1_geounit,
                "relL2_geounit"         : self.relL2_geounit,
                "LInf_geounit"          : self.LInf_geounit,
                "avgLInf_geounit"       : self.avgLInf_geounit,
                "tvd_geounit"           : self.tvd_geounit,
                "tot_pop_geounit"       : self.tot_pop_geounit,
                "sparsity_geounit"      : self.sparsity_geounit,
                "true_sparsity_geounit" : self.true_sparsity_geounit,
                "L1_GQxVA_marg"         : self.L1_GQxVA_marg,
                "true_GQxVA_marg"       : self.true_GQxVA_marg,
                "syn_GQxVA_marg"        : self.syn_GQxVA_marg,
                "GQxVA_marg"            : self.GQxVA_marg,
                "Cenrace_marg"          : self.Cenrace_marg
            }
        
        self.error_metrics = {}
    
    def __repr__(self):
        """
        Representation of an ErrorMetricsNode
        """
        output = []
        for (k,v) in self.error_metrics.items():
            output.append("{}: {}\n".format(str(k), str(v)))
        
        return "".join(output)
    
    def calculateErrorMetrics(self, metric_keys):
        for key in metric_keys:
            assert key in self.error_metric_fxn_dict, "Provided metric key '{}' not found.".format(key)
            self.error_metrics[key] = self.error_metric_fxn_dict[key]()
        
        return self
    
    def updateCompPair(self, new_pair):
        assert new_pair[0] in dir(self.geo_node) and new_pair[1] in dir(self.geo_node), "Invalid geounitNode data attributes: {}. Choose a different comparison pair.".format(new_pair)
        self.comp1 = getattr(self.geo_node, new_pair[0])
        self.comp2 = getattr(self.geo_node, new_pair[1])
        
        return self
    
    def getErrorMetricKeys(self):
        return list(self.error_metric_fxn_dict.keys())
    
    def divideMetricsBy(self, n):
        assert n != 0, "Cannot divide by zero."
        for key in list(self.error_metrics.keys()):
            self.error_metrics[key] /= n
        
        return self
    
    def getMetricByKey(self, key):
        return self.error_metrics[key]
    
    def geoCount(self):
        return 1
    
    def L1_cell(self):
        """
        Calculates the L1 error for each cell
        Outputs:
            a sparse array containing the L1 error for each cell
        """
        return (self.comp2 - self.comp1).abs()
    
    def L1_geounit(self):
        """
        Calculates the total L1 error across all cells
        Outputs:
            the total L1 error
        """
        return (self.L1_cell()).sum()
    
    def avgL1_geounit(self):
        """
        Calculates the average L1 error
        Outputs:
            the average L1 error
        Notes:
            total L1 error / sum of syn
        """
        return self.L1_geounit() / self.comp2_count
    
    def L2_geounit(self):
        """
        Calculates the total L2 error across all cells
        Outputs:
            the total L2 error
        """
        return np.sqrt((self.comp2 - self.comp1).square().sum())
    
    def avgL2_geounit(self):
        """
        Calculates the average L2 error
        Outputs:
            the average L2 error
        Notes:
            total L2 error / sum of syn
        """
        return self.L2_geounit() / self.comp2_count
    
    def relL1_cell(self, constant=10.0):
        """
        Calculates the relative L1 error for each cell
        Inputs:
            constant (float): a value to add to the denominator to avoid dividing by zero
        Outputs:
            a numpy array containing the relative L1 error for each cell
        """
        return np.divide(self.L1_cell().toDense(), constant+self.comp1.toDense())
    
    def relL1_geounit(self, constant=10.0):
        """
        Calculates the total relative L1 error across all cells
        Inputs:
            constant (float): a value to add to the denominator to avoid dividing by zero
        Outputs:
            the total relative L1 error
        """
        return self.relL1_cell(constant).sum()
    
    def relL2_geounit(self, constant=10.0):
        """
        Calculates the total relative L2 error across all cells
        Inputs:
            constant (float): a value to add to the denominator to avoid dividing by zero
        Outputs:
            the total relative L2 error 
        """

        L2 = (self.comp2 - self.comp1).square()
        L2rel = np.divide(L2.toDense(), (constant+self.comp1.toDense()))
        return L2rel.sum()
    
    def LInf_geounit(self):
        """
        Calculates the total L-infinity error across all cells
        Outputs:
            the total L-infinity error
        """
        return (self.comp2 - self.comp1).max()
    
    def avgLInf_geounit(self):
        """
        Calculates the average L-infinity error
        Outputs:
            the average L-infinity error
        Notes:
            total L-infinity error / sum of syn
        """
        return self.LInf_geounit().toDense() / self.comp2_count
    
    def tvd_geounit(self):
        """
        Calculates the total TVD across all cells
        Outputs:
            the total TVD
        """
        tvd = 0.5 * self.L1_geounit() / self.comp1_count
        return tvd
    
    def tot_pop_geounit(self):
        """
        Calculates the population size
        Outputs:
        """
        return self.comp1_count
    
    def sparsity_geounit(self):
        """
        Calculates the sparsity
        Outputs:
        """
        sparsity = np.sum(self.comp2.toDense()==0)
        
        return sparsity
    
    def true_sparsity_geounit(self):
        """
        Calculates the sparsity
        Outputs:
        """
        sparsity = np.sum(self.comp1.toDense()==0)
        
        return sparsity
    
    def L1_GQxVA_marg(self):
        """
        Calculates the L1 error between the marginalized GQxVA variables
        Assumes (gq, va, ethnicity, race)
        """
        comp1_dense = self.comp1.toDense()
        comp2_dense = self.comp2.toDense()
        
        gqxva_comp1 = comp1_dense.sum(axis=(2,3))
        gqxva_comp2 = comp2_dense.sum(axis=(2,3))
        
        L1 = np.abs(gqxva_comp2 - gqxva_comp1)
        
        return L1

    def true_GQxVA_marg(self):
        """
        Calculates the marginalized GQxVA variables
        Assumes (gq, va, ethnicity, race)
        """
        comp1_dense = self.comp1.toDense()
        gqxva_comp1 = comp1_dense.sum(axis=(2,3))
        
        return gqxva_comp1

    def syn_GQxVA_marg(self):
        """
        Calculates the marginalized GQxVA variables
        Assumes (gq, va, ethnicity, race)
        """
        comp2_dense = self.comp2.toDense()
        gqxva_comp2 = comp2_dense.sum(axis=(2,3))
        
        return gqxva_comp2
    
    def GQxVA_marg(self):
        raw_dense = self.comp1.toDense()
        raw_gqxva = raw_dense.sum(axis=(2,3))
        
        syn_dense = self.comp2.toDense()
        syn_gqxva = syn_dense.sum(axis=(2,3))
        
        return [raw_gqxva, syn_gqxva]

    def Cenrace_marg(self):
        raw_dense = self.comp1.toDense()
        raw_cenrace = raw_dense.sum(axis=(0,1,2))
        
        syn_dense = self.comp2.toDense()
        syn_cenrace = syn_dense.sum(axis=(0,1,2))
        
        return [raw_cenrace, syn_cenrace]