#!/usr/bin/env python3.5
# driver.py
#
# William N. Sexton and Simson L. Garfinkel
#
# Major Modification log:
#  2018-06-12  bam - refactored DAS to modularize code found in the run function
#  2017-12-10  slg - refactored the creation of objects for the DAS() object.
#  2017-11-19  slg - rewrite for abstract modular design, created experiment runner
#  2017-08-10  wns - initial framework working
#  2017-07-20  slg - created file

""" This is the main driver for the Disclosure Avoidance Subsystem (DAS).
    It executes the disclosure avoidance programs:
    it runs a setup module and data reader, runs the selected DAS engine,
    calls the output writer, and evaluates the output against the input.

    For systems that use Apache Spark, the driver run command is:

        spark-submit driver.py path/to/config.ini 

    For systems that do not use Spark, the driver run command is:

        python3 driver.py path/to/config.ini

       or:

        python3 path/to/driver.py  config.ini

    Note that the driver.py can be included and run in another program.

"""

import sys
import os
import time

# DAS-specific imports:
sys.path.append(os.getcwd())
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "ctools"))
sys.path.append(os.path.join(os.path.dirname(__file__), "dfxml/python"))

# System Libraries
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from configparser import ConfigParser
from ctools.hierarchical_configparser import HierarchicalConfigParser
import datetime
import json
import logging
import logging.handlers
import numpy
import os
import os.path
import pickle
import re
import subprocess
import sys
import time
import ctools
import ctools.s3


# DAS-specific libraries
import ctools
import ctools.clogging
import experiment
from dfxml.writer import DFXMLWriter
from das_testpoints import log_testpoint

DEFAULT = 'DEFAULT'
ENVIRONMENT = "ENVIRONMENT"
SETUP = "setup"
READER = "reader"
ENGINE = "engine"
ERROR_METRICS = "error_metrics"
WRITER = "writer"
VALIDATOR = "validator"
TAKEDOWN = "takedown"

# LOGGING
LOGGING_SECTION = 'logging'
LOGFILENAME_OPTION='logfilename'
LOGLEVEL_OPTION='loglevel'
LOGFOLDER_OPTION='logfolder'

ROOT = 'root'  # where the experiment is running
LOGFILENAME = 'logfilename'  # 
DEFAULT_LOGFILENAME = 'das'
OUTPUT_FNAME = 'output_fname'
OUTPUT_DIR = "output_dir"

# EXPERIMENT values
EXPERIMENT = 'experiment'
RUN_EXPERIMENT_FLAG = "run_experiment_flag"
EXPERIMENT_SCAFFOLD = 'scaffold'
EXPERIMENT_DIR = 'dir'  # the directory in which the experiment is taking place
EXPERIMENT_CONFIG = 'config'  # the name of the configuration file
EXPERIMENT_XLABEL = 'xlabel'  # what to label the X axis
EXPERIMENT_YLABEL = 'ylabel'  # what to label the Y axis
EXPERIMENT_GRID = 'grid'  # Draw the grid? True/False
EXPERIMENT_GRAPH_FNAME = 'graph_fname'  # filename for figure we are saving
EXPERIMENT_GRAPH_DATA_FNAME = 'graph_data_fname'  # Filename for the graph data
EXPERIMENT_AVERAGEX = 'averagex'  # should all Y values for a certain X be averaged?
EXPERIMENT_TITLE = 'title'
EXPERIMENT_DRAW_LEGEND = 'draw_legend'
EXPERIMENT_GRAPHX = 'graphx'
EXPERIMENT_GRAPHY = 'graphy'


def config_validate(config, extra_sections=[]):
    """Make sure mandatory sections exist"""
    for section in [SETUP, READER, ENGINE, WRITER, VALIDATOR, TAKEDOWN] + extra_sections:
        if section not in config:
            logging.error("config file missing section '{}'".format(section))
            raise RuntimeError("config file missing section {}".format(section))


def config_apply_environment(config):
    """Look for the ENVIRONMENT section and apply the variables to the environment
    Note: By default, section names are case sensitive, but variable names are not.
    Because the convention is that environment variables are all upper-case, we uppercase them.
    """

    import os
    if ENVIRONMENT in config:
        for var in config[ENVIRONMENT]:
            name = var.upper()
            value = config[ENVIRONMENT][var]
            logging.info("os.environ: {}={}".format(name, value))
            os.environ[name] = value


### numpy integers can't be serialized; we need our own serializer
### https://stackoverflow.com/questions/27050108/convert-numpy-type-to-python/27050186#27050186
class DriverEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


def strtobool(val,default=None):
    if val in ["", None] and default!=None:
        return default
    v = val.lower()
    if v in ['y','yes','t','true','on','1']: return True
    if v in ['n','no','f','false','off','0']: return False
    raise ValueError

class AbstractDASModule(object):
    def __init__(self, name=None, config=None, setup=None, **kwargs):
        if config != None:
            assert type(config) in [HierarchicalConfigParser, ConfigParser]
        self.name = name
        self.config = config
        self.setup = setup
        self.t0    = time.time()

    def getconfig(self, key, default=None, section=None):
        if section == None:
            section = self.name
        try:
            val = self.config[section][key]
            logging.debug("config[{}][{}]={}".format(section, key, val))
            return val
        except KeyError:
            if default != None:
                logging.debug("config[{}][{}] NOT FOUND; returning default {}".format(section, key, default))
                return str(default)
        logging.info("config[{}][{}] does not exist".format(section, key))
        raise KeyError("Required configuration variable '{}' does not exist in section '[{}]'".format(key, section))

    def getint(self, key, **kwargs):
        return int(self.getconfig(key, **kwargs))

    def getfloat(self, key, **kwargs):
        return float(self.getconfig(key, **kwargs))

    def getboolean(self, key, default=None, section=None):
        # https://stackoverflow.com/questions/715417/converting-from-a-string-to-boolean-in-python
        # Python does not have a good builtin for converting a string to a boolean, so we create one.
        return strtobool(self.getconfig(key, section=section), default=default)

    def getiter(self, key, sep=',', **kwargs):
        return map(lambda s: s.strip(), re.split(sep,self.getconfig(key, **kwargs)))

    def gettuple(self, key, **kwargs):
        return tuple(self.getiter(self, key, **kwargs))

    def getiter_of_ints(self, key, **kwargs):
        return map(int, self.getiter(key, **kwargs))

    def gettuple_of_ints(self, key, **kwargs):
        return tuple(self.getiter_of_ints(key, **kwargs))

    def getiter_of_floats(self, key, **kwargs):
        return map(float, self.getiter(key, **kwargs))

    def gettuple_of_floats(self, key, **kwargs):
        return tuple(self.getiter_of_floats(key, **kwargs))

    def getconfitems(self, section):
        """
        Filters out DEFAULTs from config items of the section
        :param section: section of config files
        :return: iterator of config items in the section
        """
        if self.config.has_section(section):
            return list(filter(lambda item: item not in self.config.items('DEFAULT'), self.config.items(section)))
        else:
            return {}


class AbstractExperiment(AbstractDASModule):
    def __init__(self, das=None, **kwargs):
        super().__init__(**kwargs)
        self.das = das

    def runExperiment(self):
        return None


class AbstractDASExperiment(AbstractExperiment):
    """This is the experiment driver. This is where the loops will be done"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.loops = experiment.build_loops(self.config)
        self.state = experiment.initial_state(self.loops)

    def increment_state(self):
        # """Given a set of loops and a state, increment the state to the next position, handling roll-over.
        # Return the next state. If we are finished, return None.
        # Remember, loops is loops[rank][section,variable,start,stop,step]
        # Notice that we perform decimal arithmetic to avoid the 0.1 + 0.1 problem
        #
        # """
        # state = list(self.state)  # convert to a list that can be modified
        # rank = 0
        # while rank < len(state):
        #     if self.loops[rank][5] == "MUL":
        #         state[rank] *= self.loops[rank][4]
        #     elif self.loops[rank][5] == "ADD":
        #         state[rank] += self.loops[rank][4]
        #     elif self.loops[rank][5] == "LIST":
        #         state[rank] = self.loops[rank][4][self.loops[rank][4].index(state[rank]) + 1] if state[rank] < self.loops[rank][3] else \
        #         state[rank] + 1000
        #
        #     if state[rank] <= self.loops[rank][3]:
        #         self.state = tuple(state)  # found a new state
        #     # Reset this rank to the starting position and go to the next rank
        #     state[rank] = self.loops[rank][2]  # reset to start
        #     rank += 1  # go to next rank
        # # Ran out
        # self.state = None
        # return self
        self.state = experiment.increment_state(self.loops,self.state)
        return self

    def substitute_config(self):
        """Generate a new config given a current config and a state of the loops."""

        for rank in range(len(self.loops)):
            section = self.loops[rank][0]
            var = self.loops[rank][1]
            self.das.config[section][var] = str(self.state[rank])

        return self

    def runExperiment(self):

        scaffold = Scaffolding(config=self.config)
        scaffold.experimentSetup()

        while self.state is not None:
            self.substitute_config()
            self.das.run()
            self.increment_state()

        scaffold.experimentTakedown()

        return None

    def experimentSetup(self):
        return None

    def experimentTakedown(self):
        return None


class AbstractDASSetup(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def setup_func(self):
        """Setup Function. Note special name."""
        return None


class AbstractDASReader(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willRead(self):
        return True

    def read(self):
        """Read the data; return a reference. Location to read specified in config file."""
        return None  # no read data in prototype

    def didRead(self):
        return


class AbstractDASEngine(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willRun(self):
        return True

    def run(self, original_data):
        """Nothing to do in the prototype"""
        return

    def didRun(self):
        return


class AbstractDASErrorMetrics(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willRun(self):
        return True

    def run(self, data):
        """Nothing to do in the prototype"""
        return None

    def didRun(self):
        return


class AbstractDASWriter(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willWrite(self):
        return True

    def write(self, privatized_data):
        """Return the written data"""
        return privatized_data  # by default, just return the privatized_data, nothing is written

    def didWrite(self):
        return


class AbstractDASValidator(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willValidate(self):
        return True

    def validate(self, original_data, written_data_reference, **kwargs):
        """No validation in prototype"""
        return True

    def didValidate(self):
        return

    def storeResults(self, data):
        """data is a dictionary with results. The default implementation
        stores them in a file called 'results' specified in the config file"""
        with open(self.getconfig('results_fname', default='results.json'), "a") as f:
            json.dump(data, f, cls=DriverEncoder)
            f.write("\n")


class AbstractDASTakedown(AbstractDASModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def willTakedown(self):
        return True

    def takedown(self):
        """No takedown in prototype"""
        return True

    def removeWrittenData(self, reference):
        """Delete what's referred to by reference. Do not call superclass"""
        raise RuntimeError("No method defined to removeWrittenData({})".format(reference))

    def didTakedown(self):
        return True


class Scaffolding(object):
    """ Scaffolding for an experiment"""

    def __init__(self, config):
        assert type(config) == HierarchicalConfigParser
        self.config = config
        scaffoldstr = config[EXPERIMENT].get(EXPERIMENT_SCAFFOLD, None)
        if not scaffoldstr:
            logging.info("No scaffolding")
            self.scaffold = None
            return
        (scaffold_file, scaffold_class_name) = scaffoldstr.split(".")
        try:
            scaffold_module = __import__(scaffold_file) if scaffold_file else None
        except ModuleNotFoundError as e:
            logging.exception("Scaffolding import failed. current directory: {}".format(os.getcwd()))
            raise e
        self.scaffold = getattr(scaffold_module, scaffold_class_name)(config=config)

    def experimentSetup(self):
        if self.scaffold:
            self.scaffold.experimentSetup(self.config)

    def experimentTakedown(self):
        if self.scaffold:
            self.scaffold.experimentTakedown(self.config)


class DAS(object):
    """ The Disclosure Avoidance System """

    def __init__(self, config):
        """ Initialize a DAS given a config file. This creates all of the objects that will be used"""
        assert type(config) in  [HierarchicalConfigParser, ConfigParser]
        self.config = config

        # Get the input file and the class for each
        logging.debug("Reading filenames and class names from config file")

        # This section can possibly combined with the following section importing the modules and creating the objects,
        # so that the default objects can be created by just using AbstractDASxxxxxx() constructor
        try:
            (setup_file, setup_class_name) = config[SETUP][SETUP].rsplit(".", 1)
        except KeyError:
            (setup_file, setup_class_name) = ('driver', 'AbstractDASSetup')
        try:
            (reader_file, reader_class_name) = config[READER][READER].rsplit(".", 1)
        except KeyError:
            (reader_file, reader_class_name) = ('driver', 'AbstractDASReader')
        try:
            (engine_file, engine_class_name) = config[ENGINE][ENGINE].rsplit(".", 1)
        except KeyError:
            (engine_file, engine_class_name) = ('driver', 'AbstractDASEngine')
        try:
            (error_metrics_file, error_metrics_class_name) = config[ERROR_METRICS][ERROR_METRICS].rsplit(".", 1)
        except KeyError:
            (error_metrics_file, error_metrics_class_name) = ('driver', 'AbstractDASErrorMetrics')
        try:
            (writer_file, writer_class_name) = config[WRITER][WRITER].rsplit(".", 1)
        except KeyError:
            (writer_file, writer_class_name) = ('driver', 'AbstractDASWriter')
        try:
            (validator_file, validator_class_name) = config[VALIDATOR][VALIDATOR].rsplit(".", 1)
        except KeyError:
            (validator_file, validator_class_name) = ('driver', 'AbstractDASValidator')
        try:
            (takedown_file, takedown_class_name) = config[TAKEDOWN][TAKEDOWN].rsplit(".", 1)
        except KeyError:
            (takedown_file, takedown_class_name) = ('driver', 'AbstractDASTakedown')

        logging.debug(
            "classes: {} {} {} {} {} {} {}".format(setup_class_name, engine_class_name, error_metrics_class_name,
                reader_class_name, writer_class_name, validator_class_name, takedown_class_name))

        # Import the modules
        logging.debug(
            "__import__ files: {} {} {} {} {} {} {}".format(setup_file, engine_file, error_metrics_file, reader_file,
                writer_file, validator_file, takedown_file))
        try:
            setup_module = __import__(setup_file, fromlist=[setup_class_name])
            engine_module = __import__(engine_file, fromlist=[engine_class_name])
            reader_module = __import__(reader_file, fromlist=[reader_class_name])
            error_metrics_module = __import__(error_metrics_file, fromlist=[error_metrics_class_name])
            writer_module = __import__(writer_file, fromlist=[writer_class_name])
            validator_module = __import__(validator_file, fromlist=[validator_class_name])
            takedown_module = __import__(takedown_file, fromlist=[takedown_class_name])
        except ImportError as e:
            print("Module import failed.")
            print("current directory: {}".format(os.getcwd()))
            print("__file__: {}".format(__file__))
            raise e

        # Create the instances
        logging.debug(
            "modules: {} {} {} {} {} {} {}".format(setup_module, engine_module, error_metrics_module, reader_module,
                                                   writer_module, validator_module, takedown_module))

        logging.info("Creating and running DAS setup object")
        setup_obj = getattr(setup_module, setup_class_name)(config=config, name=SETUP)
        setup_data = setup_obj.setup_func()
        logging.debug("DAS setup returned {}".format(setup_data))

        # Now create the other objects
        self.reader = getattr(reader_module, reader_class_name)(config=config, setup=setup_data, name=READER)
        self.engine = getattr(engine_module, engine_class_name)(config=config, setup=setup_data, name=ENGINE)
        self.error_metrics = getattr(error_metrics_module, error_metrics_class_name)(config=config, setup=setup_data,
                                                                                     name=ERROR_METRICS)
        self.writer = getattr(writer_module, writer_class_name)(config=config, setup=setup_data, name=WRITER)
        self.validator = getattr(validator_module, validator_class_name)(config=config, setup=setup_data,
                                                                         name=VALIDATOR)
        self.takedown = getattr(takedown_module, takedown_class_name)(config=config, setup=setup_data, name=TAKEDOWN)

        log_testpoint("T03-003S")
        logging.debug("DAS object complete")

    def runReader(self):
        logging.info("Creating and running DAS reader")
        if self.reader.willRead() == False:
            logging.info("self.reader.willRead() returned false")
            raise RuntimeError("reader willRead() returned False")
        log_testpoint("T03-004S", "Running Reader module")
        original_data = self.reader.read()
        logging.debug("original_data={}".format(original_data))
        self.reader.didRead()
        return original_data

    def runEngine(self, original_data):
        logging.info("Creating and running DAS engine")
        if self.engine.willRun() == False:
            logging.info("self.engine.willRun() returned false")
            raise RuntimeError("engine willRun() returned False")
        log_testpoint("T03-004S", "Running Engine module")
        privatized_data = self.engine.run(original_data)
        logging.debug("privatized_data={}".format(privatized_data))
        self.engine.didRun()
        return privatized_data

    def runErrorMetrics(self, privatized_data):
        logging.info("Creating and running DAS error_metrics")
        if self.error_metrics.willRun() == False:
            logging.info("self.error_metrics.willRun() returned false")
            raise RuntimeError("error_metrics willRun() returned False")
        log_testpoint("T03-004S", "Running Error Metrics module")
        errorMetrics_data = self.error_metrics.run(privatized_data)
        logging.debug("Error Metrics data = {}".format(errorMetrics_data))
        self.error_metrics.didRun()
        return errorMetrics_data

    def runWriter(self, privatized_data):
        logging.info("Creating and running DAS writer")
        if self.writer.willWrite() == False:
            logging.info("self.writer.willWrite() returned false")
            raise RuntimeError("engine willWrite() returned False")
        log_testpoint("T03-004S", "Running Writer module")
        written_data = self.writer.write(privatized_data)
        logging.debug("written_data={}".format(written_data))
        self.writer.didWrite()
        return written_data

    def runValidator(self, original_data, written_data):
        # Now run the validator on the read data and the written results
        logging.info("Creating and running DAS validator")
        if self.validator.willValidate() == False:
            logging.info("self.validator.willValidate() returned false")
            raise RuntimeError("validator willValidate() returned False")
        log_testpoint("T03-004S", "Running Validator module")
        valid = self.validator.validate(original_data, written_data)
        logging.debug("valid={}".format(valid))
        if not valid:
            logging.info("self.validator.validate() returned false")
            raise RuntimeError("Did not validate.")
        self.validator.didValidate()

        # If we were asked to get graphx and graphy, get it.
        data = {}
        if EXPERIMENT in self.config:
            for var in ['graphx', 'graphy']:
                if var in self.config[EXPERIMENT]:
                    (a, b) = self.config[EXPERIMENT][var].split('.')
                    assert a == 'validator'
                    func = getattr(self.validator, b)
                    data[var] = func()

        # Finally take down
        return valid

    def runTakedown(self, written_data):
        logging.info("Creating and running DAS takedown")
        if self.takedown.willTakedown() == False:
            logging.info("self.takedown.willTakedown() returned false")
            raise RuntimeError("validator willTakedown() returned False")
        self.takedown.takedown()
        if self.takedown.getboolean("delete_output", False):
            logging.info("deleting output {}".format(written_data))
            self.takedown.removeWrittenData(written_data)
        self.takedown.didTakedown()

    def run(self):
        """ Run the DAS. Returns data collected as a dictionary if an EXPERIMENT section is specified in the config file."""

        # First run the engine and write the results
        # Create the instances is now done when running

        original_data = self.runReader()
        privatized_data = self.runEngine(original_data)
        errorMetrics_data = self.runErrorMetrics(privatized_data)
        written_data = self.runWriter(privatized_data)
        valid = self.runValidator(original_data, written_data)
        self.runTakedown(written_data)
        log_testpoint("T03-005S")
        data = {}
        return data


def main():
    """Driver. Typically run from __main__ in the program that uses the driver."""
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("config", help="Main Config File")
    parser.add_argument("--experiment",
                        help="Run an experiment according to the [experiment] section, with the results in this directory",
                        action='store_true')
    parser.add_argument("--isolation", help="Specifies isolation mode for experiments",
                        choices=['sameprocess', 'subprocess'], default='sameprocess')
    parser.add_argument("--graphdata", help="Just draw the graph from the data that was already collected.",
                        action='store_true')

    ctools.clogging.add_argument(parser)
    args = parser.parse_args()

    if not os.path.exists(args.config):
        raise RuntimeError("{} does not exist".format(args.config))

    if args.graphdata and args.experiment is None:
        parser.error("--graphdata requires --experiment")

    ###
    ### Read the configuration file
    ###

    config = HierarchicalConfigParser()
    config.read(args.config)

    ###
    ### Logging must be set up before any logging is done
    ### By default is is in the current directory, but if we run an experiment, put the logfile in that directory
    ### Added option to put logs in a subfolder specified in the config
    isodate = datetime.datetime.now().isoformat()[0:19]
    if config.has_section(LOGGING_SECTION) and config.has_option(LOGGING_SECTION,LOGFOLDER_OPTION) and config.has_option(LOGGING_SECTION,LOGFILENAME_OPTION):
        logfname = f"{config[LOGGING_SECTION][LOGFOLDER_OPTION]}/{config[LOGGING_SECTION][LOGFILENAME_OPTION]}-{isodate}-{os.getpid()}.log"
    else:
        logfname = f"{isodate}-{os.getpid()}.log"
    dfxml = DFXMLWriter(filename=logfname.replace(".log",".dfxml"), prettyprint=True)


    # Left here for backward compatibility, to be removed in future versions
    if args.experiment:
        if not os.path.exists(args.experiment):
            os.makedirs(args.experiment)
        if not os.path.isdir(args.experiment):
            raise RuntimeError("{} is not a directory".format(args.experiment))
        config['DEFAULT'][ROOT] = args.experiment
        logfname = os.path.join(args.experiment, logfname)
    ####

    # Make sure the directory for the logfile exists. If not, make it.
    logdirname = os.path.dirname(logfname)
    if logdirname and not os.path.exists(logdirname):
        print("driver.py: os.mkdir({})".format(logdirname))
        os.mkdir(logdirname)

    ctools.clogging.setup(args.loglevel,syslog=True, filename=logfname)
    logging.info("START {}  log level: {}".format(os.path.abspath(__file__), args.loglevel))

    t0 = time.time()

    log_testpoint("T03-002S")

    #########################
    # Set up the experiment #
    #########################
    
    # if there is no experiment section in the config file, add one
    if EXPERIMENT not in config:
        config.add_section(EXPERIMENT)
    
    # If there is no run experiment flag in the config section, add it
    run_experiment = config[EXPERIMENT].getint(RUN_EXPERIMENT_FLAG,0)
    
    # If --experiment was specified, set run_experiment to run
    if args.experiment:
        run_experiment = 1

    ### Now validate and apply the config file
    config_validate(config)
    config_apply_environment(config)

    #############################
    # Create the DAS
    #############################
    das = DAS(config)

    #############################
    # DAS Running Section.
    # Option 1 - run_experiment
    # Option 2 - just run the das
    #############################
    logging.debug("Just before Experiment")

    if run_experiment:
        # set up the Experiment module
        try:
            (experiment_file, experiment_class_name) = config[EXPERIMENT][EXPERIMENT].rsplit(".", 1)
        except KeyError:
            (experiment_file, experiment_class_name) = ('driver','AbstractDASExperiment')
        try:
            experiment_module = __import__(experiment_file, fromlist=[experiment_class_name])
        except ImportError as e:
            print("Module import failed.")
            print("current directory: {}".format(os.getcwd()))
            print("__file__: {}".format(__file__))
            raise e

        experiment = getattr(experiment_module, experiment_class_name)(das=das, config=das.config, name=EXPERIMENT)
        logging.debug("Running DAS Experiment. Logfile: {}".format(logfname))

        experiment_data = experiment.runExperiment()

    else:
        #### Run the DAS without an experiment
        logging.debug("Running DAS without an experiment. Logfile: {}".format(logfname))
        try:
            data = das.run()
        except Exception as e:
            log_testpoint("T03-005F")
            raise(e)

    ###
    ### Shutdown
    ###
    t1 = time.time()
    t = t1 - t0
    logging.info("Elapsed time: {:.2} seconds".format(t))
    logging.info("END {}".format(os.path.abspath(__file__)))
    logging.shutdown()
    print("*****************************************************")
    print("driver.py: Run completed in {:,.2f} seconds. Logfile: {}".format(t, logfname))


if __name__ == '__main__':
    main()
