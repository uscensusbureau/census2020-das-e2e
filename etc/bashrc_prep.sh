#!/bin/bash

echo 'export LD_LIBRARY_PATH=/usr/local/gurobi752/linux64/lib:$HOME/hadoop-3.1.2/lib/native
export GUROBI_HOME=/usr/local/gurobi752/linux64
export PATH=$PATH:/usr/local/gurobi752/linux64/bin
export GRB_LICENSE_FILE=$HOME/gurobi.lic
export PATH=$PATH:$HOME/spark-2.4.4-bin-hadoop2.7/bin
export PATH=$PATH:/usr/local/anaconda3/bin
export PYSPARK_PYTHON=/usr/local/anaconda3/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/local/anaconda3/bin/python3.6
export SPARK_HOME='$HOME/spark-2.4.4-bin-hadoop2.7'
export PATH=$SPARK_HOME:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export DAS_VERSION=Standalone' >> ~/.bashrc
