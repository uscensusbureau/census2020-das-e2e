# This script runs the 2018 End-to-End Census Disclosure Avoidance System as a standalone cluster within an AWS EC2 instance using AMI ID ami-0de53d8956e8dcf80
# with input from the 1940 Census as distributed by IPUMS.

export EXT1940USCB=/home/$USER/das_files/EXT1940USCB.dat
export LOCALDEST=/home/$USER/das_files/output
export GRB_ISV_NAME='Standalone'
export GRB_APP_NAME='DAS'

CONFIG=E2E_1940_STANDALONE_CONFIG.ini

cmd() {
  echo
  echo $*
  $*
}

# File Locations 

export EXT1940USCB=/home/$USER/das_files/EXT1940USCB.dat
export LOCALDEST=/home/$USER/das_files/output

# Spark Standalone configuration
NUM_EXECUTORS=1
DRIVER_MEMORY=5g
EXECUTOR_MEMORY=16g
EXECUTOR_CORES=4
DRIVER_CORES=10
EXECUTOR_MEMORY_OVERHEAD=20g
DRIVER_MAXRESULTS_SIZE=0g

cmd() {
  echo
  echo $*
  $*
}

# File Locations 

export EXT1940USCB=$HOME/das_files/EXT1940USCB.dat
export LOCALDEST=$HOME/das_files/output

# Spark Standalone configuration
NUM_EXECUTORS=1
DRIVER_MEMORY=5g
EXECUTOR_MEMORY=16g
EXECUTOR_CORES=4
DRIVER_CORES=10
EXECUTOR_MEMORY_OVERHEAD=20g
DRIVER_MAXRESULTS_SIZE=0g

echo Validating runtime environment

: "${EXT1940USCB:?EXT1940USCB is not set. Plase set to the location of the 1940 USCB file in S3 or HDFS.}"
: "${LOCALDEST:?LOCALDEST is not set. Please set to the destination for the MDF.}"
: "${GUROBI_HOME:?GUROBI_HOME is not set. }"
: "${GRB_LICENSE_FILE:?GRB_LICENSE_FILE is not set. }"

echo DAS started at `date`

# Specify the location of the output files in the local file system:
export MDF_PER=$LOCALDEST/MDF_PER.txt
export MDF_UNIT=$LOCALDEST/MDF_UNIT.txt
export MDF_PERCAT=$LOCALDEST/MDF_PER.dat
export MDF_UNITCAT=$LOCALDEST/MDF_UNIT.dat
export MDF_CERT=$LOCALDEST/MDF_CERTIFICATE.pdf
export MDF_RESULTS=$LOCALDEST/MDF_RESULTS.zip

echo Verifying that Gurobi is operational and can be run from Spark
cmd py.test tests/gurobi_0_test.py || exit 1
cmd py.test tests/gurobi_1_test.py || exit 1
cmd py.test tests/gurobi_2_test.py || exit 1

# Erase output locations
rm -rf $MDF_UNIT
rm -rf $MDF_PER
rm -rf $MDF_CERT

# Directory where we are running
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd $DIR
export PATH=$PATH:$DIR

# Logdir is required for Spark, but it is not created by default using AWS...
LOGDIR=/home/$USER/das-log
mkdir -p $LOGDIR || exit 1

# Create the ZIP file to get the code to CORE nodes.
# Export it so that it can be used in programs/das_setup.py
# In the future, this is done automatically by ctools.cspark
ZIPFILE=/tmp/das_decennial$$.zip
export ZIPFILE
zip -r -q $ZIPFILE . || exit 1

## Check to make sure the config file exists
if [ ! -r "$CONFIG" ]; then
  echo Cannot read DAS config file $CONFIG
  exit 1
fi

echo Running Spark job on `date` with $CONFIG
echo You can follow the progress with '"tail -f /var/log/local1.log"'
echo ""

spark-submit --py-files $ZIPFILE \
    --files $ZIPFILE \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$LOGDIR \
    --conf spark.driver.maxResultSize=$DRIVER_MAXRESULTS_SIZE \
    --conf spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD \
    --driver-memory $DRIVER_MEMORY \
    --driver-cores $DRIVER_CORES \
    --num-executors $NUM_EXECUTORS \
    --executor-cores $EXECUTOR_CORES \
    --executor-memory $EXECUTOR_MEMORY \
    --conf spark.network.timeout=3000 das_framework/driver.py $CONFIG \
    --loglevel DEBUG  || exit 1

echo Combining the output files
cmd python3 s3cat.py $MDF_UNIT --demand_success || exit 1
cmd python3 s3cat.py $MDF_PER  --demand_success  || exit 1
cmd zip $MDF_RESULTS $MDF_PERCAT $MDF_UNITCAT
echo DAS done at `date`
exit 0
