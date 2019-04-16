#!/bin/bash
#
# This script runs the 2018 End-to-End Census Disclosure Avoidance System
# with input from the 1940 Census as distributed by IPUMS.

CONFIG=E2E_1940_CONFIG.ini

cmd() {
  echo
  echo $*
  $*
}

# Spark Configuration Variables
NUM_EXECUTORS=60
DRIVER_MEMORY=5g
EXECUTOR_MEMORY=20g
EXECUTOR_CORES=4
DRIVER_CORES=4
EXECUTOR_MEMORY_OVERHEAD=30g
DRIVER_MAXRESULTS_SIZE=0g

echo Validating runtime environment

: "${EXT1940USCB:?EXT1940USCB is not set. Plase set to the location of the 1940 USCB file in S3 or HDFS.}"
: "${S3DEST:?S3DEST is not set. Please set to the destination for the MDF in S3.}"
: "${AWS_DEFAULT_REGION:?AWS_DEFAULT_REGION is not set. }"
: "${TZ:?TZ is not set. }"
: "${GUROBI_HOME:?GUROBI_HOME is not set. }"
: "${GRB_LICENSE_FILE:?GRB_LICENSE_FILE is not set. }"
: "${GRB_ISV_NAME:?GRB_ISV_NAME is not set. }"
: "${GRB_APP_NAME:?GRB_APP_NAME is not set. }"

echo DAS started at `date`

# Specify the location of the output files in the AWS S3 file system:
export MDF_PER=$S3DEST/MDF_PER.txt
export MDF_UNIT=$S3DEST/MDF_UNIT.txt
export MDF_CERT=$S3DEST/MDF_CERTIFICATE.pdf

if [ x$NO_SELFTEST != x ]; then
  echo Will not run self-test
else
  echo Verifying that Gurbi is operational and can be run from Spark
  cmd py.test tests/gurobi_0_test.py || exit 1
  cmd py.test tests/gurobi_1_test.py || exit 1
  cmd py.test tests/gurobi_2_test.py || exit 1
fi

echo
echo Erasing output locations
echo 

cmd aws s3 rm --recursive $MDF_UNIT
cmd aws s3 rm --recursive $MDF_PER
cmd aws s3 rm $MDF_UNIT
cmd aws s3 rm $MDF_PER
cmd aws s3 rm $MDF_CERT

if [ $USER != 'hadoop' ]; then
  echo This script must be run as the hadoop user on AWS.
  exit 1
fi

# Directory where we are running
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd $DIR
export PATH=$PATH:$DIR

# Logdir is required for Spark, but it is not created by default using AWS...
LOGDIR=/mnt/tmp/logs
mkdir -p $LOGDIR || exit 1

# Create the ZIP file to get the code to CORE nodes.
# Export it so that it can be used in programs/das_setup.py
# In the future, this is done automatically by ctools.cspark
#
ZIPFILE=/tmp/das_decennial$$.zip
export ZIPFILE
zip -r -q $ZIPFILE . -i '*.py' '*.sh' '*.ini' || exit 1
ls -l $ZIPFILE
unzip -l $ZIPFILE

## Check to make sure the config file exists
if [ ! -r "$CONFIG" ]; then
  echo Cannot read DAS config file $CONFIG
  exit 1
fi


echo Running Spark job on `date` with $CONFIG
echo You can follow the progress with '"tail -f /var/log/local1.log"'
echo ""

cmd spark-submit --py-files $ZIPFILE --driver-memory $DRIVER_MEMORY \
    --num-executors $NUM_EXECUTORS \
    --files $ZIPFILE \
    --executor-memory $EXECUTOR_MEMORY \
    --executor-cores $EXECUTOR_CORES \
    --driver-cores $DRIVER_CORES  \
    --conf spark.driver.maxResultSize=$DRIVER_MAXRESULTS_SIZE \
    --conf spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$LOGDIR --master yarn --conf spark.submit.deployMode=client \
    --conf spark.network.timeout=3000 das_framework/driver.py $CONFIG \
    --loglevel DEBUG  || exit 1

echo Combining the output files. 
echo This will force the metadata files to be downloaded and combined
echo with the first part. The original part files will be left in place.
echo
cmd python s3cat.py $MDF_UNIT --demand_success
cmd python s3cat.py $MDF_PER  --demand_success
cmd aws s3 ls $S3DEST/
echo DAS done at `date`
exit 0
