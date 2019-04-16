#!/bin/bash
#
#
# Run a DAS job with:
# bash run_cluster.sh

echo 
echo This file is included for documentation purposes, but it cannot
echo be run outside of the US Census Bureau, as it requires the confidential
echo data from the 2018 End-to-End test for proper operation.
echo
exit 1

CONFIG=E2E_2018_CONFIG.ini

# Make sure that destinations have been set
: "${MDF_UNIT:?MDF_UNIT is not set}"
: "${MDF_PER:?MDF_PER is not set}"


# Be sure output is clear
# It can be either directory or a single file, so we need both --recursive delete and regular delete
aws s3 rm --recursive $MDF_UNIT
aws s3 rm --recursive $MDF_PER
aws s3 rm $MDF_UNIT
aws s3 rm $MDF_PER

if [ $USER != 'hadoop' ]; then
  echo This script must be run as the hadoop user
  exit 1
fi

# Directory where we are running
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd $DIR
export PATH=$PATH:$PWD

# Logdir
LOGDIR=/mnt/tmp/logs
mkdir -p $LOGDIR || exit 1


# Find the cef
$( python find_cef.py )

# Make sure that the environment variables are set
# https://stackoverflow.com/questions/307503/whats-a-concise-way-to-check-that-environment-variables-are-set-in-a-unix-shell

: "${CEF_UNIT:?CEF UNIT file could not be found}"
: "${CEF_PER:?CEF PER file could not be found}"
: "${GRFC:?GRFC file could not be found}"


# Create the ZIP file to get the code to CORE nodes.
# Export it so that it can be used in programs/das_setup.py
ZIPFILE=/tmp/das_decennial$$.zip
export ZIPFILE
zip -r -q $ZIPFILE . -i '*.py' '*.sh' '*.ini' || exit 1

# Create ZIP file with non-code files containing data (Spark will not look for them in
# files added as py-files.
# Commented, since we can just supply the same zip file again
#DATAZIPFILE=/tmp/ddecdatafiles$$.zip
#export DATAZIPFILE
#zip $DATAZIPFILE das_framework/DAS_TESTPOINTS.csv || exit 1

## Check to make sure the config file exists
if [ ! -r "$CONFIG" ]; then
  echo Cannot read DAS config file $CONFIG
  exit 1
fi

echo Running DAS on `date` with $CONFIG

spark-submit --py-files $ZIPFILE --driver-memory 5g --num-executors 25 \
    --files $ZIPFILE \
    --executor-memory 16g --executor-cores 4 --driver-cores 10  \
    --conf spark.driver.maxResultSize=0g --conf spark.executor.memoryOverhead=20g --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$LOGDIR --master yarn --conf spark.submit.deployMode=client \
    --conf spark.network.timeout=3000 das_framework/driver.py $CONFIG --loglevel DEBUG  || exit 1

echo Combining the output files
python s3cat.py $MDF_UNIT --demand_success || exit 1
python s3cat.py $MDF_PER --demand_success  || exit 1
echo DAS done at `date`

exit 0
