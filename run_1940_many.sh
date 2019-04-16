#!/bin/bash
#
# make many runs of the DAS and zip up the results.
CONFIG=E2E_1940_CONFIG.ini
EPSILONS="0.25 0.50 0.75 1.0 2.0 4.0 6.0 8.0"
OUTDIR=$HOME/data
export NO_SELFTEST=1
# Set the epsilon range

for epsilon in $EPSILONS ; do
  sed "s/epsilon_budget_total.*/epsilon_budget_total: $epsilon/" E2E_1940_CONFIG.ini  > E2E_1940_CONFIG.ini$$
  mv -f E2E_1940_CONFIG.ini$$ E2E_1940_CONFIG.ini
  for i in {1..4} ; do
    DEST_ZIPFILE=$OUTDIR/EXT1940USCB-EPSILON${epsilon}-RUN${i}.zip
    if [ ! -r $DEST_ZIPFILE ]; then
        echo ================ RUN $i EPSILON $epsilon ================
	export S3DEST=$DAS_S3ROOT/1940/pub/e${epsilon}-run$i
	bash run_1940.sh                            || exit 1 
	aws s3 cp $S3DEST/MDF_PER.txt MDF_PER.txt   || exit 1
	aws s3 cp $S3DEST/MDF_UNIT.txt MDF_UNIT.txt || exit 1
	zip -m $DEST_ZIPFILE MDF_PER.txt MDF_UNIT.txt    || exit 1
	echo =========================================================
    fi
  done
done

