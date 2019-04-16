spark-submit \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.xml" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.xml" \
    log_check.py

