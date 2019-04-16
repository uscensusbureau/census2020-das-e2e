adult.data: Makefile
	wget https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data
	mv adult.data adult.$$
	echo "age, workclass, fnlwgt, education, education-num, marital-status, occupation, Transport-moving, relationship, race, sex, capital-gain, capital-loss, hours-per-week, native-country" > adult.data
	cat adult.$$ >> adult.data
	/bin/rm -f adult.$$

