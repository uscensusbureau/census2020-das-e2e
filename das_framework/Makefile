all:
	echo no all to make

demo:
	python3 driver.py config.ini

test:
	py.test

clean:
	rm -f *~ *.log *.out

demo1:
	@echo Just run the program with the current .ini file and append the results to the file
	python3 categories_framework.py categories.ini

FNAME := exp-$(shell echo $$$$)

demo2:
	@echo Run a full experiment, with the output in $(FNAME)
	python3 categories_framework.py --experiment=$(FNAME) categories.ini

