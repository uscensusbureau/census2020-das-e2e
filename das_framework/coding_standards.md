# Coding standards for DAS project

## General rules and philosophy
* Functions should not change their input arguments unless all of the following are true:
** The entire purpose of the function is to make the change.
** The change to inputs is clearly documented in the function's docstring.
** The function name indicates that something will be changed (e.g. `extend_array(array,length)`). 
* In general, do not delete items from the middle of arrays. It is slow and difficult to debug. 
* For debugging, use a logging module rather than printf() statements. The logging module can be set to log to the console *and* to a file, and the log level can be changed at runtime. Logging with Python should be done using the Python [logging](https://docs.python.org/3.4/library/logging.html) facility.

* [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)
* Software matters more than the data. 
* Write for reproducibility and repeatability
* Avoid [premature optimization](http://ubiquity.acm.org/article.cfm?id=1513451)

## Invoking Programs
* In general, all configuration information should be kept in a config file.
* The only command line option that should be *required* is the full path of the config file.

## Specifics for Python
* The DAS will follow the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html), except as noted below.
* Tabs should be expanded to spaces and not be embedded in programs. See https://www.jwz.org/doc/tabs-vs-spaces.html for details.
* Python code should be indented by 4 spaces.
* Python 3.4 unless there is a specific reason to be using Python 2.7. **(NOTE: We cannot code to Python 3.6 because it is not supported on Amazon Elastic Map Reduce as of July 15, 2017.)**



### Using Python Modules
* Explain what modules are used and why.


## General Rules for Tests
* Unit tests are done with the py.test framework
* Each major function should have a unit test

## General Coding Practices
* Use lots of assert() statement
* test with pylint

# Aspirational Goals
* Formal verification
* Formally verified random number generators
* Migration to language with strong-types.
* Add support for [typing hints](https://docs.python.org/3/library/typing.html) (New in python 3.5)

