# pyspark_unit_test_mocking
A brief example of unit testing of pyspark with unittest module and using mocks.
The repository contains a class with a query over a Parquet Dataframes with some Spark operations like join, udf, withColumn...
Unit test are done with unittest package and some methods are mocked
Code style is checked with flake8.
### Versions
Tested with:
- Python 3.6.9
- Spark 2.4.4 (Hadoop 2.7)
### Getting Started
```
#Run process
$ SPARK_HOME/bin/spark-submit people_query.py
#Run tests
$ python -m unittest people_query_test.py
```

