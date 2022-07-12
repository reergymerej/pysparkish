# PySparkish

## Problem

You have a codebase that uses PySpark.

You want to run the tests, but not _actually_ create slow ass Spark contexts.

--------------------------------------------------------------------------------

## Example

_Look at `[example/](example/tests/test_main.py)` for details._

Running a test to check a manually created DataFrame takes around 8 seconds to
run, including pytest setup.

| Task                   |Duration      |
|------------------------|--------------|
|getOrCreate             |0:00:03.238240|
|get actual df           |0:00:02.165822|
|create expected df      |0:00:00.030158|
|assert_pyspark_df_equal |0:00:02.491562|

If you are using fixtures (which you should be) and recreating the PySpark
context with each test (which you should), you will be looking at **7 extra
seconds _per test_!**

* This will make you not want to run tests.
* Which will make you not write tests.
* Which will make you a bad person.

