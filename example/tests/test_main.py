from datetime import datetime

from example.main import get_df
from pyspark_test import assert_pyspark_df_equal
from pyspark.sql import Row
from pyspark.sql.session import SparkSession


def test_get_df():
    before = datetime.now()
    spark: SparkSession = SparkSession.builder.getOrCreate()
    after = datetime.now()
    print(f'getOrCreate: {after - before}')

    before = datetime.now()
    actual = get_df(spark)
    after = datetime.now()
    print(f'get actual df: {after - before}')

    before = datetime.now()
    expected = spark.createDataFrame([
        Row(a=1, b=2,),
        Row(a=2, b=3,),
    ])
    after = datetime.now()
    print(f'create expected df: {after - before}')

    before = datetime.now()
    assert_pyspark_df_equal(actual, expected)
    after = datetime.now()
    print(f'assert_pyspark_df_equal: {after - before}')
