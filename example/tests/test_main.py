from example.main import get_df
from pyspark_test import assert_pyspark_df_equal
from pyspark.sql import Row
from pyspark.sql.session import SparkSession


def test_get_df():
    spark: SparkSession = SparkSession.builder.getOrCreate()
    actual = get_df(spark)
    expected = spark.createDataFrame([
        Row(a=1, b=2,),
        Row(a=2, b=3,),
    ])
    assert_pyspark_df_equal(actual, expected)
