from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql import Row


def get_df(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame([
        Row(a=1, b=2,),
        Row(a=2, b=3,),
    ])
    return df


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    get_df(spark)
