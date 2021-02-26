import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

log = None


def get_spark_session():
    return SparkSession.builder \
        .master("local") \
        .appName("unit testing example") \
        .getOrCreate()


def get_logger(spark):
    global log
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)


def read_parquet_dataframe(spark, path):
    log.info('Reading Parquet DataFrame: %s' % path)
    return spark.read.parquet(path)


def inner_join_equality(df1, df2, colName):
    exp = df1[colName] == df2[colName]
    return df1.join(df2, exp, 'inner').drop(df1[colName])


age_udf = udf(lambda age: "Adult" if age >= 18 else "Teenager"
              if 18 > age > 10
              else "Child", StringType())


def main():
    base_path = os.path.dirname(__file__)
    spark = get_spark_session()
    get_logger(spark)
    countries = read_parquet_dataframe(spark, base_path +
                                       '/datasets/parquet/countries/')
    population = read_parquet_dataframe(spark, base_path +
                                        '/datasets/parquet/population/')
    joined = inner_join_equality(population, countries, 'Country')
    joined.withColumn('Age Segment', age_udf(joined['Age'])).show()


if __name__ == '__main__':
    main()
