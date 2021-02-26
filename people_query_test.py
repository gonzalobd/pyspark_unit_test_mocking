import unittest
import logging
import people_query as people_query
from unittest.mock import patch
from pyspark.sql import SparkSession


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.INFO)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                .master('local[2]')
                .appName('my-local-testing-pyspark-context')
                .enableHiveSupport()
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        cls.test_df_people = cls.spark.\
            createDataFrame(
                            [('Pepe', 20, 'Spain')],
                            ['Name', 'Age', 'Country'])
        cls.test_df_countries = cls.spark.\
            createDataFrame(
                            [('Spain', 'Europe')],
                            ['Country', 'Continent'])

    def test_get_spark_session(self):
        spark = people_query.get_spark_session()
        expected = '''<class 'pyspark.sql.session.SparkSession'>'''
        self.assertEquals(
                            str(type(spark)),
                            expected)

    def test_inner_join_equality(self):
        out = people_query.\
            inner_join_equality(self.test_df_people,
                                self.test_df_countries, 'Country')
        expected = self.spark.\
            createDataFrame(
                            [('Pepe', 20, 'Spain', 'Europe')],
                            ['Name', 'Age', 'Country', 'Continent'])
        self.assertEquals(out.collect(), expected.collect())

    @patch('people_query.inner_join_equality')
    @patch('people_query.get_spark_session')
    @patch('people_query.read_parquet_dataframe')
    def test_main(self, mock_read_parquet_df,
                  mock_get_sparksession, mock_inner_join_equality):
        mock_read_parquet_df.return_value = self.test_df_people
        mock_get_sparksession.return_value = self.spark
        mock_inner_join_equality.return_value = self.test_df_people
        people_query.main()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
