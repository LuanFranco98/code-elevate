from etl import CorridasProcessorBronze

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql import functions as f
import os
from datetime import datetime


@pytest.fixture(scope="module")
def spark():
    """
    Pytest fixture to create a SparkSession for the entire test module.
    It ensures that a single SparkSession is used across all tests in the module,
    and it is stopped after all tests are done.
    """
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("CorridasProcessorBronzeTests") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture(autouse=True)
def clean_spark_session_after_each_test(spark):
    """
    Fixture to clear the cache and reset Spark for each test.
    This helps prevent interference between tests.
    """
    yield
    spark.catalog.clearCache()


@pytest.fixture
def mock_environment_dates(monkeypatch):
    """
    Pytest fixture to temporarily set environment variables for START_DATE and END_DATE.
    This is crucial because the transform_data method directly accesses os.environ.
    monkeypatch ensures these changes are reverted after the test.
    """
    monkeypatch.setenv("START_DATE", "01-01-2016")
    monkeypatch.setenv("END_DATE", "01-31-2016")

def test_transform_data_filters_correctly(spark):
    sample_data = [
        Row(DATA_INICIO="01-15-2016 10:00", DATA_FIM="01-15-2016 10:30", CATEGORIA="Negócio",
            LOCAL_INICIO="A", LOCAL_FIM="B", DISTANCIA="10", PROPOSITO="Almoço"),
        Row(DATA_INICIO="02-01-2016 09:00", DATA_FIM="02-01-2016 09:30", CATEGORIA="Lazer",
            LOCAL_INICIO="C", LOCAL_FIM="D", DISTANCIA="20", PROPOSITO="Viagem"), # end date filter
        Row(DATA_INICIO=None, DATA_FIM="01-10-2016 11:00", CATEGORIA="Pessoal",
            LOCAL_INICIO="E", LOCAL_FIM="F", DISTANCIA="5", PROPOSITO="Compras") # null data inicio filter
    ]
    raw_df = spark.createDataFrame(sample_data)

    processor = CorridasProcessorBronze("input", "output", "01-01-2016", "01-31-2016")
    transformed_df = processor.transform_data(raw_df)

    assert transformed_df.count() == 1

