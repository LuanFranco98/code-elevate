import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from etl.process_corridas import CorridasProcessor


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-spark") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_transform_data(spark):
    # Simula dados de entrada
    data = [
        ("01-01-2022 08", "Negócio", "Casa", "Reunião", 1.5),
        ("01-01-2022 09", "Pessoal", "Trabalho", "Outro", 2.2),
        ("01-01-2022 10", "Negócio", "Shopping", "Reunião", 0.7),
    ]
    columns = ["DATA_INICIO", "CATEGORIA", "LOCAL_INICIO", "PROPOSITO", "DISTANCIA"]
    df_mock = spark.createDataFrame(data, columns)

    # Instancia a classe (não precisa de caminho real para teste)
    processor = CorridasProcessor("fake.csv", "fake_out.parquet")

    df_result = processor.transform_data(df_mock)

    assert df_result.count() == 1 

    assert df_result.collect()[0]["QT_CORR"] == 3
    assert df_result.collect()[0]["QT_CORR_NEG"] == 2
    assert df_result.collect()[0]["QT_CORR_PESS"] == 1
    assert df_result.collect()[0]["VL_MAX_DIST"] == 2.2
    assert df_result.collect()[0]["VL_MIN_DIST"] == 0.7
    assert round(df_result.collect()[0]["VL_AVG_DIST"], 2) == round((1.5 + 2.2 + 0.7) / 3, 2)
    assert df_result.collect()[0]["QT_CORR_REUNI"] == 2
    assert df_result.collect()[0]["QT_CORR_NAO_REUNI"] == 1


def test_read_csv():
    #TODO
    assert 1 == 1

def test_save_parquet():
    #TODO
    assert 1 == 1


    
