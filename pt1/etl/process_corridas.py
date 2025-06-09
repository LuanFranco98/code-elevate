from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, count, when, max as spark_max,
    min as spark_min, avg, date_format
)
import os


class CorridasProcessor:
    def __init__(self, input_path: str, output_path: str):
        """
        Inicializa a classe, setando os paths e incializando o spark.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("ProcessamentoCorridas") \
            .getOrCreate()

    def read_data(self) -> DataFrame:
        """
        Lê o CSV de entrada e retorna um DataFrame Spark.
        """
        return self.spark.read.option("header", True).csv(self.input_path)

    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Aplica transformações e retorna o DataFrame agregado.
        """
        df = df.withColumn("DATA_INICIO", to_date("DATA_INICIO", "MM-dd-yyyy HH")) \
               .withColumn("DISTANCIA", col("DISTANCIA").cast("double")) \
               .withColumn("DT_REF", date_format("DATA_INICIO", "yyyy-MM-dd"))

        df_agg = df.groupBy("DT_REF").agg(
            count("*").alias("QT_CORR"),
            count(when(col("CATEGORIA") == "Negócio", True)).alias("QT_CORR_NEG"),
            count(when(col("CATEGORIA") == "Pessoal", True)).alias("QT_CORR_PESS"),
            spark_max("DISTANCIA").alias("VL_MAX_DIST"),
            spark_min("DISTANCIA").alias("VL_MIN_DIST"),
            avg("DISTANCIA").alias("VL_AVG_DIST"),
            count(when(col("PROPOSITO") == "Reunião", True)).alias("QT_CORR_REUNI"),
            count(when((col("PROPOSITO").isNotNull()) & (col("PROPOSITO") != "Reunião"), True)).alias("QT_CORR_NAO_REUNI")
        )

        return df_agg

    def write_output(self, df: DataFrame):
        """
        Escreve o DataFrame final em CSV.
        """
        output_dir = os.path.dirname(self.output_path)
        os.makedirs(output_dir, exist_ok=True)

        df.coalesce(1).write.mode("overwrite").parquet(self.output_path)

        # df.coalesce(1).write \
        #     .option("header", True) \
        #     .mode("overwrite") \
        #     .csv(self.output_path)

        print(f"Arquivo salvo em: {self.output_path}")

    def run(self):
        """
        Executa o pipeline completo.
        """
        df_raw = self.read_data()
        df_final = self.transform_data(df_raw)
        self.write_output(df_final)
        self.spark.stop()


if __name__ == "__main__":
    INPUT_PATH = "data/info_transportes.csv"
    OUTPUT_PATH = "output/info_corridas_do_dia.parquet"

    processor = CorridasProcessor(INPUT_PATH, OUTPUT_PATH)
    processor.run()
