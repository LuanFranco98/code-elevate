from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import LongType
from datetime import datetime 
import os

class CorridasProcessorGold:
    def __init__(self, input_path: str, output_path: str, start_date, end_date):
        """
        Inicializa a classe, setando os paths e incializando o spark.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("ProcessamentoCorridasGold") \
            .getOrCreate()
        self.start_date = start_date
        self.end_date = end_date

    def read_data(self) -> DataFrame:
        print("Starting silver to gold process.")
        try:
            df_silver =  (
                self.spark.read
                .parquet(self.input_path)
            )
            return df_silver
        except Exception as e:
            print(f"Error reading silver parquet from {self.input_path}.Error: {e}")
            raise e
    
    def transform_data(self, df_silver):
        try:
            parsed_start_date = datetime.strptime(os.environ["START_DATE"], "%m-%d-%Y")
            parsed_start_date= parsed_start_date.strftime("%Y-%m-%d")
            parsed_end_date = datetime.strptime(os.environ["END_DATE"], "%m-%d-%Y")
            parsed_end_date= parsed_end_date.strftime("%Y-%m-%d")

            
            df_gold = (
                df_silver
                .filter(
                    f.col("DATA_INICIO").isNotNull() &
                    f.col("DATA_INICIO").between(parsed_start_date, parsed_end_date)
                )
                .groupBy(f.col("DATA_INICIO").alias("DT_REFE"))
                .agg(
                    f.count("*").alias("QT_CORR"),
                    f.sum(f.when((f.col("CATEGORIA") == "Negocio"), 1).otherwise(0)).alias("QT_CORR_NEG"),
                    f.sum(f.when((f.col("CATEGORIA") == "Pessoal"), 1).otherwise(0)).alias("QT_CORR_PESS"),
                    f.max(f.col("DISTANCIA").cast(LongType())).alias("VL_MAX_DIST"),
                    f.min(f.col("DISTANCIA").cast(LongType())).alias("VL_MIN_DIST"),
                    f.mean(f.col("DISTANCIA").cast(LongType())).alias("VL_AVG_DIST"),
                    f.sum(f.when((f.col("PROPOSITO") == "Reunião"), 1).otherwise(0)).alias("QT_CORR_REUNI"),
                    f.sum(f.when((f.col("PROPOSITO") != "Reunião"), 1).otherwise(0)).alias("QT_CORR_NAO_REUNI"),
                )       
            )

            return df_gold
        except Exception as e:
            print(f"Error transforming silver table to gold: {e}")
            raise e

    def save_data(self, df):
        try:
            print(f"Trying to save {df.count()} rows")
            output_dir = os.path.dirname(self.output_path)
            os.makedirs(output_dir, exist_ok=True)

            (
                df.write.mode("overwrite")
                .partitionBy("DT_REFE")
                .option(
                    "replaceWhere",
                    f"DT_REFE >= '{self.start_date}' AND DT_REFE <= ''{self.end_date}"
                )
                .parquet(self.output_path)
            )

            print(f"File saved at: {self.output_path}")
        except Exception as e:
            print(f"Error saving gold parquet: {e}")
            raise e


    def run(self):
        df_raw = self.read_data()
        df_final = self.transform_data(df_raw)
        self.save_data(df_final)
        self.spark.stop()

