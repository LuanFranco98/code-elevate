from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from datetime import datetime, date 
import os

class CorridasProcessorSilver:
    def __init__(self, input_path: str, output_path: str, start_date, end_date):
        """
        Inicializa a classe, setando os paths e incializando o spark.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("ProcessamentoCorridasSilver") \
            .getOrCreate()
        self.start_date = start_date
        self.end_date = end_date

    def read_data(self) -> DataFrame:
        print("Starting bronze to silver process.")
        try:
            df_bronze =  (
                self.spark.read
                .parquet(self.input_path)
            )
            return df_bronze
        except Exception as e:
            print(f"Error reading bronze parquet from {self.input_path}.Error: {e}")
            raise e
    
    def transform_data(self, df_bronze):
        try:
            parsed_start_date = datetime.strptime(os.environ["START_DATE"], "%m-%d-%Y")
            parsed_start_date= parsed_start_date.strftime("%Y-%m-%d")
            parsed_end_date = datetime.strptime(os.environ["END_DATE"], "%m-%d-%Y")
            parsed_end_date= parsed_end_date.strftime("%Y-%m-%d")

            
            df_silver = (
                df_bronze
                .select(
                    f.date_format(
                        f.to_date(f.col("DATA_INICIO"), "MM-dd-yyyy HH:mm"),
                        "yyyy-MM-dd"
                    ).alias("DATA_INICIO"),
                    f.date_format(
                        f.to_date(f.col("DATA_FIM"), "MM-dd-yyyy HH:mm"),
                        "yyyy-MM-dd"
                    ).alias("DATA_FIM"),
                    f.col("DISTANCIA").cast("float").alias("DISTANCIA"),
                    f.coalesce(f.col("PROPOSITO"), f.lit("Desconhecido")).alias("PROPOSITO"),
                    f.col("CATEGORIA"),
                    f.col("LOCAL_INICIO"),
                    f.col("LOCAL_FIM"),
                )
                .filter(
                    f.col("DATA_INICIO").isNotNull() &
                    f.col("DATA_INICIO").between(parsed_start_date, parsed_end_date)
                )
            )

            return df_silver
        except Exception as e:
            print(f"Error transforming bronze table to silver: {e}")
            raise e

    def save_data(self, df):
        try:
            print(f"Trying to save {df.count()} rows")
            output_dir = os.path.dirname(self.output_path)
            os.makedirs(output_dir, exist_ok=True)

            (
                df.write.mode("overwrite")
                .partitionBy("DATA_INICIO")
                .option(
                    "replaceWhere",
                    f"DATA_INICIO >= '{self.start_date}' AND DATA_INICIO <= ''{self.end_date}"
                )
                .parquet(self.output_path)
            )

            print(f"File saved at: {self.output_path}")
        except Exception as e:
            print(f"Error saving silver parquet: {e}")
            raise e


    def run(self):
        df_raw = self.read_data()
        df_final = self.transform_data(df_raw)
        self.save_data(df_final)
        self.spark.stop()

