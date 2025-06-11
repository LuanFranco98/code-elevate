from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from datetime import datetime, date 
import os

class CorridasProcessorBronze:
    def __init__(self, input_path: str, output_path: str, start_date, end_date):
        """
        Inicializa a classe, setando os paths e incializando o spark.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("ProcessamentoCorridasBronze") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        self.start_date = start_date
        self.end_date = end_date

    def read_data(self) -> DataFrame:
        print("Starting raw to bronze process.")
        try:
            raw_df =  (
                self.spark.read
                .option("header", True)
                .option("delimiter", ";")
                .csv(self.input_path)
            )
            return raw_df
        except Exception as e:
            print(f"Error reading csv: {e}")
            raise e
    
    def transform_data(self, raw_df):
        try:
            df = (
                raw_df.select(
                    f.to_date(f.col("DATA_INICIO"), "MM-dd-yyyy HH:mm").alias("DATA_INICIO"),
                    f.col("DATA_FIM"),
                    f.col("CATEGORIA"),
                    f.col("LOCAL_INICIO"),
                    f.col("LOCAL_FIM"),
                    f.col("DISTANCIA"),
                    f.col("PROPOSITO"),
                )
                .filter(
                    f.col("DATA_INICIO").isNotNull() &
                    (
                        f.date_format(f.col("DATA_INICIO"), "MM-dd-yyyy").between(os.environ["START_DATE"], os.environ["END_DATE"])
                    )
                )
            )

            return df
        except Exception as e:
            print(f"Error selecting csv columns: {e}")
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
            print(f"Error saving bronze parquet: {e}")
            raise e


    def run(self):
        df_raw = self.read_data()
        df_final = self.transform_data(df_raw)
        self.save_data(df_final)
        self.spark.stop()

