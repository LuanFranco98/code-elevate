from bronze import CorridasProcessorBronze
from silver import CorridasProcessorSilver
from gold import CorridasProcessorGold
import os

if __name__ == "__main__":
    processor_bronze = CorridasProcessorBronze(
        os.environ["BRONZE_INPUT_PATH"], 
        os.environ["BRONZE_OUTPUT_PATH"], 
        os.environ["START_DATE"], 
        os.environ["END_DATE"], 
    )

    processor_bronze.run()

    processor_silver = CorridasProcessorSilver(
        os.environ["SILVER_INPUT_PATH"], 
        os.environ["SILVER_OUTPUT_PATH"], 
        os.environ["START_DATE"], 
        os.environ["END_DATE"], 
    )

    processor_silver.run()

    processor_gold = CorridasProcessorGold(
        os.environ["GOLD_INPUT_PATH"], 
        os.environ["GOLD_OUTPUT_PATH"], 
        os.environ["START_DATE"], 
        os.environ["END_DATE"], 
    )

    processor_gold.run()

