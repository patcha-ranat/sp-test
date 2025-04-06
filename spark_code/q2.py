from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import format_number
import logging


def set_logger(level_name: str):
    # set logger
    logger = logging.getLogger(level_name)
    logger.setLevel(logging.INFO)
    
    # set formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt="%Y-%m-%d %I:%M:%S %p")
    
    # set handler
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger

def main():
    # initialize logger
    logger = set_logger("Driver - Q2")

    # initialize spark session
    spark = SparkSession.builder \
                        .appName("sp-03-spark") \
                        .master("local[8]") \
                        .getOrCreate()
    try:
        # read data
        read_options = {
            "header": "true", 
            "inferSchema": "true",
        }

        data_pricing_path = "data/pricing_project_dataset.csv"
        data_platform_number_path = "data/platform_number.csv"

        logger.info("Reading CSV Files to Spark DataFrame")
        df_pricing = spark.read.format("csv") \
                                .options(**read_options) \
                                .load(data_pricing_path)

        df_platform_order = spark.read.format("csv") \
                                    .options(**read_options) \
                                    .load(data_platform_number_path)

        logger.info("Cleaning DataFrame")

        # simple cleaning
        clean_df_pricing = df_pricing.withColumnRenamed("grass_region", "region")

        # Q3.2 Calculating Net Competitiveness Considering Priority
        logger.info("Calculating Net Competitiveness Considering Priority")
        clean_df_pricing.select("shopee_item_id", "shopee_model_id", "shopee_model_competitiveness_status") \
                .withColumn("new_cpt_status", 
                    when(col("shopee_model_competitiveness_status") == "Shopee < CPT", 1) \
                    .when(col("shopee_model_competitiveness_status") == "Shopee = CPT", 2) \
                    .when(col("shopee_model_competitiveness_status") == "Shopee > CPT", 3) \
                    .otherwise(4)
                )\
                .select(
                    "shopee_item_id",
                    "shopee_model_id",
                    "shopee_model_competitiveness_status",
                    "new_cpt_status",
                    min("new_cpt_status").over(Window.partitionBy(["shopee_item_id", "shopee_model_id"])).alias("priority")
                )\
                .where(col("new_cpt_status") == col("priority"))\
                .select(
                    "shopee_item_id",
                    "shopee_model_id",
                    "shopee_model_competitiveness_status"
                ) \
                .show()

    except Exception as err:
        raise err

    finally:
        spark.stop()

if __name__ == '__main__':
    main()
