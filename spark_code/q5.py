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
    logger = set_logger("Driver - Q5")

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
        clean_df_platform = df_platform_order.withColumnRenamed("platform order", "platform_order")
        clean_df_pricing = df_pricing.withColumnRenamed("grass_region", "region")

        # Q3.5
        ## --------------------------- Reproducing Q3.3 output
        logger.info("Preparing DataFrame")
        logger.info("Reproducing Q3.3 Output")
        calculate_order = clean_df_pricing.select("region", "shopee_item_id", "shopee_model_id", "shopee_order")
        calculate_platform = clean_df_platform.select("region", "platform_order")
        logger.info("Calculating Contribution Rank and Select Tops Items")
        final_order = calculate_order.groupBy(["region", "shopee_item_id", "shopee_model_id"])\
                                    .agg(sum("shopee_order").alias("sum_shopee_order")) \
                                    .join(calculate_platform, on="region", how="left")\
                                    .withColumn("contribute_ratio", format_number(col("sum_shopee_order") / col("platform_order"), 10)) \
                                    .withColumn("order_threshold", format_number(col("sum_shopee_order")*0.3, 0)) \
                                    .withColumn("contribution_rank", row_number().over(Window.partitionBy(["region", "shopee_item_id", "shopee_model_id"]).orderBy("contribute_ratio"))) \
                                    .where(col("contribution_rank") <= col("order_threshold"))
        final_order.groupBy(["region"]) \
            .agg(count_distinct("shopee_item_id").alias("number_of_item_tops_30"))
        
        ## --------------------------- Starting Q3.5 Output
        logger.info("Calculating Q3.5 Output")
        top_30_model = final_order.select("region", "shopee_item_id", "shopee_model_id")\
                            .withColumn("is_top_30_model", lit(1))
        output = clean_df_pricing.join(top_30_model, on=["region", "shopee_item_id", "shopee_model_id"], how="left") \
                        .fillna({"is_top_30_model": 0}) \
                        .drop("_c0")
        
        logger.info("Registering Temp View to Internal Spark Session")
        # registering to temp view within spark session
        output.createOrReplaceTempView("local_price_competitive_by_sku")
        logger.warning("Please, check '03_Python script_Patcharanat.ipynb' to see the saved temp view.")

        # print output
        output.select("region", "shopee_item_id", "shopee_model_id", "is_top_30_model").show()

    except Exception as err:
        raise err

    finally:
        spark.stop()

if __name__ == '__main__':
    main()
