from pyspark.sql import SparkSession
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
    logger = set_logger("Driver - Q1")

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

        # Q3.1
        ## --------------------------- Calculate Order Coverage
        logger.info("Calculating Order Coverage")
        # Aggregate `pricing_project_dataset` to Sum number of Order by Region
        agg_df_pricing = clean_df_pricing.groupBy(["region"]) \
                                        .agg(sum("shopee_order").alias("sum_shopee_order"))

        # Join with `platform_number` to find order coverage
        agg_df_pricing = agg_df_pricing.join(clean_df_platform, on="region", how="left")\
                                        .withColumn("order_coverage_by_region", format_number(col("sum_shopee_order")/col("platform_order"), 6)) \
                                        .select("region", "order_coverage_by_region")

        ## --------------------------- Calculate # of Item
        logger.info("Calculating Number of Item")
        # Aggregate `pricing_project_dataset` to Find number of Item by Region
        number_item_pricing = clean_df_pricing.groupBy(["region"]) \
                                            .agg(count("shopee_item_id").alias("number_of_item_by_region"))

        # Join count item with aggregated order coverage
        agg_df_pricing = agg_df_pricing.join(number_item_pricing, on="region", how="left")

        ## --------------------------- Calculate Net Competitiveness
        logger.info("Calculating net Competitiveness")
        # Intermediate step to find Net Competitiveness according to formula
        intermediate_net_comp = clean_df_pricing.groupBy(["region", "shopee_model_competitiveness_status"]) \
                                                .agg(count("shopee_model_id").alias("number_of_shopee_model_id"))

        # Another intermediate step to find Net Competitiveness according to formula
        win_comp = intermediate_net_comp.where(col("shopee_model_competitiveness_status") == "Shopee > CPT").withColumnRenamed("number_of_shopee_model_id", "number_of_larger_shopee_model")
        lose_comp = intermediate_net_comp.where(col("shopee_model_competitiveness_status") == "Shopee < CPT").withColumnRenamed("number_of_shopee_model_id", "number_of_lower_shopee_model")
        total_comp = clean_df_pricing.groupBy(["region"]).agg(count("shopee_model_id").alias("total_shopee_model"))

        # Join intermediate results and Apply formula
        final_net_comp = win_comp.join(lose_comp, on="region", how="left")\
                                    .join(total_comp, on="region", how="left")\
                                    .select("region", "number_of_larger_shopee_model", "number_of_lower_shopee_model", "total_shopee_model")\
                                    .withColumn("net_competitiveness", format_number((col("number_of_lower_shopee_model") - col("number_of_larger_shopee_model")) / col("total_shopee_model"), 6))\
                                    .select("region", "net_competitiveness")

        # ## --------------------------- Join Final
        final_net_comp = final_net_comp.join(agg_df_pricing, on="region", how="left")

        logger.info("Logging Result DataFrame")
        final_net_comp.select("region", "order_coverage_by_region", "net_competitiveness", "number_of_item_by_region").show()

    except Exception as err:
        raise err

    finally:
        spark.stop()

if __name__ == '__main__':
    main()
