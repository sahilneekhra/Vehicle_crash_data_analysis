from pyspark.sql import SparkSession
from src.main.utility.logger_config import logger

def spark_session():
    spark = SparkSession.builder.appName("Vehicle_crash_data_analysis_with_pyspark").getOrCreate()
    logger.info("spark session %s", spark)
    return spark