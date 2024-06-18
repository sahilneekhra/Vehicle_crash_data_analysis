from src.main.data_processor import *
from src.main.utility.spark_session import spark_session
from src.main.utility.logger_config import logger
from resources import config


def main():
    # Initialize Spark session
    spark=spark_session()

    # Load configuration
    input_path = config.input_path
    output_path = config.output_path

    # Process data
    processor = DataProcessor(spark, input_path, output_path)

    logger.info("before Calling Data Analyszer")
    processor.analyszer_data()

    # Stopping Spark session
    spark.stop()


if __name__ == "__main__":
    main()
