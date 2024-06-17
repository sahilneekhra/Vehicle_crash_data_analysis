from pyspark.sql.functions import *
from src.main.utility.logger_config import logger
from src.main.transformation.analysis import *


class DataProcessor:

    def __init__(self, spark, in_path: str, out_path: str):
        """
        Initialize DataProcessor with Spark session, input path, and output path.

        :param spark: Spark session object
        :param in_path: Input directory path for data files
        :param out_path: Output directory path for results
        """
        self.spark = spark
        self.in_path = in_path
        self.out_path = out_path

    def read_data(self, filename: str) -> DataFrame:
        """
        Read a CSV file into a DataFrame.

        :param filename: Name of the CSV file
        :return: DataFrame containing the data from the CSV file
        """
        logger.info(f"Reading data from {filename}")
        df = self.spark.read.csv(f"{self.in_path}/{filename}", header=True, inferSchema=True)
        logger.info(f"Finished reading data from {filename}")
        return df

    def write_data(self, data_to_save: DataFrame| str | int, filename: str):
        """
        Write a DataFrame to a CSV file.

        :param df: DataFrame to write
        :param filename: Name of the output CSV file
        """
        logger.info(f"Writing data to {filename}")
        if isinstance(data_to_save, DataFrame):
            data_to_save.write.csv(f"{self.out_path}/{filename}", header=True, mode="overwrite")
        else:
            with open(f"{self.out_path}/{filename}.csv","w") as file:
                file.write(str(data_to_save))
        logger.info(f"Finished writing data from {filename}")


    def analyszer_data(self):
        """
        Main method to perform all the data analyses and log the results.
        """
        logger.info("Inside Data Analyszer")
        # Reading necessary files
        df_primary_person_use = self.read_data("Primary_Person_use.csv")
        df_charges_use = self.read_data("Charges_use.csv")
        df_damages_use = self.read_data("Damages_use.csv")
        df_endorse_use = self.read_data("Endorse_use.csv")
        df_restrict_use = self.read_data("Restrict_use.csv")
        df_units_use = self.read_data("Units_use.csv")

        # Analysis -1 Find the number of crashes (accidents) in which number of males killed are greater than 2?
        logger.info("Checking Analysis 1")
        result1 = count_male_killed(df_primary_person_use)
        self.write_data(result1,'Analysis1')
        #print(result1)

        # Analysis -2: How many two wheelers are booked for crashes?
        logger.info("Checking Analysis 2")
        result2 = two_wheelers_booked(df_units_use)
        self.write_data(result2,'Analysis2')
        #print(result2)

        # Analysis -3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
        logger.info("Checking Analysis 3")
        result3 = driver_died_airbags_not_deployed(df_primary_person_use, df_units_use)
        self.write_data(result3,'Analysis3')
        #result3.show()

        # Analysis -4: Determine number of Vehicles with driver having valid licences involved in hit and run?
        logger.info("Checking Analysis 4")
        result4 = count_hit_and_run_with_valid_license(df_primary_person_use, df_units_use)
        self.write_data(result4,'Analysis4')
        #print(result4)

        # Analysis -5: Which state has highest number of accidents in which females are not involved?
        logger.info("Checking Analysis 5")
        result5 = state_with_most_accidents_not_involving_females(df_primary_person_use)
        self.write_data(result5, 'Analysis5')
        #print(result5)

        # Analysis -6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        logger.info("Checking Analysis 6")
        result6 = top_vehicle_makes_for_injuries(df_primary_person_use, df_units_use)
        self.write_data(result6, 'Analysis6')
        #result6.show()

        # Analysis -7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        logger.info("Checking Analysis 7")
        result7 = top_ethnic_group_per_body_style(df_primary_person_use, df_units_use)
        self.write_data(result7, 'Analysis7')
        #result7.show()

        # Analysis -8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        logger.info("Checking Analysis 8")
        result8 = top_zip_codes_with_alcohol_crashes(df_primary_person_use, df_units_use)
        self.write_data(result8, 'Analysis8')
        #result8.show()

        # Analysis -9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        logger.info("Checking Analysis 9")
        result9 = count_distinct_crashes_no_damage_insured(df_damages_use, df_units_use)
        self.write_data(result9, 'Analysis9')
        #print(result9)

        # Analysis -10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        logger.info("Checking Analysis 10")
        result10 = top_vehicle_makes_with_speeding_offences(df_primary_person_use, df_units_use, df_charges_use)
        self.write_data(result10, 'Analysis10')
        #result10.show()
