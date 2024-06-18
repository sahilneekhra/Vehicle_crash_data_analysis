from pyspark.sql.functions import *
from src.main.utility.logger_config import logger
from pyspark.sql.window import Window


def count_male_killed(df_primary_person_use: DataFrame) -> int:
    """
    Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?

    :param df_primary_person_use: DataFrame containing primary person data
    :return: Number of crashes with more than 2 males killed
    """
    logger.info("Starting analysis: count_male_killed")
    male_filter = df_primary_person_use['PRSN_GNDR_ID'] == 'MALE'
    kill_filter = df_primary_person_use['PRSN_INJRY_SEV_ID'] == 'KILLED'

    df_fatalities = df_primary_person_use.filter(male_filter & kill_filter) \
        .groupBy('CRASH_ID') \
        .agg(sum('DEATH_CNT').alias('TOTAL_FATALITIES')) \
        .filter(col('TOTAL_FATALITIES') > 2)
    logger.info("Completed analysis: count_male_killed")
    return df_fatalities.count()


def two_wheelers_booked(df_units_use: DataFrame) -> int:
    """
    Analysis 2: How many two wheelers are booked for crashes?

    :param df_units_use: DataFrame containing unit data
    :return: Number of two-wheelers booked for crashes
    """
    logger.info("Starting analysis: two_wheeler_filter")
    two_wheeler_filter = df_units_use['VEH_BODY_STYL_ID'].contains("MOTORCYCLE")

    df_two_wheelers = df_units_use.filter(two_wheeler_filter)
    logger.info("Completed analysis: two_wheelers_booked")
    return df_two_wheelers.count()


def driver_died_airbags_not_deployed(df_primary_person_use: DataFrame, df_units_use: DataFrame) -> DataFrame:
    """
    Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

    :param df_primary_person_use: DataFrame containing primary person data
    :param df_units_use: DataFrame containing unit data
    :return: DataFrame with the top 5 vehicle makes
    """
    logger.info("Starting analysis: driver_died_airbags_not_deployed")
    airbag_filter = df_primary_person_use['PRSN_AIRBAG_ID'] == 'NOT DEPLOYED'
    person_died_filter = df_primary_person_use['PRSN_INJRY_SEV_ID'] == 'KILLED'
    driver_filter = df_primary_person_use['PRSN_TYPE_ID'] == 'DRIVER'
    cars_list = ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR', 'VAN',
                 'POLICE CAR/TRUCK', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE']

    df_list_vehicle_crashed = df_primary_person_use.filter(airbag_filter & person_died_filter & driver_filter) \
        .select('CRASH_ID').distinct()
    df_cars = df_units_use.filter(df_units_use['VEH_BODY_STYL_ID'].isin(cars_list))
    df_result = df_list_vehicle_crashed.join(df_cars, "CRASH_ID", 'inner') \
        .groupBy("VEH_MAKE_ID") \
        .agg(count("CRASH_ID").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(5)
    logger.info("Completed analysis: driver_died_airbags_not_deployed")
    return df_result.select(col("VEH_MAKE_ID"))


def count_hit_and_run_with_valid_license(df_primary_person_use: DataFrame, df_units_use: DataFrame) -> int:
    """
    Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?

    :param df_primary_person_use: DataFrame containing primary person data
    :param df_units_use: DataFrame containing unit data
    :return: Number of vehicles involved in hit and run with valid licenses
    """
    logger.info("Starting analysis: count_hit_and_run_with_valid_license")
    hit_and_run_filter = df_units_use['VEH_HNR_FL'] == 'Y'
    valid_license_filter = df_primary_person_use['DRVR_LIC_TYPE_ID'].isin(
        ['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.']) & \
                           ~df_primary_person_use['DRVR_LIC_CLS_ID'].isin(['UNKNOWN', 'UNLICENSED'])

    df_valid_licensed_drivers = df_primary_person_use.filter(valid_license_filter).select(
        col("CRASH_ID")).distinct()
    df_hit_and_run = df_units_use.filter(hit_and_run_filter)
    result = df_hit_and_run.join(df_valid_licensed_drivers, "CRASH_ID", "inner").count()
    logger.info("Completed analysis: count_hit_and_run_with_valid_license")
    return result


def state_with_most_accidents_not_involving_females(df_primary_person_use: DataFrame) -> str:
    """
    Analysis 5: Which state has highest number of accidents in which females are not involved?

    :param df_primary_person_use: DataFrame containing primary person data
    :return: State with the highest number of accidents not involving females
    """
    logger.info("Starting analysis: state_with_most_accidents_not_involving_females")
    not_female_filter = df_primary_person_use['PRSN_GNDR_ID'] != 'FEMALE'

    df_accidents_by_state = df_primary_person_use.filter(not_female_filter) \
        .groupBy('DRVR_LIC_STATE_ID') \
        .agg(count("CRASH_ID").alias("ACC_COUNT")) \
        .orderBy(col("ACC_COUNT").desc()) \
        .limit(1)
    result = df_accidents_by_state.collect()[0]['DRVR_LIC_STATE_ID']
    logger.info("Completed analysis: state_with_most_accidents_not_involving_females")
    return result


def top_vehicle_makes_for_injuries(df_primary_person_use: DataFrame, df_units_use: DataFrame) -> DataFrame:
    """
    Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death

    :param df_primary_person_use: DataFrame containing primary person data
    :param df_units_use: DataFrame containing unit data
    :return: DataFrame with the top 3rd to 5th VEH_MAKE_IDs
    """
    logger.info("Starting analysis: top_vehicle_makes_for_injuries")
    non_injured_person_filter = df_primary_person_use['PRSN_INJRY_SEV_ID'].isin(['NOT INJURED', 'UNKNOWN'])

    df_injured_person = df_primary_person_use.filter(~non_injured_person_filter).select("CRASH_ID").distinct()
    df_vehicles_involved = df_units_use.join(df_injured_person, "CRASH_ID", "inner")

    df_vehicle_injuries = df_vehicles_involved.na.drop().groupBy("VEH_MAKE_ID").agg(
        count("CRASH_ID").alias("count_crash"))

    window_spec = Window.orderBy(col("count_crash").desc())

    df_result = df_vehicle_injuries.withColumn("rowNum_Vehicle", row_number().over(window_spec)) \
        .filter(col("rowNum_Vehicle").between(3, 5))
    logger.info("Completed analysis: top_vehicle_makes_for_injuries")
    return df_result.select(col("VEH_MAKE_ID"))


def top_ethnic_group_per_body_style( df_primary_person_use: DataFrame, df_units_use: DataFrame) -> DataFrame:
    """
    Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

    :param df_primary_person_use: DataFrame containing primary person data
    :param df_units_use: DataFrame containing unit data
    :return: DataFrame with the top ethnic group for each body style
    """
    logger.info("Starting analysis: top_ethnic_group_per_body_style")
    df_joined_after_select = df_primary_person_use.select('PRSN_ETHNICITY_ID', 'CRASH_ID') \
        .join(df_units_use.select('VEH_BODY_STYL_ID', 'CRASH_ID'), "CRASH_ID", "inner")
    df_ethenic_group_count = df_joined_after_select.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID') \
        .agg(count("PRSN_ETHNICITY_ID").alias("perGroupCount"))
    window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('perGroupCount').desc())
    df_result = df_ethenic_group_count.withColumn("perGroupRank", rank().over(window_spec)) \
        .filter(col('perGroupRank') == 1)
    logger.info("Completed analysis: top_ethnic_group_per_body_style")
    return df_result.drop('perGroupRank', 'perGroupCount', 'CRASH_ID')


def top_zip_codes_with_alcohol_crashes(df_primary_person_use: DataFrame,
                                       df_units_use: DataFrame) -> DataFrame:
    """
    Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

    :param df_primary_person_use: DataFrame containing primary person data
    :param df_units_use: DataFrame containing unit data
    :return: DataFrame with the top 5 zip codes
    """
    logger.info("Starting analysis: top_zip_codes_with_alcohol_crashes")
    cars_list = ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR', 'VAN',
                 'POLICE CAR/TRUCK', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE']
    df_alcohol_involved = df_primary_person_use.filter(
        (col('PRSN_ALC_RSLT_ID') == 'Positive') & (col('DRVR_ZIP').isNotNull()))
    df_merged_unit_primary = df_alcohol_involved.join(df_units_use, "CRASH_ID", "inner")

    df_zipcode_crashId = df_merged_unit_primary.filter(df_merged_unit_primary['VEH_BODY_STYL_ID'].isin(cars_list)) \
        .select(['CRASH_ID', 'DRVR_ZIP'])

    df_result = df_zipcode_crashId.groupBy('DRVR_ZIP') \
        .agg(count("CRASH_ID").alias("crash_count")) \
        .orderBy(col("crash_count").desc()) \
        .limit(5)
    logger.info("Completed analysis: top_zip_codes_with_alcohol_crashes")
    return df_result.select(col("DRVR_ZIP"))


def count_distinct_crashes_no_damage_insured(df_damages_use: DataFrame, df_units_use: DataFrame) -> int:
    """
    Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

    :param df_damages_use: DataFrame containing damage data
    :param df_units_use: DataFrame containing unit data
    :return: Count of distinct crash IDs
    """
    logger.info("Starting analysis: count_distinct_crashes_no_damage_insured")
    df_damaged_property = df_damages_use.filter(col('DAMAGED_PROPERTY').isNotNull()).select("CRASH_ID").distinct()

    df_damage_range = df_units_use.select('FIN_RESP_TYPE_ID', 'CRASH_ID', 'VEH_DMAG_SCL_1_ID',
                                          'VEH_DMAG_SCL_2_ID') \
        .filter(df_units_use['FIN_RESP_TYPE_ID'].contains('INSURANCE')) \
        .withColumn("DAMAGE_RANGE1", regexp_extract(col('VEH_DMAG_SCL_1_ID'), "\\d+", 0)) \
        .withColumn("DAMAGE_RANGE2", regexp_extract(col('VEH_DMAG_SCL_2_ID'), "\\d+", 0))

    df_acceptable_damage = df_damage_range.filter(
        (df_damage_range['DAMAGE_RANGE1'] > 4) | (df_damage_range['DAMAGE_RANGE2'] > 4)) \
        .select("CRASH_ID").join(df_damaged_property, "CRASH_ID", "left_Anti")
    result = df_acceptable_damage.select('CRASH_ID').distinct().count()
    logger.info("Completed analysis: count_distinct_crashes_no_damage_insured")
    return result


def top_vehicle_makes_with_speeding_offences(df_primary_person_use: DataFrame, df_units_use: DataFrame,
                                             df_charges_use: DataFrame) -> DataFrame:
    """
    Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,used
    top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences(to be deduced from the data)

    :param df_primary_person_use: DataFrame containing primary person data
    :param df_units_use: DataFrame containing unit data
    :param df_charges_use: DataFrame containing charges data
    :return: DataFrame with the top 5 vehicle makes
    """
    logger.info("Starting analysis: top_vehicle_makes_with_speeding_offences")
    # 1: Get the Top 25 states with the highest number of offences
    df_offences_by_state = df_primary_person_use.groupBy("DRVR_LIC_STATE_ID").agg(count("*").alias("offence_count"))
    top_25_states = [row["DRVR_LIC_STATE_ID"] for row in
                     df_offences_by_state.orderBy(col("offence_count").desc()).limit(25).collect()]

    # 2: Get the top 10 most common vehicle colors involved in crashes
    df_colors = df_units_use.groupBy("VEH_COLOR_ID").agg(count("*").alias("color_count"))
    top_10_colors = [row["VEH_COLOR_ID"] for row in
                     df_colors.orderBy(col("color_count").desc()).limit(10).collect()]

    # 3: Filter licensed drivers and those who got charged with speeding-related offences

    speeding_offences = df_charges_use.filter(col("CHARGE").contains("SPEED"))
    licensed_drivers = df_primary_person_use.filter(
        col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]) &
        ~col("DRVR_LIC_CLS_ID").isin(["UNKNOWN", "UNLICENSED"])
    )
    # 4: Joining all dataframes
    df_joined_filtered = df_units_use.join(speeding_offences, "CRASH_ID", "inner") \
        .join(licensed_drivers, "CRASH_ID", "inner") \
        .filter((col("DRVR_LIC_STATE_ID").isin(top_25_states)) & (col("VEH_COLOR_ID").isin(top_10_colors)))

    top_vehicle_makes = df_joined_filtered.groupBy("VEH_MAKE_ID") \
        .agg(count("CRASH_ID").alias("crash_count")) \
        .orderBy(col("crash_count").desc()) \
        .limit(5)
    logger.info("Completed analysis: top_vehicle_makes_with_speeding_offences")
    return top_vehicle_makes.select(col("VEH_MAKE_ID"))
