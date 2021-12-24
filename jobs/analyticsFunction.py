import traceback
import logging
from config import *
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from utils import *

logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
logger = logging.getLogger('driver_logger')
logger.setLevel(logging.DEBUG)


def number_of_accidents_male (df, output_path):
    ''' Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?'''
    try:
        logger.info("entered number_of_accidents_male function")
        df_number_of_accidents_male=df.filter((df.PRSN_GNDR_ID=="MALE")\
                                                                 & (df.PRSN_INJRY_SEV_ID=="KILLED")).count()
        columns=['description','count']
        data = [['number_of_male_killed',df_number_of_accidents_male]]
        number_of_accidents_male_df = spark.createDataFrame(data,columns)
        write_data(number_of_accidents_male_df, output_path)
    except Exception as e:
        traceback.print_exc()
        failure_reason=str('{}'.format(e))
        logger.info(failure_reason)



def two_wheelers_booked_crashes (df_primary_person_use,df_units_use, output_path):
    '''#Analysis 2: How many two wheelers are booked for crashes?'''
    try:
        logger.info("entered two_wheelers_booked_crashes function")
        df_two_wheeler_booked=  df_primary_person_use.alias("p").\
            join(df_units_use.alias("u"), ["CRASH_ID","UNIT_NBR"],"inner")\
            .filter(col("p.PRSN_HELMET_ID") != "NOT APPLICABLE")\
            .count()
        columns=['description','count']
        data=[['No_of_two_wheeler_booked_for_crashes',df_two_wheeler_booked]]
        df_two_wheelers_booked_crashes=spark.createDataFrame(data,columns)
        write_data(df_two_wheelers_booked_crashes, output_path)
    except Exception as e:
        traceback.print_exc()
        failure_reason=str('{}'.format(e))
        logger.info(failure_reason)

def state_with_highest_crashes_by_female (df_primary_person_use, output_path):
    '''Analysis 3: Which state has highest number of accidents in which females are involved?'''
    try:
        logger.info("entered state_with_highest_crashes_by_female function")
        df_max_no_of_accidents=df_primary_person_use.filter((df_primary_person_use.PRSN_GNDR_ID=="FEMALE"))\
            .groupBy(df_primary_person_use["DRVR_LIC_STATE_ID"]).agg(count("CRASH_ID").alias("no_of_accidents"))\
            .orderBy(desc("no_of_accidents")).first()
        columns=['DRVR_LIC_STATE_ID','no_of_accidents']
        df_state_with_highest_crashes_by_female=spark.createDataFrame([df_max_no_of_accidents])
        write_data(df_state_with_highest_crashes_by_female, output_path)
    except Exception as e:
        traceback.print_exc()
        failure_reason = str('{}'.format(e))
        logger.info(failure_reason)

def veh_make_ids_5th_to_15th (df_primary_person_use,df_units_use, output_path):
    '''Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death?'''
    try:
        logger.info("entered veh_make_ids_5th_to_15th function")
        df_injuries_model=df_units_use.join(df_primary_person_use,df_units_use.CRASH_ID==df_primary_person_use.CRASH_ID,'inner').\
            filter((df_primary_person_use.PRSN_INJRY_SEV_ID).contains('INJURY') \
                  | (df_primary_person_use.PRSN_INJRY_SEV_ID=='KILLED'))\
            .groupBy('VEH_MAKE_ID').agg(count(df_primary_person_use.CRASH_ID).alias("number_of_injuries_and_death"))

        df_get_rank = df_injuries_model.withColumn("row_number", row_number().over(Window.orderBy(desc("number_of_injuries_and_death"))))
        df_veh_make_ids_5th_to_15th=df_get_rank.filter(df_get_rank.row_number.between(5,15))\
        .select("VEH_MAKE_ID","number_of_injuries_and_death")
        write_data(df_veh_make_ids_5th_to_15th, output_path)
    except Exception as e:
        traceback.print_exc()
        failure_reason=str('{}'.format(e))
        logger.info(failure_reason)

def top_ethnic_user_group_of_unique_body_style (df_units_use,df_primary_person_use, output_path):
    '''Analysis 5:For all the body styles involved in crashes, mention the top ethnic user group of each unique body style'''
    try:
        logger.info("entered top_ethnic_user_group_of_unique_body_style function")
        df_ethnic_group=df_units_use.join(df_primary_person_use,df_units_use.CRASH_ID==df_primary_person_use.CRASH_ID,'inner').\
            groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').count()
        df_final_rank=df_ethnic_group.withColumn("row_number", row_number().over(Window.partitionBy("VEH_BODY_STYL_ID")\
                                                                                 .orderBy(desc("count"))))
        df_top_ethnic_user_group_of_unique_body_style=df_final_rank.filter(df_final_rank.row_number==1)\
            .select(df_final_rank.VEH_BODY_STYL_ID,df_final_rank.PRSN_ETHNICITY_ID)
        write_data(df_top_ethnic_user_group_of_unique_body_style, output_path)
    except Exception as e:
        traceback.print_exc()
        failure_reason=str('{}'.format(e))
        logger.info(failure_reason)

def highest_no_of_crashes_with_alcohols (df_units_use,df_primary_person_use, output_path):
     '''Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)'''
     try:
        logger.info("entered highest_no_of_crashes_with_alcohols function")
        df_highest_no_of_crashes_with_alcohols=df_units_use.alias('u').join(df_primary_person_use.alias('p'),\
                                                                            df_units_use.CRASH_ID==df_primary_person_use.CRASH_ID,'inner').\
            filter((col("p.PRSN_HELMET_ID") == "NOT APPLICABLE") & (col("p.PRSN_ALC_RSLT_ID") == "Positive")\
                   & (col("u.VEH_BODY_STYL_ID").contains('CAR')))\
            .groupBy('DRVR_ZIP').agg(count("u.CRASH_ID").alias("no_of_accidents")).fillna('UNKNOWN').\
            withColumn("row_number", row_number().over(Window.orderBy(desc("no_of_accidents"))))\
            .filter(col("row_number").between(1,5)).select('DRVR_ZIP','no_of_accidents')
        write_data(df_highest_no_of_crashes_with_alcohols, output_path)
     except Exception as e:
        traceback.print_exc()
        failure_reason=str('{}'.format(e))
        logger.info(failure_reason)

def distinct_veh_no_prop_damage (df_units_use,df_damage_use, output_path):
    '''Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance'''
    try:
        logger.info("entered distinct_veh_no_prop_damage function")
        df_distinct_veh_no_prop_damage=df_units_use.join(df_damage_use,df_units_use.CRASH_ID==df_damage_use.CRASH_ID,'left_anti').\
            filter((df_units_use.VEH_DMAG_SCL_1_ID != 'NA') &(df_units_use.FIN_RESP_TYPE_ID.contains("INSURANCE"))\
                   & ((df_units_use.VEH_DMAG_SCL_1_ID.isin("DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"))\
                   | (df_units_use.VEH_DMAG_SCL_2_ID.isin("DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST")))).\
            select(countDistinct(df_units_use.CRASH_ID).alias("distinct_crash_id_with_noDamage"))
        write_data(df_distinct_veh_no_prop_damage, output_path)
    except Exception as e:
        traceback.print_exc()
        failure_reason=str('{}'.format(e))
        logger.info(failure_reason)

def top_5_vehicle_drivers_with_speed (df_units_use,df_charge_use, df_primary_person_use,output_path):
    '''Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)'''
    try:
        logger.info("entered top_5_vehicle_drivers_with_speed function")
        df_top25_States_WithMax_Offenses=df_units_use.join(df_charge_use,["CRASH_ID","UNIT_NBR"],'inner').\
        filter(df_units_use.VEH_LIC_STATE_ID != 'NA').groupBy("VEH_LIC_STATE_ID").agg(count("CHARGE").alias("offenses_count")).\
        withColumn("top25",dense_rank().over(Window.orderBy(desc("offenses_count")))).\
        filter(col("top25").between(1,25)).\
        select('VEH_LIC_STATE_ID','offenses_count')


        df_top10_vehicle_colors=df_units_use.join\
        (df_top25_States_WithMax_Offenses,df_units_use.VEH_LIC_STATE_ID==df_top25_States_WithMax_Offenses.VEH_LIC_STATE_ID,'inner').\
        groupBy(df_units_use.VEH_COLOR_ID).agg(count(df_units_use.VEH_COLOR_ID).alias("vehicleColor")).\
        withColumn("top10VehicleColor", dense_rank().over(Window.orderBy(desc("vehicleColor")))).\
        filter(col("top10VehicleColor").between(1, 10)).\
        select(df_units_use.VEH_COLOR_ID)

        df_top5_vehicle_makers_intermediate_result=df_units_use.alias("u").join \
        (df_top25_States_WithMax_Offenses.alias("t25"),col("u.VEH_LIC_STATE_ID") == col("t25.VEH_LIC_STATE_ID"),'inner').\
        join(df_top10_vehicle_colors.alias("t10"),col("u.VEH_COLOR_ID")==col("t10.VEH_COLOR_ID"),'inner').\
        select(col("u.CRASH_ID"),col("u.UNIT_NBR"),col("u.VEH_LIC_STATE_ID"),col("u.VEH_COLOR_ID"),col("u.VEH_MAKE_ID"))

        df_top_vehicle_makers_result=df_primary_person_use.alias("p").\
        filter(col("DRVR_LIC_TYPE_ID") == 'DRIVER LICENSE').\
        join(df_charge_use.alias("c"),["CRASH_ID","UNIT_NBR","PRSN_NBR"],"inner").\
        filter(col("c.CHARGE").contains("SPEED"))\
        .join(df_top5_vehicle_makers_intermediate_result.alias("t5interim"), ["CRASH_ID", "UNIT_NBR"], "inner")\
        .select("p.CRASH_ID", "p.UNIT_NBR", "c.CHARGE","t5interim.VEH_LIC_STATE_ID", "t5interim.VEH_COLOR_ID","t5interim.VEH_MAKE_ID")\
        .groupBy("t5interim.VEH_MAKE_ID")\
        .agg(count("t5interim.VEH_MAKE_ID").alias("top5VehicleMakes"))\
        .orderBy(desc("top5VehicleMakes")).select(col("VEH_MAKE_ID")).take(5)
        columns = ["VEH_MAKE_ID"]

        df_top_5_vehicle_drivers_with_speed=spark.createDataFrame(df_top_vehicle_makers_result,columns)
        write_data(df_top_5_vehicle_drivers_with_speed, output_path)
    except Exception as e:
        traceback.print_exc()
        failure_reason=str('{}'.format(e))
        logger.info(failure_reason)

if __name__ == "__main__":
    '''starting the carCrash Analytics'''
    # Creating spark session
    try:
        logger.info("Spark carCrash analysis started")
        spark = SparkSession.builder\
            .master(is_local_mode)\
            .appName("carCrash").getOrCreate()
        # reading all the data
        df_charge_use = read_data(spark,charge_use_in)
        df_primary_person_use = read_data(spark, primary_person_use_in)
        df_damage_use = read_data(spark, damages_use_in)
        df_endorse_use = read_data(spark, endorse_use_in)
        df_restrict_use = read_data(spark, restrict_use_in)
        df_units_use = read_data(spark, units_use_in)


        # calling analytical transformation functions
        number_of_accidents_male(df_primary_person_use, number_of_accidents_male_out)
        two_wheelers_booked_crashes(df_primary_person_use,df_units_use, two_wheelers_booked_crashes_out)
        state_with_highest_crashes_by_female(df_primary_person_use, state_with_highest_crashes_by_female_out)
        veh_make_ids_5th_to_15th(df_primary_person_use,df_units_use,veh_make_ids_5th_to_15th_out)
        top_ethnic_user_group_of_unique_body_style(df_units_use,df_primary_person_use,top_ethnic_user_group_of_unique_body_style_out)
        highest_no_of_crashes_with_alcohols(df_units_use,df_primary_person_use,highest_no_of_crashes_with_alcohols_out)
        distinct_veh_no_prop_damage(df_units_use, df_damage_use, distinct_veh_no_prop_damage_out)
        top_5_vehicle_drivers_with_speed(df_units_use, df_charge_use, df_primary_person_use, top_5_vehicle_drivers_with_speed_out)

        #closing the sparksession after job completion
        spark.stop()
    except Exception as e:
        traceback.print_exc()
        failure_reason = str('{}'.format(e))
        logger.error("car_crash!-",e)
        #logger.info(failure_reason)
        spark.stop()
        sys.exit("{Error}: "+failure_reason)

