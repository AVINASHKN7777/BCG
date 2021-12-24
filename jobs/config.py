
import sys
import argparse

def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--charge_use')
    parser.add_argument('--person_use')
    parser.add_argument('--units_use')
    parser.add_argument('--damages_use')
    parser.add_argument('--endorse_use')
    parser.add_argument('--restrict_use')

    parser.add_argument('--number_of_accidents_path')
    parser.add_argument('--two_wheelers_booked_path')
    parser.add_argument('--state_crash_path')
    parser.add_argument('--veh_make_ids_path')
    parser.add_argument('--top_ethnic_user_path')
    parser.add_argument('--highest_no_of_crashes_path')
    parser.add_argument('--distinct_veh_path')
    parser.add_argument('--top_5_vehicle_path')
    return parser.parse_args(argv)

#Input file location
# print("args_length ", sys.argv)
is_local_mode = 'cluster' if len(sys.argv) > 1 else 'local'
job_args = _parse_args(sys.argv[1:])
charge_use_in = job_args.charge_use if is_local_mode != 'local' else "test/input/chargesUse/data.csv"
primary_person_use_in = job_args.person_use if is_local_mode != 'local' else "test/input/primaryPersonUse/data.csv"
units_use_in = job_args.units_use if is_local_mode != 'local' else "test/input/unitsUse/data.csv"
damages_use_in = job_args.damages_use if is_local_mode != 'local' else "test/input/damagesUse/data.csv"
endorse_use_in = job_args.endorse_use if is_local_mode != 'local' else "test/input/endorseUse/data.csv"
restrict_use_in = job_args.restrict_use if is_local_mode != 'local' else "test/input/restrictUse/data.csv"
# charge_use_in = "test/input/chargesUse/data.csv"

#analysis Output location
number_of_accidents_male_out = job_args.number_of_accidents_path if is_local_mode != 'local' else  "test/output/number_of_accidents_male/"
two_wheelers_booked_crashes_out = job_args.two_wheelers_booked_path if is_local_mode != 'local' else "test/output/two_wheelers_booked_crashes/"
state_with_highest_crashes_by_female_out = job_args.state_crash_path if is_local_mode != 'local' else "test/output/state_with_highest_crashes_by_female/"
veh_make_ids_5th_to_15th_out = job_args.veh_make_ids_path if is_local_mode != 'local' else "test/output/veh_make_ids_5th_to_15th/"
top_ethnic_user_group_of_unique_body_style_out = job_args.top_ethnic_user_path if is_local_mode != 'local' else "test/output/top_ethnic_user_group_of_unique_body_style/"
highest_no_of_crashes_with_alcohols_out = job_args.highest_no_of_crashes_path if is_local_mode != 'local' else "test/output/highest_no_of_crashes_with_alcohols/"
distinct_veh_no_prop_damage_out = job_args.distinct_veh_path if is_local_mode != 'local' else "test/output/distinct_veh_no_prop_damage/"
top_5_vehicle_drivers_with_speed_out = job_args.top_5_vehicle_path if is_local_mode != 'local' else "test/output/top_5_vehicle_drivers_with_speed/"
