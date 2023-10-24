# import cProfile
# import pstats

""" Include containing folder for testing """
import sys, os
import datetime

sys.path.insert(0, os.path.abspath('../src'))
print(os.path.abspath('../src'), os.getppid(), os.getpid())
# ---------------------------------------

""" 
Test in local pycharm env:
    Uncomment the lines below to test in local machine.
    Otherwise keep them commented 
"""
# from ing.src import ing
# from ing.src import S3Access
# -----------------------------


""" 
Test in aws:
    Uncomment the lines below to test on aws sagemaker. 
    Otherwise keep them commented.
"""
import ing
# -----------------------------

# s3 specific libraries
import s3fs
import boto3
s3 = s3fs.S3FileSystem(anon=False)


"""
Usual module/package imports go below here
"""
import pandas as pd
import glob
import pprint

from elbow_function import get_elbow
# -----------------------------


# ------------------- COMMON -------------------
NEWS_DOMAIN_TO_CLASS_FILE = "s3://mips-main-2/UFTM_classification-v2/news_table-v3-UT60.csv"
news_domain_classes_df = pd.read_csv(NEWS_DOMAIN_TO_CLASS_FILE,
                                     usecols=['Domain', 'tufm_class', 'Language'])
news_domain_classes_df.rename(columns={'Domain': 'news_domain', 'tufm_class': 'class', 'Language': 'lang'},
                              inplace=True)

required_dirs = ["./OUTPUTS"]
for target_dir in required_dirs:
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

        
# ------------------- Scenario Metadata -------------------
scenario_metadata_paths = [f"s3://{dirpath}" for dirpath in s3.glob("s3://mips-phase-2/scenarios/*/metadata_*.csv")]
pprint.pprint(scenario_metadata_paths)

scenario_to_date = { os.path.basename(os.path.dirname(this_path)): datetime.datetime.strptime(pd.read_csv(this_path).iloc[0]["Date"] , "%Y-%m-%d").astimezone(datetime.timezone.utc) for this_path in scenario_metadata_paths }
pprint.pprint(scenario_to_date)

scenario_to_datafiles = { os.path.basename(os.path.dirname(this_path)): [f"s3://{s3path}" for s3path in s3.glob("{}/raw_brandwatch/*".format(os.path.dirname(this_path)))] for this_path in scenario_metadata_paths }
pprint.pprint(scenario_to_datafiles)


def calculate_te_for_scenario(in_scenario_name):
    print(in_scenario_name)

    START_DATE = scenario_to_date[in_scenario_name] - datetime.timedelta(14)
    END_DATE = scenario_to_date[in_scenario_name] + datetime.timedelta(35)

    FREQUENCY = '12H'
    MIN_PLAT_SIZE = 200
    WINDOW_SHIFT_BY_DAYS = 1
    INIT_WINDOW_SIZE_BY_DAYS = 1
    AS_GROWING = True

    paths = scenario_to_datafiles[in_scenario_name]
    print(paths)

    required_dirs = [f"./OUTPUTS/{in_scenario_name}", f"./OUTPUTS/{in_scenario_name}/dynamic", f"./OUTPUTS/{in_scenario_name}/dynamic/growing", f"./OUTPUTS/{in_scenario_name}/dynamic/moving"]
    for target_dir in required_dirs:
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

    # let data manager handle data
    data_manager = ing.DataManager(f"./OUTPUTS/{in_scenario_name}")
    data_manager.read_data_files(paths)
    
    if data_manager.all_osn_msgs_df.datetime.min() <= START_DATE and END_DATE <= data_manager.all_osn_msgs_df.datetime.max():
        print("Data found in the given range.")

        # preprocess
        data_manager.preprocess(news_domain_classes_df, START_DATE, END_DATE)

        # generate tables
        data_manager.generate_data_tables(MIN_PLAT_SIZE, None)

        print(f"Start: {START_DATE} \nEnd: {END_DATE}")
        print(f"Period length: {END_DATE - START_DATE}")
        
        print(data_manager.indv_actors_df["msgs_count"].value_counts().sort_index(ascending=False).cumsum())
        x_msgcount, y_numusers = data_manager.indv_actors_df["msgs_count"].value_counts().sort_index(ascending=False).cumsum().reset_index().values.T
        point = get_elbow(x_msgcount, y_numusers, True, f"./OUTPUTS/{in_scenario_name}/elbow_indvactors_vs_msgcount.png")
        min_msg_count = point.x
        
        print(f"Min msg count per actor: {min_msg_count}")
        print(f"Users with more than {min_msg_count} messsages posted\n",
              data_manager.all_users_df[data_manager.all_users_df["msgs_count"] >= min_msg_count].reset_index())

        print(f"Platform Actors with more than {MIN_PLAT_SIZE} users\n",
              data_manager.actors_df[(data_manager.actors_df["actor_type"] == "plat") &
                                     (data_manager.actors_df["num_users"] >= MIN_PLAT_SIZE)].reset_index())

        actor_id_list = data_manager.indv_actors_df[(data_manager.indv_actors_df["msgs_count"] >= min_msg_count)].index.to_list()
        print(f"Actors #: {len(actor_id_list)}")

        te_calculator = ing.TransferEntropyCalculator(data_manager, in_add_superclasses=False)

        te_calculator.calculate_te_network_series(actor_id_list, START_DATE, END_DATE, FREQUENCY, WINDOW_SHIFT_BY_DAYS, INIT_WINDOW_SIZE_BY_DAYS, AS_GROWING, data_manager.output_dir_path)
        
    else:
        print("Error: START_DATE and END_DATE are not within the data set.")


if __name__ == "__main__":
    
    for scenario_name in scenario_to_datafiles.keys():
        calculate_te_for_scenario(scenario_name)
            