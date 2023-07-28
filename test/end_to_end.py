# import cProfile
# import pstats

""" Include containing folder for testing """
import sys, os
import datetime

sys.path.insert(0, os.path.abspath('../src'))
print(os.path.abspath('../src'))
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
# -----------------------------

# DATA_DIRECTORY_LIST = ["s3://mips-main/initial_data_collection/raw_data/brandwatch/", "s3://mips-main/initial_data_collection/TE_ready_data/V2/"]  # DONE
DATA_DIRECTORY_LIST = ["s3://mips-phase-2/initial_query_data/raw_data/brandwatch/"]
#DATA_DIRECTORY_LIST = [ "C:/STUFF/RESEARCH/Brandwatch/mariupol_hospital_clean", "C:/STUFF/RESEARCH/smyrna/Smyrna/data_collection/test_ing"]
# DATA_DIRECTORY_LIST = ["C:/STUFF/RESEARCH/smyrna/Smyrna/data_collection/test_ing/indv"]

NEWS_DOMAIN_TO_CLASS_FILE = "s3://mips-main-2/UFTM_classification-v2/news_table-v3-UT60.csv"
# NEWS_DOMAIN_TO_CLASS_FILE = "C:/STUFF/RESEARCH/Brandwatch/news_table-v3-UT60.csv"

START_DATE = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
END_DATE = datetime.datetime(2022, 5, 1, tzinfo=datetime.timezone.utc)
FREQUENCY = '6H'
MIN_PLAT_SIZE = 500
MIN_ACTIVITY_PER_MONTH = 30
MIN_TOTAL_RECEIVED_RETWEETS = 15
WINDOW_SHIFT_BY_DAYS = 7
INIT_WINDOW_SIZE_BY_DAYS = 7
AS_GROWING = True

paths = []
for data_dir in DATA_DIRECTORY_LIST:
    paths += s3.glob(os.path.join(data_dir, "*.csv*"))
paths = [f"s3://{p}" for p in paths]
print(paths)

news_domain_classes_df = pd.read_csv(NEWS_DOMAIN_TO_CLASS_FILE,
                                     usecols=['Domain', 'tufm_class', 'Language'])
news_domain_classes_df.rename(columns={'Domain': 'news_domain', 'tufm_class': 'class', 'Language': 'lang'},
                              inplace=True)

if __name__ == "__main__":

    required_dirs = ["./OUTPUTS", "./OUTPUTS/dynamic", "./OUTPUTS/dynamic/growing", "./OUTPUTS/dynamic/moving"]
    for target_dir in required_dirs:
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

    # let data manager handle data
    data_manager = ing.DataManager("./OUTPUTS")
    data_manager.read_data_files(paths)
    # data_manager.all_osn_msgs_df

    # preprocess
    data_manager.preprocess(news_domain_classes_df, START_DATE, END_DATE)
    # data_manager.all_osn_msgs_df


    # generate tables
    min_msg_count = MIN_ACTIVITY_PER_MONTH * (END_DATE - START_DATE).days // 30

    print(f"minimum message count : {min_msg_count}")

    data_manager.generate_data_tables(MIN_PLAT_SIZE, min_msg_count)

    print(f"Start: {START_DATE} \nEnd: {END_DATE}")
    print(f"Period length: {END_DATE - START_DATE}")
    print(f"Expected actor min messages per month: {MIN_ACTIVITY_PER_MONTH}")
    print(f"Min msg count: {min_msg_count}")
    print(f"Users with more than {min_msg_count} messsages posted\n",
          data_manager.all_users_df[data_manager.all_users_df["msgs_count"] > min_msg_count].reset_index())

    print(f"Platform Actors with more than {MIN_PLAT_SIZE} users\n",
          data_manager.actors_df[(data_manager.actors_df["actor_type"] == "plat") &
                                 (data_manager.actors_df["num_users"] > MIN_PLAT_SIZE)].reset_index())

    # actor_id_list = data_manager.indv_actors_df.index[:]
    
    actor_id_list = data_manager.indv_actors_df[(data_manager.indv_actors_df["msgs_count"] > min_msg_count) & (data_manager.indv_actors_df["received_share_count"] > MIN_TOTAL_RECEIVED_RETWEETS)].index.to_list()
    print(f"Actors #: {len(actor_id_list)}")

    data_windows_df = ing.TransferEntropyCalculator.calculate_a_date_series(START_DATE, END_DATE, WINDOW_SHIFT_BY_DAYS, INIT_WINDOW_SIZE_BY_DAYS, AS_GROWING)

    for idx, row in data_windows_df.iterrows():
        # profiler_obj = cProfile.Profile()
        # profiler_obj.enable()
        print("calculating te...")
        te_calculator = ing.TransferEntropyCalculator(data_manager, in_add_superclasses=False)
        current_start_date = row["start_date"]
        current_end_date = row["end_date"]
        actor_timeseries_dict_list = te_calculator.calculate_te_network_step1(actor_id_list, current_start_date, current_end_date, FREQUENCY)
        print(actor_timeseries_dict_list)
        te_df = te_calculator.calculate_te_network_step2(actor_id_list, actor_timeseries_dict_list)
        # profiler_obj.disable()
        print("all done!")
        file_name = "actor_te_edges_df_{}_{}".format(str(current_start_date).replace("-", "_"), str(current_end_date).replace("-", "_"))
        folder_type = "growing" if AS_GROWING else "moving"
        compression_options = dict(method='zip', archive_name=f'{file_name}.csv')
        te_df.to_csv(f"./OUTPUTS/dynamic/{folder_type}/{file_name}.csv.zip", index=False, compression=compression_options)
        print("saved")

    # stats = pstats.Stats(profiler_obj).strip_dirs().sort_stats("cumtime")
    # stats.print_stats(50)  # top 10 rows


#
# if __name__ == "__main__":
#
#     # Read input files
#     paths = []
#     for data_dir in DATA_DIRECTORY_LIST:
#         paths += s3.glob(os.path.join(data_dir, "*.csv*"))
#     paths = [f"s3://{p}" for p in paths]
#     print(paths)
#
#     news_domain_classes_df = pd.read_csv(NEWS_DOMAIN_TO_CLASS_FILE,
#                                          usecols=['Domain', 'tufm_class', 'Language'])
#     news_domain_classes_df.rename(columns={'Domain': 'news_domain', 'tufm_class': 'class', 'Language': 'lang'},
#                                   inplace=True)
#
#     # let data manager handle data
#     data_manager = ing.DataManager("./OUTPUTS")
#     data_manager.read_data_files(paths)
#
#     # preprocess
#     data_manager.preprocess(news_domain_classes_df, START_DATE, END_DATE)
#
#     # generate tables
#     min_msg_count = MIN_ACTIVITY_PER_MONTH * (END_DATE - START_DATE).days // 30
#     print(f"minimum message count : {min_msg_count}")
#     data_manager.generate_data_tables(MIN_PLAT_SIZE, min_msg_count)
#
#     print("calculating te...")
#     te_calculator = ing.TransferEntropyCalculator(data_manager)
#     current_start_date = START_DATE
#     current_end_date = END_DATE
#     te_df = te_calculator.calculate_te_network(current_start_date, current_end_date, FREQUENCY)
#
#     print("all done!")
#
#     compression_options = dict(method='zip', archive_name='actor_te_edges_df.csv')
#     te_df.to_csv("actor_te_edges_df.csv.zip", index=False, compression=compression_options)
#
