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

from elbow_function import get_elbow
# -----------------------------


# ------------------- COMMON -------------------
NEWS_DOMAIN_TO_CLASS_FILE = "s3://mips-main-2/UFTM_classification-v2/news_table-v3-UT60.csv"
news_domain_classes_df = pd.read_csv(NEWS_DOMAIN_TO_CLASS_FILE,
                                     usecols=['Domain', 'tufm_class', 'Language'])
news_domain_classes_df.rename(columns={'Domain': 'news_domain', 'tufm_class': 'class', 'Language': 'lang'},
                              inplace=True)

required_dirs = ["./OUTPUTS", "./OUTPUTS/datasets"]
for target_dir in required_dirs:
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
        
OUTPUT_DIR = "./OUTPUTS/datasets"

def calculate_te_for_dataset(in_dataset_name,
                             in_data_file_path_list,
                             in_start_datetime,
                             in_end_datetime,
                             in_frequency = '12H',
                             in_min_plat_size = 200,
                             in_min_msg_count = None,
                             in_window_shift_by_days = 1,
                             in_init_window_shift_by_days = 1):
    
    if not in_start_datetime < in_end_datetime:
        print(f"Error: Start date ({in_start_datetime}) is not less than End date ({in_end_datetime}) !")
        return None
    
    dataset_output_dir = f"{OUTPUT_DIR}/{in_dataset_name}"
    required_dirs = [f"{dataset_output_dir}", 
                     f"{dataset_output_dir}/dynamic", 
                     f"{dataset_output_dir}/dynamic/growing", 
                     f"{dataset_output_dir}/dynamic/moving"]
    for target_dir in required_dirs:
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
            
    # let data manager handle data
    data_manager = ing.DataManager(dataset_output_dir)
    data_manager.read_data_files(in_data_file_path_list)
        
    if in_end_datetime <= data_manager.all_osn_msgs_df.datetime.min() or data_manager.all_osn_msgs_df.datetime.max() <= in_start_datetime:
        print("Error: The provided date range ({} to {}) is not within the range of the dataset ({} to {})!".format(in_start_datetime, 
                                                                                                                     in_end_datetime, 
                                                                                                                     data_manager.all_osn_msgs_df.datetime.min(),
                                                                                                                     data_manager.all_osn_msgs_df.datetime.max()))
        return None
    
    # preprocess
    data_manager.preprocess(news_domain_classes_df, in_start_datetime, in_end_datetime)

    # generate tables
    data_manager.generate_data_tables(in_min_plat_size, in_min_msg_count)
    
    print(f"Start: {in_start_datetime} \nEnd: {in_end_datetime}")
    print(f"Period length: {in_end_datetime - in_start_datetime}")
        
    min_msg_count = in_min_msg_count
    if in_min_msg_count is None:
        print(data_manager.indv_actors_df["msgs_count"].value_counts().sort_index(ascending=False).cumsum())
        x_msgcount, y_numusers = data_manager.indv_actors_df["msgs_count"].value_counts().sort_index(ascending=False).cumsum().reset_index().values.T
        point = get_elbow(x_msgcount, y_numusers, True, f"{dataset_output_dir}/elbow_indvactors_vs_msgcount.png")
        min_msg_count = point.x
    
    print(f"Min msg count per actor: {min_msg_count}")
    print(f"Users with more than {min_msg_count} messsages posted\n", 
          data_manager.all_users_df[data_manager.all_users_df["msgs_count"] >= min_msg_count].reset_index())
    
    print(f"Platform Actors with more than {in_min_plat_size} users\n", 
          data_manager.actors_df[(data_manager.actors_df["actor_type"] == "plat") & 
                                 (data_manager.actors_df["num_users"] >= in_min_plat_size)].reset_index())
    
    actor_id_list = data_manager.indv_actors_df[(data_manager.indv_actors_df["msgs_count"] >= min_msg_count)].index.to_list()
    print(f"Actors #: {len(actor_id_list)}")
    
    for AS_GROWING in [True, False]:
            te_calculator = ing.TransferEntropyCalculator(data_manager, in_add_superclasses=False)
            folder_type = "growing" if AS_GROWING else "moving"
            te_calculator.calculate_te_network_series(actor_id_list, 
                                                      in_start_datetime, 
                                                      in_end_datetime, 
                                                      in_frequency, 
                                                      in_window_shift_by_days, 
                                                      in_init_window_shift_by_days, 
                                                      AS_GROWING, 
                                                      os.path.join(data_manager.output_dir_path, f"dynamic/{folder_type}"))
        
    return True
                                   
if __name__ == "__main__":
    
    calculate_te_for_dataset("Skripal4",
                             [f"s3://{path}" for path in s3.glob("s3://mips-main/initial_data_collection/raw_data/brandwatch/*.csv*")],
                             datetime.datetime(2018, 3, 1).astimezone(datetime.timezone.utc),
                             datetime.datetime(2018, 5, 1).astimezone(datetime.timezone.utc),
                             in_frequency = '12H',
                             in_min_plat_size = 200,
                             in_window_shift_by_days = 4,
                             in_init_window_shift_by_days = 4)
    
    print("================== SKRIPAL DONE ==================")
    
    calculate_te_for_dataset("Ukraine4",
                             [f"s3://{path}" for path in s3.glob("s3://mips-phase-2/initial_query_data/raw_data/brandwatch/*.csv*")],
                             datetime.datetime(2022, 1, 1).astimezone(datetime.timezone.utc),
                             datetime.datetime(2022, 5, 1).astimezone(datetime.timezone.utc),
                             in_frequency = '12H',
                             in_min_plat_size = 500,
                             in_window_shift_by_days = 4,
                             in_init_window_shift_by_days = 4)
    
    print("================== UKRAINE DONE ==================")
    
    
    
    
