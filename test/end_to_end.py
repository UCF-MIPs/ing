
""" Include containing folder for testing """
import sys, os
import datetime

sys.path.insert(0, os.path.abspath('/home/ec2-user/SageMaker/ing/src'))
print(os.path.abspath('.'))
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
# -----------------------------

DATA_DIRECTORY_LIST = ["s3://mips-main/initial_data_collection/raw_data/brandwatch/", "s3://mips-main/initial_data_collection/TE_ready_data/V2/"]
NEWS_DOMAIN_TO_CLASS_FILE = "s3://mips-main-2/UFTM_classification-v2/news_table-v3-UT60.csv"
START_DATE = datetime.datetime(2018, 3, 1, tzinfo=datetime.timezone.utc)
END_DATE = datetime.datetime(2018, 5, 2, tzinfo=datetime.timezone.utc)
FREQUENCY = '6H'
MIN_PLAT_SIZE = 30
MIN_ACTIVITY_PER_MONTH = 15



if __name__ == "__main__":

    # Read input files
    any_source_reader = ing.AnyDataSourceReader()
    # paths = any_source_reader.get_file_paths_list(DATA_DIRECTORY)
    
    paths = []
    for data_dir in DATA_DIRECTORY_LIST:
        paths += s3.glob(os.path.join(data_dir, "*.csv*"))
    paths = [f"s3://{p}" for p in paths]
    print(paths)
    all_osn_msgs_df = any_source_reader.read_files_list(paths)
    print(all_osn_msgs_df)

    news_domain_classes_df = pd.read_csv(NEWS_DOMAIN_TO_CLASS_FILE,
                                         usecols=['Domain', 'tufm_class', 'Language'])
    news_domain_classes_df.rename(columns={'Domain': 'news_domain', 'tufm_class': 'class', 'Language': 'lang'},
                                  inplace=True)

    # let data manager handle data
    data_manager = ing.DataManager(all_osn_msgs_df, ".")

    # preprocess
    data_manager.preprocess(news_domain_classes_df, START_DATE, END_DATE)

    # generate tables
    min_msg_count = MIN_ACTIVITY_PER_MONTH * (END_DATE - START_DATE).days // 30
    print(f"minimum message count : {min_msg_count}")
    data_manager.generate_data_tables(MIN_PLAT_SIZE, min_msg_count)

    print("calculating te...")
    te_calculator = ing.TransferEntropyCalculator(data_manager)
    current_start_date = START_DATE
    current_end_date = END_DATE
    te_df = te_calculator.calculate_te_network(current_start_date, current_end_date, FREQUENCY)

    print("all done!")
    
    compression_options = dict(method='zip', archive_name='actor_te_edges_df.csv')
    te_df.to_csv("actor_te_edges_df.csv.zip", index=False, compression=compression_options)

