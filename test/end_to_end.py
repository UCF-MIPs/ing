
""" Include containing DATA_DIRECTORY for testing """
import sys, os
import datetime

sys.path.insert(0, os.path.abspath('.'))
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
from src import ing
# -----------------------------


"""
Usual module/package imports go below here
"""
import pandas as pd
# -----------------------------

DATA_DIRECTORY = "C:\\STUFF\\RESEARCH\\smyrna\\Smyrna\\data_collection\\test_ing"
NEWS_DOMAIN_TO_CLASS_FILE = "C:\\STUFF\\RESEARCH\\smyrna\\Smyrna\\data_collection\\news_table-v3-UT60.csv"
START_DATE = datetime.datetime(2018, 3, 1, tzinfo=datetime.timezone.utc)
END_DATE = datetime.datetime(2018, 5, 1, tzinfo=datetime.timezone.utc)
FREQUENCY = '6H'
MIN_PLAT_SIZE = 30
MIN_ACTIVITY_PER_MONTH = 15


if __name__ == "__main__":

    # Read input files
    any_source_reader = ing.AnyDataSourceReader()
    paths = any_source_reader.get_file_paths_list(DATA_DIRECTORY)
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
    te_df = te_calculator.calculate_te_network(datetime.datetime(2018, 3, 5, tzinfo=datetime.timezone.utc), datetime.datetime(2018, 3, 10, tzinfo=datetime.timezone.utc), FREQUENCY)

    print("all done!")

