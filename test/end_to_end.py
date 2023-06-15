import sys, os
sys.path.insert(0, os.path.abspath('.'))
print(os.path.abspath('.'))
import pandas as pd

# from ing.src import S3Access
from src import ing

S3_INPUT_BRANDWATCH_DIR = 's3://mips-main/initial_data_collection/raw_data/brandwatch'
S3_NEWS_DOMAIN_TUFM_FILE = "s3://mips-main-2/UFTM_classification-v2/news_table-v3-UT60.csv"


def read_domain_to_class_classification():
    tufmdf = pd.read_csv(S3_NEWS_DOMAIN_TUFM_FILE)
    tufmdf.rename(columns={'Domain': 'news_domain'}, inplace=True)
    assert set(tufmdf['tufm_class'].unique()) == {'TF', 'TM', 'UF', 'UM'}, "News domains should belong to one of the classes in {'TF', 'TM', 'UF', 'UM'}"
    news_domain_to_tufm_class = tufmdf.set_index('news_domain')['tufm_class'].to_dict()
    return news_domain_to_tufm_class

print("1")
news_domain_to_tufm_class = read_domain_to_class_classification()

print("2")
news_domain_identifier = ing.NewsDomainIdentifier(news_domain_to_tufm_class.keys())

print("3")
bd = ing.BrandwatchData(S3_INPUT_BRANDWATCH_DIR, True)
print(bd)

print("4")
bw_df = bd.get_data()
print(bw_df)

print(bw_df.info())

#
# s3a = S3Access()
# print(s3a.get_buckets_list())
