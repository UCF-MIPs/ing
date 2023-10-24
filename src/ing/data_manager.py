import multiprocessing
import os.path
import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd

from .news_domain_classifier import NewsDomainClassifier
from .news_domain_identifier import NewsDomainIdentifier
from .any_data_source_reader import AnyDataSourceReader
from .url_expander import URLExpander
from .time_keeper import TimeKeeper


def ndi_find_all_matches(ndi, x):
    return ndi.find_all_matches(x) if type(x) is str else []


class DataManager:
    """
    Keeps track of all data in memory.

    Attributes
    ----------
        output_dir_path : str
            The location of csv data files
        state : Literal["NO_DATA", "RAW_DATA", "CLEAN_DATA", "TABLE_DATA"]
            A string that describes the current state of the DataManager.
        all_osn_msgs_df : pd.DataFrame
        filtered_osn_msgs_view_df : pd.DataFrame
            Filtered values from all_osn_msgs_df to fit a given StartDate and EndDate criteria.
        indv_actors_df : pd.DataFrame
            Individual actors dataframe. This DataFrame will contain user_id, actor_id relationship and other required columns.
    """

    def __init__(self, in_output_dir_path: str):
        self.output_dir_path = in_output_dir_path
        self.next_actor_idx = 0
        self.state = "NO_DATA"
        self.all_users_df = None
        self.actors_df = None
        self.plat_actors_df = None
        self.indv_actors_df = None
        self.all_osn_msgs_df = None
        self.filtered_osn_msgs_view_df = self.all_osn_msgs_df
        self.reset(in_output_dir_path)  # added for consistency

    def reset(self, in_output_dir_path: str = None):
        self.output_dir_path = in_output_dir_path
        self.next_actor_idx = 0
        self.state = "NO_DATA"
        self.all_users_df = None
        self.actors_df = None
        self.plat_actors_df = None
        self.indv_actors_df = None
        self.all_osn_msgs_df = None
        self.filtered_osn_msgs_view_df = self.all_osn_msgs_df

    def read_data_files(self, in_data_file_paths_list: List[str]):
        adsr = AnyDataSourceReader()
        if self.state != "NO_DATA":
            print(f"ERROR: Some data already exists!\nDataManager state is {self.state}")
            return
        tk = TimeKeeper("Reading data")
        self.all_osn_msgs_df = adsr.read_files_list(in_data_file_paths_list)
        self.filtered_osn_msgs_view_df = self.all_osn_msgs_df
        self.state = "RAW_DATA"
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")
        tk.done()

    def __generate_article_urls_columns(self, in_all_osn_msgs_msgid_and_text_values: List[Tuple[str, str]]) -> List[str]:
        URLex = URLExpander()
        for msg_id, text in in_all_osn_msgs_msgid_and_text_values:
            URLex.consume_potential_urls_from_text(msg_id, text)
        self.all_osn_msgs_df["article_urls"] = self.all_osn_msgs_df["msg_id"].apply(lambda x: URLex.get_article_urls(x))
        self.all_osn_msgs_df["article_urls_count"] = self.all_osn_msgs_df["article_urls"].apply(lambda x: len(x))
        self.all_osn_msgs_df["article_urls"] = self.all_osn_msgs_df["article_urls"].apply(lambda x: str(x))

    def preprocess(self, in_news_domain_classes_df: pd.DataFrame,
                   in_start_date: datetime.datetime = None, in_end_date: datetime.datetime = None):
        """
        This method should be run before any other methods in this class.
        Preprocess all the Online Social Network Messages in the given dataframe.
            0. Filtered data to fit the given start datetime and end datetime.
            1. Removes all rows that have empty/null values for the columns: datetime, platform, source_msg_id.
            2. Add the article_urls_count column by counting the number of  URLs in the value for article_urls column.
            3. Add the msg_id column which is an auto increment value that works as the index for each message.
            4. Add the news_domains column which contains the news domain of each article_url.
            5. Add the class column which contains class of each news_domain ( by using in_news_domain_classes_df).
            6. Add a class_X column for each class X that was identified. class_X column contains the number of URLs
                from that class.
        Parameters
        ----------
        in_news_domain_classes_df :
            The classification of news domains into classes.
        in_start_date :
            Inclusive start date of the data set to select from
        in_end_date :
            Inclusive end date of the data set to select from

        Returns
        -------
            Preprocessed dataframe is returned.
        """

        if self.state != "RAW_DATA":
            print(f"ERROR: RAW_DATA does not exist!\nDataManager state is {self.state}")
            return

        #  0. Filter out dates
        tk = TimeKeeper("Filter out dates")
        if in_start_date is not None:
            self.all_osn_msgs_df = self.all_osn_msgs_df[(in_start_date <= self.all_osn_msgs_df['datetime'])]
        if in_end_date is not None:
            self.all_osn_msgs_df = self.all_osn_msgs_df[(self.all_osn_msgs_df['datetime'] <= in_end_date)]
        print(f"Number of data points in between [{in_start_date}] --> [{in_end_date}] duration : ({self.all_osn_msgs_df.shape[0]})")
        
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")

        #  1. Remove nan
        tk.next("Remove nan")
        self.all_osn_msgs_df = self.all_osn_msgs_df[~(self.all_osn_msgs_df['datetime'].isna() |
                                                      self.all_osn_msgs_df['platform'].isna() |
                                                      self.all_osn_msgs_df['source_user_id'].isna() |
                                                      self.all_osn_msgs_df['source_msg_id'].isna())].reset_index(
            drop=True)
        
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")

        #  2. add msg_id
        tk.next("add msg_id")
        self.all_osn_msgs_df.rename_axis("msg_id", inplace=True)
        self.all_osn_msgs_df.reset_index(inplace=True)
        self.all_osn_msgs_df["msg_id"] = self.all_osn_msgs_df["msg_id"].apply(lambda x: f"m{x}")
        
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")

        #  3. Add article urls related columns
        tk.next("Add article urls related columns")
        self.__generate_article_urls_columns(self.all_osn_msgs_df[['msg_id', 'search_article_urls']].values)
        # self.all_osn_msgs_df['article_urls_count'] = self.all_osn_msgs_df['article_urls'].apply(
        #     lambda x: x.count(', ') + 1 if type(x) is str else 0)
        self.all_osn_msgs_df = self.all_osn_msgs_df[self.all_osn_msgs_df['article_urls_count'] > 0]
        
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")

        # 4. identify news_domains
        tk.next("identify news_domains")
        ndi = NewsDomainIdentifier(in_news_domain_classes_df['news_domain'].unique())
        # self.all_osn_msgs_df['news_domains'] = self.all_osn_msgs_df['article_urls'].apply(lambda x: ndi.find_all_matches(x) if type(x) is str else [])
        with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as pool:
            self.all_osn_msgs_df['news_domains'] = pool.starmap(ndi_find_all_matches, [[ndi, v] for v in self.all_osn_msgs_df['article_urls']])
            
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")

        # 5. identify class of each news_domain
        tk.next("identify class of each news_domain")
        ndc = NewsDomainClassifier(in_news_domain_classes_df, {'TF', 'TM', 'UF', 'UM'})
        self.all_osn_msgs_df['classes'] = self.all_osn_msgs_df['news_domains'].apply(
            lambda x: [ndc.get_class(nd) for nd in x])
        
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")

        # 6. counts of each class marked at each class_X column
        tk.next("counts of each class marked at each class_X column")
        self.all_osn_msgs_df[['class_TM', 'class_TF', 'class_UM', 'class_UF']] = self.all_osn_msgs_df['classes'].apply(
            lambda x: pd.Series([x.count('TM'), x.count('TF'), x.count('UM'), x.count('UF')]))
        
        print(f"\t new shape: {self.all_osn_msgs_df.shape}")

        self.state = "CLEAN_DATA"
        tk.done()

    def generate_data_tables(self, in_min_platform_size: int = None, in_min_user_messages_count: int = None):
        """
        Make sure this is run after running preprocess function.
        Parameters
        ----------
        in_min_platform_size :
            Minimum number of people in a filtered platform
            If None, then platforms are included without filtering by size.
        in_min_user_messages_count :
            Minimum number of messages created by a filtered actor
            If None, then all users are included without filtering by number of messges.
        """
        if self.state != "CLEAN_DATA":
            print(f"ERROR: CLEAN_DATA does not exist!\nDataManager state is {self.state}")
            return

        tk = TimeKeeper("Generating data tables")
        self.__generate_user_id()
        self.__generate_platform_actor_id(in_min_platform_size)
        self.__generate_individual_actor_id(in_min_user_messages_count)
        # set indexes
        self.all_osn_msgs_df.set_index("msg_id", inplace=True)
        self.all_users_df.set_index("user_id", inplace=True)
        self.actors_df.set_index("actor_id", inplace=True)
        self.plat_actors_df.set_index("actor_id", inplace=True)
        self.indv_actors_df.set_index("actor_id", inplace=True)
        # save files
        self.__save_data_files()
        self.state = "TABLE_DATA"
        tk.done()

    def filter_osn_msgs_view(self, in_start_date: datetime.datetime, in_end_date: datetime.datetime):
        self.filtered_osn_msgs_view_df = self.all_osn_msgs_df[(in_start_date <= self.all_osn_msgs_df['datetime']) &
                                                              (self.all_osn_msgs_df['datetime'] <= in_end_date)]

    def get_actors_msgs(self, in_actor_id: str, in_use_filtered_view: bool):
        if in_actor_id not in self.actors_df.index:
            return None
        actor_type = self.actors_df.loc[in_actor_id]["actor_type"]
        if actor_type == "indv":
            user_id = self.indv_actors_df.loc[in_actor_id]["user_id"]
            # print(f"indv : {user_id}")
            if in_use_filtered_view:
                return self.filtered_osn_msgs_view_df[self.filtered_osn_msgs_view_df["user_id"] == user_id]
            else:
                return self.all_osn_msgs_df[self.all_osn_msgs_df["user_id"] == user_id]
        if actor_type == "plat":
            platform = self.plat_actors_df.loc[in_actor_id]["platform"]
            # print(f"plat : {platform}")
            if in_use_filtered_view:
                return self.filtered_osn_msgs_view_df[self.filtered_osn_msgs_view_df["platform"] == platform]
            else:
                return self.all_osn_msgs_df[self.all_osn_msgs_df["platform"] == platform]
        return None

    def __save_data_files(self):
        tk = TimeKeeper("saving data files to disk")
        self.__save_csv_zip_file(self.all_users_df, "all_users_df")
        self.__save_csv_zip_file(self.all_osn_msgs_df, 'all_osn_msgs_df')
        self.__save_csv_zip_file(self.actors_df, "actors_df")
        self.__save_csv_zip_file(self.indv_actors_df, "indv_actors_df")
        self.__save_csv_zip_file(self.plat_actors_df, "plat_actors_df")
        tk.done()

    def __save_csv_zip_file(self, in_dataframe: pd.DataFrame, in_file_name: str):
        file_path = os.path.join(self.output_dir_path, f'{in_file_name}.csv.zip')
        compression_options = dict(method='zip', archive_name=f'{in_file_name}.csv')
        print(f"Dataframe: {in_file_name} \t shape: {in_dataframe.shape}\nSaving to : {os.path.abspath(file_path)}")
        in_dataframe.to_csv(file_path, compression=compression_options)

    def __generate_user_id(self, in_dump_temp: bool = False):
        """
        Generate user_id
        Parameters
        ----------
        in_dump_temp :
            Saves the data generated as temp_*.csv file/s
        """

        tk = TimeKeeper("generating user_id values")
        # create user_id and all_users_df object
        temp_users_1 = self.all_osn_msgs_df.groupby(["platform", "source_user_id"], dropna=True).size().reset_index()[
            ["platform", "source_user_id"]]
        temp_users_2 = \
            self.all_osn_msgs_df.groupby(["platform", "parent_source_user_id"], dropna=True).size().reset_index()[
                ["platform", "parent_source_user_id"]].rename(columns={"parent_source_user_id": "source_user_id"})
        self.all_users_df = pd.concat([temp_users_1, temp_users_2])
        self.all_users_df.drop_duplicates(subset=["platform", "source_user_id"], inplace=True, ignore_index=True)
        self.all_users_df = self.all_users_df.groupby(["platform", "source_user_id"], dropna=True).size().rename(
            'num_users').reset_index().drop(columns=["num_users"]).rename_axis("user_id").reset_index()
        self.all_users_df["user_id"] = self.all_users_df["user_id"].apply(lambda x: f"u{x}")
        user_num_msgs = self.all_osn_msgs_df.groupby(["platform", "source_user_id"], dropna=True).size().rename(
            'msgs_count').reset_index()
        self.all_users_df = self.all_users_df.merge(user_num_msgs, how='left', on=["platform", "source_user_id"])
        self.all_users_df["msgs_count"].fillna(0, inplace=True)
        if in_dump_temp:
            self.__save_csv_zip_file(self.all_users_df, "temp_users_df")

        tk.next("updating all_osn_msgs")
        # add user_id column to all_osn_msgs_df
        src_user_to_user_id = self.all_users_df.set_index(["platform", "source_user_id"])["user_id"].to_dict()
        self.all_osn_msgs_df[['user_id', 'parent_user_id']] = self.all_osn_msgs_df.apply(lambda row: pd.Series([
            src_user_to_user_id[(row['platform'], row['source_user_id'])],
            src_user_to_user_id[(row['platform'], row['parent_source_user_id'])] if not pd.isnull(row['parent_source_user_id']) else None
        ]), axis=1)
        if in_dump_temp:
            self.__save_csv_zip_file(self.all_osn_msgs_df, 'temp_all_osn_msgs_df')

        self.actors_df = pd.DataFrame([],
                                      columns=["actor_id", "actor_type", "actor_label", "actor_long_label",
                                               "num_users"])
        tk.done()

    def __create_actor_ids(self, inout_actors_df: pd.DataFrame):
        """
        Create actor ids and increment next_actor_idx
        Parameters
        ----------
        inout_actors_df :
            Actor dataframe with "actor_id" column starting from 0 index
        """
        new_next_idx = inout_actors_df["actor_id"].max() + self.next_actor_idx + 1
        inout_actors_df["actor_id"] = inout_actors_df["actor_id"].apply(lambda x: f"a{x + self.next_actor_idx}")
        self.next_actor_idx = new_next_idx

    def __generate_individual_actor_id(self, in_min_messages_count: int = None, in_dump_temp: bool = False):
        """
        Generates actor_id values for each individual actor. Creates indv_actor_df table. Filters actors_df based on given criteria.
        Parameters
        ----------
        in_min_messages_count :
            If given, only the users that have a minimum of this amount of messages are included into the dataset.
        in_dump_temp :
            Saves the data generated as temp_*.csv file/s
        """
        tk = TimeKeeper("generating actor_id values for individuals")
        user_reception = self.all_osn_msgs_df["parent_user_id"].value_counts().rename(
            "received_share_count").rename_axis("user_id").reset_index()
        self.indv_actors_df = self.all_users_df.merge(user_reception, on="user_id", how="left")
        self.indv_actors_df["received_share_count"].fillna(0, inplace=True)
        self.indv_actors_df = self.indv_actors_df.rename_axis("actor_id").reset_index()
        self.__create_actor_ids(self.indv_actors_df)
        if in_min_messages_count is not None:
            self.indv_actors_df = self.indv_actors_df[self.indv_actors_df["msgs_count"] >= in_min_messages_count]
        indv_actors = self.indv_actors_df.apply(lambda row: pd.Series(
            [row["actor_id"], "indv", row["source_user_id"], "{}: @{}".format(row["platform"], row["source_user_id"]),
             1]), axis=1).rename(
            columns={0: "actor_id", 1: "actor_type", 2: "actor_label", 3: "actor_long_label", 4: "num_users"})
        self.actors_df = pd.concat([self.actors_df, indv_actors])
        if in_dump_temp:
            self.__save_csv_zip_file(self.indv_actors_df, "temp_indv_actors_df")
            self.__save_csv_zip_file(self.actors_df, "temp_actors_df")
        tk.done()

    def __generate_platform_actor_id(self, in_min_size: int = None, in_dump_temp: bool = False):
        """
        Generates actor_id values for each platform. Creates plat_actor_df table. Filters platform actors_df by given criteria.

        Parameters
        ----------
        in_min_size :
            If given, discard platforms that have less than this number of users.
        in_dump_temp :
            Saves the data generated as temp_*.csv file/s
        """
        tk = TimeKeeper("generating actor_id values for platforms")
        # create actor_ids for platforms
        self.plat_actors_df = self.all_osn_msgs_df["platform"].value_counts().rename(
            "users_count").reset_index().rename_axis("actor_id").reset_index()
        self.__create_actor_ids(self.plat_actors_df)
        if in_min_size is not None:
            self.plat_actors_df = self.plat_actors_df[self.plat_actors_df["users_count"] >= in_min_size]
        plat_actors = self.plat_actors_df.apply(
            lambda row: pd.Series([row["actor_id"], "plat", row["platform"], row["platform"], row["users_count"]]),
            axis=1).rename(
            columns={0: "actor_id", 1: "actor_type", 2: "actor_label", 3: "actor_long_label", 4: "num_users"})
        self.actors_df = pd.concat([self.actors_df, plat_actors])
        if in_dump_temp:
            self.__save_csv_zip_file(self.plat_actors_df, "temp_plat_actors_df")
            self.__save_csv_zip_file(self.actors_df, "temp_actors_df")
        tk.done()
