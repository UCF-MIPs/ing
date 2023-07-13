import os.path
import datetime
import pandas as pd

from .news_domain_classifier import NewsDomainClassifier
from .news_domain_identifier import NewsDomainIdentifier


class DataManager:

    def __init__(self, in_all_osn_msgs_df: pd.DataFrame, in_output_dir_path: str):
        self.all_osn_msgs_df = in_all_osn_msgs_df
        self.output_dir_path = in_output_dir_path
        self.next_actor_idx = 0
        self.all_users = None
        self.actors_df = None
        self.platform_actors_df = None
        self.indv_actors_df = None
        self.filtered_osn_msgs_view_df = self.all_osn_msgs_df

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
        #  0. Filter out dates
        if in_start_date is not None:
            self.all_osn_msgs_df = self.all_osn_msgs_df[(in_start_date <= self.all_osn_msgs_df['datetime'])]
        if in_end_date is not None:
            self.all_osn_msgs_df = self.all_osn_msgs_df[(self.all_osn_msgs_df['datetime'] <= in_end_date)]

        #  1. Remove nan
        self.all_osn_msgs_df = self.all_osn_msgs_df[~(self.all_osn_msgs_df['datetime'].isna() |
                                                    self.all_osn_msgs_df['platform'].isna() |
                                                    self.all_osn_msgs_df['source_user_id'].isna() |
                                                    self.all_osn_msgs_df['source_msg_id'].isna())].reset_index(drop=True)
        #  2. Add article count column
        self.all_osn_msgs_df['article_urls_count'] = self.all_osn_msgs_df['article_urls'].apply(
            lambda x: x.count(', ') + 1 if type(x) is str else 0)

        #  3. add msg_id
        self.all_osn_msgs_df.rename_axis("msg_id", inplace=True)
        self.all_osn_msgs_df.reset_index(inplace=True)
        self.all_osn_msgs_df["msg_id"] = self.all_osn_msgs_df["msg_id"].apply(lambda x: f"m{x}")

        # 4. identify news_domains
        ndi = NewsDomainIdentifier(in_news_domain_classes_df['news_domain'].unique())
        self.all_osn_msgs_df['news_domains'] = self.all_osn_msgs_df['article_urls'].apply(
            lambda x: ndi.find_all_matches(x) if type(x) is str else [])

        # 5. identify class of each news_domain
        ndc = NewsDomainClassifier(in_news_domain_classes_df, {'TF', 'TM', 'UF', 'UM'})
        self.all_osn_msgs_df['classes'] = self.all_osn_msgs_df['news_domains'].apply(
            lambda x: [ndc.get_class(nd) for nd in x])

        # 6. counts of each class marked at each class_X column
        self.all_osn_msgs_df[['class_TM', 'class_TF', 'class_UM', 'class_UF']] = self.all_osn_msgs_df['classes'].apply(
            lambda x: pd.Series([x.count('TM'), x.count('TF'), x.count('UM'), x.count('UF')]))

    def generate_data_tables(self, in_min_platform_size: int, in_min_user_messages_count: int):
        """
        Make sure this is run after running preprocess function.
        Parameters
        ----------
        in_min_platform_size :
            Minimum number of people in a filtered platform
        in_min_user_messages_count :
            Minimum number of messages created by a filtered actor
        """
        self.__generate_user_id()
        self.__generate_platform_actor_id(in_min_platform_size)
        self.__generate_individual_actor_id(in_min_user_messages_count)
        # set indexes
        self.all_osn_msgs_df.set_index("msg_id", inplace=True)
        self.all_users.set_index("user_id", inplace=True)
        self.actors_df.set_index("actor_id", inplace=True)
        self.platform_actors_df.set_index("actor_id", inplace=True)
        self.indv_actors_df.set_index("actor_id", inplace=True)
        # save files
        self.__save_data_files()
        print("data table generation completed.")

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
            platform = self.platform_actors_df.loc[in_actor_id]["platform"]
            # print(f"plat : {platform}")
            if in_use_filtered_view:
                return self.filtered_osn_msgs_view_df[self.filtered_osn_msgs_view_df["platform"] == platform]
            else:
                return self.all_osn_msgs_df[self.all_osn_msgs_df["platform"] == platform]
        return None

    def __save_data_files(self):
        print("saving data files to disk ...")
        self.__save_csv_zip_file(self.all_users, "users_df")
        self.__save_csv_zip_file(self.all_osn_msgs_df, 'all_osn_msgs_df')
        self.__save_csv_zip_file(self.actors_df, "actors_df")
        self.__save_csv_zip_file(self.indv_actors_df, "indv_actors_df")
        self.__save_csv_zip_file(self.platform_actors_df, "plat_actors_df")

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
        print("generating user_id values ...")
        # create user_id and all_users object
        temp_users_1 = self.all_osn_msgs_df.groupby(["platform", "source_user_id"], dropna=False).size().reset_index()[
            ["platform", "source_user_id"]]
        temp_users_2 = \
            self.all_osn_msgs_df.groupby(["platform", "parent_source_user_id"], dropna=False).size().reset_index()[
                ["platform", "parent_source_user_id"]].rename(columns={"parent_source_user_id": "source_user_id"})
        self.all_users = pd.concat([temp_users_1, temp_users_2])
        self.all_users.drop_duplicates(subset=["platform", "source_user_id"], inplace=True, ignore_index=True)
        self.all_users = self.all_users.groupby(["platform", "source_user_id"], dropna=False).size().rename(
            'num_users').reset_index().drop(columns=["num_users"]).rename_axis("user_id").reset_index()
        self.all_users["user_id"] = self.all_users["user_id"].apply(lambda x: f"u{x}")
        user_num_msgs = self.all_osn_msgs_df.groupby(["platform", "source_user_id"], dropna=False).size().rename(
            'msgs_count').reset_index()
        self.all_users = self.all_users.merge(user_num_msgs, how='left', on=["platform", "source_user_id"])
        self.all_users["msgs_count"].fillna(0, inplace=True)
        if in_dump_temp:
            self.__save_csv_zip_file(self.all_users, "temp_users_df")

        print("updating all_osn_msgs ....")
        # add user_id column to all_osn_msgs_df
        src_user_to_user_id = self.all_users.set_index(["platform", "source_user_id"])["user_id"].to_dict()
        self.all_osn_msgs_df[['user_id', 'parent_user_id']] = self.all_osn_msgs_df.apply(lambda row: pd.Series([
            src_user_to_user_id[(row['platform'], row['source_user_id'])],
            src_user_to_user_id[(row['platform'], row['parent_source_user_id'])]
        ]), axis=1)
        if in_dump_temp:
            self.__save_csv_zip_file(self.all_osn_msgs_df, 'temp_all_osn_msgs_df')

        self.actors_df = pd.DataFrame([],
                                      columns=["actor_id", "actor_type", "actor_label", "actor_long_label", "num_users"])

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
        print("generating actor_id values for individuals ...")
        user_reception = self.all_osn_msgs_df["parent_user_id"].value_counts().rename(
            "received_share_count").rename_axis("user_id").reset_index()
        self.indv_actors_df = self.all_users.merge(user_reception, on="user_id", how="left")
        self.indv_actors_df["received_share_count"].fillna(0, inplace=True)
        self.indv_actors_df = self.indv_actors_df.rename_axis("actor_id").reset_index()
        self.__create_actor_ids(self.indv_actors_df)
        if in_min_messages_count is not None:
            self.indv_actors_df = self.indv_actors_df[self.indv_actors_df["msgs_count"] >= in_min_messages_count]
        indv_actors = self.indv_actors_df.apply(lambda row: pd.Series(
            [row["actor_id"], "indv", row["source_user_id"], "{}: @{}".format(row["platform"], row["source_user_id"]),
             1]), axis=1).rename(columns={0: "actor_id", 1: "actor_type", 2: "actor_label", 3: "actor_long_label", 4: "num_users"})
        self.actors_df = pd.concat([self.actors_df, indv_actors])
        if in_dump_temp:
            self.__save_csv_zip_file(self.indv_actors_df, "temp_indv_actors_df")
            self.__save_csv_zip_file(self.actors_df, "temp_actors_df")

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
        print("generating actor_id values for platforms ...")
        # create actor_ids for platforms
        self.platform_actors_df = self.all_osn_msgs_df["platform"].value_counts().rename(
            "users_count").reset_index().rename_axis("actor_id").reset_index()
        self.__create_actor_ids(self.platform_actors_df)
        if in_min_size is not None:
            self.platform_actors_df = self.platform_actors_df[self.platform_actors_df["users_count"] >= in_min_size]
        plat_actors = self.platform_actors_df.apply(
            lambda row: pd.Series([row["actor_id"], "plat", row["platform"], row["platform"], row["users_count"]]),
            axis=1).rename(
            columns={0: "actor_id", 1: "actor_type", 2: "actor_label", 3: "actor_long_label", 4: "num_users"})
        self.actors_df = pd.concat([self.actors_df, plat_actors])
        if in_dump_temp:
            self.__save_csv_zip_file(self.platform_actors_df, "temp_plat_actors_df")
            self.__save_csv_zip_file(self.actors_df, "temp_actors_df")


