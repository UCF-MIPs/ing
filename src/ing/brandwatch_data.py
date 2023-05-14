import os.path
import glob
from typing import List

import pandas as pd

# s3 specific libraries
import s3fs
import boto3

from .interface_data_source import IDataSource


class BrandwatchData(IDataSource):
    s3 = s3fs.S3FileSystem(anon=False)

    bw_gui_column_dict = {'Date': 'datetime', 'Author': 'source_user_id', 'Full Text': 'content', 'Title': 'title',
                          'Thread Id': 'parent_source_msg_id', 'Thread Author': 'parent_source_user_id',
                          'Domain': 'platform', 'Expanded URLs': 'article_url', 'Url': 'source_msg_id'}

    bw_api_column_dict = {'date': 'datetime', 'author': 'source_user_id', 'fullText': 'content', 'title': 'title',
                          'threadId': 'parent_source_msg_id', 'threadAuthor': 'parent_source_user_id',
                          'domain': 'platform', 'expandedUrls': 'article_url', 'url': 'source_msg_id'}

    def get_data(self) -> pd.DataFrame:
        file_paths_list = self.get_file_paths_list()
        return self.read_bw_files_list(file_paths_list)

    def __init__(self, in_source_folder: str, in_is_from_s3: bool):
        """

        Parameters
        ----------
        in_source_folder
            Path of the folder that contains Brandwatch datafiles as "*.csv.zip" files.
            All those files are read by this file reader.
        in_is_from_s3
            Must be True if the path is an s3 URL.
            Must be False if reading from local disk.
        """
        self.is_from_s3 = in_is_from_s3
        self.source_folder = in_source_folder

    def read_bw_gui_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, skiprows=6, parse_dates=['Date'],
                         dtype={key: str for key in self.bw_gui_column_dict},
                         usecols=[key for key in self.bw_gui_column_dict])
        df = df.rename(columns=self.bw_gui_column_dict)
        return df

    def read_bw_api_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, skiprows=6, parse_dates=['Date'],
                         dtype={key: str for key in self.bw_api_column_dict},
                         usecols=[key for key in self.bw_api_column_dict])
        df = df.rename(columns=self.bw_api_column_dict)
        return df

    def read_bw_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, nrows=1)
        if all([key in df.columns for key in self.bw_api_column_dict]):
            return self.read_bw_api_file(in_file_path)

        df = pd.read_csv(in_file_path, skiprows=6, nrows=1)
        if all([key in df.columns for key in self.bw_gui_column_dict]):
            return self.read_bw_gui_file(in_file_path)

        raise Exception("File do not contain Brandwatch GUI or API columns!")

    def read_bw_files_list(self, in_file_path_list: List[str]) -> pd.DataFrame:
        """
        Reads all "*.csv" files that have the from of Brandwatch mentions file structure.
        Removes duplicates based on the source_msg_id ("URL") column.
        """
        result_df = pd.concat([self.read_bw_file(bw_data_file) for bw_data_file in in_file_path_list])
        result_df.drop_duplicates(subset='source_msg_id', keep="first", inplace=True)
        result_df.reset_index(drop=True, inplace=True)
        return result_df

    def get_file_paths_list(self) -> List[str]:
        """
        Get list of all "*.csv.zip" files form the source_folder path.
        """
        data_files = []
        prefix_path = ''
        if self.is_from_s3:
            data_files = self.s3.glob(os.path.join(self.source_folder, "*.csv.zip"))
            prefix_path = 's3://'
        else:
            data_files = glob.glob(os.path.join(self.source_folder, "*.csv.zip"))
        return [f"{prefix_path}{file_path}" for file_path in data_files]

    # def read_brandwatch_data(self, filter_platform_domain_set, is_using_s3):
    #     """
    #     Reads data from all the csv files in the given directory
    #     :param data_directory: Path to the directory that contains the csv files
    #     :type data_directory: str
    #     :return: pandas Dataframe that contains all the data from all csv files
    #     :rtype: pd.Dataframe
    #     """
    #     data_files = []
    #     if is_using_s3:
    #         data_files = s3.glob(os.path.join(data_directory, "*.csv*"))
    #     else:
    #         data_files = glob.glob(os.path.join(data_directory, "*.csv*"))
    #     prefix_path = ''
    #     if is_using_s3:
    #         prefix_path = 's3://'
    #     print(data_files)
    #     df_list = []
    #     for idx, file in enumerate(data_files):
    #         print(f"Reading {idx + 1} of {len(data_files)} files.\nFile name: {file}")
    #         df = pd.read_csv(prefix_path + data_files[idx], skiprows=6, parse_dates=['Date'],
    #                          dtype={'Twitter Author ID': str, 'Author': str,
    #                                 'Full Text': str, 'Title': str,
    #                                 'Thread Id': str, 'Thread Author': str,
    #                                 'Domain': str, 'Expanded URLs': str,
    #                                 'Avatar': str, 'Parent Blog Name': str, 'Root Blog Name': str},
    #                          usecols=['Date', 'Author', 'Full Text', 'Title', 'Thread Id',
    #                                   'Thread Author', 'Domain', 'Expanded URLs', 'Url']
    #                          )
    #         # df = df[['Date', 'Hashtags', 'Twitter Author ID', 'Author', 'Url', 'Thread Id', 'Thread Author', 'Domain']]
    #         df = df.rename(columns=bwgui_column_dict)
    #         df_list.append(df)
    #
    #     start_time = time.time()
    #     result_df = pd.concat(df_list)
    #     end_time = time.time()
    #     print(f"{(end_time - start_time) / 60} mins for concat dataframes")
    #
    #     start_time = time.time()
    #     result_df.drop_duplicates(subset='source_msg_id', keep="first", inplace=True)
    #     end_time = time.time()
    #     print(f"{(end_time - start_time) / 60} mins for drop duplicates")
    #
    #     start_time = time.time()
    #     if SAVE_OUTPUT_FILES:
    #         result_df['platform'].value_counts().rename('users_count').rename_axis('platform').to_csv(OUTPUT_PLATFORM_COUNTS_FILE)
    #     result_df = result_df[result_df['platform'].isin(filter_platform_domain_set)]
    #     end_time = time.time()
    #     print(f"{(end_time - start_time) / 60} mins for filtering platforms")
    #
    #     result_df.reset_index(drop=True, inplace=True)
    #     print(result_df.shape)
    #     return result_df
