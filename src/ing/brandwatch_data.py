import os.path
import glob
from typing import List

import pandas as pd
import s3fs
import os
import glob
import time

# s3 specific libraries
import s3fs
import boto3

from .interface_data_source import IDataSource


class BrandwatchData(IDataSource):
    bw_gui_column_dict = {'Date': 'datetime', 'Author': 'source_user_id', 'Full Text': 'content', 'Title': 'title',
                          'Thread Id': 'parent_source_msg_id', 'Thread Author': 'parent_source_user_id',
                          'Domain': 'platform', 'Expanded URLs': 'article_url', 'Url': 'source_msg_id'}

    bw_api_column_dict = {'date': 'datetime', 'author': 'source_user_id', 'fullText': 'content', 'title': 'title',
                          'threadId': 'parent_source_msg_id', 'threadAuthor': 'parent_source_user_id',
                          'domain': 'platform', 'expandedUrls': 'article_url', 'url': 'source_msg_id'}

    def get_data(self) -> pd.DataFrame:
        s3object = s3fs.S3FileSystem(anon=False)
        file_paths_list = self.get_file_paths_list(s3object)
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

    def get_file_paths_list(self, in_s3_object: s3fs.S3FileSystem) -> List[str]:
        """
        Get list of all "*.csv.zip" files form the source_folder path.
        """
        data_files = []
        if self.is_from_s3:
            data_files = in_s3_object.glob(os.path.join(self.source_folder, "*.csv*"))
        else:
            data_files = glob.glob(os.path.join(self.source_folder, "*.csv*"))
        prefix_path = ''
        if self.is_from_s3:
            data_files = in_s3_object.glob(os.path.join(self.source_folder, "*.csv.zip"))
            prefix_path = 's3://'
        else:
            data_files = glob.glob(os.path.join(self.source_folder, "*.csv.zip"))
        return [f"{prefix_path}{file_path}" for file_path in data_files]
