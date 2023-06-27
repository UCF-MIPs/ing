import glob
import os.path
from typing import List

import pandas as pd
import s3fs

from .reddit_data_reader import RedditDataReader
from .brandwatch_data_reader import BrandwatchDataReader
from .fourchan_data_reader import FourChanDataReader


class AnyDataSourceReader:

    def __init__(self):
        self.reddit_reader = RedditDataReader()
        self.bw_reader = BrandwatchDataReader()
        self.fourchan_reader = FourChanDataReader()

    def read_data_file(self, in_file_path: str) -> pd.DataFrame:
        df = None
        if df is None:
            df = self.bw_reader.read_data_file(in_file_path)
        if df is None:
            df = self.reddit_reader.read_data_file(in_file_path)
        if df is None:
            df = self.fourchan_reader.read_data_file(in_file_path)
        return df

    def read_files_list(self, in_file_path_list: List[str]) -> pd.DataFrame:
        """
        Reads all "*.csv" files that have the from of Brandwatch mentions file structure.
        Removes duplicates based on the source_msg_id ("URL") column.
        """
        result_df = pd.concat([self.read_data_file(data_file) for data_file in in_file_path_list])
        result_df.drop_duplicates(subset='source_msg_id', keep="first", inplace=True)
        result_df.reset_index(drop=True, inplace=True)
        return result_df

    def get_file_paths_list(self, in_source_folder, in_is_from_s3=False, in_s3_object: s3fs.S3FileSystem = None) -> \
    List[str]:
        """
        Get list of all "*.csv" files form the source_folder path.
        """
        data_files = []
        prefix_path = ''
        if in_is_from_s3:
            data_files = in_s3_object.glob(os.path.join(in_source_folder, "*"))
            prefix_path = 's3://'
        else:
            data_files = glob.glob(os.path.join(in_source_folder, "*"))
            data_files = [file for file in data_files if not os.path.isdir(file)]
        return [f"{prefix_path}{file_path}" for file_path in data_files]
