from typing import List, Optional

import pandas as pd

from .interface_source_data_reader import IDataSourceReader


class BrandwatchDataReader(IDataSourceReader):
    bw_gui_column_dict = {'Date': 'datetime', 'Author': 'source_user_id', 'Full Text': 'content', 'Title': 'title',
                          'Thread Id': 'parent_source_msg_id', 'Thread Author': 'parent_source_user_id',
                          'Domain': 'platform', 'Url': 'source_msg_id'}

    bw_gui_url_columns = ['Url', 'Display URLs', 'Expanded URLs', 'Media URLs', 'Original Url', 'Short URLs',
                          'Thread URL', 'Broadcast Media Url', 'Title', 'Full Text']

    bw_api_column_dict = {'date': 'datetime', 'author': 'source_user_id', 'fullText': 'content', 'title': 'title',
                          'threadId': 'parent_source_msg_id', 'threadAuthor': 'parent_source_user_id',
                          'domain': 'platform', 'url': 'source_msg_id'}

    bw_api_url_columns = ['url', 'displayUrls', 'expandedUrls', 'mediaUrls', 'originalUrl', 'shortUrls',
                          'threadURL', 'broadcastMediaUrl', 'title', 'fullText']

    def __init__(self):
        """
        Generates a Brandwatch data reader object.
        """
        self.required_bw_gui_column_names = [gui_col for gui_col in self.bw_gui_column_dict
                                             if self.bw_gui_column_dict[gui_col] in self.required_columns] + self.bw_gui_url_columns
        self.required_bw_api_column_names = [api_col for api_col in self.bw_api_column_dict
                                             if self.bw_api_column_dict[api_col] in self.required_columns] + self.bw_api_url_columns

    def read_bw_gui_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, skiprows=6,
                         dtype={key: str for key in self.bw_gui_column_dict},
                         usecols=self.required_bw_gui_column_names)
        df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d %H:%M:%S.%f", utc=True)
        df['search_article_urls'] = df.apply(lambda row: ", ".join([row[col] for col in self.bw_gui_url_columns if type(row[col]) is str]), axis=1)
        df.rename(columns=self.bw_gui_column_dict, inplace=True)
        df.drop(columns=self.bw_gui_url_columns, errors='ignore', inplace=True)
        return df

    def read_bw_api_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, parse_dates=['date'], date_format="%Y-%m-%dT%H:%M:%S.%f%z",
                         dtype={key: str for key in self.bw_api_column_dict},
                         usecols=self.required_bw_api_column_names)
        df['search_article_urls'] = df.apply(lambda row: ", ".join([row[col] for col in self.bw_api_url_columns if type(row[col]) is str]), axis=1)
        df.rename(columns=self.bw_api_column_dict, inplace=True)
        df.drop(columns=self.bw_api_url_columns, errors='ignore', inplace=True)
        return df

    def read_data_file(self, in_file_path: str, in_supress_exception: bool = True) -> Optional[pd.DataFrame]:
        df = pd.read_csv(in_file_path, nrows=1)
        if all([key in df.columns for key in self.bw_api_column_dict]):
            print(f"Brandwatch API data : {in_file_path}")
            return self.read_bw_api_file(in_file_path)

        df = pd.read_csv(in_file_path, skiprows=6, nrows=1)
        if all([key in df.columns for key in self.bw_gui_column_dict]):
            print(f"Brandwatch GUI data : {in_file_path}")
            return self.read_bw_gui_file(in_file_path)

        if in_supress_exception:
            return None
        else:
            raise Exception("File do not contain Brandwatch GUI or API columns!")

    def read_data_files_list(self, in_file_path_list: List[str]) -> pd.DataFrame:
        """
        Reads all files that have the from of Brandwatch mentions file structure.
        Removes duplicates based on the source_msg_id ("URL") column.
        """
        result_df = pd.concat([self.read_data_file(data_file) for data_file in in_file_path_list])
        result_df.drop_duplicates(subset='source_msg_id', keep="first", inplace=True)
        result_df.reset_index(drop=True, inplace=True)
        return result_df
