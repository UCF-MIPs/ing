from typing import Optional, List

import pandas as pd

from .interface_source_data_reader import IDataSourceReader


class FourChanDataReader(IDataSourceReader):
    fourchan_column_dict = {'archived': 'archived', 'archived_on': 'archived_on', 'bumplimit': 'bumplimit',
                            'capcode': 'capcode', 'closed': 'closed', 'com': 'content',
                            'country': 'country', 'country_name': 'country_name', 'ext': 'ext',
                            'extracted_poster_id': 'source_user_id', 'filedeleted': 'filedeleted',
                            'filename': 'filename', 'fsize': 'fsize', 'h': 'h', 'id': 'id', 'imagelimit': 'imagelimit',
                            'images': 'images', 'm_img': 'm_img', 'md5': 'md5',
                            'name': 'name', 'no': 'source_msg_id', 'now': 'now', 'perspectives': 'perspectives',
                            'replies': 'replies', 'semantic_url': 'semantic_url',
                            'since4pass': 'since4pass', 'spoiler': 'spoiler', 'sticky': 'sticky', 'sub': 'sub',
                            'tail_size': 'tail_size', 'tim': 'tim', 'time': 'time',
                            'datetime': 'datetime', 'datetime_dateformat': 'datetime_dateformat', 'tn_h': 'tn_h',
                            'tn_w': 'tn_w', 'troll_country': 'troll_country',
                            'unique_ips': 'unique_ips', 'w': 'w', 'xa18': 'xa18', 'xa19l': 'xa19l', 'xa19s': 'xa19s',
                            'xh17': 'xh17', 'xh17c': 'xh17c'}

    def __init__(self):
        """
        Generates a 4Chan data reader object.
        """
        self.required_4chan_column_names = [fc_col for fc_col in self.fourchan_column_dict
                                            if self.fourchan_column_dict[fc_col] in self.required_columns]
        self.missing_columns = list(set(self.required_columns).difference(
            [self.fourchan_column_dict[col] for col in self.required_4chan_column_names]))

    def read_4chan_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, dtype={key: str for key in self.fourchan_column_dict},
                         usecols=self.required_4chan_column_names)
        df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%d %H:%M:%S.%f", utc=True)
        df = df.rename(columns=self.fourchan_column_dict)
        df['search_article_urls'] = df.apply(lambda row: ", ".join([row[col] for col in df.columns if type(row[col]) is str]), axis=1)
        df['title'] = df['content']
        df['parent_source_msg_id'] = ""
        df['parent_source_user_id'] = ""
        df['platform'] = "4chan.org"
        return df

    def read_data_file(self, in_file_path: str, in_supress_exception: bool = True) -> Optional[pd.DataFrame]:
        df = pd.read_csv(in_file_path, nrows=1)
        if all([key in df.columns for key in self.fourchan_column_dict]):
            print(f"4Chan data : {in_file_path}")
            return self.read_4chan_file(in_file_path)

        if in_supress_exception:
            return None
        else:
            raise Exception("File do not contain 4Chan columns!")

    def read_data_files_list(self, in_file_path_list: List[str]) -> pd.DataFrame:
        """
        Reads all files that have the from of 4Chan mentions file structure.
        Removes duplicates based on the source_msg_id column.
        """
        result_df = pd.concat([self.read_data_file(data_file) for data_file in in_file_path_list])
        result_df.drop_duplicates(subset='source_msg_id', keep="first", inplace=True)
        result_df.reset_index(drop=True, inplace=True)
        return result_df
