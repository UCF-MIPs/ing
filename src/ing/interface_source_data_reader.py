import abc
from typing import List, Optional

import pandas as pd


class IDataSourceReader(metaclass=abc.ABCMeta):
    required_columns = ['datetime', 'source_msg_id', 'source_user_id', 'content', 'title', 'parent_source_msg_id',
                        'parent_source_user_id', 'platform', 'article_url']

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'read_data_file') and
                callable(subclass.read_data_file) and
                hasattr(subclass, 'read_data_files_list') and
                callable(subclass.read_data_files_list) or
                NotImplemented)

    @abc.abstractmethod
    def read_data_file(self, in_file_path: str, in_supress_exception: bool = True) -> Optional[pd.DataFrame]:
        """Read given file"""
        raise NotImplementedError

    @abc.abstractmethod
    def read_data_files_list(self, in_file_path_list: List[str]) -> pd.DataFrame:
        """Read given files list"""
        raise NotImplementedError
