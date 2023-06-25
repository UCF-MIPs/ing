from typing import List, Optional

import pandas as pd

from .interface_source_data_reader import IDataSourceReader


class RedditDataReader(IDataSourceReader):
    reddit_submissions_column_dict = {'archived': 'archived', 'author': 'source_user_id',
                                      'call_to_action': 'call_to_action', 'can_gild': 'can_gild',
                                      'contest_mode': 'contest_mode',
                                      'created_utc': 'created_utc', 'datetime': 'datetime', 'domain': 'domain',
                                      'gilded': 'gilded', 'hidden': 'hidden', 'hide_score': 'hide_score',
                                      'id': 'source_msg_id', 'is_crosspostable': 'is_crosspostable',
                                      'is_reddit_media_domain': 'is_reddit_media_domain', 'is_self': 'is_self',
                                      'is_video': 'is_video', 'locked': 'locked', 'no_follow': 'no_follow',
                                      'num_comments': 'num_comments', 'num_crossposts': 'num_crossposts',
                                      'over_18': 'over_18', 'permalink': 'permalink', 'pinned': 'pinned',
                                      'score': 'score', 'selftext': 'content', 'send_replies': 'send_replies',
                                      'spoiler': 'spoiler', 'stickied': 'stickied', 'subreddit': 'subreddit',
                                      'subreddit_id': 'subreddit_id',
                                      'subreddit_subscribers': 'subreddit_subscribers',
                                      'subreddit_type': 'subreddit_type', 'thumbnail': 'thumbnail', 'title': 'title',
                                      'url': 'url'}

    reddit_comments_column_dict = {'author': 'source_user_id', 'body': 'content', 'can_gild': 'can_gild',
                                   'controversiality': 'controversiality', 'created_utc': 'created_utc',
                                   'datetime': 'datetime', 'edited': 'edited', 'id': 'source_msg_id',
                                   'is_submitter': 'is_submitter', 'link_id': 'link_id', 'no_follow': 'no_follow',
                                   'parent_id': 'parent_source_msg_id', 'permalink': 'permalink',
                                   'retrieved_on': 'retrieved_on', 'score': 'score', 'send_replies': 'send_replies',
                                   'stickied': 'stickied', 'subreddit': 'subreddit', 'subreddit_id': 'subreddit_id',
                                   'subreddit_type': 'subreddit_type'}

    def __init__(self):
        """
        Generates a RedditDataReader object which reads reddit data files.
        """
        self.required_reddit_submissions_column_names = [sub_col for sub_col in self.reddit_submissions_column_dict if
                                                         self.reddit_submissions_column_dict[sub_col] in self.required_columns]
        self.required_reddit_comments_column_names = [com_col for com_col in self.reddit_comments_column_dict if
                                                      self.reddit_comments_column_dict[com_col] in self.required_columns]

    def read_reddit_submissions_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, parse_dates=['datetime'],
                         dtype={key: str for key in self.reddit_submissions_column_dict},
                         usecols=self.required_reddit_submissions_column_names)
        df = df.rename(columns=self.reddit_submissions_column_dict)
        df['parent_source_msg_id'] = ""
        df['platform'] = "reddit"
        df['article_url'] = ""
        df['parent_source_user_id'] = ""
        return df

    def read_reddit_comments_file(self, in_file_path: str) -> pd.DataFrame:
        df = pd.read_csv(in_file_path, parse_dates=['datetime'],
                         dtype={key: str for key in self.reddit_comments_column_dict},
                         usecols=self.required_reddit_comments_column_names)
        df = df.rename(columns=self.reddit_comments_column_dict)
        df['title'] = df['content']
        df['platform'] = "reddit"
        df['article_url'] = ""
        df['parent_source_user_id'] = ""
        return df

    def read_data_file(self, in_file_path: str, in_supress_exception: bool = True) -> Optional[pd.DataFrame]:
        df = pd.read_csv(in_file_path, nrows=1)

        if all([key in df.columns for key in self.reddit_submissions_column_dict]):
            print(f"Reddit Submissions data: {in_file_path}")
            return self.read_reddit_submissions_file(in_file_path)

        if all([key in df.columns for key in self.reddit_comments_column_dict]):
            print(f"Reddit Comments data: {in_file_path}")
            return self.read_reddit_comments_file(in_file_path)

        if in_supress_exception:
            return None
        else:
            raise Exception("File do not contain Reddit submissions or comments columns!")

    def read_data_files_list(self, in_file_path_list: List[str]) -> pd.DataFrame:
        """
        Reads all files that have the from of reddit data file structure.
        Removes duplicates based on the source_msg_id column.
        """
        result_df = pd.concat([self.read_data_file(data_file) for data_file in in_file_path_list])
        result_df.drop_duplicates(subset='source_msg_id', keep="first", inplace=True)
        result_df.reset_index(drop=True, inplace=True)
        return result_df
