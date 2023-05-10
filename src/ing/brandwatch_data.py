import pandas as pd

from .interface_data_source import IDataSource


class BrandwatchData(IDataSource):

    def get_data(self) -> pd.DataFrame:
        pass

    def __init__(self, in_source_folder):
        self.source_folder = in_source_folder

    def read_brandwatch_data(self, filter_platform_domain_set, is_using_s3):
        """
        Reads data from all the csv files in the given directory
        :param data_directory: Path to the directory that contains the csv files
        :type data_directory: str
        :return: pandas Dataframe that contains all the data from all csv files
        :rtype: pd.Dataframe
        """
        data_files = []
        if is_using_s3:
            data_files = s3.glob(os.path.join(data_directory, "*.csv*"))
        else:
            data_files = glob.glob(os.path.join(data_directory, "*.csv*"))
        prefix_path = ''
        if is_using_s3:
            prefix_path = 's3://'
        print(data_files)
        df_list = []
        for idx, file in enumerate(data_files):
            print(f"Reading {idx + 1} of {len(data_files)} files.\nFile name: {file}")
            df = pd.read_csv(prefix_path + data_files[idx], skiprows=6, parse_dates=['Date'],
                             dtype={'Twitter Author ID': str, 'Author': str,
                                    'Full Text': str, 'Title': str,
                                    'Thread Id': str, 'Thread Author': str,
                                    'Domain': str, 'Expanded URLs': str,
                                    'Avatar': str, 'Parent Blog Name': str, 'Root Blog Name': str},
                             usecols=['Date', 'Author', 'Full Text', 'Title', 'Thread Id',
                                      'Thread Author', 'Domain', 'Expanded URLs', 'Url']
                             )
            # df = df[['Date', 'Hashtags', 'Twitter Author ID', 'Author', 'Url', 'Thread Id', 'Thread Author', 'Domain']]
            df = df.rename(columns=brandwatch_column_dict)
            df_list.append(df)

        start_time = time.time()
        result_df = pd.concat(df_list)
        end_time = time.time()
        print(f"{(end_time - start_time) / 60} mins for concat dataframes")

        start_time = time.time()
        result_df.drop_duplicates(subset='source_msg_id', keep="first", inplace=True)
        end_time = time.time()
        print(f"{(end_time - start_time) / 60} mins for drop duplicates")

        start_time = time.time()
        if SAVE_OUTPUT_FILES:
            result_df['platform'].value_counts().rename('users_count').rename_axis('platform').to_csv(
                OUTPUT_PLATFORM_COUNTS_FILE)
        result_df = result_df[result_df['platform'].isin(filter_platform_domain_set)]
        end_time = time.time()
        print(f"{(end_time - start_time) / 60} mins for filtering platforms")

        result_df.reset_index(drop=True, inplace=True)
        print(result_df.shape)
        return result_df
