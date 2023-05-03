import configparser
import os.path
import typing

import boto3


class S3Access:

    @staticmethod
    def get_default_credentials_path():
        return os.path.join(os.path.expanduser("~"), ".aws/credentials")

    def __init__(self, in_section=None, in_credentials_file=None, in_region_name='us-east-1'):
        section = 'default' if in_section is None else in_section
        creds_file = S3Access.get_default_credentials_path() if in_credentials_file is None else in_credentials_file
        cp = configparser.ConfigParser()
        cp.read(creds_file)
        self.s3 = boto3.resource(
            service_name='s3',
            region_name=in_region_name,
            aws_access_key_id=cp[section]["aws_access_key_id"],
            aws_secret_access_key=cp[section]["aws_secret_access_key"],
            aws_session_token=cp[section]["aws_session_token"]
        )

    def get_buckets_list(self) -> typing.List[str]:
        """
        Get the list of buckets in the s3.

        Returns
        -------
            List of buckets as a list of strings.
        """
        buckets_list = [bucket.name for bucket in self.s3.buckets.all()]
        return buckets_list

    def get_keys_list(self, in_bucket_name: str, in_sub_folder_path: str) -> typing.List[str]:
        """
                Get the list of keys in a sub-folder of an s3 bucket, filtered by a custom filtering function.

                Parameters
                ----------
                in_bucket_name :
                    Name of the bucket
                    Example:
                        "mips-main"
                in_sub_folder_path :
                    Name of the subfolder without "/" at beginning or end of the string.
                    Example:
                        "initial_data_collection/raw_data/brandwatch"

                Returns
                -------
                    The list of keys from the sub-folder of the bucket.

                Examples
                --------
                The following will get all keys from the
                s3 location : "s3://mips-main/initial_data_collection/raw_data/brandwatch/"

                >>> s3a = S3Access()
                >>> s3a.get_keys_list("mips-main", "initial_data_collection/raw_data/brandwatch")
                ['initial_data_collection/raw_data/brandwatch/2018_03_13_to_2018_03_13_file1.csv.zip',
                'initial_data_collection/raw_data/brandwatch/2018_03_14_to_2018_03_14_file2.csv.zip',
                'initial_data_collection/raw_data/brandwatch/2018_03_14_to_2018_03_14_file3.xyz']
                """
        bucket = self.s3.Bucket(in_bucket_name)
        key_list = [object_summary.key for object_summary in bucket.objects.filter(Prefix=in_sub_folder_path)]
        return key_list

    def get_filtered_keys_list(self, in_bucket_name: str, in_sub_folder_path: str,
                               in_filter_function: typing.Callable[[str], bool]) -> typing.List[str]:
        """
        Get the list of keys in a sub-folder of an s3 bucket, filtered by a custom filtering function.

        Parameters
        ----------
        in_bucket_name :
            Name of the bucket
            Example:
                "mips-main"
        in_sub_folder_path :
            Name of the subfolder without "/" at beginning or end of the string.
            Example:
                "initial_data_collection/raw_data/brandwatch"
        in_filter_function :
            A function that takes a string as an input and returns a bool.
            If and only if the return value is True for the input object, it will be included in the returned keys list.
                Example:
                    Pass the following lambda function to filter keys that end with ".csv" string pattern:
                    `lambda x: x.endswith(".csv")`

        Returns
        -------
            The list of keys from the sub-folder of the bucket that match the filter

        Examples
        --------
        The following will get all keys that end with ".csv.zip" files from the
        s3 location : "s3://mips-main/initial_data_collection/raw_data/brandwatch/"

        >>> s3a = S3Access()
        >>> s3a.get_filtered_keys_list("mips-main", "initial_data_collection/raw_data/brandwatch", lambda x: x.endswith(".csv.zip"))
        ['initial_data_collection/raw_data/brandwatch/2018_03_13_to_2018_03_13_file1.csv.zip',
        'initial_data_collection/raw_data/brandwatch/2018_03_14_to_2018_03_14_file2.csv.zip']
        """
        bucket = self.s3.Bucket(in_bucket_name)
        key_list = [object_summary.key for object_summary in bucket.objects.filter(Prefix=in_sub_folder_path)
                    if in_filter_function(object_summary.key)]
        return key_list
