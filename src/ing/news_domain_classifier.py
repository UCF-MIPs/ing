from typing import List, Set

import pandas as pd


class NewsDomainClassifier:

    def __init__(self, in_news_domain_classes_df: pd.DataFrame, in_classes_set: Set = None):
        """
        Creates an object that stores the classification of news domains.
        Parameters
        ----------
        in_news_domain_classes_df :
            Dataframe object that contains the two columns: news_domain and class
            class column should contain the class of the corresponding news_domain in that row.
        in_classes_set :
            Set of all classes for verification purposes. If set to None (default value is None), it is assumed that the
            set of classes is {'TF', 'TM', 'UF', 'UM'}.
        """
        if in_classes_set is None:
            in_classes_set = {'TF', 'TM', 'UF', 'UM'}
        self.news_domain_classes_df = in_news_domain_classes_df
        assert set(in_news_domain_classes_df['class'].unique()).issubset(in_classes_set), \
            f"News domains should belong to one of the classes from {in_classes_set}"
        self.news_domain_to_class_dict = in_news_domain_classes_df.set_index('news_domain')['class'].to_dict()

    def get_class(self, in_news_domain: str) -> str:
        return self.news_domain_to_class_dict[in_news_domain]

    def get_domain_list(self) -> List[str]:
        return self.news_domain_to_class_dict.keys()
