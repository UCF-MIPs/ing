import re
from typing import List


class NewsDomainIdentifier:
    """
    Given a colleciton of news domains, finds all matching news domains for given text.
    """

    def __init__(self, in_all_news_domains: List[str], in_full_links_only: bool = True):
        self.pattern_to_news_domain_priority_pair = {}
        for news_domain in in_all_news_domains:
            # high priority for complete match
            self.pattern_to_news_domain_priority_pair[re.compile(news_domain)] = (news_domain, 10000)
            if not in_full_links_only:
                # partial matches computed only if in_full_links_only set to False
                self.__compute_partial_matches(news_domain)

    def __compute_partial_matches(self, news_domain: str):
        partitioned_news_domain = set(news_domain.split('.'))
        for a_part_of_news_domain in partitioned_news_domain:
            if len(a_part_of_news_domain) > 4:
                # low priority for partial match
                self.pattern_to_news_domain_priority_pair[re.compile(a_part_of_news_domain)] = (news_domain, 1)

    def find_all_matches(self, in_url_string: str) -> List[str]:
        """
        Returns a list of all matching news domains ordered such that most matching news domain is at the 0th index.

        Parameters
        ----------
        in_url_string :
            The URL from which you need to find the domain.
        Returns
        -------
            A list of domain names ordered by best match first, the least matching last.
        """
        all_matches = []
        for pattern in self.pattern_to_news_domain_priority_pair:
            match_obj = pattern.search(in_url_string)
            if match_obj:
                matched_object = self.pattern_to_news_domain_priority_pair[pattern]
                priority = (match_obj.end() - match_obj.start()) * matched_object[1]
                all_matches.append([matched_object[0], pattern, priority])
        # sort by priority
        all_matches = sorted(all_matches, key=lambda x: x[2], reverse=True)
        # return only the matched domains
        return [e[0] for e in all_matches]
