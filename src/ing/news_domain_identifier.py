import re

class NewsDomainIdentifier:
    """
    Given a colleciton of news domains, finds all matching news domains for given text.
    """
    
    def __init__(self, in_news_domains_set, in_full_links_only=True):
        self.pattern_to_news_domain_name = {}
        for nd in in_news_domains_set:
            # high priority for complete match
            self.pattern_to_news_domain_name[re.compile(nd)] = (nd, 10000)
            if not in_full_links_only:
                # partial matches computed only if in_full_links_only set to False
                valid_split_strs = set(nd.split('.'))
                for sp in valid_split_strs:
                    if len(sp) > 4:
                        # low priority for partial match
                        self.pattern_to_news_domain_name[re.compile(sp)] = (nd, 1)

    def find_all_matches(self, in_text):
        all_matches = []
        for pattern in self.pattern_to_news_domain_name:
            match_obj = pattern.search(in_text)
            if match_obj:
                priority = (match_obj.end() - match_obj.start()) * self.pattern_to_news_domain_name[pattern][1]
                all_matches.append( [self.pattern_to_news_domain_name[pattern][0], pattern, priority] )
        # sort by priority
        all_matches = sorted(all_matches, key=lambda x: x[2], reverse=True)
        # return only the matched domains
        return [e[0] for e in all_matches]

