from typing import Tuple, Union, List, Set

import requests
import re
import multiprocessing


def request_resolve_url(in_short_url: str) -> Tuple[str, Union[str, None], int]:
    """
    Resolves given URL by using a get request.

    Parameters
    ----------
    in_short_url :
        The URL

    Returns
    -------
        The input URL and the received full URL if the URL is valid, and status code

    """
    try:
        response = requests.get(in_short_url)
    except requests.exceptions.RequestException:
        return in_short_url, None, -1

    return in_short_url, response.url, response.status_code


class URLExpander:

    def __init__(self):
        """
        basic_url_pattern :
            Regex for identifying basic structure of a URL from a webpage text
            The regex contains named groups for ease of understanding.

                (?P<Protocol>https?://)?(?P<Host>[A-z][A-z0-9-]*\.[A-z][A-z0-9-.]*)(?P<PathAndSearch>/[^<>\"#{}|\\^~[\]`\s]*)?

            HTTP Pattern is :
                http://<host>:<port>/<path>?<searchpart>

                Source: RFC 1738: https://datatracker.ietf.org/doc/html/rfc1738#autoid-14

        """
        basic_url_pattern = r"(?P<Protocol>https?://)?(?P<Host>[A-z][A-z0-9-]*\.[A-z][A-z0-9-.]*)(?P<PathAndSearch>/[^<>\"#{}|\\^~[\]`\s]*)?"

        remove_wbr_breaks_pattern = r"<\s*[Ww][Bb][Rr]\s*/?\s*>"

        no_host_pattern = r"https?://"

        self.re_wbr_pattern = re.compile(remove_wbr_breaks_pattern)
        self.re_basic_url = re.compile(basic_url_pattern)
        self.re_no_host_prefix = re.compile(no_host_pattern)
        self.all_potential_urls = set()
        self.msgid_to_potential_urls = {}
        self.potential_url_to_resolved_url = {}

    def get_article_urls(self, in_msg_id):
        #============================ SHOULD BE DEVELPED TO USE FOLLOWING LINE!!!=====================================#
        # return [self.potential_url_to_resolved_url[purl] for purl in self.msgid_to_potential_urls[in_msg_id]]
        return self.msgid_to_potential_urls[in_msg_id]

    def consume_potential_urls_from_text(self, in_msg_id: Union[int, str], in_text: str) -> None:
        """
        Updates the URLExpander object with the existence of msg_id, text pairs.

        Parameters
        ----------
        in_msg_id :
            String or int as an identifier that will identify the message which the text is related to.
        in_text :
            The text to be searched for URLs.
        """
        # remove <wbr> tags
        no_wbr_text = self.re_wbr_pattern.sub("", in_text)
        # find basic url pattern
        url_like_strings = [match_obj.group() for match_obj in self.re_basic_url.finditer(no_wbr_text)]
        # append http:// if not present
        fixed_potential_urls = self.fix_issues_of_potential_urls(url_like_strings)
        self.all_potential_urls.update(fixed_potential_urls)
        self.msgid_to_potential_urls[in_msg_id] = fixed_potential_urls

    def fix_issues_of_potential_urls(self, in_potential_urls: List[str]) -> List[str]:
        """
        Adds the http:// part to the potential_url strings in the list if they don't have it.
        """
        return [u if self.re_no_host_prefix.match(u) else f"http://{u}" for u in in_potential_urls]

    def resolve_potential_urls_list(self):
        """
        Resolves the urls using get requests to find the expanded version of urls.
        """
        with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
            results = p.map(request_resolve_url, list(self.all_potential_urls))
            for short_url, long_url, status_code in results:
                self.potential_url_to_resolved_url[short_url] = long_url
