from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import requests
import bs4


def get_soup(url):
    print(url)
    session = requests.Session()
    retry = Retry(connect=5, backoff_factor=2)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    req = session.get(url)
    soup = bs4.BeautifulSoup(req.content, 'lxml')
    print(req.status_code)
    return req.status_code, soup

