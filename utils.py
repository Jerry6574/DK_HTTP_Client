from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import requests
import bs4
import preprocessing
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import math


class Queue:
    """
    Implement a FIFO queue data structure.
    """
    def __init__(self, initial_queue=None):
        if initial_queue is None:
            self.items = []
        else:
            self.items = initial_queue

    def empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

    def __str__(self):
        return str(self.items)


def get_soup(url):
    """
    Given a url, send an http request.
    :return: resp.status_code, soup
    resp.status_code is the response's status code, i.e. 200: success, 404: Not Found, 403: Forbidden, etc.
    soup is a BeautifulSoup object of the input url.
    """
    print('url:', url)
    session = requests.Session()
    retry = Retry(connect=5, backoff_factor=2)
    adapter = HTTPAdapter(max_retries=retry)

    session.mount('http://', adapter)
    session.mount('https://', adapter)

    resp = session.get(url)
    soup = bs4.BeautifulSoup(resp.content, 'lxml')

    print('status_code:', resp.status_code)
    return resp.status_code, soup


def get_num_page(inner_join_df_in, chromedriver_path=preprocessing.CHROMEDRIVER_PATH):
    inner_join_df_out = inner_join_df_in
    inner_join_df_out['num_page'] = 0
    inner_join_df_out['index'] = inner_join_df_out.index

    spg_url_list = inner_join_df_out[['index', 'spg_url']].values.tolist()

    spg_url_queue = Queue(spg_url_list)
    enqueue_counter = 0

    while not spg_url_queue.empty():
        spg_id_url = spg_url_queue.dequeue()
        [index, spg_url] = spg_id_url

        chrome_options = Options()
        chrome_options.add_argument("--headless")

        browser = webdriver.Chrome(chrome_options=chrome_options, executable_path=chromedriver_path)

        try:
            browser.get(spg_url)

            num_item = int(browser.find_element_by_id('matching-records-count').text.replace(',', ''))
            num_page = math.ceil(num_item / 500)

            print(spg_url, num_page)
            inner_join_df_out.at[index, 'num_page'] = num_page
        except AttributeError:
            spg_url_queue.enqueue(spg_id_url)
            enqueue_counter += 1
            print('enqueue_counter: ', enqueue_counter)

        browser.quit()

    inner_join_df_out.drop(columns='index', inplace=True)

    return inner_join_df_out
