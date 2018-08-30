import multiprocessing as mp
from multiprocessing.dummy import Pool
from functools import partial
import os
import pandas as pd
import time
import bs4
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


CHROMEDRIVER_PATH = r"lib/chromedriver"


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


def init_webdriver(chromedriver_path=CHROMEDRIVER_PATH):
    """
    Initialize a chrome webdriver for parsing HTML and interacting with web elements.
    """
    # Launch webdriver as a headless browser.
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    browser = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)

    return browser


def get_soup(url):
    """
    Given a url, send an http request.
    :return: resp.status_code, soup
    resp.status_code is the response's status code, i.e. 200: success, 404: Not Found, 403: Forbidden, etc.
    soup is a BeautifulSoup object of the input url.
    """
    session = requests.Session()
    retry = Retry(connect=5, backoff_factor=2)
    adapter = HTTPAdapter(max_retries=retry)

    session.mount('http://', adapter)
    session.mount('https://', adapter)

    resp = session.get(url)
    soup = bs4.BeautifulSoup(resp.content, 'lxml')

    return resp.status_code, soup


def mp_func(func, iterable, mode, sec_arg=None, has_return=True):
    if mode == 'thread':
        pool = Pool()
    elif mode == 'process':
        pool = mp.Pool()
    else:
        print("Please use a valid mode: 'thread' or 'process'")
        return

    if sec_arg is not None:
        if has_return:
            return pool.map(partial(func, sec_arg), iterable)
        else:
            pool.map(partial(func, sec_arg), iterable)
    else:
        if has_return:
            return pool.map(func, iterable)
        else:
            pool.map(func, iterable)


def get_abs_spg_dirs(import_path):
    relative_pg_dirs = os.listdir(import_path)

    abs_pg_dirs = [os.path.join(import_path, pg_dir) for pg_dir in relative_pg_dirs]
    abs_spg_dirs = []

    for pg_dir in abs_pg_dirs:
        relative_spg_dirs = os.listdir(pg_dir)
        abs_spg_dirs += [os.path.join(pg_dir, relative_spg_dir) for relative_spg_dir in relative_spg_dirs]

    return abs_spg_dirs


def concat_spg_csv(abs_spg_dir, concat_xl_dir):

    df_list = []
    for root, dirs, files in os.walk(abs_spg_dir):
        for name in files:
            csv_file = os.path.join(root, name)
            print("Processing", csv_file)
            df_list.append(pd.read_csv(csv_file))

    concat_df = pd.concat(df_list, ignore_index=True)
    concat_filename = "_".join(abs_spg_dir.split('\\')[-2:]) + ".xlsx"

    concat_df.to_excel(os.path.join(concat_xl_dir, concat_filename))


def df_columns(path):
    print('Reading:', path)
    df = pd.read_excel(path)
    return df.columns.tolist()


def intersect(n_list):
    result = set.intersection(*map(set, n_list))
    return result


def extract_cols(file_path, mode='intersect'):
    intersect_cols = ['Digi-Key Part Number', 'Manufacturer', 'Manufacturer Part Number', 'Quantity Available',
                      'Unit Price (USD)', 'Datasheets', 'Series', 'Part Status', 'Minimum Quantity',
                      'Description', 'Image', '@ qty', 'Factory Stock']
    price_cols = ['Digi-Key Part Number', 'Unit Price (USD)', 'Part Status']

    print("Working on", file_path)
    if mode == 'intersect':
        df_intersect_cols = pd.read_excel(file_path)[intersect_cols]

    elif mode == 'price':
        df_intersect_cols = pd.read_excel(file_path)[price_cols]

    else:
        raise ValueError("Must use intersect or price mode. ")
    return df_intersect_cols


def concat_spg(read_dir):
    relative_dirs = os.listdir(read_dir)
    abs_dirs = [os.path.join(read_dir, spg_dir) for spg_dir in relative_dirs]

    df_list_intersect_cols = mp_func(extract_cols, abs_dirs, mode="process")

    df_concat_intersect_cols = pd.concat(df_list_intersect_cols, ignore_index=True)

    return df_concat_intersect_cols
