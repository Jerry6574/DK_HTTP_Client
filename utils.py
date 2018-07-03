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


def main():
    abs_spg_dirs = get_abs_spg_dirs(r"C:\Users\jerryw\Desktop\06-29-18 Product Index")
    concat_xl_dir = r"C:\Users\jerryw\Desktop\06-29-18 Product Index Concat XL"
    mp_func(concat_spg_csv, abs_spg_dirs, has_return=False, sec_arg=concat_xl_dir, mode='process')


if __name__ == '__main__':
    t1 = time.time()
    main()
    t2 = time.time() - t1
    print("Took", t2, "seconds")
