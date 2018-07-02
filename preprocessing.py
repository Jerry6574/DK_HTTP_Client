from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import re
import time
import math
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import requests
import bs4


SUPPLIER_PATH = r"data/supplier.xlsx"
DIGIKEY_HOME_PAGE = "https://www.digikey.com"
PG_PATH = r"data/pg.xlsx"
SPG_PATH = r"data/spg.xlsx"
SUPPLIER_SPG_PATH = r'data/supplier_spg.xlsx'
PRODUCT_INDEX_URL = 'https://www.digikey.com/products/en'
SUPPLIER_CENTER_URL = 'https://www.digikey.com/en/supplier-centers'
CHROMEDRIVER_PATH = r"lib/chromedriver"


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


def select_active_spg(inner_join_df_in):
    inner_join_df_out = inner_join_df_in
    inner_join_df_out['part-status'] = 'Active'
    inner_join_df_out['index'] = inner_join_df_out.index

    spg_list = inner_join_df_out[['index', 'spg_url', 'supplier_code']].values.tolist()
    spg_queue = Queue(spg_list)
    enqueue_counter = 0

    while not spg_queue.empty():
        spg_row = spg_queue.dequeue()
        [index, spg_url, supplier_code] = spg_row
        supplier_spg_url = spg_url + "?v=" + supplier_code

        try:
            supplier_spg_status = find_supplier_spg_status(supplier_spg_url)
            if supplier_spg_status == 'Obsolete':
                inner_join_df_out.at[index, 'part-status'] = 'Obsolete'

        except NoSuchElementException:
            spg_queue.enqueue(spg_row)
            enqueue_counter += 1
            print('enqueue_counter: ', enqueue_counter)

    inner_join_df_out.drop(columns='index', inplace=True)
    inner_join_df_out = inner_join_df_out[inner_join_df_out['part-status'] == 'Active']

    return inner_join_df_out


def find_supplier_spg_status(supplier_spg_url):

    browser = init_webdriver()
    browser.get(supplier_spg_url)
    try:
        part_status = "".join(browser.find_element_by_id('part-status').text.split())
    except NoSuchElementException:

        status_xpath = '//*[@id="prod-att-table"]//*/td[contains(text(), "{0}") or contains(text(), "{1}")]'.format("Obsolete", "Active")
        part_status = "".join(browser.find_element_by_xpath(status_xpath).text.split())

    print('supplier_spg_url', supplier_spg_url, 'is', part_status)
    return part_status


def get_num_page(inner_join_df_in):
    inner_join_df_out = inner_join_df_in
    inner_join_df_out['num_page'] = 0
    inner_join_df_out['index'] = inner_join_df_out.index

    spg_url_list = inner_join_df_out[['index', 'spg_url']].values.tolist()

    spg_url_queue = Queue(spg_url_list)
    enqueue_counter = 0

    while not spg_url_queue.empty():
        spg_row = spg_url_queue.dequeue()
        [index, spg_url] = spg_row

        browser = init_webdriver()

        try:
            browser.get(spg_url)

            num_item = int(browser.find_element_by_id('matching-records-count').text.replace(',', ''))
            num_page = math.ceil(num_item / 500)

            print('spg_url', spg_url, 'num_page', num_page)
            inner_join_df_out.at[index, 'num_page'] = num_page
        except NoSuchElementException:
            spg_url_queue.enqueue(spg_row)
            enqueue_counter += 1
            print('enqueue_counter: ', enqueue_counter)

        browser.quit()

    inner_join_df_out.drop(columns='index', inplace=True)

    return inner_join_df_out


def init_webdriver(chromedriver_path=CHROMEDRIVER_PATH):
    """
    Initialize a chrome webdriver for parsing HTML and interacting with web elements. 
    """

    # Launch webdriver as a headless browser.
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # Launch webdriver to supplier_center page.
    browser = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)
    return browser


def get_supplier_df(supplier_center_url=SUPPLIER_CENTER_URL):
    """
    Parse HTML of the supplier_center page and build a DataFrame of supplier data.
    :return: supplier_df, supplier DataFrame, 
    has columns=['supplier', 'supplier_url', 'supplier_code', 'supplier_id']
    """
    browser = init_webdriver()
    browser.get(supplier_center_url)

    # Collect a list of supplier anchor tags.
    supplier_anchors = browser.find_elements_by_class_name('supplier-listing-link')

    # Extract a list supplier and supplier_url from supplier_anchors.
    supplier_list = []

    for anchor in supplier_anchors:
        supplier = anchor.text.replace('/', '_')
        supplier_url = anchor.get_attribute('href')
        supplier_url_key = supplier_url.split('/')[-1]
        supplier_list.append([supplier, supplier_url, supplier_url_key])

    browser.quit()

    supplier_df = pd.DataFrame(supplier_list, columns=['supplier', 'supplier_url', 'supplier_url_key'])
    supplier_codes = []
    for url in supplier_df['supplier_url']:
        supplier_code = get_supplier_code(url)
        print('supplier_code:', supplier_code)
        supplier_codes.append(supplier_code)
        time.sleep(0.1)

    supplier_df['supplier_code'] = supplier_codes

    # Set index to start from 1.
    supplier_df.index += 1

    # Insert supplier_id column that has same data as the index.
    # supplier_id to be used for SQL purpose if needed.
    supplier_df['supplier_id'] = supplier_df.index

    return supplier_df


def get_supplier_code(supplier_url):
    """
    Given a supplier_url, acquire its supplier_code by parsing one of the sub-product group url. 
    :return: supplier_code if exist, else 'NaN'
    """
    supplier_code_pattern = re.compile(r'v=.+')
    _, soup = get_soup(supplier_url)
    try:
        # Find first sub-product group url.
        spg_url = soup.find('table', attrs={'id': 'table_arw_wrapper'}).find('li').find('a')['href']

        # Split sub-product group url into a list.
        # Supplier code exists in the last element after "v="
        # [2:] to remove "v=" and keep only the supplier code.
        supplier_code = re.findall(supplier_code_pattern, spg_url.split('/')[-1])[0][2:]

        return supplier_code

    # Supplier code does not exists.
    # Mainly due to mergers and acquisitions.
    except AttributeError:
        return 'NaN'


def get_pg_df(product_index_url=PRODUCT_INDEX_URL):
    """
    Parse PRODUCT_INDEX_URL page and build a DataFrame of product group data. 
    :return: pg_df, product group DataFrame, has columns=['product_group', 'pg_url', 'pg_url_key', 'pg_id]
    """
    _, product_index_soup = get_soup(product_index_url)
    pg_regex = re.compile('catfiltertopitem.*')
    pg_h2 = product_index_soup.find_all('h2', attrs={'class': pg_regex})
    pg_list = []

    for h2 in pg_h2:
        anchor = h2.find('a')
        product_group = anchor.text.replace('/', '_')
        pg_url = DIGIKEY_HOME_PAGE + anchor['href']
        pg_url_key = anchor['href'].split('/')[-2]

        pg_list.append([product_group, pg_url, pg_url_key])

    pg_df = pd.DataFrame(pg_list, columns=['pg', 'pg_url', 'pg_url_key'])
    pg_df.index += 1
    pg_df['pg_id'] = pg_df.index

    return pg_df


def get_spg_df(pg_path=PG_PATH):
    """
    Parse PRODUCT_INDEX_URL page and build a spg_df.
    Then merge pg_id to spg_df on pg_url_key. 
    :return: spg_df, sub-product group DataFrame, 
    has columns=['spg', 'spg_url', 'spg_url_key', 'pg_url_key', 'spg_id', 'pg_id']
    """
    pg_lookup = pd.read_excel(pg_path)[['pg_id', 'pg_url_key']]

    _, product_index_soup = get_soup(PRODUCT_INDEX_URL)

    spg_ul = product_index_soup.find_all('ul', attrs={'class': 'catfiltersub'})

    spg_list = []

    # Collect spg data with spg_list
    for ul in spg_ul:
        spg_anchors = ul.find_all('a')
        for anchor in spg_anchors:
            spg = anchor.text.replace('/', '_')
            spg_url = DIGIKEY_HOME_PAGE + anchor['href']
            spg_url_key = anchor['href'].split('/')[-2]
            pg_url_key = anchor['href'].split('/')[-3]
            spg_list.append([spg, spg_url, spg_url_key, pg_url_key])

    spg_df = pd.DataFrame(spg_list, columns=['spg', 'spg_url', 'spg_url_key', 'pg_url_key'])
    spg_df.index += 1
    spg_df['spg_id'] = spg_df.index

    spg_df = spg_df.merge(pg_lookup, on='pg_url_key', how='left')
    spg_df.drop(column='pg_url_key', inplace=True)
    return spg_df


def get_supplier_spg_df(supplier_path=SUPPLIER_PATH, spg_path=SPG_PATH, pg_path=PG_PATH):
    """
    supplier_df and spg_df have N:M relationship. Create a join table between them. 
    :return: supplier_spg_df, has columns=['supplier_spg_id', 'supplier_id', 'spg_id']
    """
    supplier_df = pd.read_excel(supplier_path)[['supplier_id', 'supplier_url', 'supplier_code']]
    supplier_df = supplier_df[pd.notnull(supplier_df['supplier_code'])]

    # 'pg_url_key' and 'spg_url_key' form a list of unique keys
    supplier_spg_df = pd.DataFrame(columns=['pg_url_key', 'spg_url_key', 'supplier_id'])

    spg_df = pd.read_excel(spg_path)[['spg_url_key', 'spg_id', 'pg_id']]
    pg_df = pd.read_excel(pg_path)[['pg_url_key', 'pg_id']]

    spg_df = spg_df.merge(pg_df, on='pg_id', how='left')

    for supplier_id_url in supplier_df.itertuples(index=True, name='Pandas'):
        supplier_id = getattr(supplier_id_url, 'supplier_id')
        supplier_url = getattr(supplier_id_url, 'supplier_url')

        _, supplier_soup = get_soup(supplier_url)
        all_li = supplier_soup.find('table', attrs={'id': 'table_arw_wrapper'}).find_all('li')
        temp_spg_list = [[li.find('a')['href'].split('/')[-3],
                          li.find('a')['href'].split('/')[-2]] for li in all_li]
        temp_spg_df = pd.DataFrame(temp_spg_list, columns=['pg_url_key', 'spg_url_key'])
        temp_spg_df['supplier_id'] = supplier_id
        supplier_spg_df = supplier_spg_df.append(temp_spg_df)

        time.sleep(0.1)

    supplier_spg_df = supplier_spg_df.merge(spg_df, on=['pg_url_key', 'spg_url_key'], how='left')
    supplier_spg_df.drop(columns=['pg_url_key', 'spg_url_key', 'pg_id'], inplace=True)

    supplier_spg_df.index += 1
    supplier_spg_df['supplier_spg_id'] = supplier_spg_df.index

    return supplier_spg_df


def get_prelim_data_all():
    supplier_df = get_supplier_df()
    supplier_df.to_excel(SUPPLIER_PATH)

    pg_df = get_pg_df()
    pg_df.to_excel(PG_PATH)

    spg_df = get_spg_df()
    spg_df.to_excel(SPG_PATH)

    supplier_spg_df = get_supplier_spg_df()
    supplier_spg_df.to_excel(SUPPLIER_SPG_PATH)


def get_prelim_data_pg():
    pg_df = get_pg_df()
    pg_df.to_excel(PG_PATH)


def get_prelim_data_spg():
    try:
        spg_df = get_spg_df()
        spg_df.to_excel(SPG_PATH)

    except FileNotFoundError:
        get_prelim_data_pg()
        spg_df = get_spg_df()
        spg_df.to_excel(SPG_PATH)


def get_prelim_data_supplier():
    supplier_df = get_supplier_df()
    supplier_df.to_excel(SUPPLIER_PATH)


def get_prelim_data_supplier_spg():
    try:
        supplier_spg_df = get_supplier_spg_df()
        supplier_spg_df.to_excel(SUPPLIER_SPG_PATH)

    except FileNotFoundError:
        get_prelim_data_all()


def get_prelim_data(mode='All'):
    """
    Execute all functions that get preliminary data in the order of correct dependency. 
    """
    if mode == 'All':
        get_prelim_data_all()

    elif mode == 'supplier':
        get_prelim_data_supplier()

    elif mode == 'pg':
        get_prelim_data_pg

    elif mode == 'spg':
        get_prelim_data_spg()

    elif mode == 'supplier_spg':
        get_prelim_data_supplier_spg()

    else:
        print('Mode "{0}" selection was invalid.'.format(mode))
        print('Please try again')


def main():
    get_prelim_data()


if __name__ == '__main__':
    main()
