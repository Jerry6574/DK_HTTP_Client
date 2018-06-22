from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
import re
import time
from utils import get_soup
import numpy as np

SUPPLIER_PATH = r"data/supplier.xlsx"
DIGIKEY_HOME_PAGE = "https://www.digikey.com"
PG_PATH = r"data/pg.xlsx"
SPG_PATH = r"data/spg.xlsx"
SUPPLIER_SPG_PATH = r'data/supplier_spg.xlsx'
PRODUCT_INDEX_URL = 'https://www.digikey.com/products/en'
SUPPLIER_CENTER_URL = 'https://www.digikey.com/en/supplier-centers'
CHROMEDRIVER_PATH = r"lib\chromedriver"


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
    has columns=['supplier_name', 'supplier_url', 'supplier_code', 'supplier_id']
    """
    browser = init_webdriver()
    browser.get(supplier_center_url)

    # Collect a list of supplier anchor tags.
    supplier_anchors = browser.find_elements_by_class_name('supplier-listing-link')

    # Extract a list supplier_name and supplier_url from supplier_anchors.
    supplier_list = []

    for anchor in supplier_anchors:
        supplier_name = anchor.text.replace('/', '_')
        supplier_url = anchor.get_attribute('href')
        supplier_url_key = supplier_url.split('/')[-1]
        supplier_list.append([supplier_name, supplier_url, supplier_url_key])

    browser.quit()

    supplier_df = pd.DataFrame(supplier_list, columns=['supplier_name', 'supplier_url', 'supplier_url_key'])
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

    pg_df = pd.DataFrame(pg_list, columns=['product_group', 'pg_url', 'pg_url_key'])
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
    return spg_df


def get_supplier_spg_df(supplier_path=SUPPLIER_PATH, spg_path=SPG_PATH):
    """
    supplier_df and spg_df have N:M relationship. Create a join table between them. 
    :return: supplier_spg_df, has columns=['supplier_spg_id', 'supplier_id', 'spg_id']
    """
    supplier_df = pd.read_excel(supplier_path)[['supplier_id', 'supplier_url', 'supplier_code']]
    supplier_df = supplier_df[pd.notnull(supplier_df['supplier_code'])]
    supplier_spg_df = pd.DataFrame(columns=['spg_url_key', 'supplier_id'])

    spg_df = pd.read_excel(spg_path)[['spg_url_key', 'spg_id']]

    for supplier_id_url in supplier_df.itertuples(index=True, name='Pandas'):
        supplier_id = getattr(supplier_id_url, 'supplier_id')
        supplier_url = getattr(supplier_id_url, 'supplier_url')

        _, supplier_soup = get_soup(supplier_url)
        all_li = supplier_soup.find('table', attrs={'id': 'table_arw_wrapper'}).find_all('li')
        temp_spg_list = [li.find('a')['href'].split('/')[-2] for li in all_li]
        temp_spg_df = pd.DataFrame(temp_spg_list, columns=['spg_url_key'])
        temp_spg_df['supplier_id'] = supplier_id
        supplier_spg_df = supplier_spg_df.append(temp_spg_df)

        time.sleep(0.1)

    supplier_spg_df = supplier_spg_df.merge(spg_df, on='spg_url_key', how='left')
    supplier_spg_df.drop(columns=['spg_url_key'])

    supplier_spg_df.index += 1
    supplier_spg_df['supplier_spg_id'] = supplier_spg_df.index

    return supplier_spg_df


def get_prelim_data():
    # supplier_df = get_supplier_df()
    # supplier_df.to_excel(SUPPLIER_PATH)
    #
    # pg_df = get_pg_df()
    # pg_df.to_excel(PG_PATH)
    #
    # spg_df = get_spg_df()
    # spg_df.to_excel(SPG_PATH)

    supplier_spg_df = get_supplier_spg_df()
    supplier_spg_df.to_excel('supplier_spg.xlsx')


def main():
    get_prelim_data()


if __name__ == '__main__':
    main()
