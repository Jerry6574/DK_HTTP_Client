import pandas as pd
import re
import time
from utils import get_soup, init_webdriver, mp_func


SUPPLIER_PATH = r"prelim_data/supplier.xlsx"
DIGIKEY_HOME_PAGE = "https://www.digikey.com"
PG_PATH = r"prelim_data/pg.xlsx"
SPG_PATH = r"prelim_data/spg.xlsx"
SUPPLIER_SPG_PATH = r'prelim_data/supplier_spg.xlsx'
PRODUCT_INDEX_URL = 'https://www.digikey.com/products/en'
SUPPLIER_CENTER_URL = 'https://www.digikey.com/en/supplier-centers'


def get_supplier_df(supplier_center_url=SUPPLIER_CENTER_URL, export=False, export_path=SUPPLIER_PATH):
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

    supplier_df = pd.DataFrame(supplier_list, columns=['supplier', 'supplier_url', 'supplier_url_key'])
    # supplier_codes = []
    # for url in supplier_df['supplier_url']:
    #     supplier_code = get_supplier_code(url)
    #     print('supplier_code:', supplier_code)
    #     supplier_codes.append(supplier_code)
    #     time.sleep(0.1)

    supplier_codes = mp_func(get_supplier_code, supplier_df.supplier_url.tolist(), mode='thread')

    browser.quit()
    supplier_df['supplier_code'] = supplier_codes

    # Set index to start from 1.
    supplier_df.index += 1

    # Insert supplier_id column that has same data as the index.
    # supplier_id to be used for SQL purpose if needed.
    supplier_df['supplier_id'] = supplier_df.index

    if export is True:
        supplier_df.to_excel(export_path)
        return

    return supplier_df


def get_supplier_code(supplier_url):
    """
    Given a supplier_url, acquire its supplier_code by parsing one of the sub-product group url. 
    :return: supplier_code if exist, else 'NaN'
    """
    supplier_code_pattern = re.compile(r'v=.+')
    status_code, soup = get_soup(supplier_url)
    print('supplier_url', supplier_url, '\n', 'status_code', status_code)

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


def get_pg_df(product_index_url=PRODUCT_INDEX_URL, export=False, export_path=PG_PATH):
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

    if export is True:
        pg_df.to_excel(export_path)
        return

    return pg_df


def get_spg_df(pg_path=PG_PATH, export=False, export_path=SPG_PATH):
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
    spg_df.drop(columns='pg_url_key', inplace=True)

    if export is True:
        spg_df.to_excel(export_path)
        return

    return spg_df


def get_supplier_spg_df(supplier_path=SUPPLIER_PATH, spg_path=SPG_PATH, pg_path=PG_PATH,
                        export=False, export_path=SUPPLIER_SPG_PATH):
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

    if export is True:
        supplier_spg_df.to_excel(export_path)
        return

    return supplier_spg_df


def get_all_prelim_data():
    get_supplier_df(export=True)
    get_pg_df(export=True)
    get_spg_df(export=True)
    get_supplier_spg_df(export=True)


def main():
    get_supplier_df(export=True)


if __name__ == '__main__':
    t1 = time.time()
    main()
    runtime = time.time() - t1
    print('Runtime', runtime)
