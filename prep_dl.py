from utils import Queue, init_webdriver
from selenium.common.exceptions import NoSuchElementException
import math
import pandas as pd
import sqlite3


def get_dl_spg_df(db_path, supplier_id, active_spg=True):
    """
    Create an inner join DataFrame suited for DKDL class where DataFrame is filtered by supplier_id.
    """
    conn = sqlite3.connect(db_path)
    dl_spg_sql = "SELECT sub_product_group.spg_id, " \
                 "product_group.pg_url_key, " \
                 "sub_product_group.spg_url, " \
                 "sub_product_group.spg_url_key, " \
                 "supplier.supplier_code " \
                 "FROM product_group INNER JOIN sub_product_group " \
                 "ON sub_product_group.pg_id = product_group.pg_id " \
                 "INNER JOIN supplier_spg ON sub_product_group.spg_id = supplier_spg.spg_id " \
                 "INNER JOIN supplier ON supplier.supplier_id = supplier_spg.supplier_id " \
                 "WHERE supplier_spg.supplier_id = {0}".format(supplier_id)

    dl_spg_df = pd.read_sql(dl_spg_sql, conn)

    if active_spg:
        dl_spg_df = select_active_spg(dl_spg_df)

    dl_spg_df = get_num_page(dl_spg_df)

    conn.commit()
    conn.close()

    return dl_spg_df


def select_active_spg(dl_spg_df_in):
    dl_spg_df_out = dl_spg_df_in
    dl_spg_df_out['part-status'] = 'Active'
    dl_spg_df_out['index'] = dl_spg_df_out.index

    spg_list = dl_spg_df_out[['index', 'spg_url', 'supplier_code']].values.tolist()
    spg_queue = Queue(spg_list)
    enqueue_counter = 0

    while not spg_queue.empty():
        spg_row = spg_queue.dequeue()
        [index, spg_url, supplier_code] = spg_row
        supplier_spg_url = spg_url + "?v=" + supplier_code

        try:
            supplier_spg_status = find_supplier_spg_status(supplier_spg_url)
            if supplier_spg_status == 'Obsolete':
                dl_spg_df_out.at[index, 'part-status'] = 'Obsolete'

        except NoSuchElementException:
            spg_queue.enqueue(spg_row)
            enqueue_counter += 1
            print('enqueue_counter: ', enqueue_counter)

    dl_spg_df_out = dl_spg_df_out[dl_spg_df_out['part-status'] == 'Active']
    dl_spg_df_out.drop(columns=['index', 'part-status'], inplace=True)
    return dl_spg_df_out


def find_supplier_spg_status(supplier_spg_url):

    browser = init_webdriver(mode='scrape')
    browser.get(supplier_spg_url)
    try:
        part_status = "".join(browser.find_element_by_id('part-status').text.split())
    except NoSuchElementException:

        status_xpath = '//*[@id="prod-att-table"]//*/td' \
                       '[contains(text(), "{0}") or contains(text(), "{1}")]'.format("Obsolete", "Active")

        part_status = "".join(browser.find_element_by_xpath(status_xpath).text.split())

    print('supplier_spg_url', supplier_spg_url, 'is', part_status)
    return part_status


def get_num_page(dl_spg_df_in):
    dl_spg_df_out = dl_spg_df_in
    dl_spg_df_out['num_page'] = 0
    dl_spg_df_out['index'] = dl_spg_df_out.index

    spg_url_list = dl_spg_df_out[['index', 'spg_url']].values.tolist()

    spg_url_queue = Queue(spg_url_list)
    enqueue_counter = 0

    while not spg_url_queue.empty():
        spg_row = spg_url_queue.dequeue()
        [index, spg_url] = spg_row

        browser = init_webdriver(mode='scrape')

        try:
            browser.get(spg_url)

            num_item = int(browser.find_element_by_id('matching-records-count').text.replace(',', ''))
            num_page = math.ceil(num_item / 500)

            print('spg_url', spg_url, 'num_page', num_page)
            dl_spg_df_out.at[index, 'num_page'] = num_page
        except NoSuchElementException:
            spg_url_queue.enqueue(spg_row)
            enqueue_counter += 1
            print('enqueue_counter: ', enqueue_counter)

        browser.quit()

    dl_spg_df_out.drop(columns='index', inplace=True)

    return dl_spg_df_out