import sqlite3
import pandas as pd
import dkdb
import preprocessing
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import math


class Queue:
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


INNER_JOIN_PATH = r"data/inner_join_df.xlsx"


def query_by_supplier(db_path, supplier_id):
    """
    Create an inner join DataFrame suited for DKDL class where DataFrame is filtered by supplier_id.
    """
    conn = sqlite3.connect(db_path)
    inner_join_sql = "SELECT sub_product_group.spg_id, " \
                     "product_group.pg_url_key, " \
                     "sub_product_group.spg_url, " \
                     "sub_product_group.spg_url_key FROM " \
                     "product_group INNER JOIN sub_product_group " \
                     "ON sub_product_group.pg_id = product_group.pg_id " \
                     "INNER JOIN supplier_spg ON sub_product_group.spg_id = supplier_spg.spg_id " \
                     "WHERE supplier_spg.supplier_id = {0}".format(supplier_id)
    inner_join_df = pd.read_sql(inner_join_sql, conn)
    inner_join_df = get_num_page(inner_join_df)

    conn.commit()
    conn.close()

    return inner_join_df


def get_num_page(inner_join_df_in, chromedriver_path=preprocessing.CHROMEDRIVER_PATH):
    inner_join_df_out = inner_join_df_in
    inner_join_df_out['num_page'] = 0
    inner_join_df_out['idx'] = inner_join_df_out.index

    spg_id_url_list = inner_join_df_out[['idx', 'spg_url']].values.tolist()

    spg_id_url_queue = Queue(spg_id_url_list)
    enqueue_counter = 0

    while not spg_id_url_queue.empty():
        spg_id_url = spg_id_url_queue.dequeue()
        [spg_id, spg_url] = spg_id_url

        chrome_options = Options()
        chrome_options.add_argument("--headless")

        browser = webdriver.Chrome(chrome_options=chrome_options, executable_path=chromedriver_path)

        try:
            browser.get(spg_url)

            num_item = int(browser.find_element_by_id('matching-records-count').text.replace(',', ''))
            num_page = math.ceil(num_item / 500)

            print(spg_url, num_page)
            inner_join_df_out.at[spg_id, 'num_page'] = num_page
        except AttributeError:
            spg_id_url_queue.enqueue(spg_id_url)
            enqueue_counter += 1
            print('enqueue_counter: ', enqueue_counter)

        browser.quit()

    inner_join_df_out.drop(columns='idx', inplace=True)

    return inner_join_df_out


def main():
    supplier_id = 80
    inner_join_df = query_by_supplier(dkdb.DB_PATH, supplier_id=supplier_id)
    inner_join_df.to_excel(INNER_JOIN_PATH)


if __name__ == '__main__':
    main()
