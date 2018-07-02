import sqlite3
import pandas as pd
import dkdb
from preprocessing import get_num_page
import preprocessing
import os
import datetime


DESKTOP = os.path.join(os.environ['USERPROFILE'], 'Desktop')
INNER_JOIN_PATH = r"data/inner_join_df.xlsx"


def query_spg_by_supplier(db_path, supplier_id):
    """
    Create an inner join DataFrame suited for DKDL class where DataFrame is filtered by supplier_id.
    """
    conn = sqlite3.connect(db_path)
    inner_join_sql = "SELECT sub_product_group.spg_id, " \
                     "product_group.pg_url_key, " \
                     "sub_product_group.spg_url, " \
                     "sub_product_group.spg_url_key, " \
                     "supplier.supplier_code " \
                     "FROM product_group INNER JOIN sub_product_group " \
                     "ON sub_product_group.pg_id = product_group.pg_id " \
                     "INNER JOIN supplier_spg ON sub_product_group.spg_id = supplier_spg.spg_id " \
                     "INNER JOIN supplier ON supplier.supplier_id = supplier_spg.supplier_id " \
                     "WHERE supplier_spg.supplier_id = {0}".format(supplier_id)

    inner_join_df = pd.read_sql(inner_join_sql, conn)
    inner_join_df = preprocessing.select_active_spg(inner_join_df)
    inner_join_df = get_num_page(inner_join_df)

    conn.commit()
    conn.close()

    return inner_join_df


class DKDL:
    def __init__(self, chromedriver_path=preprocessing.CHROMEDRIVER_PATH,
                 product_index_dir=os.path.join(DESKTOP, "product_index_" + datetime.datetime.now().strftime("%Y%m%d-%H%M")),
                 inner_join_path=INNER_JOIN_PATH):
        self.chromedriver_path = chromedriver_path
        self.product_index_dir = product_index_dir
        self.inner_join_path = inner_join_path


def main():
    supplier_id = 80
    inner_join_df = query_spg_by_supplier(dkdb.DB_PATH, supplier_id=supplier_id)
    inner_join_df.to_excel(INNER_JOIN_PATH)


if __name__ == '__main__':
    main()
