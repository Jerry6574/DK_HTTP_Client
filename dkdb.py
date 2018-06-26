import pandas as pd
import sqlite3
import preprocessing
import os

DB_PATH = r"db/dkdb.db"


class DKDB:

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        if not os.path.exists(db_path):
            DKDB.create_tables(self)
            DKDB.insert_all(self)

    def create_tables(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS product_group("
                       "pg_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, "
                       "pg TEXT NOT NULL, "
                       "pg_url TEXT NOT NULL, "
                       "pg_url_key TEXT NOT NULL)")

        cursor.execute("CREATE TABLE IF NOT EXISTS sub_product_group("
                       "spg_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, "
                       "pg_id INTEGER NOT NULL, "
                       "spg TEXT NOT NULL, "
                       "spg_url TEXT NOT NULL, "
                       "spg_url_key TEXT NOT NULL, "
                       "FOREIGN KEY (pg_id) REFERENCES product_group(pg_id))")

        cursor.execute("CREATE TABLE IF NOT EXISTS supplier("
                       "supplier_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, "
                       "supplier TEXT NOT NULL, "
                       "supplier_url TEXT NOT NULL, "
                       "supplier_url_key TEXT NOT NULL, "
                       "supplier_code TEXT NOT NULL)")

        cursor.execute("CREATE TABLE IF NOT EXISTS supplier_spg("
                       "supplier_spg_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, "
                       "supplier_id INTEGER NOT NULL, "
                       "spg_id INTEGER NOT NULL, "
                       "FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id), "
                       "FOREIGN KEY (spg_id) REFERENCES sub_product_group(spg_id))")
        conn.commit()
        cursor.close()
        conn.close()

    def insert_pg(self, pg_path=preprocessing.PG_PATH):
        conn = sqlite3.connect(self.db_path)

        pg_df = pd.read_excel(pg_path)
        pg_df.to_sql('product_group', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_spg(self, spg_path=preprocessing.SPG_PATH):
        conn = sqlite3.connect(self.db_path)

        spg_df = pd.read_excel(spg_path)
        spg_df.to_sql('sub_product_group', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_supplier(self, supplier_path=preprocessing.SUPPLIER_PATH):
        conn = sqlite3.connect(self.db_path)

        supplier_df = pd.read_excel(supplier_path)
        supplier_df.dropna(how='any', inplace=True)
        supplier_df.to_sql('supplier', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_supplier_spg(self, supplier_spg_path=preprocessing.SUPPLIER_SPG_PATH):
        conn = sqlite3.connect(self.db_path)

        supplier_spg_df = pd.read_excel(supplier_spg_path)
        supplier_spg_df.dropna(how='any', inplace=True)
        supplier_spg_df.to_sql('supplier_spg', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_all(self):
        DKDB.insert_pg(self)
        DKDB.insert_spg(self)
        DKDB.insert_supplier(self)
        DKDB.insert_supplier_spg(self)

    def inner_join_pg_spg(self):
        conn = sqlite3.connect(self.db_path)
        inner_join_sql = "SELECT sub_product_group.spg_id, " \
                         "product_group.pg_url_key, " \
                         "sub_product_group.spg_url, " \
                         "sub_product_group.spg_url_key FROM " \
                         "product_group INNER JOIN sub_product_group " \
                         "ON sub_product_group.pg_id = product_group.pg_id"
        inner_join_pg_spg_df = pd.read_sql(inner_join_sql, conn)

        conn.commit()
        conn.close()
        return inner_join_pg_spg_df


def main():
    dkdb = DKDB()
    inner_join_pg_spg_df = dkdb.inner_join_pg_spg()
    inner_join_pg_spg_df.to_excel("data/inner_join_pg_spg.xlsx")


if __name__ == '__main__':
    main()
