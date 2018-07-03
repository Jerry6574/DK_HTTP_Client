import pandas as pd
import sqlite3
import metadata

DB_PATH = r"db/metadb.db"


class MetaDB:

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

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

    def insert_pg(self, pg_path=metadata.PG_PATH):
        conn = sqlite3.connect(self.db_path)

        pg_df = pd.read_excel(pg_path)
        pg_df.to_sql('product_group', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_spg(self, spg_path=metadata.SPG_PATH):
        conn = sqlite3.connect(self.db_path)

        spg_df = pd.read_excel(spg_path)
        spg_df.to_sql('sub_product_group', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_supplier(self, supplier_path=metadata.SUPPLIER_PATH):
        conn = sqlite3.connect(self.db_path)

        supplier_df = pd.read_excel(supplier_path)
        supplier_df.dropna(how='any', inplace=True)
        supplier_df.to_sql('supplier', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_supplier_spg(self, supplier_spg_path=metadata.SUPPLIER_SPG_PATH):
        conn = sqlite3.connect(self.db_path)

        supplier_spg_df = pd.read_excel(supplier_spg_path)
        supplier_spg_df.dropna(how='any', inplace=True)
        supplier_spg_df.to_sql('supplier_spg', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    def insert_all(self):
        MetaDB.insert_pg(self)
        MetaDB.insert_spg(self)
        MetaDB.insert_supplier(self)
        MetaDB.insert_supplier_spg(self)


def main():
    metadb = MetaDB()
    metadb.insert_all()


if __name__ == '__main__':
    main()
