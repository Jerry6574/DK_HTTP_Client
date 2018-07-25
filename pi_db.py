import sqlite3


class PIDB:

    def __init__(self, db_path):
        self.db_path = db_path

    def create_tables(self):
