import multiprocessing as mp
from functools import partial
import os
import pandas as pd
import time


def mp_func(func, iterable, sec_arg=None, has_return=True):
    pool = mp.Pool(4)
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


def concat_spg_csv(abs_spg_dir):

    df_list = []
    for root, dirs, files in os.walk(abs_spg_dir):
        for name in files:
            csv_file = os.path.join(root, name)
            print("Processing", csv_file)
            df_list.append(pd.read_csv(csv_file))

    concat_df = pd.concat(df_list, ignore_index=True)
    concat_filename = "_".join(abs_spg_dir.split('\\')[-2:]) + ".xlsx"

    concat_df.to_excel(os.path.join(abs_spg_dir, concat_filename))


def main():

    abs_spg_dirs = get_abs_spg_dirs(r"C:\Users\jerryw\Desktop\06-29-18 Product Index")
    mp_func(concat_spg_csv, abs_spg_dirs, has_return=False)


if __name__ == '__main__':
    t1 = time.time()
    main()
    t2 = time.time() - t1
    print("Took", t2, "seconds")
