import time
from utils import concat_spg


def main():
    # abs_spg_dirs = get_abs_spg_dirs(r"C:\Users\jerryw\Desktop\AWSW and Competitor Product on DK 2018-06-29")
    # concat_xl_dir = r"C:\Users\jerryw\Desktop\07-20-18 Product Index Concat XL"
    #
    # mp_func(concat_spg_csv, abs_spg_dirs, has_return=False, sec_arg=concat_xl_dir, mode='process')

    df_concat_intersect_cols = concat_spg(r"C:\Users\jerryw\Desktop\product_index_20180703-20 concat xl")
    df_concat_intersect_cols = df_concat_intersect_cols[df_concat_intersect_cols['Manufacturer'] == 'Assmann WSW Components']
    df_concat_intersect_cols.to_excel(r"C:\Users\jerryw\Desktop\AWSW on DK.xlsx")


if __name__ == '__main__':
    t1 = time.time()
    main()
    t2 = time.time() - t1
    print("Took", t2, "seconds")
