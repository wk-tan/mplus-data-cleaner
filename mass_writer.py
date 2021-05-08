"""Mass write raw and clean df to s3"""

import glob
import os

import pandas as pd

from src.utils import (
    duplicate_check,
    find_prev_date_str,
    load_and_clean_data,
    write_clean_df,
)

# from aws.checker import duplicate_check
# from aws.s3 import create_s3_session
# from aws.s3_task import find_prev_date_str, list_exist_date_str, load_and_clean_data
# from aws.s3_writer import write_clean_df, write_to_s3_from_file


# s3 = create_s3_session()

csvs_path = r"C:\Users\jy\GoogleDrive2\_mpdata\_mpdata"
# csvs_path = r"C:\Users\jy\GoogleDrive2\mpdata"

csvs = glob.glob(os.path.join(csvs_path, "2*.csv"))
csvs.sort()

for j, current_csv in enumerate(csvs):
    try:
        # load and clean current df
        current_df = pd.read_csv(current_csv, encoding="cp1256")
        current_date_str = os.path.basename(current_csv).split(".")[0].split("_")[0]
        current_clean_df = load_and_clean_data(current_date_str, current_df)

        # find and clean prev df, then check if duplicated (holiday)
        prev_date_str = find_prev_date_str(current_date_str)
        if prev_date_str is not None:
            prev_clean_df = load_and_clean_data(prev_date_str)
            is_duplicated = duplicate_check(current_clean_df, prev_clean_df)
        else:
            is_duplicated = False

        if not is_duplicated:
            # write raw and clean df to s3
            # write_to_s3_from_file(current_csv)
            write_clean_df(current_clean_df)
            # print("Done:", current_csv, j)
        else:
            print("Duplicated (Potential Holiday):", current_date_str)

    except Exception as err:
        print("ERROR:", err, current_csv)
