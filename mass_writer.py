"""Mass write raw and clean df to s3"""

import glob
import os

import pandas as pd

from aws.checker import duplicate_check
from aws.s3 import create_s3_session
from aws.s3_task import find_prev_date_str, list_exist_date_str, load_and_clean_data
from aws.s3_writer import write_clean_df, write_to_s3_from_file

s3 = create_s3_session()

csvs_path = r"C:\Users\jy\GoogleDrive2\_mpdata\_mpdata"
csvs = glob.glob(os.path.join(csvs_path, "2*.csv"))

for j, current_csv in enumerate(csvs):
    # load and clean current df
    current_df = pd.read_csv(current_csv, encoding="cp1256")
    current_date_str = os.path.basename(current_csv).split(".")[0].split("_")[0]
    current_clean_df = load_and_clean_data(current_date_str, current_df)

    # check if current raw and clean dfs in s3
    is_exist_raw = current_date_str in list_exist_date_str(root_dir="mplus")
    is_exist_clean = current_date_str in list_exist_date_str(root_dir="clean_mplus")
    if is_exist_raw and is_exist_clean:
        print("Skip:", current_csv)
        continue

    # find and clean prev df, then check if duplicated (holiday)
    prev_date_str = find_prev_date_str(current_date_str)
    if prev_date_str is not None:
        prev_clean_df = load_and_clean_data(prev_date_str)
        is_duplicated = duplicate_check(current_clean_df, prev_clean_df)
    else:
        is_duplicated = False

    if not is_duplicated:
        # write raw and clean df to s3
        write_to_s3_from_file(current_csv)
        write_clean_df(current_clean_df)
        print("Done:", current_csv)
    else:
        print("Duplicated (Potential Holiday):", current_date_str)
