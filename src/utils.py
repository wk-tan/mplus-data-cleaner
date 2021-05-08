import os
from io import StringIO

import numpy as np
import pandas as pd
import pendulum
from dateutil.parser import parse

from .aws.s3 import list_objects_v2, put_object, upload_file
from .mploader import LoadMP
from .parser import parse_keypath

CLEAN_MPLUS_S3_DIR = "clean_mplus_v2"


def load_and_clean_data(date_str, input_df=None):
    """
    Load raw object from s3, clean the data and return cleaned_df.
    If input_df (raw df) is given, then skip query from s3.
    Args:
        date_str ([str]): date_str in the format "%Y%m%d, %Y-%m-%d or  %Y/%m/%d"

    Returns:
        [df]: cleaned_df
    """
    cleaned_df = LoadMP(date_str, input_df).fdf
    return cleaned_df


def write_clean_df(clean_df):
    """Write clean df to s3

    Args:
        clean_df ([df]): clean_df
    """
    date_param = clean_df["Date"][0].strftime("%Y/%m/%d")
    csv_buffer = StringIO()
    clean_df.to_csv(csv_buffer, index=False)
    _ = put_object(
        bucket="malaysia-stock-eod-data",
        key=f"{CLEAN_MPLUS_S3_DIR}/{date_param}/data.csv",
        body=csv_buffer.getvalue(),
    )


def write_to_s3_from_file(csv_filepath):
    """Write csv file to s3 given csv file path (on Asus Laptop/mass writer)

    Args:
        csv_filepath ([str]): csv_filepath
    """
    try:
        basename = os.path.basename(csv_filepath)
        date_str = basename.split(".")[0].split("_")[0]
        date_dt = parse(date_str)
        date_param = date_dt.strftime("%Y/%m/%d")
        _ = upload_file(
            bucket="malaysia-stock-eod-data",
            key=f"mplus/{date_param}/data.csv",
            upload_path=csv_filepath,
        )
    except Exception as err:
        print("Error @write_to_s3_from_file:", err)


def duplicate_check(df0, df1):
    """Check identical cleaned_df using volume data:
    1. merge on code,
    2. compare volume,
    3. return true if 99% identical volume
    """
    check_cols = ["Code", "Volume"]
    df0 = df0[check_cols]  # .copy()
    df1 = df1[check_cols]  # .copy()
    dff = pd.merge(df0, df1, left_on="Code", right_on="Code")
    u1 = dff["Volume_x"] == dff["Volume_y"]
    mval = np.mean(u1)
    identical = mval > 0.99
    return identical


def find_prev_date_str(date_str):
    """Find previous (latest) date_str before `date_str`

    Args:
        date_str ([str]): [description]

    Returns:
        [str]: prev_date_str
    """
    date_dt = pendulum.parse(date_str)
    prefix_0 = date_dt.strftime(f"{CLEAN_MPLUS_S3_DIR}/%Y/%m")
    prefix_1 = date_dt.add(months=-1).strftime(f"{CLEAN_MPLUS_S3_DIR}/%Y/%m")
    data_strs_0 = [
        parse_keypath(k["Key"])
        for k in list_objects_v2(bucket="malaysia-stock-eod-data", prefix=prefix_0)
    ]
    data_strs_1 = [
        parse_keypath(k["Key"])
        for k in list_objects_v2(bucket="malaysia-stock-eod-data", prefix=prefix_1)
    ]
    date_strs = data_strs_0 + data_strs_1

    date_dts = pd.to_datetime(date_strs)
    date_dts = date_dts.sort_values()
    prev_dates = date_dts[(date_dts < parse(date_str))]
    if len(prev_dates) > 0:
        return prev_dates[-1].strftime("%Y%m%d")
    else:
        return None
