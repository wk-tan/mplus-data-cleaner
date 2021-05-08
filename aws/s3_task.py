import pandas as pd
from dateutil.parser import parse

from aws.mploader import LoadMP
from aws.s3 import create_s3_session

s3 = create_s3_session()


def parse_keypath(key):
    """Parse keypath

    Args:
        key ([str]): path in s3 (key)

    Returns:
        [str]: date_str
    """
    return "".join(key.split("/")[1:4])


def list_exist_date_str(root_dir="mplus"):
    """List existing date_strs from a root directory in s3

    Args:
        root_dir (str, optional): [description]. Defaults to "mplus".

    Returns:
        [list:str]: [date_strs]
    """
    date_strs = [
        parse_keypath(obj["Key"])
        for obj in s3.list_objects_v2(Bucket="malaysia-stock-eod-data")["Contents"]
        if obj["Key"].split("/")[0] == root_dir
    ]
    return date_strs


def find_prev_date_str(date_str):
    """Find previous (latest) date_str before `date_str`

    Args:
        date_str ([str]): [description]

    Returns:
        [str]: prev_date_str
    """
    date_strs = list_exist_date_str(root_dir="clean_mplus")
    date_dts = pd.to_datetime(date_strs)
    date_dts.sort_values()
    prev_dates = date_dts[(date_dts < parse(date_str))]
    if len(prev_dates) > 0:
        return prev_dates[-1].strftime("%Y%m%d")
    else:
        return None


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
