import os
from io import StringIO

import boto3
import pandas as pd
from dateutil.parser import parse

from aws.s3 import create_s3_session


def load_from_s3(date_str):
    """Load raw object from s3

    Args:
        date_str ([type]): [description]
    Returns:
        [df]: raw df
    """
    # date_str = "2020-01-05"
    try:
        s3 = create_s3_session()
        date_dt = parse(date_str)
        date_param = date_dt.strftime("%Y/%m/%d")
        response = s3.get_object(
            Bucket="malaysia-stock-eod-data", Key=f"mplus/{date_param}/data.csv"
        )
        csv_txt = response["Body"].read().decode("cp1256")
        df = pd.read_csv(StringIO(csv_txt), encoding="cp1256")
        return df
    except Exception as err:
        print("Error @load_from_s3:", err)


def parse_keypath(key):
    """Parse keypath

    Args:
        key ([str]): path in s3 (key)

    Returns:
        [str]: date_str
    """
    return "".join(key.split("/")[1:4])


def get_latest_keypath(n=-1):
    """Get the last `n` date_str

    Args:
        n (int, optional): last `n`. Defaults to -1.

    Returns:
        [str]: date_str
    """
    s3 = create_s3_session()
    keys = [
        obj["Key"]
        for obj in s3.list_objects(Bucket="malaysia-stock-eod-data")["Contents"]
        if obj["Key"].split("/")[0] == "mplus"
    ]
    parsed_keys = [int(parse_keypath(key)) for key in keys]
    parsed_keys.sort()
    return str(parsed_keys[n])
