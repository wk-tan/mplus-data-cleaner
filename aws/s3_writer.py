import glob
import os

import boto3
import pandas as pd
from dateutil.parser import parse

from aws.s3 import create_s3_session


def write_to_s3(csv_filepath):
    """Write csv file to s3 (on Asus Laptop)

    Args:
        csv_filepath ([str]): csv_filepath
    """
    try:
        s3 = create_s3_session()
        basename = os.path.basename(csv_filepath)
        date_str = basename.split(".")[0].split("_")[0]
        date_dt = parse(date_str)
        date_param = date_dt.strftime("%Y/%m/%d")
        response = s3.upload_file(
            Filename=csv_filepath,
            Bucket="malaysia-stock-eod-data",
            Key=f"mplus/{date_param}/data.csv",
        )
    except Exception as err:
        print("Error @write_to_s3:", err)


def mass_write(path):
    """Write all mplus csv files to S3 in the directory `path`

    Args:
        path ([str]): mplus data directory
    """
    csv_files = glob.glob(os.path.join(path, "2*.csv"))
    for csv_filepath in csv_files:
        write_to_s3(csv_filepath)
