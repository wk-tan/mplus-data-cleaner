import glob
import os
import re

import boto3

from aws.mploader import LoadMP
from aws.s3 import create_s3_session
from aws.s3_loader import load_from_s3


def clean_data(date_str):
    """Load raw object from s3, clean the data and return cleaned_df
    Args:
        date_str ([str]): date_str in the format "%Y%m%d, %Y-%m-%d or  %Y/%m/%d"
    """
    cleaned_df = LoadMP(date_str).fdf
    return cleaned_df


def insert_data(cleaned_df):
    """WIP: Insert cleaned_df into db
    Args:
        cleaned_df ([df]): [description]
    """
    pass


def mass_insert():
    """WIP: Mass clean raw data, then insert into db
    Args:
        cleaned_df ([df]): [description]
    """
    s3 = create_s3_session()
    keys = [
        obj["Key"]
        for obj in s3.list_objects(Bucket="malaysia-stock-eod-data")["Contents"]
    ]
    # parse the keys into date_str
    # for loop: clean_data, insert_data
    pass
