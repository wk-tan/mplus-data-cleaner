import os
from io import StringIO

from dateutil.parser import parse

from aws.s3 import create_s3_session

s3 = create_s3_session()


def write_clean_df(clean_df):
    """Write clean df to s3

    Args:
        clean_df ([df]): clean_df
    """
    date_param = clean_df["Date"][0].strftime("%Y/%m/%d")
    csv_buffer = StringIO()
    clean_df.to_csv(csv_buffer)
    s3.put_object(
        Body=csv_buffer.getvalue(),
        Bucket="malaysia-stock-eod-data",
        Key=f"clean_mplus/{date_param}/data.csv",
    )


def write_to_s3_from_file(csv_filepath):
    """Write csv file to s3 given csv file path (on Asus Laptop/mass writer)

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
        print("Error @write_to_s3_from_file:", err)
