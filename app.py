import json
import os

from aws.checker import duplicate_check
from aws.db_writer import clean_data
from aws.s3_loader import get_latest_keypath, load_from_s3
from aws.s3_writer import write_clean_df


def handler(event, context):
    # Load S3 latest csv
    # AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    # AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

    ## From event, obtained latest csv object in S3
    keypath_today = get_latest_keypath(n=-1)  # or obtained from event

    ## Clean: df_today_cleaned
    df_today_cleaned = clean_data(keypath_today)

    # Check if duplicates (holiday)
    ## Load previous day data
    keypath_prev = get_latest_keypath(n=-2)  # or obtained from event

    ## Clean: df_prev_cleaned
    df_prev_cleaned = clean_data(keypath_prev)

    ## Compare df_today_cleaned vs df_prev_cleaned
    is_duplicated = duplicate_check(df_today_cleaned, df_prev_cleaned)

    # If not duplicates
    if not is_duplicated:
        ## Write df_today_cleaned to s3
        write_clean_df(df_today_cleaned)

    body = {
        "message": "Success V3: Clean and insert into DB. Date: {}".format(
            keypath_today
        ),
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
