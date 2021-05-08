import json
from os import F_OK
from socket import SO_RCVBUF

from aws.checker import duplicate_check
from aws.s3_task import find_prev_date_str, list_exist_date_str, load_and_clean_data
from aws.s3_writer import write_clean_df


def handler(event, context):
    # bucket = "malaysia-stock-eod-data"
    # key = "mplus/2021/05/07/data.csv"
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    current_date_str = key.split(sep="/", maxsplit=1)[1].rsplit(sep="/", maxsplit=1)[0]
    current_clean_df = load_and_clean_data(current_date_str)

    # TODO: rethink as this is not scalable
    prev_date_str = find_prev_date_str(current_date_str)
    if prev_date_str is not None:
        prev_clean_df = load_and_clean_data(prev_date_str)
        is_duplicated = duplicate_check(current_clean_df, prev_clean_df)
    else:
        is_duplicated = False

    if not is_duplicated:
        # write raw and clean df to s3
        write_clean_df(current_clean_df)
        print("Done:", current_date_str)
    else:
        print("Duplicated:", current_date_str)

    body = {
        "message": "Success V5: Clean and insert into DB. Date: {}".format(
            current_date_str
        ),
    }
    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
