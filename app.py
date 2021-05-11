import json

from google.cloud import bigquery
from src.google.bq import load_data
from src.google.gcs import upload_blob
from src.utils import (
    duplicate_check,
    find_prev_date_str,
    load_and_clean_data,
    write_clean_df,
)


def handler(event, context):
    # bucket = "malaysia-stock-eod-data"
    # key = "mplus/2021/05/07/data.csv"
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    current_date_str = key.split(sep="/", maxsplit=1)[1].rsplit(sep="/", maxsplit=1)[0]
    current_clean_df = load_and_clean_data(current_date_str)

    prev_date_str = find_prev_date_str(current_date_str)
    if prev_date_str is not None:
        prev_clean_df = load_and_clean_data(prev_date_str)
        is_duplicated = duplicate_check(current_clean_df, prev_clean_df)
    else:
        is_duplicated = False

    if not is_duplicated:
        # write clean df to s3
        write_clean_df(current_clean_df)

        # write clean df to gcs
        gcs_data_path = "cleaned_mplus/{}".format(key.split(sep="/", maxsplit=1)[1])
        upload_blob(
            bucket="malaysia-stock-eod-data",
            blob_name=gcs_data_path,
            data=current_clean_df.to_csv(index=False),
        )

        # fire API to insert gcs latest clean_df to big_query
        load_data(
            source_uri="gs://malaysia-stock-eod-data/" + gcs_data_path,
            target_destination=bigquery.Table(
                table_ref="malaysia-stock-research.malaysia_derivatives.eod_data"
            ),
            source_type="csv",
            write_disposition="append",
        )

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
