import json

from aws.checker import duplicate_check
from aws.s3_task import find_prev_date_str, list_exist_date_str, load_and_clean_data
from aws.s3_writer import write_clean_df


def handler(event, context):
    # from event, obtained current_date_str:
    current_date_str = "20210505"  ### FROM EVENT!

    # load current raw df and clean
    current_clean_df = load_and_clean_data(current_date_str)

    # check if current raw and clean dfs in s3
    is_exist_raw = current_date_str in list_exist_date_str(root_dir="mplus")
    is_exist_clean = current_date_str in list_exist_date_str(root_dir="clean_mplus")
    if is_exist_raw and is_exist_clean:
        # Skip processing
        print("Skip:", current_date_str)
    else:
        # find and clean prev df, then check if duplicated (holiday)
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
        "message": "Success V4: Clean and insert into DB. Date: {}".format(
            current_date_str
        ),
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
