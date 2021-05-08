from aws.checker import duplicate_check
from aws.db_writer import clean_data, insert_data
from aws.s3_loader import get_latest_keypath, load_from_s3

keypath_today = get_latest_keypath(n=-1)  # or obtained from event
print(keypath_today)
df_today_cleaned = clean_data(keypath_today)
print(df_today_cleaned)


[JYLocal] -> [S3] -> [Lambda] -> [S3] -> 
