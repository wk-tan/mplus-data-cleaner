from io import StringIO

import pandas as pd


def parse_object_to_df(obj_content, encoding="cp1256"):
    """parse object read from s3 to df

    Args:
        obj_content ([bytes]): object read from s3

    Returns:
        [df]: df
    """
    csv_txt = obj_content.decode(encoding)
    df = pd.read_csv(StringIO(csv_txt), encoding=encoding)
    return df


def parse_keypath(key):
    """Parse keypath

    Args:
        key ([str]): path in s3 (key)

    Returns:
        [str]: date_str
    """
    return "".join(key.split("/")[1:4])
