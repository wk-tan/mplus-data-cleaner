import numpy as np
import pandas as pd


def duplicate_check(df0, df1):
    """Check identical cleaned_df using volume data:
    1. merge on code,
    2. compare volume,
    3. return true if 99% identical volume
    """
    check_cols = ["Code", "Volume"]
    df0 = df0[check_cols]  # .copy()
    df1 = df1[check_cols]  # .copy()
    dff = pd.merge(df0, df1, left_on="Code", right_on="Code")
    u1 = dff["Volume_x"] == dff["Volume_y"]
    mval = np.mean(u1)
    identical = mval > 0.99
    return identical
