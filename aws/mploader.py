from io import StringIO

import numpy as np
import pandas as pd
from dateutil.parser import parse

from aws.s3 import create_s3_session
from aws.yaml_loader import load_yaml

s3 = create_s3_session()


def load_from_s3(date_str):
    """Load raw object from s3, return raw df

    Args:
        date_str ([type]): [description]
    Returns:
        [df]: raw df
    """
    # date_str = "2020-01-05"
    try:
        date_dt = parse(date_str)
        date_param = date_dt.strftime("%Y/%m/%d")
        response = s3.get_object(
            Bucket="malaysia-stock-eod-data", Key=f"mplus/{date_param}/data.csv"
        )
        csv_txt = response["Body"].read().decode("cp1256")
        df = pd.read_csv(StringIO(csv_txt), encoding="cp1256")
        return df
    except Exception as err:
        print("Error @load_from_s3:", err)


"""
LEGACY CLEANING CODE: ADAPTED TO LOAD FROM S3
"""


def remove_pct_plus(x):
    return str(x).replace("%", "").replace("+", "")


def remove_plus(x):
    return str(x).replace("+", "")


class LoadMP:
    SELECTED = load_yaml("./aws/columns.yaml")

    def __init__(self, date_str, input_df=None):
        self.date_dt = parse(date_str)
        self.date_str = self.date_dt.strftime("%Y%m%d")

        df = self.load(date_str, input_df)

        self.df = df
        df = self.subset(df, self.SELECTED)
        df = self.clean(df)
        self.fdf = df

    def load(self, date_str, input_df):
        # df = pd.read_csv(file, encoding="cp1256")
        if input_df is None:
            df = load_from_s3(date_str)
        else:
            df = input_df
        df["Date"] = self.date_dt
        return df

    def subset(self, df, selected):
        temp = {}
        for key, val in selected.items():
            try:
                temp[key] = df[val]
            except:
                try:
                    val2 = val.replace("*", ".")
                    val2 = val2.replace(" ", ".")
                    val2 = val2.replace("-", ".")
                    val2 = val2.replace("%", ".")
                    temp[key] = df[val2]
                except:
                    if not val == "RSS Vol":
                        print("No", val, "or", val2, "in df!")
        sub_df = pd.DataFrame(temp)
        return sub_df

    def clean_price(self, fdf, col):
        fdf[col] = (
            pd.to_numeric(fdf[col].fillna(fdf["RefPrice"]))
            if (fdf.columns == col).any()
            else np.nan
        )

    def clean_to_numeric(self, fdf, col, multiply=1):
        fdf[col] = (
            pd.to_numeric(fdf[col]) * multiply if (fdf.columns == col).any() else np.nan
        )

    def clean_to_numeric_fillna(self, fdf, col, multiply=1):
        fdf[col] = (
            pd.to_numeric(fdf[col], errors="coerce").fillna(0) * multiply
            if (fdf.columns == col).any()
            else np.nan
        )

    def clean_remove_pct_plus(self, fdf, col, multiply=1, fillna=True):
        if fillna:
            fdf[col] = (
                pd.to_numeric(fdf[col].apply(remove_pct_plus), errors="coerce").fillna(
                    0
                )
                * multiply
                if (fdf.columns == col).any()
                else np.nan
            )
        else:
            fdf[col] = (
                pd.to_numeric(fdf[col].apply(remove_pct_plus), errors="coerce")
                * multiply
                if (fdf.columns == col).any()
                else np.nan
            )

    def clean_to_datetime(self, fdf, col):
        fdf[col] = (
            pd.to_datetime(fdf[col], dayfirst=True, errors="coerce")
            if (fdf.columns == col).any()
            else np.nan
        )
        fdf[col] = (
            fdf[col].fillna(value=np.nan) if (fdf.columns == col).any() else np.nan
        )

    def clean(self, fdf):
        fdf = fdf.replace("-", np.nan)

        self.clean_price(fdf, "Open")
        self.clean_price(fdf, "High")
        self.clean_price(fdf, "Low")
        self.clean_price(fdf, "Close")
        self.clean_to_numeric(fdf, "Volume", 100)

        self.clean_remove_pct_plus(fdf, "PctChange", 0.01)
        self.clean_remove_pct_plus(fdf, "PriceChange", 1)
        self.clean_remove_pct_plus(fdf, "BidChange", 1)
        self.clean_remove_pct_plus(fdf, "PriceSwg", 1)
        self.clean_remove_pct_plus(fdf, "PctSwg", 0.01)

        self.clean_price(fdf, "PrvClose")
        self.clean_price(fdf, "VWAP")

        self.clean_to_datetime(fdf, "FYEDate")
        self.clean_to_numeric_fillna(fdf, "Div", 0.01)
        self.clean_remove_pct_plus(fdf, "DY", 0.01)
        self.clean_to_numeric(fdf, "EPS", 0.01)
        self.clean_to_numeric(fdf, "NAB", 1)
        self.clean_to_numeric(fdf, "NetProfit", 1)
        self.clean_to_numeric(fdf, "PE", 1)
        self.clean_to_numeric(fdf, "Revenue", 1)

        self.clean_to_datetime(fdf, "L4QDate")
        self.clean_to_numeric_fillna(fdf, "L4QDiv", 0.01)
        self.clean_remove_pct_plus(fdf, "L4QDY", 0.01)
        self.clean_to_numeric(fdf, "L4QEPS", 0.01)
        self.clean_to_numeric(fdf, "L4QNAB", 1)
        self.clean_to_numeric(fdf, "L4QNetProfit", 1)
        self.clean_to_numeric(fdf, "L4QPE", 1)
        self.clean_to_numeric(fdf, "L4QRevenue", 1)

        self.clean_remove_pct_plus(fdf, "CPpct", 0.01, False)
        self.clean_to_numeric(fdf, "ExercisePrice", 1)
        self.clean_to_datetime(fdf, "MaturityDate")
        self.clean_to_numeric(fdf, "Gearing", 1)
        self.clean_remove_pct_plus(fdf, "Premium", 0.01, False)
        self.clean_to_numeric(fdf, "UnderlyingClose", 1)

        # 2020/11/26 addition
        self.clean_to_numeric(fdf, "Par", 1)
        self.clean_remove_pct_plus(fdf, "Bpct", 1, False)
        self.clean_remove_pct_plus(fdf, "MBpct", 1, False)
        self.clean_remove_pct_plus(fdf, "ABpct", 1, False)
        self.clean_remove_pct_plus(fdf, "Btranpct", 1, False)
        self.clean_remove_pct_plus(fdf, "MBtranpct", 1, False)
        self.clean_remove_pct_plus(fdf, "ABtranpct", 1, False)
        self.clean_remove_pct_plus(fdf, "Bvalpct", 1, False)
        self.clean_remove_pct_plus(fdf, "MBvalpct", 1, False)
        self.clean_remove_pct_plus(fdf, "ABvalpct", 1, False)

        # Additions
        fdf["PctChange1"] = fdf["Close"] / fdf["RefPrice"] - 1
        fdf["PriceChange1"] = fdf["Close"] - fdf["RefPrice"]

        # fdf.set_index("Date", inplace = True)
        return fdf
