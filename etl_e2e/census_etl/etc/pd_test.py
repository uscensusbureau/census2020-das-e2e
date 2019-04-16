#!/usr/bin/env python
import sys
import pandas as pd

def top_value_count(x, n=5):
    return pd.value_counts().head(n)

def analyze_df(df):
    print(dir(df))
    print("Shape: {}".format(df.shape))
    print("Columns: {}".format(df.columns))
    print("Description:\n{}".format(df.describe(include='all')))
    print("\n")
    for col in df.columns:
        print("Column: {}  Top 5 values:".format(col,""))
        print(df[col].value_counts(sort=True,ascending=False).head(5))
        print("\n")

if __name__=="__main__":
    pd.set_option('display.float_format', lambda x: '%.1f' % x)
    df = pd.read_csv(sys.argv[1])
    analyze_df(df)
