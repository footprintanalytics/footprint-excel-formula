import pandas as pd
import gaia_utils as gu

gu.set_token("demo_token")

sql = """
SELECT tvl, day FROM demo_table
"""
table_data = gu.load_dataframe(gu.LoadDataframeType.SQL, sql)


def calc_diff(df, base_column, periods, sort_column):
    df.set_index(sort_column)
    res = base_column.diff(periods=periods)
    return res


def pct_change(df, base_column, date_column, periods=1, freq=None):
    date_column = pd.to_datetime(date_column)
    base_column.index = date_column
    res = base_column.pct_change(periods=periods, freq=freq)
    res.index = df.index
    return res


df = table_data.copy(deep=True)

df['tvl_diff'] = calc_diff(df, df["tvl"], 7, df["day"])
df['tvl_change'] = pct_change(df, df["tvl"], df["day"], 1, "D")

df.to_json(orient="split", date_format="iso", date_unit="s", double_precision=4)

gu.submit(
    project_id="demo_project_id",
    dataset_id="demo_dataset_id",
    table_name="demo_table_name",
    pd_data=df,
    reset_force=True
)
