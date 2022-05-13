import pandas as pd
import gaia_utils as gu

gu.set_token("demo_token")

sql = """
SELECT tvl, day FROM demo_table
"""
table_data = gu.load_dataframe(gu.LoadDataframeType.SQL, sql)




def calc_diff(df, base_column, periods, sort_column):
    df.set_index(sort_column)
    return df[base_column].diff(periods=periods)


def pct_change(df, base_column, date_column, periods=1, freq=None):
    df[base_column].index = pd.to_datetime(df[date_column])
    return df[base_column].pct_change(periods=periods, freq=freq).reset_index()[base_column]





df = table_data.copy(deep=True)

df['tvl_diff'] = calc_diff(df, "tvl" , 7,  "day" )
df['tvl_change'] = pct_change(df, "tvl",  "day" , 1, "D")

df.to_json(orient="split", date_format="iso", date_unit="s", double_precision=4)

gu.submit(
    project_id="demo_project_id",
    dataset_id="demo_dataset_id",
    table_name="demo_table_name",
    pd_data=df,
    reset_force=True
)
