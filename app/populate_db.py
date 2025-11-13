import pandas as pd
import sqlite3

from sqlalchemy import null
import db

def table_show_other(c, original_csv, other_table, column):
  df = pd.DataFrame(columns=["show_id", f"{column}"])
  new_table_rows = []
  for original_row in original_csv.itertuples(index=False):
    show_id = original_row.show_id
    original_row = getattr(original_row, column)
    try:
      for new_row in other_table.itertuples(index=False):
        attr_name = getattr(new_row, "name")
        if attr_name in original_row:
          new_table_rows.append({"show_id": f"{show_id}", f"{column}": attr_name})
    except Exception as e:
      continue # original_row n tem valor, ia dar erro
  df = pd.concat([df, pd.DataFrame(new_table_rows)], ignore_index=True)
  print(df)
  df.to_sql(f"{column}_show_id", c, if_exists="replace", index=False)


def custom_table_create(c, original_csv, column, table_name):
  df = original_csv[[column]].copy()
  df[column] = df[column].str.split(", ")
  df = df.explode(column)
  df = df.dropna(subset=[column])
  df[column] = df[column].str.strip()
  df = df.drop_duplicates(subset=[column], ignore_index=True)
  df = df.rename(columns={column: "name"})
  df['id'] = range(1, len(df) + 1)
  df = df[["id", "name"]]
  df.to_sql(table_name, c, if_exists="replace", index=False)
  table_show_other(c, original_csv, df, column)
  original_csv = original_csv.drop(columns=[column])
  return original_csv

def first_populate(c):
  original_csv = pd.read_csv("4-DisneyPlus/DisneyPlus.csv")
  original_csv["show_id"] = original_csv["show_id"].str[1:].astype(int)
  original_csv = custom_table_create(c, original_csv, "cast", "actors")
  original_csv = custom_table_create(c, original_csv, "listed_in", "genre")
  original_csv.to_sql("Shows", c, if_exists="replace", index=False)
  return c