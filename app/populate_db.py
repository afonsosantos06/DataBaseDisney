import pandas as pd
import sqlite3
import db

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
  original_csv = original_csv.drop(columns=[column])
  return original_csv

def first_populate(c):
  original_csv = pd.read_csv("4-DisneyPlus/DisneyPlus.csv")
  original_csv["show_id"] = original_csv["show_id"].str[1:].astype(int)
  original_csv = custom_table_create(c, original_csv, "cast", "actors")
  original_csv = custom_table_create(c, original_csv, "listed_in", "genre")
  original_csv.to_sql("Shows", c, if_exists="replace", index=False)
  return c